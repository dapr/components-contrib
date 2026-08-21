/*
Copyright 2021 The Dapr Authors
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package amqp

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net/url"
	"reflect"
	"strconv"
	"sync"
	"sync/atomic"
	time "time"

	amqp "github.com/Azure/go-amqp"

	contribMetadata "github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/kit/logger"
)

const (
	publishRetryWaitSeconds = 2
	publishMaxRetries       = 3

	// defaultCloseTimeout bounds how long closing a link or session may block.
	defaultCloseTimeout = 5 * time.Second
)

// amqpPubSub type allows sending and receiving data to/from an AMQP 1.0 broker
type amqpPubSub struct {
	session     *amqp.Session
	metadata    *metadata
	logger      logger.Logger
	publishLock sync.RWMutex
	wg          sync.WaitGroup
	closed      atomic.Bool
	closeCh     chan struct{}
}

// NewAMQPPubsub returns a new AMQPPubSub instance
func NewAMQPPubsub(logger logger.Logger) pubsub.PubSub {
	return &amqpPubSub{
		logger:  logger,
		closeCh: make(chan struct{}),
	}
}

// Init parses the metadata and creates a new Pub Sub Client.
func (a *amqpPubSub) Init(ctx context.Context, metadata pubsub.Metadata) error {
	amqpMeta, err := parseAMQPMetaData(metadata, a.logger)
	if err != nil {
		return err
	}

	a.metadata = amqpMeta

	s, err := a.connect(ctx)
	if err != nil {
		return err
	}

	a.session = s

	return err
}

// Publish the topic to amqp pubsub
func (a *amqpPubSub) Publish(ctx context.Context, req *pubsub.PublishRequest) error {
	a.publishLock.Lock()
	defer a.publishLock.Unlock()

	if a.closed.Load() {
		return pubsub.NewTerminalError(errors.New("component is closed"))
	}

	if req.Topic == "" {
		return pubsub.NewTerminalError(errors.New("topic name is empty"))
	}

	address := a.metadata.addressFor(req.Topic)
	if address == "" {
		return pubsub.NewTerminalError(fmt.Errorf("topic %q maps to an empty AMQP address", req.Topic))
	}

	m := amqp.NewMessage(req.Data)

	// If the request has ttl specified, put it on the message header
	ttlProp := req.Metadata["ttlInSeconds"]
	if ttlProp != "" {
		ttlInSeconds, err := strconv.Atoi(ttlProp)
		if err != nil {
			a.logger.Warnf("Invalid ttl received from message %d", ttlInSeconds)
		} else {
			m.Header.TTL = time.Second * time.Duration(ttlInSeconds)
		}
	}

	sender, err := a.session.NewSender(ctx,
		address,
		nil,
	)
	if err != nil {
		a.logger.Errorf("Unable to create link to %s: %v", address, err)
		return pubsub.NewRetriableError(err)
	}

	// The link is opened per publish, so it has to be closed again here;
	// otherwise every published message leaks a link on the broker.
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), defaultCloseTimeout)
		defer cancel()
		if cerr := sender.Close(closeCtx); cerr != nil {
			a.logger.Warnf("failed to close the sender link for %s: %v", address, cerr)
		}
	}()

	// Publish the message, retrying a bounded number of times before giving up.
	for attempt := 0; ; attempt++ {
		err = sender.Send(ctx, m, nil)
		if err == nil {
			return nil
		}

		if attempt >= publishMaxRetries {
			break
		}

		a.logger.Warnf("Failed to publish a message to %s, retrying: %v", address, err)

		select {
		case <-time.After(publishRetryWaitSeconds * time.Second):
		case <-ctx.Done():
			return pubsub.NewRetriableError(ctx.Err())
		}
	}

	return pubsub.NewRetriableError(err)
}

func (a *amqpPubSub) Subscribe(ctx context.Context, req pubsub.SubscribeRequest, handler pubsub.Handler) error {
	if a.closed.Load() {
		return errors.New("component is closed")
	}

	address := a.metadata.addressFor(req.Topic)
	if address == "" {
		return fmt.Errorf("topic %q maps to an empty AMQP address", req.Topic)
	}

	receiver, err := a.session.NewReceiver(ctx,
		address,
		nil,
	)
	if err != nil {
		a.logger.Errorf("Unable to create a receiver for %s: %v", address, err)
		return err
	}

	a.logger.Infof("Attempting to subscribe to %s", address)
	a.wg.Add(2)
	subCtx, cancel := context.WithCancel(ctx)
	go func() {
		defer a.wg.Done()
		defer cancel()
		select {
		case <-a.closeCh:
		case <-subCtx.Done():
		}
	}()
	go func() {
		defer a.wg.Done()
		a.subscribeForever(subCtx, receiver, handler, req.Topic, address)
	}()

	return nil
}

// subscribeForever delivers messages from the receiver link until the context
// is cancelled or the link fails.
// topic is the Dapr topic name the messages are delivered under, address is the
// AMQP address the receiver link is attached to.
func (a *amqpPubSub) subscribeForever(ctx context.Context, receiver *amqp.Receiver, handler pubsub.Handler, topic string, address string) {
	defer func() {
		a.logger.Infof("closing receiver for %s", address)
		closeCtx, cancel := context.WithTimeout(context.Background(), defaultCloseTimeout)
		defer cancel()
		if err := receiver.Close(closeCtx); err != nil {
			a.logger.Warnf("failed to close the receiver link for %s: %v", address, err)
		}
	}()

	for {
		// Receive next message
		msg, err := receiver.Receive(ctx, nil)
		if err != nil {
			if ctx.Err() != nil {
				// The subscription is being torn down.
				return
			}
			// Receive only fails on a cancelled context or on a link that is
			// done for good, in which case it returns the same error
			// immediately every time. Returning here rather than continuing
			// avoids spinning on it.
			a.logger.Errorf("Ending the subscription to %s, the receiver link failed: %v", address, err)
			return
		}

		if msg == nil {
			continue
		}

		if err = handler(ctx, newPubsubMessage(topic, msg)); err != nil {
			a.logger.Errorf("Error processing message from %s: %v", address, err)
			if err = receiver.RejectMessage(ctx, msg, nil); err != nil {
				a.logger.Errorf("failed to NAK a message from %s: %v", address, err)
			} else {
				a.logger.Debugf("NAKd a message")
			}

			continue
		}

		if err = receiver.AcceptMessage(ctx, msg); err != nil {
			a.logger.Errorf("failed to acknowledge a message from %s: %v", address, err)
		} else {
			a.logger.Debugf("ACKed a message")
		}
	}
}

// newPubsubMessage converts a message received from the broker into a Dapr
// pub/sub message delivered under the topic the subscription was created for.
func newPubsubMessage(topic string, msg *amqp.Message) *pubsub.NewMessage {
	data := msg.GetData()

	// if data is empty, then check the value field for data
	if len(data) == 0 {
		data = []byte(fmt.Sprint(msg.Value))
	}

	return &pubsub.NewMessage{
		Data:  data,
		Topic: topic,
	}
}

// Connect to the AMQP broker
func (a *amqpPubSub) connect(ctx context.Context) (*amqp.Session, error) {
	uri, err := url.Parse(a.metadata.URL)
	if err != nil {
		return nil, err
	}

	clientOpts := a.createClientOptions(uri)

	a.logger.Infof("Attempting to connect to %s", a.metadata.URL)
	client, err := amqp.Dial(ctx, a.metadata.URL, &clientOpts)
	if err != nil {
		return nil, fmt.Errorf("%s dialing AMQP server: %w", errorMsgPrefix, err)
	}

	// Open a session
	session, err := client.NewSession(ctx, nil)
	if err != nil {
		if cerr := client.Close(); cerr != nil {
			a.logger.Warnf("failed to close the connection after a failed session: %v", cerr)
		}
		return nil, fmt.Errorf("%s creating AMQP session: %w", errorMsgPrefix, err)
	}

	return session, nil
}

func (a *amqpPubSub) newTLSConfig() *tls.Config {
	tlsConfig := new(tls.Config)

	if a.metadata.ClientCert != "" && a.metadata.ClientKey != "" {
		cert, err := tls.X509KeyPair([]byte(a.metadata.ClientCert), []byte(a.metadata.ClientKey))
		if err != nil {
			a.logger.Warnf("unable to load client certificate and key pair. Err: %v", err)

			return tlsConfig
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}

	if a.metadata.CaCert != "" {
		tlsConfig.RootCAs = x509.NewCertPool()
		if ok := tlsConfig.RootCAs.AppendCertsFromPEM([]byte(a.metadata.CaCert)); !ok {
			a.logger.Warnf("unable to load ca certificate.")
		}
	}

	return tlsConfig
}

func (a *amqpPubSub) createClientOptions(uri *url.URL) amqp.ConnOptions {
	var opts amqp.ConnOptions

	scheme := uri.Scheme

	switch scheme {
	case "amqp":
		if a.metadata.Anonymous {
			opts.SASLType = amqp.SASLTypeAnonymous()
		} else {
			opts.SASLType = amqp.SASLTypePlain(a.metadata.Username, a.metadata.Password)
		}
	case "amqps":
		opts.SASLType = amqp.SASLTypePlain(a.metadata.Username, a.metadata.Password)
		opts.TLSConfig = a.newTLSConfig()
	}

	return opts
}

// Close the session
func (a *amqpPubSub) Close() error {
	defer a.wg.Wait()
	a.publishLock.Lock()
	defer a.publishLock.Unlock()

	if a.closed.CompareAndSwap(false, true) {
		close(a.closeCh)
	}

	// Init may have failed before a session was established.
	if a.session == nil {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), defaultCloseTimeout)
	defer cancel()
	err := a.session.Close(ctx)
	if err != nil {
		a.logger.Warnf("failed to close the connection: %v", err)
	}
	return err
}

// Feature list for AMQP PubSub
func (a *amqpPubSub) Features() []pubsub.Feature {
	return []pubsub.Feature{pubsub.FeatureSubscribeWildcards, pubsub.FeatureMessageTTL}
}

// GetComponentMetadata returns the metadata of the component.
func (a *amqpPubSub) GetComponentMetadata() (metadataInfo contribMetadata.MetadataMap) {
	metadataStruct := metadata{}
	_ = contribMetadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, contribMetadata.PubSubType)
	return
}
