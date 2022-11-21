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
	amqp "github.com/Azure/go-amqp"
	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/kit/logger"
	"net/url"
	"strconv"
	"strings"
	"sync"
	time "time"
)

const (
	publishRetryWaitSeconds = 2
	publishMaxRetries       = 3
)

// amqpPubSub type allows sending and receiving data to/from an AMQP 1.0 broker
type amqpPubSub struct {
	session           amqp.Session
	metadata          *metadata
	logger            logger.Logger
	publishLock       sync.RWMutex
	publishRetryCount int
	ctx               context.Context
	cancel            context.CancelFunc
}

// NewAMQPPubsub returns a new AMQPPubSub instance
func NewAMQPPubsub(logger logger.Logger) pubsub.PubSub {
	return &amqpPubSub{
		logger:      logger,
		publishLock: sync.RWMutex{},
	}
}

// Init parses the metadata and creates a new Pub Sub Client.
func (a *amqpPubSub) Init(metadata pubsub.Metadata) error {
	amqpMeta, err := parseAMQPMetaData(metadata, a.logger)
	if err != nil {
		return err
	}

	a.metadata = amqpMeta

	a.ctx, a.cancel = context.WithCancel(context.Background())

	s, err := a.connect()

	if err != nil {
		return err
	}

	a.session = *s

	return err
}

// Publish the topic to amqp pubsub
func (a *amqpPubSub) Publish(req *pubsub.PublishRequest) error {

	a.publishLock.Lock()
	defer a.publishLock.Unlock()

	a.publishRetryCount = 0

	if req.Topic == "" {
		return errors.New("topic name is empty")
	}

	m := amqp.NewMessage(req.Data)

	//If the request has ttl specified, put it on the message header
	ttlProp := req.Metadata["ttlInSeconds"]
	if ttlProp != "" {
		ttlInSeconds, err := strconv.Atoi(ttlProp)
		if err != nil {
			a.logger.Warnf("Invalid ttl received from message %s", ttlInSeconds)
		} else {
			m.Header.TTL = time.Second * time.Duration(ttlInSeconds)
		}
	}

	dest := req.Topic

	//Unless the request comes in to publish on a queue, publish directly on a topic
	if !strings.HasPrefix(dest, "queue://") && !strings.HasPrefix(dest, "topic://") {
		dest = "topic://" + dest
	}

	sender, err := a.session.NewSender(
		amqp.LinkTargetAddress(dest),
	)

	if err != nil {
		a.logger.Errorf("Unable to create link to %s", req.Topic, err)
	} else {

		err = sender.Send(a.ctx, m)

		//If the publish operation has failed, attemp to republish a maximum number of times
		// before giving up
		if err != nil {
			for a.publishRetryCount <= publishMaxRetries {
				a.publishRetryCount++

				// Send message
				err = sender.Send(a.ctx, m)

				if err != nil {
					a.logger.Warnf("Failed to publish a message to the broker", err)
				}
				time.Sleep(publishRetryWaitSeconds * time.Second)
			}
		}
	}

	return err

}

// Set up a subscription directly to a queue. Subscriptions to topics are not currently supported
func (a *amqpPubSub) Subscribe(ctx context.Context, req pubsub.SubscribeRequest, handler pubsub.Handler) error {

	receiver, err := a.session.NewReceiver(
		amqp.LinkSourceAddress(req.Topic),
	)

	if err == nil {
		a.logger.Infof("Attempting to subscribe to %s", req.Topic)
		go a.subscribeForever(ctx, receiver, handler)
	} else {
		a.logger.Error("Unable to create a receiver:", err)
	}

	return err

}

// function that subscribes to a queue in a tight loop
func (a *amqpPubSub) subscribeForever(ctx context.Context, receiver *amqp.Receiver, handler pubsub.Handler) {
	for {
		// Receive next message
		msg, err := receiver.Receive(ctx)

		a.logger.Debugf("Received a message %s", msg.GetData())

		pubsubMsg := &pubsub.NewMessage{
			Data:  msg.GetData(),
			Topic: msg.LinkName(),
		}

		if err != nil {
			a.logger.Errorf("failed to establish receiver")
		}

		err = handler(ctx, pubsubMsg)

		if err == nil {
			err := receiver.AcceptMessage(ctx, msg)
			a.logger.Debugf("ACKed a message")
			if err != nil {
				a.logger.Errorf("failed to acknowledge a message")
			}
		} else {
			a.logger.Errorf("Error processing message from %s", msg.LinkName())
			a.logger.Debugf("NAKd a message")
			err := receiver.RejectMessage(ctx, msg, nil)
			if err != nil {
				a.logger.Errorf("failed to NAK a message")

			}
		}

	}
}

// Connect to the AMQP broker
func (a *amqpPubSub) connect() (*amqp.Session, error) {
	uri, err := url.Parse(a.metadata.url)
	if err != nil {
		return nil, err
	}

	clientOpts := a.createClientOptions(uri)

	a.logger.Infof("Attempting to connect to %s", a.metadata.url)
	client, err := amqp.Dial(a.metadata.url, clientOpts...)
	if err != nil {
		a.logger.Fatal("Dialing AMQP server:", err)
	}

	// Open a session
	session, err := client.NewSession()
	if err != nil {
		a.logger.Fatal("Creating AMQP session:", err)
	}

	return session, nil

}

func (a *amqpPubSub) newTLSConfig() *tls.Config {
	tlsConfig := new(tls.Config)

	if a.metadata.clientCert != "" && a.metadata.clientKey != "" {
		cert, err := tls.X509KeyPair([]byte(a.metadata.clientCert), []byte(a.metadata.clientKey))
		if err != nil {
			a.logger.Warnf("unable to load client certificate and key pair. Err: %v", err)

			return tlsConfig
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}

	if a.metadata.caCert != "" {
		tlsConfig.RootCAs = x509.NewCertPool()
		if ok := tlsConfig.RootCAs.AppendCertsFromPEM([]byte(a.metadata.caCert)); !ok {
			a.logger.Warnf("unable to load ca certificate.")
		}
	}

	return tlsConfig
}

func (a *amqpPubSub) createClientOptions(uri *url.URL) []amqp.ConnOption {

	var opts []amqp.ConnOption

	scheme := uri.Scheme

	switch scheme {
	case "amqp":
		if a.metadata.anonymous == true {
			opts = append(opts, amqp.ConnSASLAnonymous())

		} else {
			opts = append(opts, amqp.ConnSASLPlain(a.metadata.username, a.metadata.password))
		}
	case "amqps":
		opts = append(opts, amqp.ConnSASLPlain(a.metadata.username, a.metadata.password))
		opts = append(opts, amqp.ConnTLS(true))
		opts = append(opts, amqp.ConnTLSConfig(a.newTLSConfig()))
	}

	return opts
}

// Close the session
func (a *amqpPubSub) Close() error {
	a.publishLock.Lock()

	defer a.publishLock.Unlock()

	err := a.session.Close(a.ctx)

	if err != nil {
		a.logger.Warnf("failed to close the connection.", err)
	}
	return err

}

// Feature list for AMQP PubSub
func (a *amqpPubSub) Features() []pubsub.Feature {
	return []pubsub.Feature{pubsub.FeatureSubscribeWildcards, pubsub.FeatureMessageTTL}
}
