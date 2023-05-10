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
	"strconv"
	"strings"
	"sync"
	time "time"

	amqp "github.com/Azure/go-amqp"

	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/kit/logger"
)

const (
	publishRetryWaitSeconds = 2
	publishMaxRetries       = 3
)

// amqpPubSub type allows sending and receiving data to/from an AMQP 1.0 broker
type amqpPubSub struct {
	session           *amqp.Session
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

	a.session = s

	return err
}

func AddPrefixToAddress(t string) string {
	dest := t

	// Unless the request comes in to publish on a queue, publish directly on a topic
	if !strings.HasPrefix(dest, "queue:") && !strings.HasPrefix(dest, "topic:") {
		dest = "topic://" + dest
	} else if strings.HasPrefix(dest, "queue:") {
		dest = strings.Replace(dest, "queue:", "queue://", 1)
	} else if strings.HasPrefix(dest, "topic:") {
		dest = strings.Replace(dest, "topic:", "topic://", 1)
	}

	return dest
}

// Publish the topic to amqp pubsub
func (a *amqpPubSub) Publish(ctx context.Context, req *pubsub.PublishRequest) error {
	a.publishLock.Lock()
	defer a.publishLock.Unlock()

	a.publishRetryCount = 0

	if req.Topic == "" {
		return errors.New("topic name is empty")
	}

	m := amqp.NewMessage(req.Data)

	// If the request has ttl specified, put it on the message header
	ttlProp := req.Metadata["ttlInSeconds"]
	if ttlProp != "" {
		ttlInSeconds, err := strconv.Atoi(ttlProp)
		if err != nil {
			a.logger.Warnf("Invalid ttl received from message %s", ttlInSeconds)
		} else {
			m.Header.TTL = time.Second * time.Duration(ttlInSeconds)
		}
	}

	sender, err := a.session.NewSender(ctx,
		AddPrefixToAddress(req.Topic),
		nil,
	)

	if err != nil {
		a.logger.Errorf("Unable to create link to %s", req.Topic, err)
	} else {
		err = sender.Send(ctx, m, nil)

		// If the publish operation has failed, attempt to republish a maximum number of times
		// before giving up
		if err != nil {
			for a.publishRetryCount <= publishMaxRetries {
				a.publishRetryCount++

				// Send message
				err = sender.Send(ctx, m, nil)

				if err != nil {
					a.logger.Warnf("Failed to publish a message to the broker", err)
				}
				time.Sleep(publishRetryWaitSeconds * time.Second)
			}
		}
	}

	return err
}

func (a *amqpPubSub) Subscribe(ctx context.Context, req pubsub.SubscribeRequest, handler pubsub.Handler) error {
	prefixedTopic := AddPrefixToAddress(req.Topic)

	receiver, err := a.session.NewReceiver(a.ctx,
		prefixedTopic,
		nil,
	)

	if err == nil {
		a.logger.Infof("Attempting to subscribe to %s", prefixedTopic)
		go a.subscribeForever(ctx, receiver, handler, prefixedTopic)
	} else {
		a.logger.Error("Unable to create a receiver:", err)
	}

	return err
}

// function that subscribes to a queue in a tight loop
func (a *amqpPubSub) subscribeForever(ctx context.Context, receiver *amqp.Receiver, handler pubsub.Handler, t string) {
	for {
		// Receive next message
		msg, err := receiver.Receive(ctx, nil)

		if msg != nil {
			data := msg.GetData()

			// if data is empty, then check the value field for data
			if data == nil || len(data) == 0 {
				data = []byte(fmt.Sprint(msg.Value))
			}

			pubsubMsg := &pubsub.NewMessage{
				Data:  data,
				Topic: receiver.LinkName(),
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
				a.logger.Errorf("Error processing message from %s", receiver.LinkName())
				a.logger.Debugf("NAKd a message")
				err := receiver.RejectMessage(ctx, msg, nil)
				if err != nil {
					a.logger.Errorf("failed to NAK a message")
				}
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
	client, err := amqp.Dial(a.ctx, a.metadata.url, &clientOpts)
	if err != nil {
		a.logger.Fatal("Dialing AMQP server:", err)
	}

	// Open a session
	session, err := client.NewSession(a.ctx, nil)
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

func (a *amqpPubSub) createClientOptions(uri *url.URL) amqp.ConnOptions {
	var opts amqp.ConnOptions

	scheme := uri.Scheme

	switch scheme {
	case "amqp":
		if a.metadata.anonymous == true {
			opts.SASLType = amqp.SASLTypeAnonymous()
		} else {
			opts.SASLType = amqp.SASLTypePlain(a.metadata.username, a.metadata.password)
		}
	case "amqps":
		opts.SASLType = amqp.SASLTypePlain(a.metadata.username, a.metadata.password)
		opts.TLSConfig = a.newTLSConfig()
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
