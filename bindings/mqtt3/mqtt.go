/*
Copyright 2023 The Dapr Authors
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

package mqtt

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cenkalti/backoff/v4"
	mqtt "github.com/eclipse/paho.mqtt.golang"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/kit/logger"
	"github.com/dapr/kit/retry"
)

// MQTT allows sending and receiving data to/from an MQTT broker.
type MQTT struct {
	producer     mqtt.Client
	producerLock sync.RWMutex
	metadata     metadata
	logger       logger.Logger
	isSubscribed atomic.Bool
	readHandler  bindings.Handler
	backOff      backoff.BackOff
	closeCh      chan struct{}
}

// NewMQTT returns a new MQTT instance.
func NewMQTT(logger logger.Logger) bindings.InputOutputBinding {
	return &MQTT{
		logger:  logger,
		closeCh: make(chan struct{}),
	}
}

// Init does MQTT connection parsing.
func (m *MQTT) Init(ctx context.Context, metadata bindings.Metadata) (err error) {
	m.metadata, err = parseMQTTMetaData(metadata, m.logger)
	if err != nil {
		return err
	}

	// TODO: Make the backoff configurable for constant or exponential
	m.backOff = backoff.NewConstantBackOff(5 * time.Second)

	return nil
}

func (m *MQTT) Operations() []bindings.OperationKind {
	return []bindings.OperationKind{
		bindings.CreateOperation,
	}
}

func (m *MQTT) getProducer(ctx context.Context) (mqtt.Client, error) {
	// Get the producer from the cache
	m.producerLock.RLock()
	producer := m.producer
	m.producerLock.RUnlock()
	if producer != nil {
		return producer, nil
	}

	// Must create a new producer
	m.producerLock.Lock()
	defer m.producerLock.Unlock()

	// Check again in case another goroutine created it in the meanwhile
	producer = m.producer
	if producer != nil {
		return producer, nil
	}

	// mqtt broker allows only one connection at a given time from a clientID.
	producerClientID := fmt.Sprintf("%s-producer", m.metadata.clientID)
	p, err := m.connect(ctx, producerClientID, false)
	if err != nil {
		return nil, err
	}
	m.producer = p

	return p, nil
}

func (m *MQTT) Invoke(ctx context.Context, req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	producer, err := m.getProducer(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create producer connection: %w", err)
	}

	// MQTT client Publish() has an internal race condition in the default autoreconnect config.
	// To mitigate sporadic failures on the Dapr side, this implementation retries 3 times at
	// a fixed 200ms interval. This is not configurable to keep this as an implementation detail
	// for this component, as the additional public config metadata required could be replaced
	// by the more general Dapr APIs for resiliency moving forwards.
	bo := backoff.WithMaxRetries(
		backoff.NewConstantBackOff(200*time.Millisecond), 3,
	)
	bo = backoff.WithContext(bo, ctx)

	topic, ok := req.Metadata[mqttTopic]
	if !ok || topic == "" {
		// If user does not specify a topic, publish via the component's default topic.
		topic = m.metadata.topic
	}
	return nil, retry.NotifyRecover(func() (err error) {
		token := producer.Publish(topic, m.metadata.qos, m.metadata.retain, req.Data)
		select {
		case <-token.Done():
			err = token.Error()
		case <-m.closeCh:
			err = errors.New("mqtt client closed")
		case <-time.After(defaultWait):
			err = errors.New("mqtt client timeout")
		case <-ctx.Done():
			// Context canceled
			err = ctx.Err()
		}
		if err != nil {
			return fmt.Errorf("failed to publish: %w", err)
		}
		return nil
	}, bo, func(err error, _ time.Duration) {
		m.logger.Debugf("Could not publish MQTT message. Retrying...: %v", err)
	}, func() {
		m.logger.Debug("Successfully published MQTT message after it previously failed")
	})
}

func (m *MQTT) Read(ctx context.Context, handler bindings.Handler) error {
	// If the subscription is already active, wait 2s before retrying (in case we're still disconnecting), otherwise return an error
	if !m.isSubscribed.CompareAndSwap(false, true) {
		m.logger.Debug("Subscription is already active; waiting 2s before retrying…")
		time.Sleep(2 * time.Second)
		if !m.isSubscribed.CompareAndSwap(false, true) {
			return errors.New("the subscription is already active")
		}
	}

	m.logger.Infof("Subscribing to topic %s (qos: %d)", m.metadata.topic, m.metadata.qos)

	// Store the handler in the object
	m.readHandler = handler

	// mqtt broker allows only one connection at a given time from a clientID
	consumerClientID := fmt.Sprintf("%s-consumer", m.metadata.clientID)

	// Establish the connection
	// This will also create the subscription in the OnConnect handler
	consumer, err := m.connect(ctx, consumerClientID, true)
	if err != nil {
		return err
	}

	// In background, watch for contexts cancelation and stop the connection
	// However, do not call "unsubscribe" which would cause the broker to stop tracking the last message received by this consumer group
	go func() {
		select {
		case <-ctx.Done():
			// nop
		case <-m.closeCh:
			// nop
		}

		m.logger.Infof("Disconnecting and stopping subscription to topic %s", m.metadata.topic)

		// Disconnect and then release the "lock"
		consumer.Disconnect(200)
		m.isSubscribed.Store(false)
	}()

	return nil
}

func (m *MQTT) connect(ctx context.Context, clientID string, isSubscriber bool) (mqtt.Client, error) {
	uri, err := url.Parse(m.metadata.url)
	if err != nil {
		return nil, err
	}
	var opts *mqtt.ClientOptions
	if isSubscriber {
		opts = m.createSubscriberClientOptions(ctx, uri, clientID)
	} else {
		opts = m.createClientOptions(uri, clientID)
	}
	client := mqtt.NewClient(opts)

	ctx, cancel := context.WithTimeout(ctx, defaultWait)
	defer cancel()
	token := client.Connect()
	select {
	case <-token.Done():
		err = token.Error()
	case <-ctx.Done():
		err = ctx.Err()
	}
	if err != nil {
		return nil, fmt.Errorf("failed to connect: %w", err)
	}

	return client, nil
}

func (m *MQTT) newTLSConfig() *tls.Config {
	tlsConfig := new(tls.Config)

	if m.metadata.clientCert != "" && m.metadata.clientKey != "" {
		cert, err := tls.X509KeyPair([]byte(m.metadata.clientCert), []byte(m.metadata.clientKey))
		if err != nil {
			m.logger.Warnf("Unable to load client certificate and key pair. Err: %v", err)
			return tlsConfig
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}

	if m.metadata.caCert != "" {
		tlsConfig.RootCAs = x509.NewCertPool()
		if ok := tlsConfig.RootCAs.AppendCertsFromPEM([]byte(m.metadata.caCert)); !ok {
			m.logger.Warnf("Unable to load CA certificate.")
		}
	}

	return tlsConfig
}

// Returns options for clients for both publisher and subscriber
func (m *MQTT) createClientOptions(uri *url.URL, clientID string) *mqtt.ClientOptions {
	opts := mqtt.NewClientOptions().
		SetClientID(clientID).
		SetCleanSession(m.metadata.cleanSession).
		// If OrderMatters is true (default), handlers must not block, which is not an option for us
		SetOrderMatters(false).
		// Disable automatic ACKs as we need to do it manually
		SetAutoAckDisabled(true).
		// Configure reconnections
		SetResumeSubs(true).
		SetAutoReconnect(true).
		SetConnectRetry(true).
		SetConnectRetryInterval(20 * time.Second)

	opts.OnConnectionLost = func(c mqtt.Client, err error) {
		m.logger.Errorf("Connection with broker with client ID '%s' lost; error: %v", clientID, err)
	}

	opts.OnReconnecting = func(c mqtt.Client, co *mqtt.ClientOptions) {
		m.logger.Infof("Attempting to reconnect to broker with client ID '%s'…", clientID)
	}

	// URL scheme backwards-compatibility
	scheme := uri.Scheme
	switch scheme {
	case "mqtt":
		scheme = "tcp"
	case "mqtts", "tcps", "tls":
		scheme = "ssl"
	}
	opts.AddBroker(scheme + "://" + uri.Host)
	opts.SetUsername(uri.User.Username())
	password, _ := uri.User.Password()
	if password != "" {
		opts.SetPassword(password)
	}

	// TLS
	opts.SetTLSConfig(m.newTLSConfig())

	return opts
}

func (m *MQTT) handleMessage(ctx context.Context) func(client mqtt.Client, mqttMsg mqtt.Message) {
	return func(client mqtt.Client, mqttMsg mqtt.Message) {
		var bo backoff.BackOff = backoff.WithContext(m.backOff, ctx)
		if m.metadata.backOffMaxRetries >= 0 {
			bo = backoff.WithMaxRetries(bo, uint64(m.metadata.backOffMaxRetries))
		}

		err := retry.NotifyRecover(
			func() error {
				m.logger.Debugf("Processing MQTT message %s/%d", mqttMsg.Topic(), mqttMsg.MessageID())
				_, err := m.readHandler(ctx, &bindings.ReadResponse{
					Data: mqttMsg.Payload(),
					Metadata: map[string]string{
						mqttTopic: mqttMsg.Topic(),
					},
				})
				if err != nil {
					return err
				}

				// Ack the message on success
				mqttMsg.Ack()
				return nil
			},
			bo,
			func(err error, d time.Duration) {
				m.logger.Errorf("Error processing MQTT message: %s/%d. Retrying…", mqttMsg.Topic(), mqttMsg.MessageID())
			},
			func() {
				m.logger.Infof("Successfully processed MQTT message after it previously failed: %s/%d", mqttMsg.Topic(), mqttMsg.MessageID())
			},
		)
		if err != nil {
			m.logger.Errorf("Failed processing MQTT message: %s/%d: %v", mqttMsg.Topic(), mqttMsg.MessageID(), err)
		}
	}
}

// Extends createClientOptions with options for subscribers only
func (m *MQTT) createSubscriberClientOptions(ctx context.Context, uri *url.URL, clientID string) *mqtt.ClientOptions {
	opts := m.createClientOptions(uri, clientID)

	// On (re-)connection, add the topic subscription
	opts.OnConnect = func(c mqtt.Client) {
		token := c.Subscribe(m.metadata.topic, m.metadata.qos, m.handleMessage(ctx))

		var err error
		select {
		case <-token.Done():
			// Subscription went through (sucecessfully or not)
			err = token.Error()
		case <-time.After(defaultWait):
			err = errors.New("timed out waiting for subscription to complete")
		case <-ctx.Done():
			err = fmt.Errorf("error while waiting for subscription token: %w", ctx.Err())
		}

		// Nothing we can do in case of errors besides logging them
		// If we get here, the connection is almost likely broken anyways, so the client will attempt a reconnection soon if it hasn't already
		if err != nil {
			m.logger.Errorf("Error starting subscriptions in the OnConnect handler: %v", err)
		}
	}

	return opts
}

func (m *MQTT) Close() error {
	m.producerLock.Lock()
	defer m.producerLock.Unlock()

	close(m.closeCh)

	if m.producer != nil {
		m.producer.Disconnect(200)
		m.producer = nil
	}

	return nil
}
