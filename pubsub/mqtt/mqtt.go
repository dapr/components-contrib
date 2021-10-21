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

package mqtt

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"net/url"
	"strconv"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	mqtt "github.com/eclipse/paho.mqtt.golang"

	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/kit/logger"
	"github.com/dapr/kit/retry"
)

const (
	// Keys.
	mqttURL               = "url"
	mqttQOS               = "qos"
	mqttRetain            = "retain"
	mqttClientID          = "consumerID"
	mqttCleanSession      = "cleanSession"
	mqttCACert            = "caCert"
	mqttClientCert        = "clientCert"
	mqttClientKey         = "clientKey"
	mqttBackOffMaxRetries = "backOffMaxRetries"

	// errors.
	errorMsgPrefix = "mqtt pub sub error:"

	// Defaults.
	defaultQOS          = 0
	defaultRetain       = false
	defaultWait         = 30 * time.Second
	defaultCleanSession = true
)

// mqttPubSub type allows sending and receiving data to/from MQTT broker.
type mqttPubSub struct {
	producer        mqtt.Client
	consumer        mqtt.Client
	metadata        *metadata
	logger          logger.Logger
	topics          map[string]pubsub.Handler
	subscribingLock sync.Mutex
	ctx             context.Context
	cancel          context.CancelFunc
}

// NewMQTTPubSub returns a new mqttPubSub instance.
func NewMQTTPubSub(logger logger.Logger) pubsub.PubSub {
	return &mqttPubSub{
		logger:          logger,
		subscribingLock: sync.Mutex{},
	}
}

// isValidPEM validates the provided input has PEM formatted block.
func isValidPEM(val string) bool {
	block, _ := pem.Decode([]byte(val))

	return block != nil
}

func parseMQTTMetaData(md pubsub.Metadata) (*metadata, error) {
	m := metadata{}

	// required configuration settings
	if val, ok := md.Properties[mqttURL]; ok && val != "" {
		m.url = val
	} else {
		return &m, fmt.Errorf("%s missing url", errorMsgPrefix)
	}

	// optional configuration settings
	m.qos = defaultQOS
	if val, ok := md.Properties[mqttQOS]; ok && val != "" {
		qosInt, err := strconv.Atoi(val)
		if err != nil {
			return &m, fmt.Errorf("%s invalid qos %s, %s", errorMsgPrefix, val, err)
		}
		m.qos = byte(qosInt)
	}

	m.retain = defaultRetain
	if val, ok := md.Properties[mqttRetain]; ok && val != "" {
		var err error
		m.retain, err = strconv.ParseBool(val)
		if err != nil {
			return &m, fmt.Errorf("%s invalid retain %s, %s", errorMsgPrefix, val, err)
		}
	}

	if val, ok := md.Properties[mqttClientID]; ok && val != "" {
		m.clientID = val
	} else {
		return &m, fmt.Errorf("%s missing consumerID", errorMsgPrefix)
	}

	m.cleanSession = defaultCleanSession
	if val, ok := md.Properties[mqttCleanSession]; ok && val != "" {
		var err error
		m.cleanSession, err = strconv.ParseBool(val)
		if err != nil {
			return &m, fmt.Errorf("%s invalid clean session %s, %s", errorMsgPrefix, val, err)
		}
	}

	if val, ok := md.Properties[mqttCACert]; ok && val != "" {
		if !isValidPEM(val) {
			return &m, fmt.Errorf("%s invalid ca certificate", errorMsgPrefix)
		}
		m.tlsCfg.caCert = val
	}
	if val, ok := md.Properties[mqttClientCert]; ok && val != "" {
		if !isValidPEM(val) {
			return &m, fmt.Errorf("%s invalid client certificate", errorMsgPrefix)
		}
		m.tlsCfg.clientCert = val
	}
	if val, ok := md.Properties[mqttClientKey]; ok && val != "" {
		if !isValidPEM(val) {
			return &m, fmt.Errorf("%s invalid client certificate key", errorMsgPrefix)
		}
		m.tlsCfg.clientKey = val
	}

	if val, ok := md.Properties[mqttBackOffMaxRetries]; ok && val != "" {
		backOffMaxRetriesInt, err := strconv.Atoi(val)
		if err != nil {
			return &m, fmt.Errorf("%s invalid backOffMaxRetries %s, %s", errorMsgPrefix, val, err)
		}
		m.backOffMaxRetries = backOffMaxRetriesInt
	}

	return &m, nil
}

// Init parses metadata and creates a new Pub Sub client.
func (m *mqttPubSub) Init(metadata pubsub.Metadata) error {
	mqttMeta, err := parseMQTTMetaData(metadata)
	if err != nil {
		return err
	}
	m.metadata = mqttMeta

	m.ctx, m.cancel = context.WithCancel(context.Background())

	// mqtt broker allows only one connection at a given time from a clientID.
	producerClientID := fmt.Sprintf("%s-producer", m.metadata.clientID)
	connCtx, connCancel := context.WithTimeout(m.ctx, defaultWait)
	p, err := m.connect(connCtx, producerClientID)
	connCancel()
	if err != nil {
		return err
	}

	m.producer = p
	m.topics = make(map[string]pubsub.Handler)

	m.logger.Debug("mqtt message bus initialization complete")

	return nil
}

// Publish the topic to mqtt pub sub.
func (m *mqttPubSub) Publish(req *pubsub.PublishRequest) error {
	// Note this can contain PII
	// m.logger.Debugf("mqtt publishing topic %s with data: %v", req.Topic, req.Data)
	m.logger.Debugf("mqtt publishing topic %s", req.Topic)

	token := m.producer.Publish(req.Topic, m.metadata.qos, m.metadata.retain, req.Data)
	t := time.NewTimer(defaultWait)
	defer func() {
		if !t.Stop() {
			<-t.C
		}
	}()
	select {
	case <-token.Done():
		// Operation completed
	case <-m.ctx.Done():
		// Context canceled
		return m.ctx.Err()
	case <-t.C:
		return fmt.Errorf("mqtt timeout while publishing")
	}
	if !token.WaitTimeout(defaultWait) || token.Error() != nil {
		return fmt.Errorf("mqtt error from publish: %v", token.Error())
	}

	return nil
}

// Subscribe to the mqtt pub sub topic.
func (m *mqttPubSub) Subscribe(ctx context.Context, req pubsub.SubscribeRequest, handler pubsub.Handler) error {
	if ctxErr := m.ctx.Err(); ctxErr != nil {
		// If the global context has been canceled, we do not allow more subscriptions
		return ctxErr
	}

	m.subscribingLock.Lock()
	defer m.subscribingLock.Unlock()

	// Start the subscription
	// When the connection is ready, add the topic
	// Use the global context here to maintain the connection
	m.startSubscription(m.ctx, func() {
		m.topics[req.Topic] = handler
	})

	// Listen for context cancelation to remove the subscription
	go func() {
		select {
		case <-ctx.Done():
		case <-m.ctx.Done():
		}
		m.subscribingLock.Lock()
		defer m.subscribingLock.Unlock()

		// If this is the last subscription or if the global context is done, close the connection entirely
		if len(m.topics) <= 1 || m.ctx.Err() != nil {
			m.consumer.Disconnect(5)
			m.consumer = nil
			delete(m.topics, req.Topic)
			return
		}

		// Reconnect with one less topic
		m.startSubscription(m.ctx, func() {
			delete(m.topics, req.Topic)
		})
	}()

	return nil
}

func (m *mqttPubSub) startSubscription(ctx context.Context, onConnRready func()) error {
	// reset synchronization
	if m.consumer != nil && m.consumer.IsConnectionOpen() {
		m.logger.Infof("re-initializing the subscriber")
		m.consumer.Disconnect(5)
		m.consumer = nil
	} else {
		m.logger.Infof("initializing the subscriber")
	}

	// mqtt broker allows only one connection at a given time from a clientID.
	consumerClientID := fmt.Sprintf("%s-consumer", m.metadata.clientID)
	connCtx, connCancel := context.WithTimeout(ctx, defaultWait)
	c, err := m.connect(connCtx, consumerClientID)
	connCancel()
	if err != nil {
		return err
	}
	m.consumer = c

	// Invoke onConnReady so changes to the topics can be made safely
	onConnRready()

	subscribeTopics := make(map[string]byte, len(m.topics))
	for k := range m.topics {
		subscribeTopics[k] = m.metadata.qos
	}

	token := m.consumer.SubscribeMultiple(
		subscribeTopics,
		m.onMessage(ctx),
	)
	subscribeCtx, subscribeCancel := context.WithTimeout(m.ctx, defaultWait)
	defer subscribeCancel()
	select {
	case <-token.Done():
		// Subscription went through
	case <-subscribeCtx.Done():
		return subscribeCtx.Err()
	}
	if err := token.Error(); err != nil {
		return fmt.Errorf("mqtt error from subscribe: %v", err)
	}

	return nil
}

// onMessage returns the callback to be invoked when there's a new message from a topic
func (m *mqttPubSub) onMessage(ctx context.Context) func(client mqtt.Client, mqttMsg mqtt.Message) {
	return func(client mqtt.Client, mqttMsg mqtt.Message) {
		mqttMsg.AutoAckOff()
		msg := pubsub.NewMessage{
			Topic: mqttMsg.Topic(),
			Data:  mqttMsg.Payload(),
		}

		topicHandler, ok := m.topics[msg.Topic]
		if !ok || topicHandler == nil {
			m.logger.Errorf("no handler defined for topic %s", msg.Topic)
			return
		}

		// TODO: Make the backoff configurable for constant or exponential
		var b backoff.BackOff = backoff.NewConstantBackOff(5 * time.Second)
		b = backoff.WithContext(b, ctx)
		if m.metadata.backOffMaxRetries >= 0 {
			b = backoff.WithMaxRetries(b, uint64(m.metadata.backOffMaxRetries))
		}
		if err := retry.NotifyRecover(func() error {
			m.logger.Debugf("Processing MQTT message %s/%d", mqttMsg.Topic(), mqttMsg.MessageID())

			if err := topicHandler(ctx, &msg); err != nil {
				return err
			}

			mqttMsg.Ack()

			return nil
		}, b, func(err error, d time.Duration) {
			m.logger.Errorf("Error processing MQTT message: %s/%d. Retrying...", mqttMsg.Topic(), mqttMsg.MessageID())
		}, func() {
			m.logger.Infof("Successfully processed MQTT message after it previously failed: %s/%d", mqttMsg.Topic(), mqttMsg.MessageID())
		}); err != nil {
			m.logger.Errorf("Failed processing MQTT message: %s/%d: %v", mqttMsg.Topic(), mqttMsg.MessageID(), err)
		}
	}
}

func (m *mqttPubSub) connect(ctx context.Context, clientID string) (mqtt.Client, error) {
	uri, err := url.Parse(m.metadata.url)
	if err != nil {
		return nil, err
	}
	opts := m.createClientOptions(uri, clientID)
	client := mqtt.NewClient(opts)
	token := client.Connect()
	select {
	case <-token.Done():
		// Connection went through
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	if err := token.Error(); err != nil {
		return nil, err
	}

	return client, nil
}

func (m *mqttPubSub) newTLSConfig() *tls.Config {
	tlsConfig := new(tls.Config)

	if m.metadata.clientCert != "" && m.metadata.clientKey != "" {
		cert, err := tls.X509KeyPair([]byte(m.metadata.clientCert), []byte(m.metadata.clientKey))
		if err != nil {
			m.logger.Warnf("unable to load client certificate and key pair. Err: %v", err)

			return tlsConfig
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}

	if m.metadata.caCert != "" {
		tlsConfig.RootCAs = x509.NewCertPool()
		if ok := tlsConfig.RootCAs.AppendCertsFromPEM([]byte(m.metadata.caCert)); !ok {
			m.logger.Warnf("unable to load ca certificate.")
		}
	}

	return tlsConfig
}

func (m *mqttPubSub) createClientOptions(uri *url.URL, clientID string) *mqtt.ClientOptions {
	opts := mqtt.NewClientOptions()
	opts.SetClientID(clientID)
	opts.SetCleanSession(m.metadata.cleanSession)
	// URL scheme backward compatibility
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
	opts.SetPassword(password)
	// tls config
	opts.SetTLSConfig(m.newTLSConfig())

	return opts
}

func (m *mqttPubSub) Close() error {
	m.subscribingLock.Lock()
	defer m.subscribingLock.Unlock()

	m.cancel()

	if m.consumer != nil {
		m.consumer.Disconnect(5)
	}
	m.producer.Disconnect(5)

	return nil
}

func (m *mqttPubSub) Features() []pubsub.Feature {
	return nil
}

func (m *mqttPubSub) Ping() error {
	return nil
}
