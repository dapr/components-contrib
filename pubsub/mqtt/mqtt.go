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
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"strconv"
	"strings"
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
	defaultQOS          = 1
	defaultRetain       = false
	defaultWait         = 30 * time.Second
	defaultCleanSession = false
)

// mqttPubSub type allows sending and receiving data to/from MQTT broker.
type mqttPubSub struct {
	producer        mqtt.Client
	consumer        mqtt.Client
	metadata        *metadata
	logger          logger.Logger
	topics          map[string]mqttPubSubSubscription
	subscribingLock sync.RWMutex
	ctx             context.Context
	cancel          context.CancelFunc
}

type mqttPubSubSubscription struct {
	handler pubsub.Handler
	alias   string
	matcher func(topic string) bool
}

// NewMQTTPubSub returns a new mqttPubSub instance.
func NewMQTTPubSub(logger logger.Logger) pubsub.PubSub {
	return &mqttPubSub{
		logger:          logger,
		subscribingLock: sync.RWMutex{},
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
	m.topics = make(map[string]mqttPubSubSubscription)

	m.logger.Debug("mqtt message bus initialization complete")

	return nil
}

// Publish the topic to mqtt pub sub.
func (m *mqttPubSub) Publish(req *pubsub.PublishRequest) error {
	if req.Topic == "" {
		return errors.New("topic name is empty")
	}

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
	if req.Topic == "" {
		return errors.New("topic name is empty")
	}

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
		m.addTopic(req.Topic, handler)
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

		topicHandler := m.handlerForTopic(msg.Topic)
		if topicHandler == nil {
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
			m.logger.Debugf("Processing MQTT message %s#%d (retained=%v)", mqttMsg.Topic(), mqttMsg.MessageID(), mqttMsg.Retained())

			if err := topicHandler(ctx, &msg); err != nil {
				return err
			}

			mqttMsg.Ack()

			return nil
		}, b, func(err error, d time.Duration) {
			m.logger.Errorf("Error processing MQTT message: %s#%d. Retrying...", mqttMsg.Topic(), mqttMsg.MessageID())
		}, func() {
			m.logger.Infof("Successfully processed MQTT message after it previously failed: %s#%d", mqttMsg.Topic(), mqttMsg.MessageID())
		}); err != nil {
			m.logger.Errorf("Failed processing MQTT message: %s#%d: %v", mqttMsg.Topic(), mqttMsg.MessageID(), err)
		}
	}
}

// Returns the handler for a message sent to a given topic, supporting wildcards and other special syntaxes.
func (m *mqttPubSub) handlerForTopic(topic string) pubsub.Handler {
	// First, try to see if we have a handler for the exact topic (no wildcards etc)
	topicHandler, ok := m.topics[topic]
	if ok && topicHandler.handler != nil {
		return topicHandler.handler
	}

	// Iterate through the topics and run the matchers
	for _, obj := range m.topics {
		if obj.alias == topic {
			return obj.handler
		}
		if obj.matcher != nil && obj.matcher(topic) {
			return obj.handler
		}
	}

	return nil
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

var sharedSubscriptionMatch = regexp.MustCompile(`^\$share\/(.*?)\/.`)

// Adds a topic to the list of subscriptions.
func (m *mqttPubSub) addTopic(origTopicName string, handler pubsub.Handler) {
	obj := mqttPubSubSubscription{
		handler: handler,
	}

	// Shared subscriptions begin with "$share/GROUPID/" and we can remove that prefix
	topicName := origTopicName
	if found := sharedSubscriptionMatch.FindStringIndex(origTopicName); found != nil && found[0] == 0 {
		topicName = topicName[(found[1] - 1):]
		obj.alias = topicName
	}

	// If the topic name contains a wildcard, we need to add a matcher
	regexStr := buildRegexForTopic(topicName)
	if regexStr != "" {
		// We built our own regex and this should never panic
		match := regexp.MustCompile(regexStr)
		obj.matcher = func(topic string) bool {
			return match.MatchString(topic)
		}
	}

	m.topics[origTopicName] = obj
}

// Returns a regular expression string that matches the topic, with support for wildcards.
func buildRegexForTopic(topicName string) string {
	// This is a bit more lax than the specs, which for example require "#" to be at the end of the string only:
	// in practice, seems that (at least some) brokers are more flexible and allow "#" in the middle of a string too
	var (
		regexStr string
		lastPos  int = -1
		okPos    bool
	)
	if strings.ContainsAny(topicName, "#+") {
		regexStr = "^"
		// It's ok to iterate over bytes here (rather than codepoints) because all characters we're looking for are always single-byte
		for i := 0; i < len(topicName); i++ {
			// Wildcard chars must either be at the beginning of the string or must follow a /
			okPos = (i == 0 || topicName[i-1] == '/')
			if topicName[i] == '#' && okPos {
				lastPos = i
				if i > 0 && i == (len(topicName)-1) {
					// Edge case: we're at the end of the string so we can allow omitting the preceding /
					regexStr += regexp.QuoteMeta(topicName[0:(i-1)]) + "(.*)"
				} else {
					regexStr += regexp.QuoteMeta(topicName[0:i]) + "(.*)"
				}
			} else if topicName[i] == '+' && okPos {
				lastPos = i
				if i > 0 && i == (len(topicName)-1) {
					// Edge case: we're at the end of the string so we can allow omitting the preceding /
					regexStr += regexp.QuoteMeta(topicName[0:(i-1)]) + `((\/|)[^\/]*)`
				} else {
					regexStr += regexp.QuoteMeta(topicName[0:i]) + `([^\/]*)`
				}
			}
		}
		regexStr += regexp.QuoteMeta(topicName[(lastPos+1):]) + "$"
	}

	if lastPos == -1 {
		return ""
	}

	return regexStr
}
