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
	"errors"
	"fmt"
	"net/url"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"golang.org/x/exp/maps"

	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/kit/logger"
	"github.com/dapr/kit/utils"
)

const (
	// Keys for request metadata
	unsubscribeOnCloseKey = "unsubscribeOnClose"
)

// mqttPubSub type allows sending and receiving data to/from MQTT broker.
type mqttPubSub struct {
	conn            mqtt.Client
	metadata        *mqttMetadata
	logger          logger.Logger
	topics          map[string]mqttPubSubSubscription
	subscribingLock sync.RWMutex
	reconnectCh     chan struct{}
	closeCh         chan struct{}
	closed          atomic.Bool
	wg              sync.WaitGroup
}

type mqttPubSubSubscription struct {
	handler pubsub.Handler
	alias   string
	matcher func(topic string) bool
}

// NewMQTTPubSub returns a new mqttPubSub instance.
func NewMQTTPubSub(logger logger.Logger) pubsub.PubSub {
	return &mqttPubSub{
		logger:      logger,
		reconnectCh: make(chan struct{}),
		closeCh:     make(chan struct{}),
	}
}

// Init parses metadata and creates a new Pub Sub client.
func (m *mqttPubSub) Init(ctx context.Context, metadata pubsub.Metadata) error {
	mqttMeta, err := parseMQTTMetaData(metadata, m.logger)
	if err != nil {
		return err
	}
	m.metadata = mqttMeta

	err = m.connect(ctx)
	if err != nil {
		return fmt.Errorf("failed to establish connection to broker: %w", err)
	}
	m.topics = make(map[string]mqttPubSubSubscription)

	m.logger.Debug("mqtt message bus initialization complete")

	return nil
}

// Publish the topic to mqtt pub sub.
func (m *mqttPubSub) Publish(ctx context.Context, req *pubsub.PublishRequest) (err error) {
	if m.closed.Load() {
		return errors.New("component is closed")
	}

	if req.Topic == "" {
		return errors.New("topic name is empty")
	}

	// Note this can contain PII
	// m.logger.Debugf("mqtt publishing topic %s with data: %v", req.Topic, req.Data)
	m.logger.Debugf("mqtt publishing topic %s", req.Topic)

	retain := m.metadata.Retain
	if val, ok := req.Metadata[mqttRetain]; ok && val != "" {
		retain, err = strconv.ParseBool(val)
		if err != nil {
			return fmt.Errorf("mqtt invalid retain %s, %s", val, err)
		}
	}

	token := m.conn.Publish(req.Topic, m.metadata.Qos, retain, req.Data)
	ctx, cancel := context.WithTimeout(ctx, defaultWait)
	defer cancel()
	select {
	case <-token.Done():
		err = token.Error()
	case <-m.closeCh:
		err = errors.New("mqtt client closed")
	case <-ctx.Done():
		// Context canceled
		err = ctx.Err()
	}
	if err != nil {
		return fmt.Errorf("failed to publish: %w", err)
	}

	return nil
}

// Subscribe to the topic on MQTT.
// Request metadata includes:
// - "unsubscribeOnClose": if true, when the subscription is stopped (context canceled), then an Unsubscribe message is sent to the MQTT broker, which will stop delivering messages to this consumer ID until the subscription is explicitly re-started with a new Subscribe call. Otherwise, messages continue to be delivered but are not handled and are NACK'd automatically. "unsubscribeOnClose" should be used with dynamic subscriptions.
func (m *mqttPubSub) Subscribe(ctx context.Context, req pubsub.SubscribeRequest, handler pubsub.Handler) error {
	if m.closed.Load() {
		return errors.New("component is closed")
	}

	topic := req.Topic
	if topic == "" {
		return errors.New("topic name is empty")
	}
	unsubscribeOnClose := utils.IsTruthy(req.Metadata[unsubscribeOnCloseKey])

	m.subscribingLock.Lock()
	defer m.subscribingLock.Unlock()

	// Add the topic then start the subscription
	m.addTopic(topic, handler)

	token := m.conn.Subscribe(topic, m.metadata.Qos, m.onMessage(ctx))
	var err error
	select {
	case <-token.Done():
		// Subscription went through (sucecessfully or not)
		err = token.Error()
	case <-ctx.Done():
		err = fmt.Errorf("error while waiting for subscription token: %w", ctx.Err())
	case <-time.After(defaultWait):
		err = errors.New("timeout waiting for subscription")
	}
	if err != nil {
		// Return an error
		delete(m.topics, topic)
		return fmt.Errorf("mqtt error from subscribe: %v", err)
	}

	m.logger.Infof("MQTT is subscribed to topic %s (qos: %d)", topic, m.metadata.Qos)

	// Listen for context cancelation to remove the subscription
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()

		select {
		case <-ctx.Done():
		case <-m.closeCh:
			// If Close has been called, nothing to do here as the entire connection
			// will be closed.
			return
		}

		// Delete the topic from the map first, which stops routing messages to handlers
		delete(m.topics, topic)

		// We will call Unsubscribe only if cleanSession is true or if "unsubscribeOnClose" in the request metadata is true
		// Otherwise, calling this will make the broker lose the position of our subscription, which is not what we want if we are going to reconnect later
		if !m.metadata.CleanSession && !unsubscribeOnClose {
			return
		}

		unsubscribeToken := m.conn.Unsubscribe(topic)
		var unsubscribeErr error
		select {
		case <-unsubscribeToken.Done():
			// Subscription went through (sucecessfully or not)
			unsubscribeErr = token.Error()
		case <-time.After(defaultWait):
			unsubscribeErr = fmt.Errorf("timeout while unsubscribing from topic %s", topic)
		}
		if unsubscribeErr != nil {
			m.logger.Warnf("Failed to ubsubscribe from topic %s: %v", topic, unsubscribeErr)
		}
	}()

	return nil
}

// onMessage returns the callback to be invoked when there's a new message from a topic
func (m *mqttPubSub) onMessage(ctx context.Context) func(client mqtt.Client, mqttMsg mqtt.Message) {
	return func(client mqtt.Client, mqttMsg mqtt.Message) {
		msg := pubsub.NewMessage{
			Topic:    mqttMsg.Topic(),
			Data:     mqttMsg.Payload(),
			Metadata: map[string]string{"retained": strconv.FormatBool(mqttMsg.Retained())},
		}

		topicHandler := m.handlerForTopic(msg.Topic)
		if topicHandler == nil {
			m.logger.Warnf("No handler defined for messages received on topic %s", msg.Topic)
			return
		}

		m.logger.Debugf("Processing MQTT message %s#%d (retained=%v)", mqttMsg.Topic(), mqttMsg.MessageID(), mqttMsg.Retained())
		err := topicHandler(ctx, &msg)
		if err != nil {
			m.logger.Errorf("Failed processing MQTT message %s#%d: %v", mqttMsg.Topic(), mqttMsg.MessageID(), err)
			return
		}

		m.logger.Debugf("Done processing MQTT message %s#%d; sending ACK", mqttMsg.Topic(), mqttMsg.MessageID())
		mqttMsg.Ack()
	}
}

// Returns the handler for a message sent to a given topic, supporting wildcards and other special syntaxes.
func (m *mqttPubSub) handlerForTopic(topic string) pubsub.Handler {
	m.subscribingLock.RLock()
	defer m.subscribingLock.RUnlock()

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

func (m *mqttPubSub) doConnect(ctx context.Context, clientID string) (mqtt.Client, error) {
	uri, err := url.Parse(m.metadata.URL)
	if err != nil {
		return nil, err
	}

	opts := m.createClientOptions(uri, clientID)
	client := mqtt.NewClient(opts)

	token := client.Connect()
	select {
	case <-token.Done():
		err = token.Error()
	case <-ctx.Done():
		err = ctx.Err()
	}
	if err != nil {
		return nil, err
	}

	return client, nil
}

// Create a connection
func (m *mqttPubSub) connect(ctx context.Context) error {
	m.subscribingLock.Lock()
	defer m.subscribingLock.Unlock()

	ctx, cancel := context.WithTimeout(ctx, defaultWait)
	defer cancel()
	conn, err := m.doConnect(ctx, m.metadata.ConsumerID)
	if err != nil {
		return err
	}
	m.conn = conn

	return nil
}

func (m *mqttPubSub) createClientOptions(uri *url.URL, clientID string) *mqtt.ClientOptions {
	opts := mqtt.NewClientOptions().
		SetClientID(clientID).
		SetCleanSession(m.metadata.CleanSession).
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
		m.logger.Errorf("Connection with broker lost; error: %v", err)
	}

	opts.OnReconnecting = func(c mqtt.Client, co *mqtt.ClientOptions) {
		m.logger.Info("Attempting to reconnect to broker…")
	}

	// On (re-)connection, add all established topic subscriptions
	opts.OnConnect = func(c mqtt.Client) {
		m.subscribingLock.RLock()
		defer m.subscribingLock.RUnlock()

		// If there's nothing to subscribe to, just return
		if len(m.topics) == 0 {
			return
		}

		// Create the list of topics to subscribe to
		subscribeTopics := make(map[string]byte, len(m.topics))
		for k := range m.topics {
			subscribeTopics[k] = m.metadata.Qos
		}

		// Note that this is a bit unusual for a pubsub component as we're using a background context for the handler.
		// This is because we can't really use a different context for each handler in a single SubscribeMultiple call, and the alternative (multiple individual Subscribe calls) is not ideal
		ctx, cancel := context.WithCancel(context.Background())
		m.wg.Add(1)
		go func() {
			defer m.wg.Done()
			defer cancel()
			<-m.closeCh
		}()
		token := c.SubscribeMultiple(
			subscribeTopics,
			m.onMessage(ctx),
		)

		var err error
		subscribeCtx, subscribeCancel := context.WithTimeout(ctx, defaultWait)
		defer subscribeCancel()
		select {
		case <-token.Done():
			// Subscription went through (sucecessfully or not)
			err = token.Error()
		case <-subscribeCtx.Done():
			err = fmt.Errorf("error while waiting for subscription token: %w", subscribeCtx.Err())
		}

		// Nothing we can do in case of errors besides logging them
		// If we get here, the connection is almost likely broken anyways, so the client will attempt a reconnection soon if it hasn't already
		if err != nil {
			m.logger.Errorf("Error starting subscriptions in the OnConnect handler: %v", err)
		}
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
	tlsConfig, err := pubsub.ConvertTLSPropertiesToTLSConfig(m.metadata.TLSProperties)
	if err != nil {
		m.logger.Warnf("failed to load TLS config: %s", err)
	} else {
		opts.SetTLSConfig(tlsConfig)
	}

	return opts
}

// Close the connection. Blocks until all subscriptions are closed.
func (m *mqttPubSub) Close() error {
	m.subscribingLock.Lock()
	defer m.subscribingLock.Unlock()

	m.logger.Debug("Closing component")

	// Clear all topics from the map as a first thing, before stopping all subscriptions (we have the lock anyways)
	maps.Clear(m.topics)

	if m.closed.CompareAndSwap(false, true) {
		close(m.closeCh)
	}

	// Disconnect
	m.conn.Disconnect(100)

	m.wg.Wait()

	return nil
}

func (m *mqttPubSub) Features() []pubsub.Feature {
	return []pubsub.Feature{pubsub.FeatureSubscribeWildcards}
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
		start    int
		okPos    bool
	)
	if strings.ContainsAny(topicName, "#+") {
		regexStr = "^"
		// It's ok to iterate over bytes here (rather than codepoints) because all characters we're looking for are always single-byte
		for i := range len(topicName) {
			// Wildcard chars must either be at the beginning of the string or must follow a /
			okPos = (i == 0 || topicName[i-1] == '/')
			if topicName[i] == '#' && okPos {
				lastPos = i
				if i > 0 && i == (len(topicName)-1) {
					// Edge case: we're at the end of the string so we can allow omitting the preceding /
					regexStr += regexp.QuoteMeta(topicName[start:(i-1)]) + "(.*)"
				} else {
					regexStr += regexp.QuoteMeta(topicName[start:i]) + "(.*)"
				}
				start = i + 1
			} else if topicName[i] == '+' && okPos {
				lastPos = i
				if i > 0 && i == (len(topicName)-1) {
					// Edge case: we're at the end of the string so we can allow omitting the preceding /
					regexStr += regexp.QuoteMeta(topicName[start:(i-1)]) + `((\/|)[^\/]*)`
				} else {
					regexStr += regexp.QuoteMeta(topicName[start:i]) + `([^\/]*)`
				}
				start = i + 1
			}
		}
		regexStr += regexp.QuoteMeta(topicName[(lastPos+1):]) + "$"
	}

	if lastPos == -1 {
		return ""
	}

	return regexStr
}

// GetComponentMetadata returns the metadata of the component.
func (m *mqttPubSub) GetComponentMetadata() (metadataInfo metadata.MetadataMap) {
	metadataStruct := mqttMetadata{}
	metadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, metadata.PubSubType)
	return
}
