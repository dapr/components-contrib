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
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"golang.org/x/exp/maps"

	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/kit/logger"
)

const (
	// errors.
	errorMsgPrefix = "mqtt pub sub error:"
)

// mqttPubSub type allows sending and receiving data to/from MQTT broker.
type mqttPubSub struct {
	conn            mqtt.Client
	metadata        *metadata
	logger          logger.Logger
	topics          map[string]mqttPubSubSubscription
	subscribingLock sync.RWMutex
	reconnectCh     chan struct{}
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
		logger:      logger,
		reconnectCh: make(chan struct{}, 1),
	}
}

// Init parses metadata and creates a new Pub Sub client.
func (m *mqttPubSub) Init(metadata pubsub.Metadata) error {
	mqttMeta, err := parseMQTTMetaData(metadata, m.logger)
	if err != nil {
		return err
	}
	m.metadata = mqttMeta

	m.ctx, m.cancel = context.WithCancel(context.Background())

	err = m.connect()
	if err != nil {
		return fmt.Errorf("failed to establish connection to broker: %w", err)
	}
	m.topics = make(map[string]mqttPubSubSubscription)

	m.logger.Debug("mqtt message bus initialization complete")

	return nil
}

// Publish the topic to mqtt pub sub.
func (m *mqttPubSub) Publish(parentCtx context.Context, req *pubsub.PublishRequest) (err error) {
	if req.Topic == "" {
		return errors.New("topic name is empty")
	}

	// Note this can contain PII
	// m.logger.Debugf("mqtt publishing topic %s with data: %v", req.Topic, req.Data)
	m.logger.Debugf("mqtt publishing topic %s", req.Topic)

	retain := m.metadata.retain
	if val, ok := req.Metadata[mqttRetain]; ok && val != "" {
		retain, err = strconv.ParseBool(val)
		if err != nil {
			return fmt.Errorf("mqtt invalid retain %s, %s", val, err)
		}
	}

	token := m.conn.Publish(req.Topic, m.metadata.qos, retain, req.Data)
	ctx, cancel := context.WithTimeout(parentCtx, defaultWait)
	defer cancel()
	select {
	case <-token.Done():
		err = token.Error()
	case <-m.ctx.Done():
		// Context canceled
		err = m.ctx.Err()
	case <-ctx.Done():
		// Context canceled
		err = ctx.Err()
	}
	if err != nil {
		return fmt.Errorf("mqtt error from publish: %v", err)
	}

	return nil
}

// Subscribe to the mqtt pub sub topic.
func (m *mqttPubSub) Subscribe(ctx context.Context, req pubsub.SubscribeRequest, handler pubsub.Handler) error {
	if ctxErr := m.ctx.Err(); ctxErr != nil {
		// If the global context has been canceled, we do not allow more subscriptions
		return ctxErr
	}

	topic := req.Topic
	if topic == "" {
		return errors.New("topic name is empty")
	}

	m.subscribingLock.Lock()
	defer m.subscribingLock.Unlock()

	// TODO: CALLBACK MUST NOT BLOCK OR SET ORDERMATTERS=FALSE

	// Add the topic then start the subscription
	m.addTopic(topic, handler)

	// Use the global context here to maintain the handler
	token := m.conn.Subscribe(topic, m.metadata.qos, m.onMessage(m.ctx))
	subscribeCtx, subscribeCancel := context.WithTimeout(m.ctx, defaultWait)
	defer subscribeCancel()
	var err error
	select {
	case <-token.Done():
		// Subscription went through (sucecessfully or not)
		err = token.Error()
	case <-subscribeCtx.Done():
		err = fmt.Errorf("error while waiting for subscription token: %w", subscribeCtx.Err())
	}
	if err != nil {
		// Return an error
		delete(m.topics, topic)
		return fmt.Errorf("mqtt error from subscribe: %v", err)
	}

	// Listen for context cancelation to remove the subscription
	go func() {
		select {
		case <-ctx.Done():
		case <-m.ctx.Done():
		}

		// If m.ctx has been canceled, nothing to do here as the entire connection will be closed
		if m.ctx.Err() != nil {
			return
		}

		// Delete the topic from the map, then stop the subscription
		m.subscribingLock.Lock()
		defer m.subscribingLock.Unlock()
		delete(m.topics, topic)
		m.conn.Unsubscribe(topic)
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
			m.logger.Errorf("no handler defined for topic %s", msg.Topic)
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
	uri, err := url.Parse(m.metadata.url)
	if err != nil {
		return nil, err
	}

	opts := m.createClientOptions(uri, clientID)
	client := mqtt.NewClient(opts)

	// Add all routes before we connect to catch messages that may be delivered before client.Subscribe is invoked
	// The routes will be overwritten later
	for topic := range m.topics {
		client.AddRoute(topic, m.onMessage(ctx))
	}

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
func (m *mqttPubSub) connect() error {
	m.subscribingLock.Lock()
	defer m.subscribingLock.Unlock()

	connCtx, connCancel := context.WithTimeout(m.ctx, defaultWait)
	conn, err := m.doConnect(connCtx, m.metadata.consumerID)
	connCancel()
	if err != nil {
		return err
	}
	m.conn = conn

	return nil
}

// Forcefully closes the connection and, after a delay, reconnects
func (m *mqttPubSub) ResetConection() {
	const reconnectDelay = 30 * time.Second

	// Do not reconnect if there's already one attempt in progress
	select {
	case m.reconnectCh <- struct{}{}:
		// nop
	default:
		// Already a reconnection attempt in progress, so abort
		return
	}

	// Disconnect
	m.logger.Info("Closing connection with broker… will reconnect in " + reconnectDelay.String())
	m.conn.Disconnect(200)

	for m.ctx.Err() == nil {
		time.Sleep(reconnectDelay)

		// Check for context cancelation before reconnecting, since we slept
		if m.ctx.Err() != nil {
			return
		}

		m.logger.Debug("Reconnecting…")
		err := m.connect()
		if err != nil {
			m.logger.Errorf("Failed to reconnect, will retry in " + reconnectDelay.String())
		} else {
			m.logger.Info("Connection with broker re-established")
			break
		}
	}

	// Release the reconnection token
	<-m.reconnectCh
}

func (m *mqttPubSub) createClientOptions(uri *url.URL, clientID string) *mqtt.ClientOptions {
	opts := mqtt.NewClientOptions().
		SetClientID(clientID).
		SetCleanSession(m.metadata.cleanSession).
		SetAutoReconnect(true).
		SetConnectRetry(true).
		SetConnectRetryInterval(30 * time.Second)

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
			subscribeTopics[k] = m.metadata.qos
		}

		// Note that this is a bit unusual for a pubsub component as we're using m.ctx on the handler, which is tied to the component rather than the individual subscription
		// This is because we can't really use a different context for each handler in a single SubscribeMultiple call, and the alternative (multiple individual Subscribe calls) is not ideal
		token := m.conn.SubscribeMultiple(
			subscribeTopics,
			m.onMessage(m.ctx),
		)

		var err error
		subscribeCtx, subscribeCancel := context.WithTimeout(m.ctx, defaultWait)
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

	// Turn off auto-ack
	opts.SetAutoAckDisabled(true)

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

func (m *mqttPubSub) Close() error {
	m.subscribingLock.Lock()
	defer m.subscribingLock.Unlock()

	// Clear all topics from the map as a first thing, before stopping all subscriptions (we have the lock anyways)
	maps.Clear(m.topics)

	// Cancel the context
	m.cancel()

	// Disconnect
	m.conn.Disconnect(200)

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
		for i := 0; i < len(topicName); i++ {
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
