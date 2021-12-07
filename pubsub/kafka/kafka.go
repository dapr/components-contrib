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

package kafka

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/cenkalti/backoff/v4"

	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/kit/logger"
	"github.com/dapr/kit/retry"
)

const (
	key                  = "partitionKey"
	skipVerify           = "skipVerify"
	caCert               = "caCert"
	clientCert           = "clientCert"
	clientKey            = "clientKey"
	consumeRetryInterval = "consumeRetryInterval"
)

// Kafka allows reading/writing to a Kafka consumer group.
type Kafka struct {
	producer      sarama.SyncProducer
	consumerGroup string
	brokers       []string
	logger        logger.Logger
	authRequired  bool
	saslUsername  string
	saslPassword  string
	initialOffset int64
	cg            sarama.ConsumerGroup
	topics        map[string]bool
	cancel        context.CancelFunc
	consumer      consumer
	config        *sarama.Config

	backOffConfig        retry.Config
	consumeRetryInterval time.Duration
}

type kafkaMetadata struct {
	Brokers              []string
	ConsumerGroup        string
	ClientID             string
	AuthRequired         bool
	SaslUsername         string
	SaslPassword         string
	InitialOffset        int64
	MaxMessageBytes      int
	TLSSkipVerify        bool
	TLSCaCert            string
	TLSClientCert        string
	TLSClientKey         string
	ConsumeRetryInterval time.Duration
	Version              sarama.KafkaVersion
}

type consumer struct {
	k        *Kafka
	ready    chan bool
	callback pubsub.Handler
	once     sync.Once
}

func (consumer *consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	if consumer.callback == nil {
		return fmt.Errorf("nil consumer callback")
	}

	b := consumer.k.backOffConfig.NewBackOffWithContext(session.Context())
	for message := range claim.Messages() {
		msg := pubsub.NewMessage{
			Topic: message.Topic,
			Data:  message.Value,
		}
		if err := retry.NotifyRecover(func() error {
			consumer.k.logger.Debugf("Processing Kafka message: %s/%d/%d [key=%s]", message.Topic, message.Partition, message.Offset, asBase64String(message.Key))
			err := consumer.callback(session.Context(), &msg)
			if err == nil {
				session.MarkMessage(message, "")
			}

			return err
		}, b, func(err error, d time.Duration) {
			consumer.k.logger.Errorf("Error processing Kafka message: %s/%d/%d [key=%s]. Retrying...", message.Topic, message.Partition, message.Offset, asBase64String(message.Key))
		}, func() {
			consumer.k.logger.Infof("Successfully processed Kafka message after it previously failed: %s/%d/%d [key=%s]", message.Topic, message.Partition, message.Offset, asBase64String(message.Key))
		}); err != nil {
			return err
		}
	}

	return nil
}

func (consumer *consumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (consumer *consumer) Setup(sarama.ConsumerGroupSession) error {
	consumer.once.Do(func() {
		close(consumer.ready)
	})

	return nil
}

// NewKafka returns a new kafka pubsub instance.
func NewKafka(l logger.Logger) pubsub.PubSub {
	return &Kafka{logger: l}
}

// Init does metadata parsing and connection establishment.
func (k *Kafka) Init(metadata pubsub.Metadata) error {
	meta, err := k.getKafkaMetadata(metadata)
	if err != nil {
		return err
	}

	k.brokers = meta.Brokers
	k.consumerGroup = meta.ConsumerGroup
	k.authRequired = meta.AuthRequired
	k.initialOffset = meta.InitialOffset

	config := sarama.NewConfig()
	config.Version = meta.Version
	config.Consumer.Offsets.Initial = k.initialOffset

	if meta.ClientID != "" {
		config.ClientID = meta.ClientID
	}

	if k.authRequired {
		k.saslUsername = meta.SaslUsername
		k.saslPassword = meta.SaslPassword
		updateAuthInfo(config, k.saslUsername, k.saslPassword)
	}
	err = updateTLSConfig(config, meta)
	if err != nil {
		return err
	}

	k.config = config

	k.producer, err = getSyncProducer(*k.config, k.brokers, meta.MaxMessageBytes)
	if err != nil {
		return err
	}

	k.topics = make(map[string]bool)

	// Default retry configuration is used if no
	// backOff properties are set.
	if err := retry.DecodeConfigWithPrefix(
		&k.backOffConfig,
		metadata.Properties,
		"backOff"); err != nil {
		return err
	}
	k.consumeRetryInterval = meta.ConsumeRetryInterval

	k.logger.Debug("Kafka message bus initialization complete")

	return nil
}

// Publish message to Kafka cluster.
func (k *Kafka) Publish(req *pubsub.PublishRequest) error {
	if k.producer == nil {
		return errors.New("component is closed")
	}
	k.logger.Debugf("Publishing topic %v with data: %v", req.Topic, req.Data)

	msg := &sarama.ProducerMessage{
		Topic: req.Topic,
		Value: sarama.ByteEncoder(req.Data),
	}

	if val, ok := req.Metadata[key]; ok && val != "" {
		msg.Key = sarama.StringEncoder(val)
	}

	partition, offset, err := k.producer.SendMessage(msg)

	k.logger.Debugf("Partition: %v, offset: %v", partition, offset)

	if err != nil {
		return err
	}

	return nil
}

func (k *Kafka) addTopic(newTopic string) []string {
	// Add topic to our map of topics
	k.topics[newTopic] = true

	topics := make([]string, len(k.topics))

	i := 0
	for topic := range k.topics {
		topics[i] = topic
		i++
	}

	return topics
}

// Close down consumer group resources, refresh once.
func (k *Kafka) closeSubscriptionResources() {
	if k.cg != nil {
		k.cancel()
		err := k.cg.Close()
		if err != nil {
			k.logger.Errorf("Error closing consumer group: %v", err)
		}

		k.consumer.once.Do(func() {
			close(k.consumer.ready)
			k.consumer.once = sync.Once{}
		})
	}
}

// Subscribe to topic in the Kafka cluster
// This call cannot block like its sibling in bindings/kafka because of where this is invoked in runtime.go.
func (k *Kafka) Subscribe(req pubsub.SubscribeRequest, handler pubsub.Handler) error {
	if k.consumerGroup == "" {
		return errors.New("kafka: consumerGroup must be set to subscribe")
	}

	topics := k.addTopic(req.Topic)

	// Close resources and reset synchronization primitives
	k.closeSubscriptionResources()

	cg, err := sarama.NewConsumerGroup(k.brokers, k.consumerGroup, k.config)
	if err != nil {
		return err
	}

	k.cg = cg

	ctx, cancel := context.WithCancel(context.Background())
	k.cancel = cancel

	ready := make(chan bool)
	k.consumer = consumer{
		k:        k,
		ready:    ready,
		callback: handler,
	}

	go func() {
		defer func() {
			k.logger.Debugf("Closing ConsumerGroup for topics: %v", topics)
			err := k.cg.Close()
			if err != nil {
				k.logger.Errorf("Error closing consumer group: %v", err)
			}
		}()

		k.logger.Debugf("Subscribed and listening to topics: %s", topics)

		for {
			k.logger.Debugf("Starting loop to consume.")

			// Consume the requested topics
			bo := backoff.WithContext(backoff.NewConstantBackOff(k.consumeRetryInterval), ctx)
			innerErr := retry.NotifyRecover(func() error {
				return k.cg.Consume(ctx, topics, &(k.consumer))
			}, bo, func(err error, t time.Duration) {
				k.logger.Errorf("Error consuming %v. Retrying...: %v", topics, err)
			}, func() {
				k.logger.Infof("Recovered consuming %v", topics)
			})
			if innerErr != nil && !errors.Is(innerErr, context.Canceled) {
				k.logger.Errorf("Permanent error consuming %v: %v", topics, innerErr)
			}

			// If the context was cancelled, as is the case when handling SIGINT and SIGTERM below, then this pops
			// us out of the consume loop
			if ctx.Err() != nil {
				return
			}
		}
	}()

	<-ready

	return nil
}

// getKafkaMetadata returns new Kafka metadata.
func (k *Kafka) getKafkaMetadata(metadata pubsub.Metadata) (*kafkaMetadata, error) {
	meta := kafkaMetadata{
		ConsumeRetryInterval: 100 * time.Millisecond,
	}
	// use the runtimeConfig.ID as the consumer group so that each dapr runtime creates its own consumergroup
	if val, ok := metadata.Properties["consumerID"]; ok && val != "" {
		meta.ConsumerGroup = val
		k.logger.Debugf("Using %s as ConsumerGroup", meta.ConsumerGroup)
		k.logger.Warn("ConsumerID is deprecated, if ConsumerID and ConsumerGroup are both set, ConsumerGroup is used")
	}

	if val, ok := metadata.Properties["consumerGroup"]; ok && val != "" {
		meta.ConsumerGroup = val
		k.logger.Debugf("Using %s as ConsumerGroup", meta.ConsumerGroup)
	}

	if val, ok := metadata.Properties["clientID"]; ok && val != "" {
		meta.ClientID = val
		k.logger.Debugf("Using %s as ClientID", meta.ClientID)
	}

	initialOffset, err := parseInitialOffset(metadata.Properties["initialOffset"])
	if err != nil {
		return nil, err
	}
	meta.InitialOffset = initialOffset

	if val, ok := metadata.Properties["brokers"]; ok && val != "" {
		meta.Brokers = strings.Split(val, ",")
	} else {
		return nil, errors.New("kafka error: missing 'brokers' attribute")
	}

	k.logger.Debugf("Found brokers: %v", meta.Brokers)

	val, ok := metadata.Properties["authRequired"]
	if !ok {
		return nil, errors.New("kafka error: missing 'authRequired' attribute")
	}
	if val == "" {
		return nil, errors.New("kafka error: 'authRequired' attribute was empty")
	}
	validAuthRequired, err := strconv.ParseBool(val)
	if err != nil {
		return nil, errors.New("kafka error: invalid value for 'authRequired' attribute")
	}
	meta.AuthRequired = validAuthRequired

	// ignore SASL properties if authRequired is false
	if meta.AuthRequired {
		if val, ok := metadata.Properties["saslUsername"]; ok && val != "" {
			meta.SaslUsername = val
		} else {
			return nil, errors.New("kafka error: missing SASL Username")
		}

		if val, ok := metadata.Properties["saslPassword"]; ok && val != "" {
			meta.SaslPassword = val
		} else {
			return nil, errors.New("kafka error: missing SASL Password")
		}
	}

	if val, ok := metadata.Properties["maxMessageBytes"]; ok && val != "" {
		maxBytes, err := strconv.Atoi(val)
		if err != nil {
			return nil, fmt.Errorf("kafka error: cannot parse maxMessageBytes: %s", err)
		}

		meta.MaxMessageBytes = maxBytes
	}

	if val, ok := metadata.Properties[clientCert]; ok && val != "" {
		if !isValidPEM(val) {
			return nil, errors.New("kafka error: invalid client certificate")
		}
		meta.TLSClientCert = val
	}
	if val, ok := metadata.Properties[clientKey]; ok && val != "" {
		if !isValidPEM(val) {
			return nil, errors.New("kafka error: invalid client key")
		}
		meta.TLSClientKey = val
	}
	// clientKey and clientCert need to be all specified or all not specified.
	if (meta.TLSClientKey == "") != (meta.TLSClientCert == "") {
		return nil, errors.New("kafka error: clientKey or clientCert is missing")
	}
	if val, ok := metadata.Properties[caCert]; ok && val != "" {
		if !isValidPEM(val) {
			return nil, errors.New("kafka error: invalid ca certificate")
		}
		meta.TLSCaCert = val
	}
	if val, ok := metadata.Properties[skipVerify]; ok && val != "" {
		boolVal, err := strconv.ParseBool(val)
		if err != nil {
			return nil, fmt.Errorf("kafka error: invalid value for '%s' attribute: %w", skipVerify, err)
		}
		meta.TLSSkipVerify = boolVal
		if boolVal {
			k.logger.Infof("kafka: you are using 'skipVerify' to skip server config verify which is unsafe!")
		}
	}
	if val, ok := metadata.Properties[consumeRetryInterval]; ok && val != "" {
		durationVal, err := time.ParseDuration(val)
		if err != nil {
			intVal, err := strconv.ParseUint(val, 10, 32)
			if err != nil {
				return nil, fmt.Errorf("kafka error: invalid value for '%s' attribute: %w", consumeRetryInterval, err)
			}
			durationVal = time.Duration(intVal) * time.Millisecond
		}
		meta.ConsumeRetryInterval = durationVal
	}

	if val, ok := metadata.Properties["version"]; ok && val != "" {
		version, err := sarama.ParseKafkaVersion(val)
		if err != nil {
			return nil, errors.New("kafka error: invalid kafka version")
		}
		meta.Version = version
	} else {
		meta.Version = sarama.V2_0_0_0
	}

	return &meta, nil
}

// isValidPEM validates the provided input has PEM formatted block.
func isValidPEM(val string) bool {
	block, _ := pem.Decode([]byte(val))

	return block != nil
}

func getSyncProducer(config sarama.Config, brokers []string, maxMessageBytes int) (sarama.SyncProducer, error) {
	// Add SyncProducer specific properties to copy of base config
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5
	config.Producer.Return.Successes = true

	if maxMessageBytes > 0 {
		config.Producer.MaxMessageBytes = maxMessageBytes
	}

	producer, err := sarama.NewSyncProducer(brokers, &config)
	if err != nil {
		return nil, err
	}

	return producer, nil
}

func updateAuthInfo(config *sarama.Config, saslUsername, saslPassword string) {
	config.Net.SASL.Enable = true
	config.Net.SASL.User = saslUsername
	config.Net.SASL.Password = saslPassword
	config.Net.SASL.Mechanism = sarama.SASLTypePlaintext
}

func updateTLSConfig(config *sarama.Config, metadata *kafkaMetadata) error {
	if !metadata.TLSSkipVerify && metadata.TLSCaCert == "" && metadata.TLSClientCert == "" {
		return nil
	}
	config.Net.TLS.Enable = true
	// nolint: gosec
	config.Net.TLS.Config = &tls.Config{InsecureSkipVerify: metadata.TLSSkipVerify}
	if metadata.TLSClientCert != "" && metadata.TLSClientKey != "" {
		cert, err := tls.X509KeyPair([]byte(metadata.TLSClientCert), []byte(metadata.TLSClientKey))
		if err != nil {
			return fmt.Errorf("unable to load client certificate and key pair. Err: %w", err)
		}
		config.Net.TLS.Config.Certificates = []tls.Certificate{cert}
	}
	if metadata.TLSCaCert != "" {
		caCertPool := x509.NewCertPool()
		if ok := caCertPool.AppendCertsFromPEM([]byte(metadata.TLSCaCert)); !ok {
			return errors.New("kafka error: unable to load ca certificate")
		}
		config.Net.TLS.Config.RootCAs = caCertPool
	}

	return nil
}

func (k *Kafka) Close() (err error) {
	k.closeSubscriptionResources()

	if k.producer != nil {
		err = k.producer.Close()
		k.producer = nil
	}

	return err
}

func (k *Kafka) Features() []pubsub.Feature {
	return nil
}

// asBase64String implements the `fmt.Stringer` interface in order to print
// `[]byte` as a base 64 encoded string.
// It is used above to log the message key. The call to `EncodeToString`
// only occurs for logs that are written based on the logging level.
type asBase64String []byte

func (s asBase64String) String() string {
	return base64.StdEncoding.EncodeToString(s)
}

func parseInitialOffset(value string) (initialOffset int64, err error) {
	initialOffset = sarama.OffsetNewest // Default
	if strings.EqualFold(value, "oldest") {
		initialOffset = sarama.OffsetOldest
	} else if strings.EqualFold(value, "newest") {
		initialOffset = sarama.OffsetNewest
	} else if value != "" {
		return 0, fmt.Errorf("kafka error: invalid initialOffset: %s", value)
	}

	return initialOffset, err
}
