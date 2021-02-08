// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package kafka

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/Shopify/sarama"
	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/dapr/pkg/logger"
)

const (
	key = "partitionKey"
)

// Kafka allows reading/writing to a Kafka consumer group
type Kafka struct {
	producer      sarama.SyncProducer
	consumerGroup string
	brokers       []string
	logger        logger.Logger
	authRequired  bool
	saslUsername  string
	saslPassword  string
	cg            sarama.ConsumerGroup
	topics        map[string]bool
	cancel        context.CancelFunc
	consumer      consumer
	config        *sarama.Config
}

type kafkaMetadata struct {
	Brokers         []string `json:"brokers"`
	ConsumerID      string   `json:"consumerID"`
	AuthRequired    bool     `json:"authRequired"`
	SaslUsername    string   `json:"saslUsername"`
	SaslPassword    string   `json:"saslPassword"`
	MaxMessageBytes int      `json:"maxMessageBytes"`
}

type consumer struct {
	ready    chan bool
	callback func(msg *pubsub.NewMessage) error
	once     sync.Once
}

func (consumer *consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		if consumer.callback != nil {
			err := consumer.callback(&pubsub.NewMessage{
				Topic: claim.Topic(),
				Data:  message.Value,
			})
			if err == nil {
				session.MarkMessage(message, "")
			}
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

// NewKafka returns a new kafka pubsub instance
func NewKafka(l logger.Logger) pubsub.PubSub {
	return &Kafka{logger: l}
}

// Init does metadata parsing and connection establishment
func (k *Kafka) Init(metadata pubsub.Metadata) error {
	meta, err := k.getKafkaMetadata(metadata)
	if err != nil {
		return err
	}

	p, err := k.getSyncProducer(meta)
	if err != nil {
		return err
	}

	k.brokers = meta.Brokers
	k.producer = p
	k.consumerGroup = meta.ConsumerID

	if meta.AuthRequired {
		k.saslUsername = meta.SaslUsername
		k.saslPassword = meta.SaslPassword
	}

	config := sarama.NewConfig()
	config.Version = sarama.V2_0_0_0

	if k.authRequired {
		updateAuthInfo(config, k.saslUsername, k.saslPassword)
	}

	k.config = config

	k.topics = make(map[string]bool)

	k.logger.Debug("Kafka message bus initialization complete")

	return nil
}

// Publish message to Kafka cluster
func (k *Kafka) Publish(req *pubsub.PublishRequest) error {
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

// Close down consumer group resources, refresh once
func (k *Kafka) closeSubscripionResources() {
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
// This call cannot block like its sibling in bindings/kafka because of where this is invoked in runtime.go
func (k *Kafka) Subscribe(req pubsub.SubscribeRequest, handler func(msg *pubsub.NewMessage) error) error {
	topics := k.addTopic(req.Topic)

	// Close resources and reset synchronization primitives
	k.closeSubscripionResources()

	cg, err := sarama.NewConsumerGroup(k.brokers, k.consumerGroup, k.config)
	if err != nil {
		return err
	}

	k.cg = cg

	ctx, cancel := context.WithCancel(context.Background())
	k.cancel = cancel

	ready := make(chan bool)
	k.consumer = consumer{
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
			// Consume the requested topic
			innerError := k.cg.Consume(ctx, topics, &(k.consumer))
			if innerError != nil {
				k.logger.Errorf("Error consuming %v: %v", topics, innerError)
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

// getKafkaMetadata returns new Kafka metadata
func (k *Kafka) getKafkaMetadata(metadata pubsub.Metadata) (*kafkaMetadata, error) {
	meta := kafkaMetadata{}
	// use the runtimeConfig.ID as the consumer group so that each dapr runtime creates its own consumergroup
	meta.ConsumerID = metadata.Properties["consumerID"]
	k.logger.Debugf("Using %s as ConsumerGroup name", meta.ConsumerID)

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

	return &meta, nil
}

func (k *Kafka) getSyncProducer(meta *kafkaMetadata) (sarama.SyncProducer, error) {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5
	config.Producer.Return.Successes = true

	if k.authRequired {
		updateAuthInfo(config, k.saslUsername, k.saslPassword)
	}

	if meta.MaxMessageBytes > 0 {
		config.Producer.MaxMessageBytes = meta.MaxMessageBytes
	}

	producer, err := sarama.NewSyncProducer(meta.Brokers, config)
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

	config.Net.TLS.Enable = true
	// nolint: gosec
	config.Net.TLS.Config = &tls.Config{
		// InsecureSkipVerify: true,
		ClientAuth: 0,
	}
}

func (k *Kafka) Close() error {
	k.closeSubscripionResources()

	return k.producer.Close()
}

func (k *Kafka) Features() []pubsub.Feature {
	return nil
}
