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
	"errors"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"

	"github.com/Shopify/sarama"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/kit/logger"
)

const (
	key = "partitionKey"
)

// Kafka allows reading/writing to a Kafka consumer group.
type Kafka struct {
	producer      sarama.SyncProducer
	topics        []string
	consumerGroup string
	brokers       []string
	publishTopic  string
	authRequired  bool
	saslUsername  string
	saslPassword  string
	initialOffset int64
	logger        logger.Logger
}

type kafkaMetadata struct {
	Brokers         []string `json:"brokers"`
	Topics          []string `json:"topics"`
	PublishTopic    string   `json:"publishTopic"`
	ConsumerGroup   string   `json:"consumerGroup"`
	AuthRequired    bool     `json:"authRequired"`
	SaslUsername    string   `json:"saslUsername"`
	SaslPassword    string   `json:"saslPassword"`
	InitialOffset   int64    `json:"initialOffset"`
	MaxMessageBytes int
}

type consumer struct {
	ready    chan bool
	callback func(*bindings.ReadResponse) ([]byte, error)
}

func (consumer *consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		if consumer.callback != nil {
			_, err := consumer.callback(&bindings.ReadResponse{
				Data: message.Value,
			})
			if err == nil {
				session.MarkMessage(message, "")
			}
		}
	}

	return nil
}

func (consumer *consumer) Setup(sarama.ConsumerGroupSession) error {
	close(consumer.ready)

	return nil
}

// NewKafka returns a new kafka binding instance.
func NewKafka(logger logger.Logger) *Kafka {
	return &Kafka{logger: logger}
}

// Init does metadata parsing and connection establishment.
func (k *Kafka) Init(metadata bindings.Metadata) error {
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
	k.topics = meta.Topics
	k.publishTopic = meta.PublishTopic
	k.consumerGroup = meta.ConsumerGroup
	k.authRequired = meta.AuthRequired
	k.initialOffset = meta.InitialOffset

	// ignore SASL properties if authRequired is false
	if meta.AuthRequired {
		k.saslUsername = meta.SaslUsername
		k.saslPassword = meta.SaslPassword
	}

	return nil
}

func (k *Kafka) Operations() []bindings.OperationKind {
	return []bindings.OperationKind{bindings.CreateOperation}
}

func (k *Kafka) Invoke(req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	msg := &sarama.ProducerMessage{
		Topic: k.publishTopic,
		Value: sarama.ByteEncoder(req.Data),
	}
	if val, ok := req.Metadata[key]; ok && val != "" {
		msg.Key = sarama.StringEncoder(val)
	}

	_, _, err := k.producer.SendMessage(msg)
	if err != nil {
		return nil, err
	}

	return nil, nil
}

// GetKafkaMetadata returns new Kafka metadata.
func (k *Kafka) getKafkaMetadata(metadata bindings.Metadata) (*kafkaMetadata, error) {
	meta := kafkaMetadata{}
	meta.ConsumerGroup = metadata.Properties["consumerGroup"]
	meta.PublishTopic = metadata.Properties["publishTopic"]

	initialOffset, err := parseInitialOffset(metadata.Properties["initialOffset"])
	if err != nil {
		return nil, err
	}
	meta.InitialOffset = initialOffset

	if val, ok := metadata.Properties["brokers"]; ok && val != "" {
		meta.Brokers = strings.Split(val, ",")
	}
	if val, ok := metadata.Properties["topics"]; ok && val != "" {
		meta.Topics = strings.Split(val, ",")
	}

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
	config.Version = sarama.V1_0_0_0

	// ignore SASL properties if authRequired is false
	if meta.AuthRequired {
		updateAuthInfo(config, meta.SaslUsername, meta.SaslPassword)
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

func (k *Kafka) Read(handler func(*bindings.ReadResponse) ([]byte, error)) error {
	if len(k.topics) == 0 {
		k.logger.Warnf("kafka binding: no topic defined, input bindings will not be started")
		return nil
	}

	config := sarama.NewConfig()
	config.Version = sarama.V1_0_0_0
	config.Consumer.Offsets.Initial = k.initialOffset
	// ignore SASL properties if authRequired is false
	if k.authRequired {
		updateAuthInfo(config, k.saslUsername, k.saslPassword)
	}
	c := consumer{
		callback: handler,
		ready:    make(chan bool),
	}

	client, err := sarama.NewConsumerGroup(k.brokers, k.consumerGroup, config)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(context.Background())

	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			if err = client.Consume(ctx, k.topics, &c); err != nil {
				k.logger.Errorf("error from c: %s", err)
			}
			// check if context was cancelled, signaling that the c should stop
			if ctx.Err() != nil {
				return
			}
			c.ready = make(chan bool)
		}
	}()

	<-c.ready

	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)
	<-sigterm
	cancel()
	wg.Wait()
	if err = client.Close(); err != nil {
		return err
	}

	return nil
}

func (consumer *consumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
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
	if err := k.producer.Close(); err != nil {
		k.logger.Errorf("kafka error: failed to close producer: %v", err)

		return err
	}

	return nil
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
