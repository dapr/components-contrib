// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package kafka

import (
	"context"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"

	"github.com/Shopify/sarama"
	"github.com/dapr/components-contrib/pubsub"
	log "github.com/sirupsen/logrus"
)

// Kafka allows reading/writing to a Kafka consumer group
type Kafka struct {
	producer      sarama.SyncProducer
	consumerGroup string
	brokers       []string
}

type kafkaMetadata struct {
	Brokers       []string `json:"brokers"`
	PublishTopic  string   `json:"publishTopic"`
	ConsumerGroup string   `json:"consumerGroup"`
}

type consumer struct {
	ready    chan bool
	callback func(msg *pubsub.NewMessage) error
}

func (consumer *consumer) Setup(sarama.ConsumerGroupSession) error {
	close(consumer.ready)
	return nil
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

// NewKafka returns a new kafka pubsub instance
func NewKafka() pubsub.PubSub {
	return &Kafka{}
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
	k.consumerGroup = meta.ConsumerGroup
	return nil
}

// Publish message to Kafka cluster
func (k *Kafka) Publish(req *pubsub.PublishRequest) error {
	_, _, err := k.producer.SendMessage(&sarama.ProducerMessage{
		Topic: req.Topic,
		Value: sarama.ByteEncoder(req.Data),
	})
	if err != nil {
		return err
	}

	return nil
}

// Subscribe to topic in the Kafka cluser
func (k *Kafka) Subscribe(req pubsub.SubscribeRequest, handler func(msg *pubsub.NewMessage) error) error {

	config := sarama.NewConfig()
	config.Version = sarama.V1_0_0_0

	cs := consumer{
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
			if err = client.Consume(ctx, []string{req.Topic}, &cs); err != nil {
				log.Errorf("error from consumer topic(%s) - %v", req.Topic, err)
			}
			// check if context was cancelled, signaling that the consumer should stop
			if ctx.Err() != nil {
				return
			}
			cs.ready = make(chan bool)
		}
	}()

	<-cs.ready

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

// getKafkaMetadata returns new Kafka metadata
func (k *Kafka) getKafkaMetadata(metadata pubsub.Metadata) (*kafkaMetadata, error) {
	meta := kafkaMetadata{}
	meta.ConsumerGroup = metadata.Properties["consumerGroup"]

	if val, ok := metadata.Properties["brokers"]; ok && val != "" {
		meta.Brokers = strings.Split(val, ",")
	}
	return &meta, nil
}

func (k *Kafka) getSyncProducer(meta *kafkaMetadata) (sarama.SyncProducer, error) {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5
	config.Producer.Return.Successes = true

	producer, err := sarama.NewSyncProducer(meta.Brokers, config)
	if err != nil {
		return nil, err
	}
	return producer, nil
}
