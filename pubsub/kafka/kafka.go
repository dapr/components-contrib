// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package kafka

import (
	"context"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/dapr/pkg/logger"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
)

// Kafka allows reading/writing to a Kafka consumer group
type Kafka struct {
	producer      sarama.SyncProducer
	consumerGroup string
	brokers       []string
	logger			logger.Logger
}

type kafkaMetadata struct {
	Brokers       []string `json:"brokers"`
	PublishTopic  string   `json:"publishTopic"`
	ConsumerGroup string   `json:"consumerGroup"`
}

type consumer struct {
	ready    chan bool
	callback func(msg *pubsub.NewMessage) error
	once sync.Once
}

func (consumer *consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	fmt.Printf("message consuming claim...")
	for message := range claim.Messages() {
		// XXX: Remove this
		fmt.Printf("message consuming claim: %v\n", message)
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
	consumer.once.Do(func () {
		close(consumer.ready)
	})

	return nil
}

// NewKafka returns a new kafka pubsub instance
func NewKafka(l logger.Logger) pubsub.PubSub {
	// XXX: Remove this override of the log level
	l.SetOutputLevel(logger.DebugLevel)
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
	k.consumerGroup = meta.ConsumerGroup
	k.logger.Debug("Kafka message bus initialization complete")
	return nil
}

// Publish message to Kafka cluster
func (k *Kafka) Publish(req *pubsub.PublishRequest) error {
	k.logger.Debugf("Publishing topic %v with data: %v", req.Topic, req.Data)
	partition, offset, err := k.producer.SendMessage(&sarama.ProducerMessage{
		Topic: req.Topic,
		Value: sarama.ByteEncoder(req.Data),
	})

	k.logger.Debugf("Partition: %v, offset: %v", partition, offset)

	if err != nil {
		return err
	}

	return nil
}

// Subscribe to topic in the Kafka cluster
// This call cannot block like its sibling in bindings/kafka because of where this is invoked in runtime.go
func (k *Kafka) Subscribe(req pubsub.SubscribeRequest, handler func(msg *pubsub.NewMessage) error) error {
	config := sarama.NewConfig()
	config.Version = sarama.V2_0_0_0

	cg, err := sarama.NewConsumerGroup(k.brokers, k.consumerGroup, config)

	if err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(context.Background())
	ready := make(chan bool)
	cs := consumer{
		ready: ready,
		callback: handler,
	}

	go func() {
		defer func() {
			k.logger.Debugf("Closing ConsumerGroup for topic %s", req.Topic)
			cg.Close()
		}()

		k.logger.Debugf("Subscribed and listening to topic: %s", req.Topic)

		for {
			// Consume the requested topic
			err := cg.Consume(ctx, []string{req.Topic}, &cs)
			if err != nil {
				k.logger.Errorf("Error consuming %s: %s", req.Topic, err)
			}

			// If the context was cancelled, as is the case when handling SIGINT and SIGTERM below, then this pops
			// us out of the consume loop
			if ctx.Err() != nil {
				return
			}
		}
	}()

	// Spin up a go routine to handle OS signals
	go func() {
		sigterm := make(chan os.Signal, 1)
		signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)
		recvdSignal := <-sigterm
		k.logger.Debug("Interrupt signal received. Shutting down pubsub listener.")
		cancel()
		err = cg.Close()
		if err != nil {
			k.logger.Errorf("Error closing consumer group client: %v", err)
		}
		// Resending the received signal in order to resume normal processing of that signal
		signal.Reset()
		proc, err := os.FindProcess(os.Getpid())

		if err != nil {
			k.logger.Errorf("Error when handling interrupt. Exiting\n%v", err)
			os.Exit(1)
			return
		}

		// Resend the signal to continue with normal handling
		if err = proc.Signal(recvdSignal); err != nil {
			k.logger.Errorf("Error resending signal. Exiting\n%v", err)
			os.Exit(1)
		}
	}()

	<-ready
	return nil
}

// getKafkaMetadata returns new Kafka metadata
func (k *Kafka) getKafkaMetadata(metadata pubsub.Metadata) (*kafkaMetadata, error) {
	meta := kafkaMetadata{}
	meta.ConsumerGroup = metadata.Properties["consumerGroup"]

	if val, ok := metadata.Properties["brokers"]; ok && val != "" {
		meta.Brokers = strings.Split(val, ",")
	}

	k.logger.Infof("Found brokers: %v", meta.Brokers)

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
