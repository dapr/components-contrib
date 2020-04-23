// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package kafka

import (
	"context"
	"crypto/tls"
	"errors"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"

	"github.com/Shopify/sarama"
	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/dapr/pkg/logger"
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
}

type kafkaMetadata struct {
	Brokers      []string `json:"brokers"`
	ConsumerID   string   `json:"consumerID"`
	AuthRequired bool     `json:"authRequired"`
	SaslUsername string   `json:"saslUsername"`
	SaslPassword string   `json:"saslPassword"`
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

	if k.authRequired {
		updateAuthInfo(config, k.saslUsername, k.saslPassword)
	}

	cg, err := sarama.NewConsumerGroup(k.brokers, k.consumerGroup, config)

	if err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(context.Background())
	ready := make(chan bool)
	cs := consumer{
		ready:    ready,
		callback: handler,
	}

	closeOnce := &sync.Once{}

	go func() {
		defer closeOnce.Do(func() {
			k.logger.Debugf("Closing ConsumerGroup for topic %s", req.Topic)
			cg.Close()
		})

		k.logger.Debugf("Subscribed and listening to topic: %s", req.Topic)

		for {
			// Consume the requested topic
			innerError := cg.Consume(ctx, []string{req.Topic}, &cs)
			if innerError != nil {
				k.logger.Errorf("Error consuming %s: %s", req.Topic, innerError)
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

	//ignore SASL properties if authRequired is false
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
	config.Net.TLS.Config = &tls.Config{
		//InsecureSkipVerify: true,
		ClientAuth: 0,
	}
}
