package pulsar

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/dapr/dapr/pkg/logger"

	"github.com/dapr/components-contrib/pubsub"
)

const (
	host             = "host"
	consumerID       = "consumerID"
	subscriptionName = "subscriptionName"
)

type Pulsar struct {
	logger   logger.Logger
	client   pulsar.Client
	metadata pulsarMetadata
}

type pulsarMetadata struct {
	Host       string `json:"host"`
	ConsumerID string `json:"consumerID"`
}

func NewPulsar(l logger.Logger) pubsub.PubSub {
	return &Pulsar{logger: l}
}

func parsePulsarMetadata(meta pubsub.Metadata) (pulsarMetadata, error) {
	m := pulsarMetadata{}
	if val, ok := meta.Properties[host]; ok && val != "" {
		m.Host = val
	} else {
		return m, errors.New("Pulsar error: missing pulsar Host")
	}
	if val, ok := meta.Properties[consumerID]; ok && val != "" {
		m.ConsumerID = val
	} else {
		return m, errors.New("Pulsar error: missing consumerID")
	}

	return m, nil
}

func (p *Pulsar) Init(metadata pubsub.Metadata) error {
	m, err := parsePulsarMetadata(metadata)
	if err != nil {
		return err
	}
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL:                        fmt.Sprintf("pulsar://%s", m.Host),
		OperationTimeout:           30 * time.Second,
		ConnectionTimeout:          30 * time.Second,
		TLSAllowInsecureConnection: true,
	})
	if err != nil {
		p.logger.Debugf("Could not instantiate Pulsar client: %v", err)
		return fmt.Errorf("Nope")
	}
	defer client.Close()
	p.client = client
	p.metadata = m
	return nil
}

func (p *Pulsar) Publish(req *pubsub.PublishRequest) error {
	producer, err := p.client.CreateProducer(pulsar.ProducerOptions{
		Topic: req.Topic,
	})

	_, err = producer.Send(context.Background(), &pulsar.ProducerMessage{
		Payload: req.Data,
	})

	defer producer.Close()

	if err != nil {
		return err
	}
	return nil
}

func (p *Pulsar) Subscribe(req pubsub.SubscribeRequest, handler func(msg *pubsub.NewMessage) error) error {
	channel := make(chan pulsar.ConsumerMessage, 100)

	options := pulsar.ConsumerOptions{
		Topic:            req.Topic,
		SubscriptionName: p.metadata.ConsumerID,
		Type:             pulsar.Shared,
	}

	options.MessageChannel = channel

	consumer, err := p.client.Subscribe(options)
	if err != nil {
		p.logger.Error(err)
	}

	defer consumer.Close()

	for cm := range channel {
		msg := cm.Message
		handler(&pubsub.NewMessage{
			Data:  msg.Payload(),
			Topic: req.Topic,
		})
		consumer.Ack(msg)
	}
	return nil
}
