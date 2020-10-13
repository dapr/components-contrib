package pulsar

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/dapr/pkg/logger"
)

const (
	host      = "host"
	enableTLS = "enableTLS"
)

type Pulsar struct {
	logger   logger.Logger
	client   pulsar.Client
	metadata pulsarMetadata
}

func NewPulsar(l logger.Logger) pubsub.PubSub {
	return &Pulsar{logger: l}
}

func parsePulsarMetadata(meta pubsub.Metadata) (*pulsarMetadata, error) {
	m := pulsarMetadata{}
	m.ConsumerID = meta.Properties["consumerID"]

	if val, ok := meta.Properties[host]; ok && val != "" {
		m.Host = val
	} else {
		return nil, errors.New("pulsar error: missing pulsar host")
	}
	if val, ok := meta.Properties[enableTLS]; ok && val != "" {
		tls, err := strconv.ParseBool(val)
		if err != nil {
			return nil, errors.New("pulsar error: invalid value for enableTLS")
		}
		m.EnableTLS = tls
	}

	return &m, nil
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
		TLSAllowInsecureConnection: !m.EnableTLS,
	})
	if err != nil {
		return fmt.Errorf("could not instantiate pulsar client: %v", err)
	}
	defer client.Close()

	p.client = client
	p.metadata = *m

	return nil
}

func (p *Pulsar) Publish(req *pubsub.PublishRequest) error {
	producer, err := p.client.CreateProducer(pulsar.ProducerOptions{
		Topic: req.Topic,
	})
	if err != nil {
		return err
	}

	_, err = producer.Send(context.Background(), &pulsar.ProducerMessage{
		Payload: req.Data,
	})
	if err != nil {
		return err
	}

	defer producer.Close()

	return nil
}

func (p *Pulsar) Subscribe(req pubsub.SubscribeRequest, handler func(msg *pubsub.NewMessage) error) error {
	channel := make(chan pulsar.ConsumerMessage, 100)

	options := pulsar.ConsumerOptions{
		Topic:            req.Topic,
		SubscriptionName: p.metadata.ConsumerID,
		Type:             pulsar.Failover,
	}

	options.MessageChannel = channel
	consumer, err := p.client.Subscribe(options)
	if err != nil {
		p.logger.Debugf("Could not subscribe %s", req.Topic)
	}

	go p.ListenMessage(consumer, req.Topic, handler)

	return nil
}

func (p *Pulsar) ListenMessage(msgs pulsar.Consumer, topic string, handler func(msg *pubsub.NewMessage) error) {
	for cm := range msgs.Chan() {
		p.HandleMessage(cm, topic, handler)
	}
}

func (p *Pulsar) HandleMessage(m pulsar.ConsumerMessage, topic string, handler func(msg *pubsub.NewMessage) error) {
	err := handler(&pubsub.NewMessage{
		Data:  m.Payload(),
		Topic: topic,
	})
	if err != nil {
		p.logger.Debugf("Could not handle topic %s", topic)
	} else {
		m.Ack(m.Message)
	}
}

func (p *Pulsar) Close() error {
	p.client.Close()

	return nil
}
