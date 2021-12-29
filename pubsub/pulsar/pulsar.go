package pulsar

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	lru "github.com/hashicorp/golang-lru"

	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/kit/logger"
	"github.com/dapr/kit/retry"
)

const (
	host            = "host"
	enableTLS       = "enableTLS"
	deliverAt       = "deliverAt"
	deliverAfter    = "deliverAfter"
	disableBatching = "disableBatching"
	tenant          = "tenant"
	namespace       = "namespace"
	persistent      = "persistent"

	defaultTenant     = "public"
	defaultNamespace  = "default"
	cachedNumProducer = 10
	// topicFormat is the format for pulsar, which have a well-defined structure: {persistent|non-persistent}://tenant/namespace/topic,
	// see https://pulsar.apache.org/docs/en/concepts-messaging/#topics for details.
	topicFormat      = "%s://%s/%s/%s"
	persistentStr    = "persistent"
	nonPersistentStr = "non-persistent"
)

type Pulsar struct {
	logger   logger.Logger
	client   pulsar.Client
	metadata pulsarMetadata

	ctx           context.Context
	cancel        context.CancelFunc
	backOffConfig retry.Config
	cache         *lru.Cache
}

func NewPulsar(l logger.Logger) pubsub.PubSub {
	return &Pulsar{logger: l}
}

func parsePulsarMetadata(meta pubsub.Metadata) (*pulsarMetadata, error) {
	m := pulsarMetadata{Persistent: true, Tenant: defaultTenant, Namespace: defaultNamespace}
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
	// DisableBatching is defaultly batching.
	m.DisableBatching = false
	if val, ok := meta.Properties[disableBatching]; ok {
		disableBatching, err := strconv.ParseBool(val)
		if err != nil {
			return nil, errors.New("pulsar error: invalid value for disableBatching")
		}
		m.DisableBatching = disableBatching
	}

	if val, ok := meta.Properties[persistent]; ok && val != "" {
		per, err := strconv.ParseBool(val)
		if err != nil {
			return nil, errors.New("pulsar error: invalid value for persistent")
		}
		m.Persistent = per
	}
	if val, ok := meta.Properties[tenant]; ok && val != "" {
		m.Tenant = val
	}
	if val, ok := meta.Properties[namespace]; ok && val != "" {
		m.Namespace = val
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

	// initialize lru cache with size 10
	// TODO: make this number configurable in pulsar metadata
	c, err := lru.NewWithEvict(cachedNumProducer, func(k interface{}, v interface{}) {
		producer := v.(pulsar.Producer)
		if producer != nil {
			producer.Close()
		}
	})
	if err != nil {
		return fmt.Errorf("could not initialize pulsar lru cache for publisher")
	}
	p.cache = c
	defer p.cache.Purge()

	p.ctx, p.cancel = context.WithCancel(context.Background())

	p.client = client
	p.metadata = *m

	// Default retry configuration is used if no
	// backOff properties are set.
	if err := retry.DecodeConfigWithPrefix(
		&p.backOffConfig,
		metadata.Properties,
		"backOff"); err != nil {
		return err
	}

	return nil
}

func (p *Pulsar) Publish(req *pubsub.PublishRequest) error {
	var (
		producer pulsar.Producer
		msg      *pulsar.ProducerMessage
		err      error
	)
	topic := p.formatTopic(req.Topic)
	cache, _ := p.cache.Get(topic)
	if cache == nil {
		p.logger.Debugf("creating producer for topic %s, full topic name in pulsar is %s", req.Topic, topic)
		producer, err = p.client.CreateProducer(pulsar.ProducerOptions{
			Topic:           req.Topic,
			DisableBatching: p.metadata.DisableBatching,
		})
		if err != nil {
			return err
		}

		p.cache.Add(topic, producer)
	} else {
		producer = cache.(pulsar.Producer)
	}

	msg, err = parsePublishMetadata(req)
	if err != nil {
		return err
	}
	if _, err = producer.Send(context.Background(), msg); err != nil {
		return err
	}

	return nil
}

// parsePublishMetadata parse publish metadata.
func parsePublishMetadata(req *pubsub.PublishRequest) (
	msg *pulsar.ProducerMessage, err error) {
	msg = &pulsar.ProducerMessage{
		Payload: req.Data,
	}
	if val, ok := req.Metadata[deliverAt]; ok {
		msg.DeliverAt, err = time.Parse(time.RFC3339, val)
		if err != nil {
			return nil, err
		}
	}
	if val, ok := req.Metadata[deliverAfter]; ok {
		msg.DeliverAfter, err = time.ParseDuration(val)
		if err != nil {
			return nil, err
		}
	}

	return
}

func (p *Pulsar) Subscribe(req pubsub.SubscribeRequest, handler pubsub.Handler) error {
	channel := make(chan pulsar.ConsumerMessage, 100)

	topic := p.formatTopic(req.Topic)
	options := pulsar.ConsumerOptions{
		Topic:            topic,
		SubscriptionName: p.metadata.ConsumerID,
		Type:             pulsar.Failover,
		MessageChannel:   channel,
	}

	consumer, err := p.client.Subscribe(options)
	if err != nil {
		p.logger.Debugf("Could not subscribe to %s, full topic name in pulsar is %s", req.Topic, topic)

		return err
	}

	go p.listenMessage(req.Topic, consumer, handler)

	return nil
}

func (p *Pulsar) listenMessage(originTopic string, consumer pulsar.Consumer, handler pubsub.Handler) {
	defer consumer.Close()

	for {
		select {
		case msg := <-consumer.Chan():
			if err := p.handleMessage(originTopic, msg, handler); err != nil && !errors.Is(err, context.Canceled) {
				p.logger.Errorf("Error processing message and retries are exhausted: %s/%#v [key=%s]. Closing consumer.", msg.Topic(), msg.ID(), msg.Key())

				return
			}

		case <-p.ctx.Done():
			// Handle the component being closed
			return
		}
	}
}

func (p *Pulsar) handleMessage(originTopic string, msg pulsar.ConsumerMessage, handler pubsub.Handler) error {
	pubsubMsg := pubsub.NewMessage{
		Data:     msg.Payload(),
		Topic:    originTopic,
		Metadata: msg.Properties(),
	}

	b := p.backOffConfig.NewBackOffWithContext(p.ctx)

	return retry.NotifyRecover(func() error {
		p.logger.Debugf("Processing Pulsar message %s/%#v", msg.Topic(), msg.ID())
		err := handler(p.ctx, &pubsubMsg)
		if err == nil {
			msg.Ack(msg.Message)
		}

		return err
	}, b, func(err error, d time.Duration) {
		p.logger.Errorf("Error processing Pulsar message: %s/%#v [key=%s]. Retrying...", msg.Topic(), msg.ID(), msg.Key())
	}, func() {
		p.logger.Infof("Successfully processed Pulsar message after it previously failed: %s/%#v [key=%s]", msg.Topic(), msg.ID(), msg.Key())
	})
}

func (p *Pulsar) Close() error {
	p.cancel()
	for _, k := range p.cache.Keys() {
		producer, _ := p.cache.Peek(k)
		if producer != nil {
			p.logger.Debugf("closing producer for topic %s", k)
			producer.(pulsar.Producer).Close()
		}
	}
	p.client.Close()

	return nil
}

func (p *Pulsar) Features() []pubsub.Feature {
	return nil
}

// formatTopic formats the topic into pulsar's structure with tenant and namespace.
func (p *Pulsar) formatTopic(topic string) string {
	persist := persistentStr
	if !p.metadata.Persistent {
		persist = nonPersistentStr
	}
	return fmt.Sprintf(topicFormat, persist, p.metadata.Tenant, p.metadata.Namespace, topic)
}
