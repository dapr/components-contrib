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

package pulsar

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	lru "github.com/hashicorp/golang-lru/v2"

	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/kit/logger"
)

const (
	host                    = "host"
	consumerID              = "consumerID"
	enableTLS               = "enableTLS"
	deliverAt               = "deliverAt"
	deliverAfter            = "deliverAfter"
	disableBatching         = "disableBatching"
	batchingMaxPublishDelay = "batchingMaxPublishDelay"
	batchingMaxSize         = "batchingMaxSize"
	batchingMaxMessages     = "batchingMaxMessages"
	tenant                  = "tenant"
	namespace               = "namespace"
	persistent              = "persistent"
	redeliveryDelay         = "redeliveryDelay"
	avroProtocol            = "avro"
	jsonProtocol            = "json"

	defaultTenant     = "public"
	defaultNamespace  = "default"
	cachedNumProducer = 10
	pulsarPrefix      = "pulsar://"
	pulsarToken       = "token"
	// topicFormat is the format for pulsar, which have a well-defined structure: {persistent|non-persistent}://tenant/namespace/topic,
	// see https://pulsar.apache.org/docs/en/concepts-messaging/#topics for details.
	topicFormat               = "%s://%s/%s/%s"
	persistentStr             = "persistent"
	nonPersistentStr          = "non-persistent"
	topicJsonSchemaIdentifier = ".jsonschema"
	topicAvroSchemaIdentifier = ".avroschema"

	// defaultBatchingMaxPublishDelay init default for maximum delay to batch messages.
	defaultBatchingMaxPublishDelay = 10 * time.Millisecond
	// defaultMaxMessages init default num of entries in per batch.
	defaultMaxMessages = 1000
	// defaultMaxBatchSize init default for maximum number of bytes per batch.
	defaultMaxBatchSize = 128 * 1024
	// defaultRedeliveryDelay init default for redelivery delay.
	defaultRedeliveryDelay = 30 * time.Second
)

type Pulsar struct {
	logger   logger.Logger
	client   pulsar.Client
	metadata pulsarMetadata
	cache    *lru.Cache[string, pulsar.Producer]
}

func NewPulsar(l logger.Logger) pubsub.PubSub {
	return &Pulsar{logger: l}
}

func parsePulsarMetadata(meta pubsub.Metadata) (*pulsarMetadata, error) {
	m := pulsarMetadata{Persistent: true, Tenant: defaultTenant, Namespace: defaultNamespace, topicSchemas: map[string]schemaMetadata{}}
	m.ConsumerID = meta.Properties[consumerID]

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
	m.BatchingMaxPublishDelay = defaultBatchingMaxPublishDelay
	if val, ok := meta.Properties[batchingMaxPublishDelay]; ok {
		batchingMaxPublishDelay, err := formatDuration(val)
		if err != nil {
			return nil, errors.New("pulsar error: invalid value for batchingMaxPublishDelay")
		}
		m.BatchingMaxPublishDelay = batchingMaxPublishDelay
	}
	m.BatchingMaxMessages = defaultMaxMessages
	if val, ok := meta.Properties[batchingMaxMessages]; ok {
		batchingMaxMessages, err := strconv.ParseUint(val, 10, 64)
		if err != nil {
			return nil, errors.New("pulsar error: invalid value for batchingMaxMessages")
		}
		m.BatchingMaxMessages = uint(batchingMaxMessages)
	}
	m.BatchingMaxSize = defaultMaxBatchSize
	if val, ok := meta.Properties[batchingMaxSize]; ok {
		batchingMaxSize, err := strconv.ParseUint(val, 10, 64)
		if err != nil {
			return nil, errors.New("pulsar error: invalid value for batchingMaxSize")
		}
		m.BatchingMaxSize = uint(batchingMaxSize)
	}
	m.RedeliveryDelay = defaultRedeliveryDelay
	if val, ok := meta.Properties[redeliveryDelay]; ok {
		redeliveryDelay, err := formatDuration(val)
		if err != nil {
			return nil, errors.New("pulsar error: invalid value for redeliveryDelay")
		}
		m.RedeliveryDelay = redeliveryDelay
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
	if val, ok := meta.Properties[pulsarToken]; ok && val != "" {
		m.Token = val
	}

	for k, v := range meta.Properties {
		if strings.LastIndex(k, topicJsonSchemaIdentifier) > 0 {
			topic := k[:strings.LastIndex(k, topicJsonSchemaIdentifier)]
			m.topicSchemas[topic] = schemaMetadata{
				protocol: jsonProtocol,
				value:    v,
			}
		} else if strings.LastIndex(k, topicAvroSchemaIdentifier) > 0 {
			topic := k[:strings.LastIndex(k, topicAvroSchemaIdentifier)]
			m.topicSchemas[topic] = schemaMetadata{
				protocol: avroProtocol,
				value:    v,
			}
		}
	}

	return &m, nil
}

func (p *Pulsar) Init(metadata pubsub.Metadata) error {
	m, err := parsePulsarMetadata(metadata)
	if err != nil {
		return err
	}
	pulsarURL := m.Host
	if !strings.HasPrefix(m.Host, "http://") &&
		!strings.HasPrefix(m.Host, "https://") {
		pulsarURL = fmt.Sprintf("%s%s", pulsarPrefix, m.Host)
	}
	options := pulsar.ClientOptions{
		URL:                        pulsarURL,
		OperationTimeout:           30 * time.Second,
		ConnectionTimeout:          30 * time.Second,
		TLSAllowInsecureConnection: !m.EnableTLS,
	}
	if m.Token != "" {
		options.Authentication = pulsar.NewAuthenticationToken(m.Token)
	}
	client, err := pulsar.NewClient(options)
	if err != nil {
		return fmt.Errorf("could not instantiate pulsar client: %v", err)
	}

	// initialize lru cache with size 10
	// TODO: make this number configurable in pulsar metadata
	c, err := lru.NewWithEvict(cachedNumProducer, func(k string, v pulsar.Producer) {
		if v != nil {
			v.Close()
		}
	})
	if err != nil {
		return fmt.Errorf("could not initialize pulsar lru cache for publisher")
	}
	p.cache = c
	defer p.cache.Purge()

	p.client = client
	p.metadata = *m

	return nil
}

func (p *Pulsar) Publish(ctx context.Context, req *pubsub.PublishRequest) error {
	var (
		msg *pulsar.ProducerMessage
		err error
	)
	topic := p.formatTopic(req.Topic)
	producer, ok := p.cache.Get(topic)

	schemaMetadata, hasSchema := p.metadata.topicSchemas[req.Topic]

	if !ok || producer == nil {
		p.logger.Debugf("creating producer for topic %s, full topic name in pulsar is %s", req.Topic, topic)
		opts := pulsar.ProducerOptions{
			Topic:                   topic,
			DisableBatching:         p.metadata.DisableBatching,
			BatchingMaxPublishDelay: p.metadata.BatchingMaxPublishDelay,
			BatchingMaxMessages:     p.metadata.BatchingMaxMessages,
			BatchingMaxSize:         p.metadata.BatchingMaxSize,
		}

		if hasSchema {
			opts.Schema = getPulsarSchema(schemaMetadata)
		}

		producer, err = p.client.CreateProducer(opts)
		if err != nil {
			return err
		}

		p.cache.Add(topic, producer)
	}

	msg, err = parsePublishMetadata(req, hasSchema)
	if err != nil {
		return err
	}
	if _, err = producer.Send(ctx, msg); err != nil {
		return err
	}

	return nil
}

func getPulsarSchema(metadata schemaMetadata) pulsar.Schema {
	switch metadata.protocol {
	case jsonProtocol:
		return pulsar.NewJSONSchema(metadata.value, nil)
	case avroProtocol:
		return pulsar.NewAvroSchema(metadata.value, nil)
	default:
		return nil
	}
}

// parsePublishMetadata parse publish metadata.
func parsePublishMetadata(req *pubsub.PublishRequest, enforceSchema bool) (
	msg *pulsar.ProducerMessage, err error,
) {
	msg = &pulsar.ProducerMessage{}

	if !enforceSchema {
		msg.Payload = req.Data
	} else {
		var obj interface{}
		err := json.Unmarshal(req.Data, &obj)

		if err != nil {
			return nil, err
		}

		msg.Value = obj
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

func (p *Pulsar) Subscribe(ctx context.Context, req pubsub.SubscribeRequest, handler pubsub.Handler) error {
	channel := make(chan pulsar.ConsumerMessage, 100)

	topic := p.formatTopic(req.Topic)

	options := pulsar.ConsumerOptions{
		Topic:               topic,
		SubscriptionName:    p.metadata.ConsumerID,
		Type:                pulsar.Shared,
		MessageChannel:      channel,
		NackRedeliveryDelay: p.metadata.RedeliveryDelay,
	}

	if schemaMetadata, ok := p.metadata.topicSchemas[req.Topic]; ok {
		options.Schema = getPulsarSchema(schemaMetadata)
	}
	consumer, err := p.client.Subscribe(options)
	if err != nil {
		p.logger.Debugf("Could not subscribe to %s, full topic name in pulsar is %s", req.Topic, topic)
		return err
	}

	go p.listenMessage(ctx, req.Topic, consumer, handler)

	return nil
}

func (p *Pulsar) listenMessage(ctx context.Context, originTopic string, consumer pulsar.Consumer, handler pubsub.Handler) {
	defer consumer.Close()

	var err error
	for {
		select {
		case msg := <-consumer.Chan():
			err = p.handleMessage(ctx, originTopic, msg, handler)
			if err != nil && !errors.Is(err, context.Canceled) {
				p.logger.Errorf("Error processing message: %s/%#v [key=%s]: %v", msg.Topic(), msg.ID(), msg.Key(), err)
			}

		case <-ctx.Done():
			p.logger.Errorf("Subscription context done. Closing consumer. Err: %s", ctx.Err())
			return
		}
	}
}

func (p *Pulsar) handleMessage(ctx context.Context, originTopic string, msg pulsar.ConsumerMessage, handler pubsub.Handler) error {
	pubsubMsg := pubsub.NewMessage{
		Data:     msg.Payload(),
		Topic:    originTopic,
		Metadata: msg.Properties(),
	}

	p.logger.Debugf("Processing Pulsar message %s/%#v", msg.Topic(), msg.ID())
	err := handler(ctx, &pubsubMsg)
	if err != nil {
		msg.Nack(msg.Message)
		return err
	}

	msg.Ack(msg.Message)
	return nil
}

func (p *Pulsar) Close() error {
	for _, k := range p.cache.Keys() {
		producer, _ := p.cache.Peek(k)
		if producer != nil {
			p.logger.Debugf("closing producer for topic %s", k)
			producer.Close()
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

func formatDuration(durationString string) (time.Duration, error) {
	if val, err := strconv.Atoi(durationString); err == nil {
		return time.Duration(val) * time.Millisecond, nil
	}

	// Convert it by parsing
	d, err := time.ParseDuration(durationString)

	return d, err
}
