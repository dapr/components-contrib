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
	"encoding/pem"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/apache/pulsar-client-go/pulsar/crypto"
	"github.com/hamba/avro/v2"
	lru "github.com/hashicorp/golang-lru/v2"

	"github.com/dapr/components-contrib/common/authentication/oauth2"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/kit/logger"
	kitmd "github.com/dapr/kit/metadata"
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
	protoProtocol           = "proto"
	partitionKey            = "partitionKey"

	defaultTenant     = "public"
	defaultNamespace  = "default"
	cachedNumProducer = 10
	pulsarPrefix      = "pulsar://"
	pulsarToken       = "token"
	// topicFormat is the format for pulsar, which have a well-defined structure: {persistent|non-persistent}://tenant/namespace/topic,
	// see https://pulsar.apache.org/docs/en/concepts-messaging/#topics for details.
	topicFormat                = "%s://%s/%s/%s"
	persistentStr              = "persistent"
	nonPersistentStr           = "non-persistent"
	topicJSONSchemaIdentifier  = ".jsonschema"
	topicAvroSchemaIdentifier  = ".avroschema"
	topicProtoSchemaIdentifier = ".protoschema"

	// defaultBatchingMaxPublishDelay init default for maximum delay to batch messages.
	defaultBatchingMaxPublishDelay = 10 * time.Millisecond
	// defaultMaxMessages init default num of entries in per batch.
	defaultMaxMessages = 1000
	// defaultMaxBatchSize init default for maximum number of bytes per batch.
	defaultMaxBatchSize = 128 * 1024
	// defaultRedeliveryDelay init default for redelivery delay.
	defaultRedeliveryDelay = 30 * time.Second
	// defaultConcurrency controls the number of concurrent messages sent to the app.
	defaultConcurrency = 100
	// defaultReceiverQueueSize controls the number of messages the pulsar sdk pulls before dapr explicitly consumes the messages.
	defaultReceiverQueueSize = 1000

	subscribeTypeKey = "subscribeType"

	subscribeTypeExclusive = "exclusive"
	subscribeTypeShared    = "shared"
	subscribeTypeFailover  = "failover"
	subscribeTypeKeyShared = "key_shared"

	processModeKey = "processMode"

	processModeAsync = "async"
	processModeSync  = "sync"

	subscribeInitialPosition = "subscribeInitialPosition"

	subscribePositionEarliest = "earliest"
	subscribePositionLatest   = "latest"

	subscribeMode = "subscribeMode"

	subscribeModeDurable    = "durable"
	subscribeModeNonDurable = "non_durable"

	compressionTypeKey  = "compressionType"
	compressionLevelKey = "compressionLevel"

	compressionTypeNone = "none"
	compressionTypeLZ4  = "lz4"
	compressionTypeZLib = "zlib"
	compressionTypeZSTD = "zstd"

	compressionLevelDefault = "default"
	compressionLevelFaster  = "faster"
	compressionLevelBetter  = "better"
)

type ProcessMode string

type Pulsar struct {
	logger      logger.Logger
	client      pulsar.Client
	metadata    pulsarMetadata
	cache       *lru.Cache[string, pulsar.Producer]
	closed      atomic.Bool
	closeCh     chan struct{}
	wg          sync.WaitGroup
	newClientFn pulsarClientFactory
}

type pulsarClientFactory func(pulsar.ClientOptions) (pulsar.Client, error)

func NewPulsar(l logger.Logger) pubsub.PubSub {
	return &Pulsar{
		logger:      l,
		closeCh:     make(chan struct{}),
		newClientFn: pulsar.NewClient,
	}
}

func parsePulsarMetadata(meta pubsub.Metadata) (*pulsarMetadata, error) {
	m := pulsarMetadata{
		Persistent:              true,
		Tenant:                  defaultTenant,
		Namespace:               defaultNamespace,
		internalTopicSchemas:    map[string]schemaMetadata{},
		DisableBatching:         false,
		BatchingMaxPublishDelay: defaultBatchingMaxPublishDelay,
		BatchingMaxMessages:     defaultMaxMessages,
		BatchingMaxSize:         defaultMaxBatchSize,
		RedeliveryDelay:         defaultRedeliveryDelay,
		MaxConcurrentHandlers:   defaultConcurrency,
		ReceiverQueueSize:       defaultReceiverQueueSize,
	}

	if err := kitmd.DecodeMetadata(meta.Properties, &m); err != nil {
		return nil, err
	}

	if m.Host == "" {
		return nil, errors.New("pulsar error: missing pulsar host")
	}

	var err error
	m.SubscriptionType, err = parseSubscriptionType(meta.Properties[subscribeTypeKey])
	if err != nil {
		return nil, errors.New("invalid subscription type. Accepted values are `exclusive`, `shared`, `failover` and `key_shared`")
	}

	m.SubscriptionInitialPosition, err = parseSubscriptionPosition(meta.Properties[subscribeInitialPosition])
	if err != nil {
		return nil, errors.New("invalid subscription initial position. Accepted values are `latest` and `earliest`")
	}

	m.SubscriptionMode, err = parseSubscriptionMode(meta.Properties[subscribeMode])
	if err != nil {
		return nil, errors.New("invalid subscription mode")
	}

	m.CompressionType, err = parseCompressionType(meta.Properties[compressionTypeKey])
	if err != nil {
		return nil, errors.New("invalid compression type. Accepted values are `none`, `lz4`, `zlib` and `zstd`")
	}

	m.CompressionLevel, err = parseCompressionLevel(meta.Properties[compressionLevelKey])
	if err != nil {
		return nil, errors.New("invalid compression level. Accepted values are `default`, `faster` and `better`")
	}

	for k, v := range meta.Properties {
		switch {
		case strings.HasSuffix(k, topicJSONSchemaIdentifier):
			topic := k[:len(k)-len(topicJSONSchemaIdentifier)]
			m.internalTopicSchemas[topic] = schemaMetadata{
				protocol: jsonProtocol,
				value:    v,
			}
		case strings.HasSuffix(k, topicAvroSchemaIdentifier):
			topic := k[:len(k)-len(topicAvroSchemaIdentifier)]
			m.internalTopicSchemas[topic] = schemaMetadata{
				protocol: avroProtocol,
				value:    v,
			}
		case strings.HasSuffix(k, topicProtoSchemaIdentifier):
			topic := k[:len(k)-len(topicProtoSchemaIdentifier)]
			m.internalTopicSchemas[topic] = schemaMetadata{
				protocol: protoProtocol,
				value:    v,
			}
		}
	}

	// Resolve credentials from file if ClientSecretPath is set
	if err := m.ClientCredentialsMetadata.ResolveCredentials(); err != nil {
		return nil, err
	}

	return &m, nil
}

func (p *Pulsar) Init(ctx context.Context, metadata pubsub.Metadata) error {
	m, err := parsePulsarMetadata(metadata)
	if err != nil {
		return err
	}
	pulsarURL := m.Host

	pulsarURL = sanitiseURL(pulsarURL)
	options := pulsar.ClientOptions{
		URL:                        pulsarURL,
		OperationTimeout:           30 * time.Second,
		ConnectionTimeout:          30 * time.Second,
		TLSAllowInsecureConnection: !m.EnableTLS,
	}

	switch {
	case len(m.Token) > 0:
		options.Authentication = pulsar.NewAuthenticationToken(m.Token)
	case len(m.ClientCredentialsMetadata.TokenURL) > 0:
		credsOpts := m.ClientCredentialsMetadata.ToOptions(p.logger)
		var cliCreds *oauth2.ClientCredentials
		cliCreds, err = oauth2.NewClientCredentials(ctx, credsOpts)
		if err != nil {
			return fmt.Errorf("could not instantiate oauth2 token provider: %w", err)
		}
		options.Authentication = pulsar.NewAuthenticationTokenFromSupplier(cliCreds.Token)
	}

	client, err := p.newClientFn(options)
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
		return errors.New("could not initialize pulsar lru cache for publisher")
	}
	p.cache = c
	defer p.cache.Purge()

	p.client = client
	p.metadata = *m

	return nil
}

func sanitiseURL(pulsarURL string) string {
	prefixes := []string{"pulsar+ssl://", "pulsar://", "http://", "https://"}

	hasPrefix := false
	for _, prefix := range prefixes {
		if strings.HasPrefix(pulsarURL, prefix) {
			hasPrefix = true
			break
		}
	}

	if !hasPrefix {
		pulsarURL = fmt.Sprintf("%s%s", pulsarPrefix, pulsarURL)
	}
	return pulsarURL
}

func (p *Pulsar) useProducerEncryption() bool {
	return p.metadata.PublicKey != "" && p.metadata.Keys != ""
}

func (p *Pulsar) useConsumerEncryption() bool {
	return p.metadata.PublicKey != "" && p.metadata.PrivateKey != ""
}

func (p *Pulsar) Publish(ctx context.Context, req *pubsub.PublishRequest) error {
	if p.closed.Load() {
		return errors.New("component is closed")
	}

	var (
		msg *pulsar.ProducerMessage
		err error
	)
	topic := p.formatTopic(req.Topic)
	producer, ok := p.cache.Get(topic)

	sm, hasSchema := p.metadata.internalTopicSchemas[req.Topic]

	if !ok || producer == nil {
		p.logger.Debugf("creating producer for topic %s, full topic name in pulsar is %s", req.Topic, topic)
		opts := pulsar.ProducerOptions{
			Topic:                   topic,
			DisableBatching:         p.metadata.DisableBatching,
			BatchingMaxPublishDelay: p.metadata.BatchingMaxPublishDelay,
			BatchingMaxMessages:     p.metadata.BatchingMaxMessages,
			BatchingMaxSize:         p.metadata.BatchingMaxSize,
			CompressionType:         getCompressionType(p.metadata.CompressionType),
			CompressionLevel:        getCompressionLevel(p.metadata.CompressionLevel),
		}

		if hasSchema {
			opts.Schema = getPulsarSchema(sm)
		}

		if p.useProducerEncryption() {
			var reader crypto.KeyReader
			if isValidPEM(p.metadata.PublicKey) {
				reader = NewDataKeyReader(p.metadata.PublicKey, "")
			} else {
				reader = crypto.NewFileKeyReader(p.metadata.PublicKey, "")
			}

			opts.Encryption = &pulsar.ProducerEncryptionInfo{
				KeyReader: reader,
				Keys:      strings.Split(p.metadata.Keys, ","),
			}
		}

		producer, err = p.client.CreateProducer(opts)
		if err != nil {
			return err
		}

		p.cache.Add(topic, producer)
	}

	msg, err = parsePublishMetadata(req, sm)
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
	case protoProtocol:
		return pulsar.NewProtoSchema(metadata.value, nil)
	default:
		return nil
	}
}

// parsePublishMetadata parse publish metadata.
func parsePublishMetadata(req *pubsub.PublishRequest, schema schemaMetadata) (
	msg *pulsar.ProducerMessage, err error,
) {
	msg = &pulsar.ProducerMessage{}

	switch schema.protocol {
	case "":
		msg.Payload = req.Data
	case jsonProtocol:
		var obj interface{}
		err = json.Unmarshal(req.Data, &obj)
		if err != nil {
			return nil, err
		}

		msg.Value = obj
	case avroProtocol:
		var obj interface{}
		avroSchema, parseErr := avro.Parse(schema.value)
		if parseErr != nil {
			return nil, parseErr
		}

		err = avro.Unmarshal(avroSchema, req.Data, &obj)
		if err != nil {
			return nil, err
		}

		msg.Value = obj
	}

	for name, value := range req.Metadata {
		if value == "" {
			continue
		}

		switch name {
		case partitionKey:
			msg.Key = value
		case deliverAt:
			msg.DeliverAt, err = time.Parse(time.RFC3339, value)
			if err != nil {
				return nil, err
			}
		case deliverAfter:
			msg.DeliverAfter, err = time.ParseDuration(value)
			if err != nil {
				return nil, err
			}
		default:
			if msg.Properties == nil {
				msg.Properties = make(map[string]string)
			}
			msg.Properties[name] = value
		}
	}

	return msg, nil
}

func parseSubscriptionType(in string) (string, error) {
	subsType := strings.ToLower(in)
	switch subsType {
	case subscribeTypeExclusive, subscribeTypeFailover, subscribeTypeShared, subscribeTypeKeyShared:
		return subsType, nil
	case "":
		return subscribeTypeShared, nil
	default:
		return "", fmt.Errorf("invalid subscription type: %s", subsType)
	}
}

// getSubscribeType doesn't do extra validations, because they were done in parseSubscriptionType.
func getSubscribeType(subsTypeStr string) pulsar.SubscriptionType {
	var subsType pulsar.SubscriptionType

	switch subsTypeStr {
	case subscribeTypeExclusive:
		subsType = pulsar.Exclusive
	case subscribeTypeFailover:
		subsType = pulsar.Failover
	case subscribeTypeShared:
		subsType = pulsar.Shared
	case subscribeTypeKeyShared:
		subsType = pulsar.KeyShared
	}

	return subsType
}

func parseSubscriptionPosition(in string) (string, error) {
	subsPosition := strings.ToLower(in)
	switch subsPosition {
	case subscribePositionEarliest, subscribePositionLatest:
		return subsPosition, nil
	case "":
		return subscribePositionLatest, nil
	default:
		return "", fmt.Errorf("invalid subscription initial position: %s", subsPosition)
	}
}

func getSubscribePosition(subsPositionStr string) pulsar.SubscriptionInitialPosition {
	var subsPosition pulsar.SubscriptionInitialPosition

	switch subsPositionStr {
	case subscribePositionEarliest:
		subsPosition = pulsar.SubscriptionPositionEarliest
	case subscribePositionLatest:
		subsPosition = pulsar.SubscriptionPositionLatest
	}
	return subsPosition
}

func parseSubscriptionMode(in string) (string, error) {
	subsMode := strings.ToLower(in)
	switch subsMode {
	case subscribeModeDurable, subscribeModeNonDurable:
		return subsMode, nil
	case "":
		return subscribeModeDurable, nil
	default:
		return "", fmt.Errorf("invalid subscription mode: %s", subsMode)
	}
}

func getSubscriptionMode(subsModeStr string) pulsar.SubscriptionMode {
	switch subsModeStr {
	case subscribeModeNonDurable:
		return pulsar.NonDurable
	default:
		return pulsar.Durable
	}
}

func parseCompressionType(in string) (string, error) {
	compType := strings.ToLower(in)
	switch compType {
	case compressionTypeNone, compressionTypeLZ4, compressionTypeZLib, compressionTypeZSTD:
		return compType, nil
	case "":
		return compressionTypeNone, nil
	default:
		return "", fmt.Errorf("invalid compression type: %s", compType)
	}
}

func getCompressionType(compTypeStr string) pulsar.CompressionType {
	switch compTypeStr {
	case compressionTypeLZ4:
		return pulsar.LZ4
	case compressionTypeZLib:
		return pulsar.ZLib
	case compressionTypeZSTD:
		return pulsar.ZSTD
	default:
		return pulsar.NoCompression
	}
}

func parseCompressionLevel(in string) (string, error) {
	compLevel := strings.ToLower(in)
	switch compLevel {
	case compressionLevelDefault, compressionLevelFaster, compressionLevelBetter:
		return compLevel, nil
	case "":
		return compressionLevelDefault, nil
	default:
		return "", fmt.Errorf("invalid compression level: %s", compLevel)
	}
}

func getCompressionLevel(compLevelStr string) pulsar.CompressionLevel {
	switch compLevelStr {
	case compressionLevelFaster:
		return pulsar.Faster
	case compressionLevelBetter:
		return pulsar.Better
	default:
		return pulsar.Default
	}
}

func (p *Pulsar) Subscribe(ctx context.Context, req pubsub.SubscribeRequest, handler pubsub.Handler) error {
	if p.closed.Load() {
		return errors.New("component is closed")
	}

	channel := make(chan pulsar.ConsumerMessage, p.metadata.MaxConcurrentHandlers)

	topic := p.formatTopic(req.Topic)

	subscribeType := p.metadata.SubscriptionType
	if s, exists := req.Metadata[subscribeTypeKey]; exists {
		subscribeType = s
	}

	options := pulsar.ConsumerOptions{
		Topic:                       topic,
		SubscriptionName:            p.metadata.ConsumerID,
		Type:                        getSubscribeType(subscribeType),
		SubscriptionInitialPosition: getSubscribePosition(subscribeInitialPosition),
		SubscriptionMode:            getSubscriptionMode(subscribeMode),
		MessageChannel:              channel,
		NackRedeliveryDelay:         p.metadata.RedeliveryDelay,
		ReceiverQueueSize:           p.metadata.ReceiverQueueSize,
		ReplicateSubscriptionState:  p.metadata.ReplicateSubscriptionState,
	}

	// Handle KeySharedPolicy for key_shared subscription type
	if options.Type == pulsar.KeyShared {
		options.KeySharedPolicy = &pulsar.KeySharedPolicy{
			Mode: pulsar.KeySharedPolicyModeAutoSplit,
		}
	}

	if p.useConsumerEncryption() {
		var reader crypto.KeyReader
		if isValidPEM(p.metadata.PublicKey) {
			reader = NewDataKeyReader(p.metadata.PublicKey, p.metadata.PrivateKey)
		} else {
			reader = crypto.NewFileKeyReader(p.metadata.PublicKey, p.metadata.PrivateKey)
		}

		options.Decryption = &pulsar.MessageDecryptionInfo{
			KeyReader: reader,
		}
	}

	if sm, ok := p.metadata.internalTopicSchemas[req.Topic]; ok {
		options.Schema = getPulsarSchema(sm)
	}
	consumer, err := p.client.Subscribe(options)
	if err != nil {
		p.logger.Debugf("Could not subscribe to %s, full topic name in pulsar is %s", req.Topic, topic)
		return err
	}
	p.logger.Debugf("Subscribed to '%s'(%s) with type '%s'", req.Topic, topic, subscribeType)

	p.wg.Add(2)
	listenCtx, cancel := context.WithCancel(ctx)
	go func() {
		defer p.wg.Done()
		defer cancel()
		select {
		case <-listenCtx.Done():
		case <-p.closeCh:
		}
	}()
	go func() {
		defer p.wg.Done()
		defer cancel()
		p.listenMessage(listenCtx, req, consumer, handler)
	}()

	return nil
}

func (p *Pulsar) listenMessage(ctx context.Context, req pubsub.SubscribeRequest, consumer pulsar.Consumer, handler pubsub.Handler) {
	defer consumer.Close()

	originTopic := req.Topic
	var err error
	for {
		select {
		case msg := <-consumer.Chan():
			if strings.ToLower(req.Metadata[processModeKey]) == processModeSync { //nolint:gocritic
				err = p.handleMessage(ctx, originTopic, msg, handler)
				if err != nil && !errors.Is(err, context.Canceled) {
					p.logger.Errorf("Error sync processing message: %s/%#v [key=%s]: %v", msg.Topic(), msg.ID(), msg.Key(), err)
				}
			} else { // async process mode by default
				// Go routine to handle multiple messages at once.
				p.wg.Add(1)
				go func(msg pulsar.ConsumerMessage) {
					defer p.wg.Done()
					err = p.handleMessage(ctx, originTopic, msg, handler)
					if err != nil && !errors.Is(err, context.Canceled) {
						p.logger.Errorf("Error async processing message: %s/%#v [key=%s]: %v", msg.Topic(), msg.ID(), msg.Key(), err)
					}
				}(msg)
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
	defer p.wg.Wait()
	if p.closed.CompareAndSwap(false, true) {
		close(p.closeCh)
	}

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

// GetComponentMetadata returns the metadata of the component.
func (p *Pulsar) GetComponentMetadata() (metadataInfo metadata.MetadataMap) {
	metadataStruct := pulsarMetadata{}
	metadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, metadata.PubSubType)
	return
}

// isValidPEM validates the provided input has PEM formatted block.
func isValidPEM(val string) bool {
	block, _ := pem.Decode([]byte(val))
	return block != nil
}
