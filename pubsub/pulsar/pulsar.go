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
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"math"
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
		avroSchema, parseErr := avro.Parse(schema.value)
		if parseErr != nil {
			return nil, fmt.Errorf("failed to parse avro schema: %w", parseErr)
		}

		var obj interface{}
		if err = json.Unmarshal(req.Data, &obj); err != nil {
			return nil, fmt.Errorf("failed to unmarshal JSON payload: %w", err)
		}

		if err = validateJSONAgainstAvroSchema(obj, avroSchema); err != nil {
			return nil, fmt.Errorf("avro schema validation failed: %w", err)
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

// validateJSONAgainstAvroSchema validates a parsed JSON value against an Avro schema.
func validateJSONAgainstAvroSchema(value interface{}, schema avro.Schema) error {
	switch s := schema.(type) {
	case *avro.RecordSchema:
		return validateRecord(value, s)
	case *avro.ArraySchema:
		return validateArray(value, s)
	case *avro.MapSchema:
		return validateMap(value, s)
	case *avro.UnionSchema:
		return validateUnion(value, s)
	case *avro.PrimitiveSchema:
		return validatePrimitive(value, s)
	case *avro.EnumSchema:
		return validateEnum(value, s)
	case *avro.FixedSchema:
		return validateFixed(value, s)
	case *avro.RefSchema:
		return validateJSONAgainstAvroSchema(value, s.Schema())
	case *avro.NullSchema:
		if value != nil {
			return fmt.Errorf("expected null, got %T", value)
		}
		return nil
	default:
		return fmt.Errorf("unsupported avro schema type: %T", schema)
	}
}

func validateRecord(value interface{}, schema *avro.RecordSchema) error {
	obj, ok := value.(map[string]interface{})
	if !ok {
		return fmt.Errorf("expected object for record %q, got %T", schema.Name(), value)
	}

	schemaFields := make(map[string]struct{}, len(schema.Fields()))
	for _, field := range schema.Fields() {
		schemaFields[field.Name()] = struct{}{}
		fieldVal, exists := obj[field.Name()]
		if !exists {
			if !field.HasDefault() {
				return fmt.Errorf("missing required field %q", field.Name())
			}
			continue
		}
		if err := validateJSONAgainstAvroSchema(fieldVal, field.Type()); err != nil {
			return fmt.Errorf("field %q: %w", field.Name(), err)
		}
	}

	// Check for extra fields not defined in the schema.
	for key := range obj {
		if _, exists := schemaFields[key]; !exists {
			return fmt.Errorf("unknown field %q is not defined in the schema for record %q", key, schema.Name())
		}
	}

	return nil
}

func validateArray(value interface{}, schema *avro.ArraySchema) error {
	arr, ok := value.([]interface{})
	if !ok {
		return fmt.Errorf("expected array, got %T", value)
	}
	for i, item := range arr {
		if err := validateJSONAgainstAvroSchema(item, schema.Items()); err != nil {
			return fmt.Errorf("array element [%d]: %w", i, err)
		}
	}
	return nil
}

func validateMap(value interface{}, schema *avro.MapSchema) error {
	obj, ok := value.(map[string]interface{})
	if !ok {
		return fmt.Errorf("expected object for map type, got %T", value)
	}
	for k, v := range obj {
		if err := validateJSONAgainstAvroSchema(v, schema.Values()); err != nil {
			return fmt.Errorf("map key %q: %w", k, err)
		}
	}
	return nil
}

func validateUnion(value interface{}, schema *avro.UnionSchema) error {
	// null is valid in a union if one of the types is null
	if value == nil {
		for _, t := range schema.Types() {
			if t.Type() == avro.Null {
				return nil
			}
		}
		return errors.New("null is not allowed by the union type")
	}

	// Try each type in the union
	var errs []string
	for _, t := range schema.Types() {
		if t.Type() == avro.Null {
			continue
		}
		if err := validateJSONAgainstAvroSchema(value, t); err == nil {
			return nil
		} else {
			errs = append(errs, err.Error())
		}
	}

	return fmt.Errorf("value does not match any type in the union: [%s]", strings.Join(errs, "; "))
}

func validatePrimitive(value interface{}, schema *avro.PrimitiveSchema) error {
	if value == nil {
		return fmt.Errorf("expected %s, got null", schema.Type())
	}

	switch schema.Type() {
	case avro.Boolean:
		if _, ok := value.(bool); !ok {
			return fmt.Errorf("expected boolean, got %T", value)
		}
	case avro.Int:
		// JSON numbers are float64 when unmarshalled via encoding/json
		f, ok := value.(float64)
		if !ok {
			return fmt.Errorf("expected integer, got %T", value)
		}
		if f != float64(int64(f)) {
			return fmt.Errorf("expected integer, got floating-point number %v", f)
		}
		// Avro int is 32-bit signed
		if f < math.MinInt32 || f > math.MaxInt32 {
			return fmt.Errorf("value %v overflows avro int (must be between %d and %d)", f, math.MinInt32, math.MaxInt32)
		}
	case avro.Long:
		// JSON numbers are float64 when unmarshalled via encoding/json
		f, ok := value.(float64)
		if !ok {
			return fmt.Errorf("expected integer, got %T", value)
		}
		if f != float64(int64(f)) {
			return fmt.Errorf("expected integer, got floating-point number %v", f)
		}
		// Avro long is 64-bit signed. Note: float64 loses precision above 2^53,
		// but the integer check above (f == float64(int64(f))) already guards
		// against values that cannot be exactly represented.
		if f < math.MinInt64 || f > math.MaxInt64 {
			return fmt.Errorf("value %v overflows avro long (must be between %d and %d)", f, int64(math.MinInt64), int64(math.MaxInt64))
		}
	case avro.Float:
		f, ok := value.(float64)
		if !ok {
			return fmt.Errorf("expected number, got %T", value)
		}
		if f > math.MaxFloat32 || f < -math.MaxFloat32 {
			return fmt.Errorf("value %v overflows avro float (32-bit)", f)
		}
	case avro.Double:
		if _, ok := value.(float64); !ok {
			return fmt.Errorf("expected number, got %T", value)
		}
	case avro.String:
		if _, ok := value.(string); !ok {
			return fmt.Errorf("expected string, got %T", value)
		}
	case avro.Bytes:
		if _, ok := value.(string); !ok {
			return fmt.Errorf("expected string for bytes, got %T", value)
		}
	}

	return nil
}

func validateEnum(value interface{}, schema *avro.EnumSchema) error {
	str, ok := value.(string)
	if !ok {
		return fmt.Errorf("expected string for enum %q, got %T", schema.Name(), value)
	}
	for _, symbol := range schema.Symbols() {
		if str == symbol {
			return nil
		}
	}
	return fmt.Errorf("value %q is not a valid symbol for enum %q (valid: %v)", str, schema.Name(), schema.Symbols())
}

func validateFixed(value interface{}, schema *avro.FixedSchema) error {
	str, ok := value.(string)
	if !ok {
		return fmt.Errorf("expected string for fixed %q, got %T", schema.Name(), value)
	}
	// Avro fixed values in JSON may be represented as raw strings (byte length
	// must match schema size) or as base64-encoded strings. We try base64 first
	// to avoid false positives where the encoded string length happens to equal
	// the schema size but the decoded byte length does not.
	if decoded, err := base64.StdEncoding.DecodeString(str); err == nil {
		if len(decoded) != schema.Size() {
			return fmt.Errorf("fixed %q expects size %d, got %d decoded bytes", schema.Name(), schema.Size(), len(decoded))
		}
		return nil
	}
	if len(str) != schema.Size() {
		return fmt.Errorf("fixed %q expects size %d, got %d bytes", schema.Name(), schema.Size(), len(str))
	}
	return nil
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
		SubscriptionInitialPosition: getSubscribePosition(p.metadata.SubscriptionInitialPosition),
		SubscriptionMode:            getSubscriptionMode(p.metadata.SubscriptionMode),
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
