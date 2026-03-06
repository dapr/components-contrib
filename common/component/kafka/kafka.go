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
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/IBM/sarama"
	aws2 "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/linkedin/goavro/v2"
	"github.com/riferrei/srclient"

	"github.com/dapr/components-contrib/common/aws"
	awsAuth "github.com/dapr/components-contrib/common/aws/auth"
	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/kit/logger"
	kitmd "github.com/dapr/kit/metadata"
	"github.com/dapr/kit/retry"
)

// Kafka allows reading/writing to a Kafka consumer group.
type Kafka struct {
	// These are used to inject mocked clients for tests
	mockConsumerGroup sarama.ConsumerGroup
	mockProducer      sarama.SyncProducer
	clients           *clients

	maxMessageBytes int
	consumerGroup   string
	brokers         []string
	logger          logger.Logger
	authType        string
	saslUsername    string
	saslPassword    string
	initialOffset   int64
	config          *sarama.Config
	escapeHeaders   bool

	subscribeTopics TopicHandlerConfig
	subscribeLock   sync.Mutex
	consumerCancel  context.CancelFunc
	consumerWG      sync.WaitGroup
	closeCh         chan struct{}
	closed          atomic.Bool
	wg              sync.WaitGroup

	// schema registry settings
	srClient                   srclient.ISchemaRegistryClient
	schemaCachingEnabled       bool
	latestSchemaCache          map[string]SchemaCacheEntry
	latestSchemaCacheTTL       time.Duration
	latestSchemaCacheWriteLock sync.RWMutex
	latestSchemaCacheReadLock  sync.Mutex

	// Whether to encode/decode Avro into Avro JSON or standard JSON
	useAvroJSON bool

	// used for background logic that cannot use the context passed to the Init function
	internalContext       context.Context
	internalContextCancel func()

	backOffConfig retry.Config

	// The default value should be true for kafka pubsub component and false for kafka binding component
	// This default value can be overridden by metadata consumeRetryEnabled
	DefaultConsumeRetryEnabled bool
	consumeRetryEnabled        bool
	consumeRetryInterval       time.Duration

	excludeHeaderMetaRegex *regexp.Regexp
	awsConfig              *aws2.Config
}

type SchemaType int

const (
	None SchemaType = iota
	Avro
)

type SchemaCacheEntry struct {
	schema         *srclient.Schema
	codec          *goavro.Codec
	expirationTime time.Time
}

func GetValueSchemaType(metadata map[string]string) (SchemaType, error) {
	schemaTypeStr, ok := kitmd.GetMetadataProperty(metadata, valueSchemaType)
	if ok {
		v, err := parseSchemaType(schemaTypeStr)
		return v, err
	}
	return None, nil
}

func parseSchemaType(sVal string) (SchemaType, error) {
	switch strings.ToLower(sVal) {
	case "avro":
		return Avro, nil
	case "none":
		return None, nil
	default:
		return None, fmt.Errorf("error parsing schema type. '%s' is not a supported value", sVal)
	}
}

func NewKafka(logger logger.Logger) *Kafka {
	return &Kafka{
		logger:          logger,
		subscribeTopics: make(TopicHandlerConfig),
		closeCh:         make(chan struct{}),
	}
}

// Init does metadata parsing and connection establishment.
func (k *Kafka) Init(ctx context.Context, metadata map[string]string) error {
	upgradedMetadata, err := k.upgradeMetadata(metadata)
	if err != nil {
		return err
	}

	meta, err := k.getKafkaMetadata(upgradedMetadata)
	if err != nil {
		return err
	}

	// this context can't use the context passed to Init because that context would be cancelled right after Init
	k.internalContext, k.internalContextCancel = context.WithCancel(context.Background())

	k.brokers = meta.internalBrokers
	k.consumerGroup = meta.ConsumerGroup
	k.initialOffset = meta.internalInitialOffset
	k.authType = meta.AuthType
	k.escapeHeaders = meta.EscapeHeaders

	config := sarama.NewConfig()
	config.Version = meta.internalVersion
	config.Consumer.Offsets.Initial = k.initialOffset
	config.Consumer.Fetch.Min = meta.consumerFetchMin
	config.Consumer.Fetch.Default = meta.consumerFetchDefault
	config.Consumer.Group.Heartbeat.Interval = meta.HeartbeatInterval
	config.Consumer.Group.Session.Timeout = meta.SessionTimeout
	k.initConsumerGroupRebalanceStrategy(config, metadata)
	config.ChannelBufferSize = meta.channelBufferSize

	config.Producer.Compression = meta.internalCompression

	config.Net.KeepAlive = meta.ClientConnectionKeepAliveInterval
	config.Metadata.RefreshFrequency = meta.ClientConnectionTopicMetadataRefreshInterval

	if meta.ClientID != "" {
		config.ClientID = meta.ClientID
	}

	err = updateTLSConfig(config, meta)
	if err != nil {
		return err
	}

	switch strings.ToLower(k.authType) {
	case oidcAuthType:
		k.logger.Info("Configuring SASL OAuth2/OIDC authentication")
		err = updateOidcAuthInfo(config, meta)
		if err != nil {
			return err
		}
	case oidcPrivateKeyJWTAuthType:
		k.logger.Info("Configuring SASL OAuth2/OIDC authentication with private key JWT")
		err = updateOidcPrivateKeyJWTAuthInfo(config, meta)
		if err != nil {
			return err
		}
	case passwordAuthType:
		k.logger.Info("Configuring SASL Password authentication")
		k.saslUsername = meta.SaslUsername
		k.saslPassword = meta.SaslPassword
		updatePasswordAuthInfo(config, meta, k.saslUsername, k.saslPassword)
	case mtlsAuthType:
		k.logger.Info("Configuring mTLS authentcation")
		err = updateMTLSAuthInfo(config, meta)
		if err != nil {
			return err
		}
	case certificateAuthType:
		// already handled in updateTLSConfig
	case awsIAMAuthType:
		k.logger.Info("Configuring AWS IAM authentication")
		opts, validateErr := k.ValidateAWS(metadata)
		if validateErr != nil {
			return fmt.Errorf("failed to validate AWS IAM authentication fields: %w", validateErr)
		}

		awsConfig, configErr := aws.NewConfig(ctx, opts)
		if configErr != nil {
			return configErr
		}

		k.awsConfig = &awsConfig
	}

	k.config = config
	sarama.Logger = SaramaLogBridge{daprLogger: k.logger}
	k.maxMessageBytes = meta.MaxMessageBytes

	// Default retry configuration is used if no
	// backOff properties are set.
	if rerr := retry.DecodeConfigWithPrefix(
		&k.backOffConfig,
		metadata,
		"backOff"); rerr != nil {
		return rerr
	}
	k.consumeRetryEnabled = meta.ConsumeRetryEnabled
	k.consumeRetryInterval = meta.ConsumeRetryInterval
	if meta.SchemaRegistryURL != "" {
		k.logger.Infof("Schema registry URL '%s' provided. Configuring the Schema Registry client.", meta.SchemaRegistryURL)
		k.srClient = srclient.CreateSchemaRegistryClient(meta.SchemaRegistryURL)
		k.srClient.CodecCreationEnabled(true)
		k.srClient.CodecJsonEnabled(!meta.UseAvroJSON)
		k.useAvroJSON = meta.UseAvroJSON
		// Empty password is a possibility
		if meta.SchemaRegistryAPIKey != "" {
			k.srClient.SetCredentials(meta.SchemaRegistryAPIKey, meta.SchemaRegistryAPISecret)
		}
		k.logger.Infof("Schema caching enabled: %v", meta.SchemaCachingEnabled)
		k.srClient.CachingEnabled(meta.SchemaCachingEnabled)
		if meta.SchemaCachingEnabled {
			k.latestSchemaCache = make(map[string]SchemaCacheEntry)
			k.logger.Debugf("Schema cache TTL: %v", meta.SchemaLatestVersionCacheTTL)
			k.latestSchemaCacheTTL = meta.SchemaLatestVersionCacheTTL
		}
	}

	clients, err := k.latestClients()
	if err != nil || clients == nil {
		return fmt.Errorf("failed to get latest Kafka clients for initialization: %w", err)
	}
	if clients.producer == nil {
		return errors.New("component is closed")
	}
	if clients.consumerGroup == nil {
		return errors.New("component is closed")
	}

	if meta.ExcludeHeaderMetaRegex != "" {
		k.excludeHeaderMetaRegex = regexp.MustCompile(meta.ExcludeHeaderMetaRegex)
	}

	k.logger.Debug("Kafka message bus initialization complete")

	return nil
}

func (k *Kafka) initConsumerGroupRebalanceStrategy(config *sarama.Config, metadata map[string]string) {
	consumerGroupRebalanceStrategy, ok := kitmd.GetMetadataProperty(metadata, "consumerGroupRebalanceStrategy")
	if !ok {
		consumerGroupRebalanceStrategy = consumerGroupRebalanceStrategyRange
	}
	switch strings.ToLower(consumerGroupRebalanceStrategy) {
	case consumerGroupRebalanceStrategySticky:
		config.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategySticky()}
	case consumerGroupRebalanceStrategyRoundRobin:
		config.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategyRoundRobin()}
	case consumerGroupRebalanceStrategyRange:
		config.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategyRange()}
	default:
		k.logger.Warnf("Invalid consumer group rebalance strategy: %s. Using default strategy: '%s'", consumerGroupRebalanceStrategy, consumerGroupRebalanceStrategyRange)
		config.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategyRange()}
	}
	k.logger.Infof("Consumer group rebalance strategy set to '%s'", config.Consumer.Group.Rebalance.GroupStrategies[0].Name())
}

func (k *Kafka) ValidateAWS(metadata map[string]string) (awsAuth.Options, error) {
	const defaultSessionName = "DaprDefaultSession"
	// This is needed as we remove the aws prefixed fields to use the builtin AWS profile fields instead.
	region := awsAuth.Coalesce(metadata["region"], metadata["awsRegion"])
	accessKey := awsAuth.Coalesce(metadata["accessKey"], metadata["awsAccessKey"])
	secretKey := awsAuth.Coalesce(metadata["secretKey"], metadata["awsSecretKey"])
	role := awsAuth.Coalesce(metadata["assumeRoleArn"], metadata["awsIamRoleArn"])
	session := awsAuth.Coalesce(metadata["sessionName"], metadata["awsStsSessionName"], defaultSessionName) // set default if no value is provided
	token := awsAuth.Coalesce(metadata["sessionToken"], metadata["awsSessionToken"])

	if region == "" {
		return awsAuth.Options{}, errors.New("metadata property AWSRegion is missing")
	}

	return awsAuth.Options{
		Logger:                k.logger,
		Region:                region,
		AccessKey:             accessKey,
		SecretKey:             secretKey,
		AssumeRoleArn:         role,
		AssumeRoleSessionName: session,
		SessionToken:          token,
		TrustAnchorArn:        metadata["trustAnchorArn"],
		TrustProfileArn:       metadata["trustProfileArn"],
		Properties:            metadata,
	}, nil
}

func (k *Kafka) Close() error {
	defer k.wg.Wait()
	defer k.consumerWG.Wait()

	errs := make([]error, 3)
	if k.closed.CompareAndSwap(false, true) {
		if k.closeCh != nil {
			close(k.closeCh)
		}
		if k.internalContext != nil {
			k.internalContextCancel()
		}

		k.subscribeLock.Lock()
		if k.consumerCancel != nil {
			k.consumerCancel()
		}
		k.subscribeLock.Unlock()

		if k.clients != nil {
			if k.clients.producer != nil {
				errs[0] = k.clients.producer.Close()
				k.clients.producer = nil
			}
			if k.clients.consumerGroup != nil {
				errs[1] = k.clients.consumerGroup.Close()
				k.clients.consumerGroup = nil
			}
		}
	}

	return errors.Join(errs...)
}

func getSchemaSubject(topic string) string {
	// For now assumes that subject is named after topic (e.g. `my-topic-value`)
	return topic + "-value"
}

func (k *Kafka) DeserializeValue(message *sarama.ConsumerMessage, config SubscriptionHandlerConfig) ([]byte, error) {
	// Null Data is valid and a tombstone record.
	// It shouldn't be going through schema validation and decoding
	// Instead directly convert to JSON `null`
	if message.Value == nil {
		return []byte("null"), nil
	}

	switch config.ValueSchemaType {
	case Avro:
		srClient, err := k.getSchemaRegistyClient()
		if err != nil {
			return nil, err
		}
		if len(message.Value) < 5 {
			return nil, errors.New("value is too short")
		}
		schemaID := binary.BigEndian.Uint32(message.Value[1:5])
		schema, err := srClient.GetSchema(int(schemaID))
		if err != nil {
			return nil, err
		}
		codec := schema.Codec() // The value returned in Avro JSON format
		native, _, err := codec.NativeFromBinary(message.Value[5:])
		if err != nil {
			return nil, err
		}
		value, err := codec.TextualFromNative(nil, native)
		if err != nil {
			return nil, err
		}
		return value, nil
	default:
		return message.Value, nil
	}
}

func (k *Kafka) getLatestSchema(topic string) (*srclient.Schema, *goavro.Codec, error) {
	srClient, err := k.getSchemaRegistyClient()
	if err != nil {
		return nil, nil, err
	}

	subject := getSchemaSubject(topic)
	if k.schemaCachingEnabled {
		k.latestSchemaCacheReadLock.Lock()
		cacheEntry, ok := k.latestSchemaCache[subject]
		k.latestSchemaCacheReadLock.Unlock()

		// Cache present and not expired
		if ok && cacheEntry.expirationTime.After(time.Now()) {
			return cacheEntry.schema, cacheEntry.codec, nil
		}
		k.logger.Debugf("Cache not found or expired for subject %s. Fetching from registry...", subject)
		schema, errSchema := srClient.GetLatestSchema(subject)
		if errSchema != nil {
			return nil, nil, errSchema
		}

		codec, errCodec := k.getCodec(schema)
		if errCodec != nil {
			return nil, nil, errCodec
		}
		defer k.latestSchemaCacheWriteLock.Unlock()
		k.latestSchemaCacheWriteLock.Lock()
		k.latestSchemaCache[subject] = SchemaCacheEntry{schema: schema, codec: codec, expirationTime: time.Now().Add(k.latestSchemaCacheTTL)}

		return schema, codec, nil
	}
	schema, err := srClient.GetLatestSchema(getSchemaSubject(topic))
	if err != nil {
		return nil, nil, err
	}
	codec, errCodec := k.getCodec(schema)
	if errCodec != nil {
		return nil, nil, errCodec
	}

	return schema, codec, nil
}

func (k *Kafka) getSchemaRegistyClient() (srclient.ISchemaRegistryClient, error) {
	if k.srClient == nil {
		return nil, errors.New("schema registry details not set")
	}

	return k.srClient, nil
}

func (k *Kafka) getCodec(schema *srclient.Schema) (*goavro.Codec, error) {
	// The data coming through is either Avro JSON or standard JSON.
	// Force creation of a new codec instance for serialization and deserialization to avoid state mutation issues.
	// https://github.com/linkedin/goavro/issues/299
	// Once the bug is fixed, we can remove this and use the codec directly from schema.Codec()
	if k.useAvroJSON {
		return goavro.NewCodec(schema.Schema())
	}
	return goavro.NewCodecForStandardJSONFull(schema.Schema())
}

func (k *Kafka) SerializeValue(topic string, data []byte, metadata map[string]string) ([]byte, error) {
	// Null Data is valid and a tombstone record.
	// It should be converted to NULL and not go through schema validation & encoding
	if bytes.Equal(data, []byte("null")) || data == nil {
		return nil, nil
	}

	valueSchemaType, err := GetValueSchemaType(metadata)
	if err != nil {
		return nil, err
	}

	switch valueSchemaType {
	case Avro:
		schema, codec, err := k.getLatestSchema(topic)
		if err != nil {
			return nil, err
		}

		native, _, err := codec.NativeFromTextual(data)
		if err != nil {
			return nil, err
		}

		valueBytes, err := codec.BinaryFromNative(nil, native)
		if err != nil {
			return nil, err
		}
		schemaIDBytes := make([]byte, 4)
		binary.BigEndian.PutUint32(schemaIDBytes, uint32(schema.ID())) //nolint:gosec

		recordValue := make([]byte, 0, len(schemaIDBytes)+len(valueBytes)+1)
		recordValue = append(recordValue, byte(0))
		recordValue = append(recordValue, schemaIDBytes...)
		recordValue = append(recordValue, valueBytes...)
		return recordValue, nil
	default:
		return data, nil
	}
}

// EventHandler is the handler used to handle the subscribed event.
type EventHandler func(ctx context.Context, msg *NewEvent) error

// BulkEventHandler is the handler used to handle the subscribed bulk event.
type BulkEventHandler func(ctx context.Context, msg *KafkaBulkMessage) ([]pubsub.BulkSubscribeResponseEntry, error)

// SubscriptionHandlerConfig is the handler and configuration for subscription.
type SubscriptionHandlerConfig struct {
	IsBulkSubscribe bool
	SubscribeConfig pubsub.BulkSubscribeConfig
	BulkHandler     BulkEventHandler
	Handler         EventHandler
	ValueSchemaType SchemaType
}

// NewEvent is an event arriving from a message bus instance.
type NewEvent struct {
	Data        []byte            `json:"data"`
	Topic       string            `json:"topic"`
	Metadata    map[string]string `json:"metadata"`
	ContentType *string           `json:"contentType,omitempty"`
}

// KafkaBulkMessage is a bulk event arriving from a message bus instance.
type KafkaBulkMessage struct {
	Entries  []KafkaBulkMessageEntry `json:"entries"`
	Topic    string                  `json:"topic"`
	Metadata map[string]string       `json:"metadata"`
}

// KafkaBulkMessageEntry is an item contained inside bulk event arriving from a message bus instance.
type KafkaBulkMessageEntry struct {
	EntryId     string            `json:"entryId"` //nolint:stylecheck
	Event       []byte            `json:"event"`
	ContentType string            `json:"contentType,omitempty"`
	Metadata    map[string]string `json:"metadata"`
}
