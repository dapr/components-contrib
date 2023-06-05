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

package rabbitmq

import (
	"encoding/json"
	"fmt"
	"net/url"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/dapr/kit/logger"

	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/pubsub"
)

type rabbitmqMetadata struct {
	pubsub.TLSProperties      `mapstructure:",squash"`
	ConsumerID                string                 `mapstructure:"consumerID"`
	ConnectionString          string                 `mapstructure:"connectionString"`
	Protocol                  string                 `mapstructure:"protocol"`
	internalProtocol          string                 `mapstructure:"-"`
	Hostname                  string                 `mapstructure:"hostname"`
	Username                  string                 `mapstructure:"username"`
	Password                  string                 `mapstructure:"password"`
	Durable                   bool                   `mapstructure:"durable"`
	EnableDeadLetter          bool                   `mapstructure:"enableDeadLetter"`
	DeleteWhenUnused          bool                   `mapstructure:"deletedWhenUnused"`
	AutoAck                   bool                   `mapstructure:"autoAck"`
	RequeueInFailure          bool                   `mapstructure:"requeueInFailure"`
	DeliveryMode              uint8                  `mapstructure:"deliveryMode"`  // Transient (0 or 1) or Persistent (2)
	PrefetchCount             uint8                  `mapstructure:"prefetchCount"` // Prefetch deactivated if 0
	ReconnectWait             time.Duration          `mapstructure:"reconnectWaitSeconds"`
	MaxLen                    int64                  `mapstructure:"maxLen"`
	MaxLenBytes               int64                  `mapstructure:"maxLenBytes"`
	ExchangeKind              string                 `mapstructure:"exchangeKind"`
	ExchangeName              string                 `mapstructure:"exchangeName"`
	RoutingKey                string                 `mapstructure:"routingKey"`
	ColumnRouting             string                 `mapstructure:"columnRouting"`
	PublisherConfirm          bool                   `mapstructure:"publisherConfirm"`
	SaslExternal              bool                   `mapstructure:"saslExternal"`
	Concurrency               pubsub.ConcurrencyMode `mapstructure:"concurrency"`
	DefaultQueueTTL           *time.Duration         `mapstructure:"ttlInSeconds"`
	DeadLetterQueueAttributes string                 `mapstructure:"deadLetterQueueAttributes"`
	QueueAttributes           string                 `mapstructure:"queueAttributes"`
	DeadLetterQueueMap        map[string]interface{}
	QueueMap                  map[string]interface{}
}

const (
	metadataConsumerIDKey = "consumerID"

	metadataConnectionStringKey = "connectionString"

	metadataProtocolKey = "protocol"
	metadataHostnameKey = "hostname"
	metadataUsernameKey = "username"
	metadataPasswordKey = "password"

	metadataDurableKey              = "durable"
	metadataEnableDeadLetterKey     = "enableDeadLetter"
	metadataDeleteWhenUnusedKey     = "deletedWhenUnused"
	metadataAutoAckKey              = "autoAck"
	metadataRequeueInFailureKey     = "requeueInFailure"
	metadataDeliveryModeKey         = "deliveryMode"
	metadataPrefetchCountKey        = "prefetchCount"
	metadataReconnectWaitSecondsKey = "reconnectWaitSeconds"
	metadataMaxLenKey               = "maxLen"
	metadataMaxLenBytesKey          = "maxLenBytes"
	metadataExchangeKindKey         = "exchangeKind"
	metadataPublisherConfirmKey     = "publisherConfirm"
	metadataSaslExternal            = "saslExternal"
	// Deprecated: It can be added in queue attributes
	metadataMaxPriority = "maxPriority"

	defaultReconnectWaitSeconds = 3

	xConsistentHashExchange = "x-consistent-hash"
	protocolAMQP            = "amqp"
	protocolAMQPS           = "amqps"
)

// createMetadata creates a new instance from the pubsub metadata.
func createMetadata(pubSubMetadata pubsub.Metadata, log logger.Logger) (*rabbitmqMetadata, error) {
	result := rabbitmqMetadata{
		internalProtocol: protocolAMQP,
		Hostname:         "localhost",
		Durable:          true,
		DeleteWhenUnused: true,
		AutoAck:          false,
		ReconnectWait:    time.Duration(defaultReconnectWaitSeconds) * time.Second,
		ExchangeKind:     fanoutExchangeKind,
		PublisherConfirm: false,
		SaslExternal:     false,
	}

	if err := metadata.DecodeMetadata(pubSubMetadata.Properties, &result); err != nil {
		return nil, err
	}

	if result.ColumnRouting == "" && result.ExchangeKind == xConsistentHashExchange {
		return &result, fmt.Errorf("%s you can't create an x-consistent-hash exchange if you don't specify a columnRouting", errorMessagePrefix)
	}

	if result.DeadLetterQueueAttributes != "" {
		err := json.Unmarshal([]byte(result.DeadLetterQueueAttributes), &result.DeadLetterQueueMap)
		if err != nil {
			return &result, fmt.Errorf("%s incorrect format of DeadLetterQueue attributes err: %w", errorMessagePrefix, err)
		}
	}

	if result.QueueAttributes != "" {
		err := json.Unmarshal([]byte(result.QueueAttributes), &result.QueueMap)
		if err != nil {
			return &result, fmt.Errorf("%s incorrect format of Queue attributes err: %w", errorMessagePrefix, err)
		}
	}

	if result.ConnectionString != "" {
		uri, err := amqp.ParseURI(result.ConnectionString)
		if err != nil {
			return &result, fmt.Errorf("%s invalid connection string: %s, err: %w", errorMessagePrefix, result.ConnectionString, err)
		}
		result.internalProtocol = uri.Scheme
	}

	if result.Protocol != "" {
		if result.ConnectionString != "" && result.internalProtocol != result.Protocol {
			return &result, fmt.Errorf("%s protocol does not match connection string, protocol: %s, connection string: %s", errorMessagePrefix, result.Protocol, result.ConnectionString)
		}
		result.internalProtocol = result.Protocol
	}

	if result.DeliveryMode > 2 {
		return &result, fmt.Errorf("%s invalid RabbitMQ delivery mode, accepted values are between 0 and 2", errorMessagePrefix)
	}

	if !exchangeKindValid(result.ExchangeKind) {
		return &result, fmt.Errorf("%s invalid RabbitMQ exchange kind %s", errorMessagePrefix, result.ExchangeKind)
	}

	ttl, ok, err := metadata.TryGetTTL(pubSubMetadata.Properties)
	if err != nil {
		return &result, fmt.Errorf("%s parse RabbitMQ ttl metadata with error: %s", errorMessagePrefix, err)
	}

	if ok {
		result.DefaultQueueTTL = &ttl
	}

	result.TLSProperties, err = pubsub.TLS(pubSubMetadata.Properties)
	if err != nil {
		return &result, fmt.Errorf("%s invalid TLS configuration: %w", errorMessagePrefix, err)
	}

	if result.SaslExternal && (result.TLSProperties.CACert == "" || result.TLSProperties.ClientCert == "" || result.TLSProperties.ClientKey == "") {
		return &result, fmt.Errorf("%s can only be set to true, when all these properties are set: %s, %s, %s", metadataSaslExternal, pubsub.CACert, pubsub.ClientCert, pubsub.ClientKey)
	}

	result.Concurrency, err = pubsub.Concurrency(pubSubMetadata.Properties)
	return &result, err
}

func (m *rabbitmqMetadata) formatQueueDeclareArgs(origin amqp.Table) amqp.Table {
	if origin == nil {
		origin = amqp.Table{}
	}
	if m.MaxLen > 0 {
		origin[argMaxLength] = m.MaxLen
	}
	if m.MaxLenBytes > 0 {
		origin[argMaxLengthBytes] = m.MaxLenBytes
	}

	return origin
}

func exchangeKindValid(kind string) bool {
	return kind == amqp.ExchangeFanout ||
		kind == amqp.ExchangeTopic ||
		kind == amqp.ExchangeDirect ||
		kind == amqp.ExchangeHeaders ||
		kind == xConsistentHashExchange
}

func (m *rabbitmqMetadata) connectionURI() string {
	if m.ConnectionString != "" {
		return m.ConnectionString
	}

	u := url.URL{
		Scheme: m.internalProtocol,
		Host:   m.Hostname,
	}

	if m.Username != "" && m.Password != "" {
		u.User = url.UserPassword(m.Username, m.Password)
	} else if m.Username != "" {
		u.User = url.User(m.Username)
	}

	return u.String()
}
