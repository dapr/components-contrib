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
	"fmt"
	"net/url"
	"strings"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/kit/logger"
	kitmd "github.com/dapr/kit/metadata"
)

type rabbitmqMetadata struct {
	pubsub.TLSProperties               `mapstructure:",squash"`
	ConsumerID                         string                 `mapstructure:"consumerID" mdignore:"true"`
	ConnectionString                   string                 `mapstructure:"connectionString"`
	Protocol                           string                 `mapstructure:"protocol"`
	internalProtocol                   string                 `mapstructure:"-"`
	Hostname                           string                 `mapstructure:"hostname"`
	Username                           string                 `mapstructure:"username"`
	Password                           string                 `mapstructure:"password"`
	Durable                            bool                   `mapstructure:"durable"`
	EnableDeadLetter                   bool                   `mapstructure:"enableDeadLetter"`
	DeleteWhenUnused                   bool                   `mapstructure:"deletedWhenUnused"`
	AutoAck                            bool                   `mapstructure:"autoAck"`
	RequeueInFailure                   bool                   `mapstructure:"requeueInFailure"`
	DeliveryMode                       uint8                  `mapstructure:"deliveryMode"`  // Transient (0 or 1) or Persistent (2)
	PrefetchCount                      uint8                  `mapstructure:"prefetchCount"` // Prefetch deactivated if 0
	ReconnectWait                      time.Duration          `mapstructure:"reconnectWaitSeconds"`
	MaxLen                             int64                  `mapstructure:"maxLen"`
	MaxLenBytes                        int64                  `mapstructure:"maxLenBytes"`
	ExchangeKind                       string                 `mapstructure:"exchangeKind"`
	ExchangeDeclareMode                string                 `mapstructure:"exchangeDeclareMode"`
	ClientName                         string                 `mapstructure:"clientName"`
	HeartBeat                          time.Duration          `mapstructure:"heartBeat"`
	PublisherConfirm                   bool                   `mapstructure:"publisherConfirm"`
	SaslExternal                       bool                   `mapstructure:"saslExternal"`
	Concurrency                        pubsub.ConcurrencyMode `mapstructure:"concurrency"`
	DefaultQueueTTL                    *time.Duration         `mapstructure:"ttlInSeconds"`
	PublishMessagePropertiesToMetadata bool                   `mapstructure:"publishMessagePropertiesToMetadata"`
}

const (
	metadataConsumerIDKey = "consumerID"

	metadataConnectionStringKey = "connectionString"
	metadataHostKey             = "host"

	metadataProtocolKey = "protocol"
	metadataHostnameKey = "hostname"
	metadataUsernameKey = "username"
	metadataPasswordKey = "password"

	metadataDurableKey                            = "durable"
	metadataEnableDeadLetterKey                   = "enableDeadLetter"
	metadataDeleteWhenUnusedKey                   = "deletedWhenUnused"
	metadataAutoAckKey                            = "autoAck"
	metadataRequeueInFailureKey                   = "requeueInFailure"
	metadataDeliveryModeKey                       = "deliveryMode"
	metadataPrefetchCountKey                      = "prefetchCount"
	metadataReconnectWaitSecondsKey               = "reconnectWaitSeconds"
	metadataMaxLenKey                             = "maxLen"
	metadataMaxLenBytesKey                        = "maxLenBytes"
	metadataExchangeKindKey                       = "exchangeKind"
	metadataExchangeDeclareModeKey                = "exchangeDeclareMode"
	metadataPublisherConfirmKey                   = "publisherConfirm"
	metadataSaslExternal                          = "saslExternal"
	metadataMaxPriority                           = "maxPriority"
	metadataClientNameKey                         = "clientName"
	metadataHeartBeatKey                          = "heartBeat"
	metadataQueueNameKey                          = "queueName"
	metadataPublishMessagePropertiesToMetadataKey = "publishMessagePropertiesToMetadata"

	defaultReconnectWaitSeconds = 3

	protocolAMQP  = "amqp"
	protocolAMQPS = "amqps"

	// exchangeDeclareModeDeclare makes the component declare the exchange itself
	// (an active AMQP exchange.declare). This is the default and the historical
	// behavior.
	exchangeDeclareModeDeclare = "declare"
	// exchangeDeclareModePassive makes the component verify that the exchange
	// already exists (a passive AMQP exchange.declare) without creating or
	// modifying it. Use this when the topology is owned by something else, such
	// as the RabbitMQ Cluster Kubernetes Topology Operator or Terraform.
	exchangeDeclareModePassive = "passive"

	// exchangeKindConsistentHash is the exchange type provided by the
	// rabbitmq_consistent_hash_exchange plugin. It is not a built-in AMQP
	// exchange type, so it is only usable against a broker with that plugin
	// enabled.
	exchangeKindConsistentHash = "x-consistent-hash"
)

// createMetadata creates a new instance from the pubsub metadata.
func createMetadata(pubSubMetadata pubsub.Metadata, log logger.Logger) (*rabbitmqMetadata, error) {
	result := rabbitmqMetadata{
		internalProtocol:                   protocolAMQP,
		Hostname:                           "localhost",
		Durable:                            true,
		DeleteWhenUnused:                   true,
		AutoAck:                            false,
		ReconnectWait:                      time.Duration(defaultReconnectWaitSeconds) * time.Second,
		ExchangeKind:                       fanoutExchangeKind,
		ExchangeDeclareMode:                exchangeDeclareModeDeclare,
		PublisherConfirm:                   false,
		SaslExternal:                       false,
		HeartBeat:                          defaultHeartbeat,
		PublishMessagePropertiesToMetadata: false,
	}

	// upgrade metadata

	if val, found := pubSubMetadata.Properties[metadataConnectionStringKey]; !found || val == "" {
		if host, found := pubSubMetadata.Properties[metadataHostKey]; found && host != "" {
			pubSubMetadata.Properties[metadataConnectionStringKey] = host
			log.Warn("[DEPRECATION NOTICE] The 'host' argument is deprecated. Use 'connectionString' or individual connection arguments instead: https://docs.dapr.io/reference/components-reference/supported-pubsub/setup-rabbitmq/")
		}
	}

	if err := kitmd.DecodeMetadata(pubSubMetadata.Properties, &result); err != nil {
		return nil, err
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

	result.ExchangeDeclareMode = strings.ToLower(result.ExchangeDeclareMode)
	if !exchangeDeclareModeValid(result.ExchangeDeclareMode) {
		return &result, fmt.Errorf("%s invalid RabbitMQ exchange declare mode %q, accepted values are %q and %q", errorMessagePrefix, result.ExchangeDeclareMode, exchangeDeclareModeDeclare, exchangeDeclareModePassive)
	}

	if err := validateExchangeKind(result.ExchangeKind, result.ExchangeDeclareMode); err != nil {
		return &result, err
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

	if result.SaslExternal && (result.CACert == "" || result.ClientCert == "" || result.ClientKey == "") {
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

func exchangeDeclareModeValid(mode string) bool {
	return mode == exchangeDeclareModeDeclare || mode == exchangeDeclareModePassive
}

// exchangeKindValid reports whether the component is able to declare an
// exchange of the given kind itself.
func exchangeKindValid(kind string) bool {
	switch kind {
	case amqp.ExchangeFanout, amqp.ExchangeTopic, amqp.ExchangeDirect, amqp.ExchangeHeaders, exchangeKindConsistentHash:
		return true
	default:
		return false
	}
}

// validateExchangeKind checks exchangeKind against what the configured declare
// mode allows. In passive mode the component never declares the exchange, so
// any kind the broker supports (including plugin-provided kinds) is accepted.
func validateExchangeKind(kind string, declareMode string) error {
	if declareMode == exchangeDeclareModePassive {
		if kind == "" {
			return fmt.Errorf("%s %s cannot be empty", errorMessagePrefix, metadataExchangeKindKey)
		}

		return nil
	}

	if !exchangeKindValid(kind) {
		return fmt.Errorf("%s invalid RabbitMQ exchange kind %q; the component can declare %s, %s, %s, %s and %s. To use an exchange of any other kind, create it out-of-band and set %s to %q", errorMessagePrefix, kind, amqp.ExchangeFanout, amqp.ExchangeTopic, amqp.ExchangeDirect, amqp.ExchangeHeaders, exchangeKindConsistentHash, metadataExchangeDeclareModeKey, exchangeDeclareModePassive)
	}

	return nil
}

// isPassiveExchangeDeclare reports whether the exchange topology is managed
// outside of Dapr.
func (m *rabbitmqMetadata) isPassiveExchangeDeclare() bool {
	return m.ExchangeDeclareMode == exchangeDeclareModePassive
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
