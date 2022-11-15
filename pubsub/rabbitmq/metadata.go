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
	"strconv"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/dapr/kit/logger"

	contribMetadata "github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/pubsub"
)

type metadata struct {
	consumerID       string
	connectionString string
	protocol         string
	hostname         string
	username         string
	password         string
	durable          bool
	enableDeadLetter bool
	deleteWhenUnused bool
	autoAck          bool
	requeueInFailure bool
	deliveryMode     uint8 // Transient (0 or 1) or Persistent (2)
	prefetchCount    uint8 // Prefetch deactivated if 0
	reconnectWait    time.Duration
	maxLen           int64
	maxLenBytes      int64
	exchangeKind     string
	publisherConfirm bool
	concurrency      pubsub.ConcurrencyMode
	defaultQueueTTL  *time.Duration
}

const (
	metadataConsumerIDKey = "consumerID"

	metadataConnectionStringKey = "connectionString"
	metadataHostKey             = "host"

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

	defaultReconnectWaitSeconds = 3
)

// createMetadata creates a new instance from the pubsub metadata.
func createMetadata(pubSubMetadata pubsub.Metadata, log logger.Logger) (*metadata, error) {
	result := metadata{
		protocol:         "amqp",
		hostname:         "localhost",
		durable:          true,
		deleteWhenUnused: true,
		autoAck:          false,
		reconnectWait:    time.Duration(defaultReconnectWaitSeconds) * time.Second,
		exchangeKind:     fanoutExchangeKind,
		publisherConfirm: false,
	}

	if val, found := pubSubMetadata.Properties[metadataConnectionStringKey]; found && val != "" {
		result.connectionString = val
	} else if val, found := pubSubMetadata.Properties[metadataHostKey]; found && val != "" {
		result.connectionString = val
		log.Warn("[DEPRECATION NOTICE] The 'host' argument is deprecated. Use 'connectionString' or individual connection arguments instead: https://docs.dapr.io/reference/components-reference/supported-pubsub/setup-rabbitmq/")
	}

	if val, found := pubSubMetadata.Properties[metadataProtocolKey]; found && val != "" {
		result.protocol = val
	}

	if val, found := pubSubMetadata.Properties[metadataHostnameKey]; found && val != "" {
		result.hostname = val
	}

	if val, found := pubSubMetadata.Properties[metadataUsernameKey]; found && val != "" {
		result.username = val
	}

	if val, found := pubSubMetadata.Properties[metadataPasswordKey]; found && val != "" {
		result.password = val
	}

	if val, found := pubSubMetadata.Properties[metadataConsumerIDKey]; found && val != "" {
		result.consumerID = val
	}

	if val, found := pubSubMetadata.Properties[metadataDeliveryModeKey]; found && val != "" {
		if intVal, err := strconv.Atoi(val); err == nil {
			if intVal < 0 || intVal > 2 {
				return &result, fmt.Errorf("%s invalid RabbitMQ delivery mode, accepted values are between 0 and 2", errorMessagePrefix)
			}
			result.deliveryMode = uint8(intVal)
		}
	}

	if val, found := pubSubMetadata.Properties[metadataDurableKey]; found && val != "" {
		if boolVal, err := strconv.ParseBool(val); err == nil {
			result.durable = boolVal
		}
	}

	if val, found := pubSubMetadata.Properties[metadataEnableDeadLetterKey]; found && val != "" {
		if boolVal, err := strconv.ParseBool(val); err == nil {
			result.enableDeadLetter = boolVal
		}
	}

	if val, found := pubSubMetadata.Properties[metadataDeleteWhenUnusedKey]; found && val != "" {
		if boolVal, err := strconv.ParseBool(val); err == nil {
			result.deleteWhenUnused = boolVal
		}
	}

	if val, found := pubSubMetadata.Properties[metadataAutoAckKey]; found && val != "" {
		if boolVal, err := strconv.ParseBool(val); err == nil {
			result.autoAck = boolVal
		}
	}

	if val, found := pubSubMetadata.Properties[metadataRequeueInFailureKey]; found && val != "" {
		if boolVal, err := strconv.ParseBool(val); err == nil {
			result.requeueInFailure = boolVal
		}
	}

	if val, found := pubSubMetadata.Properties[metadataReconnectWaitSecondsKey]; found && val != "" {
		if intVal, err := strconv.Atoi(val); err == nil {
			result.reconnectWait = time.Duration(intVal) * time.Second
		}
	}

	if val, found := pubSubMetadata.Properties[metadataPrefetchCountKey]; found && val != "" {
		if intVal, err := strconv.Atoi(val); err == nil {
			result.prefetchCount = uint8(intVal)
		}
	}

	if val, found := pubSubMetadata.Properties[metadataMaxLenKey]; found && val != "" {
		if intVal, err := strconv.ParseInt(val, 10, 64); err == nil {
			result.maxLen = intVal
		}
	}

	if val, found := pubSubMetadata.Properties[metadataMaxLenBytesKey]; found && val != "" {
		if intVal, err := strconv.ParseInt(val, 10, 64); err == nil {
			result.maxLenBytes = intVal
		}
	}

	if val, found := pubSubMetadata.Properties[metadataExchangeKindKey]; found && val != "" {
		if exchangeKindValid(val) {
			result.exchangeKind = val
		} else {
			return &result, fmt.Errorf("%s invalid RabbitMQ exchange kind %s", errorMessagePrefix, val)
		}
	}

	if val, found := pubSubMetadata.Properties[metadataPublisherConfirmKey]; found && val != "" {
		if boolVal, err := strconv.ParseBool(val); err == nil {
			result.publisherConfirm = boolVal
		}
	}

	ttl, ok, err := contribMetadata.TryGetTTL(pubSubMetadata.Properties)
	if err != nil {
		return &result, fmt.Errorf("%s parse RabbitMQ ttl metadata with error: %s", errorMessagePrefix, err)
	}

	if ok {
		result.defaultQueueTTL = &ttl
	}

	c, err := pubsub.Concurrency(pubSubMetadata.Properties)
	if err != nil {
		return &result, err
	}
	result.concurrency = c

	return &result, nil
}

func (m *metadata) formatQueueDeclareArgs(origin amqp.Table) amqp.Table {
	if origin == nil {
		origin = amqp.Table{}
	}
	if m.maxLen > 0 {
		origin[argMaxLength] = m.maxLen
	}
	if m.maxLenBytes > 0 {
		origin[argMaxLengthBytes] = m.maxLenBytes
	}

	return origin
}

func exchangeKindValid(kind string) bool {
	return kind == amqp.ExchangeFanout || kind == amqp.ExchangeTopic || kind == amqp.ExchangeDirect || kind == amqp.ExchangeHeaders
}

func (m *metadata) connectionURI() string {
	if m.connectionString != "" {
		return m.connectionString
	}

	u := url.URL{
		Scheme: m.protocol,
		Host:   m.hostname,
	}

	if m.username != "" && m.password != "" {
		u.User = url.UserPassword(m.username, m.password)
	} else if m.username != "" {
		u.User = url.User(m.username)
	}

	return u.String()
}
