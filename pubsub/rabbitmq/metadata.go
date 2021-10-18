package rabbitmq

import (
	"fmt"
	"strconv"
	"time"

	"github.com/streadway/amqp"

	"github.com/dapr/components-contrib/pubsub"
)

type metadata struct {
	consumerID       string
	host             string
	durable          bool
	enableDeadLetter bool
	deleteWhenUnused bool
	autoAck          bool
	requeueInFailure bool
	deliveryMode     uint8 // Transient (0 or 1) or Persistent (2)
	prefetchCount    uint8 // Prefetch deactivated if 0
	reconnectWait    time.Duration
	concurrency      pubsub.ConcurrencyMode
	maxLen           int64
	maxLenBytes      int64
}

// createMetadata creates a new instance from the pubsub metadata.
func createMetadata(pubSubMetadata pubsub.Metadata) (*metadata, error) {
	result := metadata{
		durable:          true,
		deleteWhenUnused: true,
		autoAck:          false,
		reconnectWait:    time.Duration(defaultReconnectWaitSeconds) * time.Second,
	}

	if val, found := pubSubMetadata.Properties[metadataHostKey]; found && val != "" {
		result.host = val
	} else {
		return &result, fmt.Errorf("%s missing RabbitMQ host", errorMessagePrefix)
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

	if val, found := pubSubMetadata.Properties[metadataDurable]; found && val != "" {
		if boolVal, err := strconv.ParseBool(val); err == nil {
			result.durable = boolVal
		}
	}

	if val, found := pubSubMetadata.Properties[metadataEnableDeadLetter]; found && val != "" {
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

	if val, found := pubSubMetadata.Properties[metadataReconnectWaitSeconds]; found && val != "" {
		if intVal, err := strconv.Atoi(val); err == nil {
			result.reconnectWait = time.Duration(intVal) * time.Second
		}
	}

	if val, found := pubSubMetadata.Properties[metadataPrefetchCount]; found && val != "" {
		if intVal, err := strconv.Atoi(val); err == nil {
			result.prefetchCount = uint8(intVal)
		}
	}

	if val, found := pubSubMetadata.Properties[metadataMaxLen]; found && val != "" {
		if intVal, err := strconv.ParseInt(val, 10, 64); err == nil {
			result.maxLen = intVal
		}
	}

	if val, found := pubSubMetadata.Properties[metadataMaxLenBytes]; found && val != "" {
		if intVal, err := strconv.ParseInt(val, 10, 64); err == nil {
			result.maxLenBytes = intVal
		}
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
