package rabbitmq

import (
	"fmt"
	"testing"

	"github.com/dapr/components-contrib/pubsub"
	"github.com/stretchr/testify/assert"
)

func getFakeProperties() map[string]string {
	props := map[string]string{}
	props[metadataHostKey] = "fakehost"
	props[metadataConsumerIDKey] = "fakeConsumerID"

	return props
}

func TestCreateMetadata(t *testing.T) {
	booleanFlagTests := []struct {
		in       string
		expected bool
	}{
		{"true", true},
		{"TRUE", true},
		{"false", false},
		{"FALSE", false},
	}

	t.Run("metadata is correct", func(t *testing.T) {
		fakeProperties := getFakeProperties()

		fakeMetaData := pubsub.Metadata{
			Properties: fakeProperties,
		}

		// act
		m, err := createMetadata(fakeMetaData)

		// assert
		assert.NoError(t, err)
		assert.Equal(t, fakeProperties[metadataHostKey], m.host)
		assert.Equal(t, fakeProperties[metadataConsumerIDKey], m.consumerID)
		assert.Equal(t, false, m.autoAck)
		assert.Equal(t, false, m.requeueInFailure)
		assert.Equal(t, true, m.deleteWhenUnused)
		assert.Equal(t, uint8(0), m.deliveryMode)
	})

	t.Run("host is not given", func(t *testing.T) {
		fakeProperties := getFakeProperties()

		fakeMetaData := pubsub.Metadata{
			Properties: fakeProperties,
		}
		fakeMetaData.Properties[metadataHostKey] = ""

		// act
		m, err := createMetadata(fakeMetaData)

		// assert
		assert.EqualError(t, err, "rabbitmq pub/sub error: missing RabbitMQ host")
		assert.Empty(t, m.host)
		assert.Empty(t, m.consumerID)
	})

	t.Run("consumerID is not given", func(t *testing.T) {
		fakeProperties := getFakeProperties()

		fakeMetaData := pubsub.Metadata{
			Properties: fakeProperties,
		}
		fakeMetaData.Properties[metadataConsumerIDKey] = ""

		// act
		m, err := createMetadata(fakeMetaData)

		// assert
		assert.EqualError(t, err, "rabbitmq pub/sub error: missing RabbitMQ consumerID")
		assert.Equal(t, fakeProperties[metadataHostKey], m.host)
		assert.Equal(t, fakeProperties[metadataConsumerIDKey], m.consumerID)
		assert.Empty(t, m.consumerID)
	})

	invalidDeliveryModes := []string{"3", "10", "-1"}

	for _, deliveryMode := range invalidDeliveryModes {
		t.Run(fmt.Sprintf("deliveryMode value=%s", deliveryMode), func(t *testing.T) {
			fakeProperties := getFakeProperties()

			fakeMetaData := pubsub.Metadata{
				Properties: fakeProperties,
			}
			fakeMetaData.Properties[metadataDeliveryModeKey] = deliveryMode

			// act
			m, err := createMetadata(fakeMetaData)

			// assert
			assert.EqualError(t, err, "rabbitmq pub/sub error: invalid RabbitMQ delivery mode, accepted values are between 0 and 2")
			assert.Equal(t, fakeProperties[metadataHostKey], m.host)
			assert.Equal(t, fakeProperties[metadataConsumerIDKey], m.consumerID)
			assert.Equal(t, uint8(0), m.deliveryMode)
		})
	}

	t.Run("deliveryMode is set", func(t *testing.T) {
		fakeProperties := getFakeProperties()

		fakeMetaData := pubsub.Metadata{
			Properties: fakeProperties,
		}
		fakeMetaData.Properties[metadataDeliveryModeKey] = "2"

		// act
		m, err := createMetadata(fakeMetaData)

		// assert
		assert.NoError(t, err)
		assert.Equal(t, fakeProperties[metadataHostKey], m.host)
		assert.Equal(t, fakeProperties[metadataConsumerIDKey], m.consumerID)
		assert.Equal(t, uint8(2), m.deliveryMode)
	})

	for _, tt := range booleanFlagTests {
		t.Run(fmt.Sprintf("autoAck value=%s", tt.in), func(t *testing.T) {
			fakeProperties := getFakeProperties()

			fakeMetaData := pubsub.Metadata{
				Properties: fakeProperties,
			}
			fakeMetaData.Properties[metadataAutoAckKey] = tt.in

			// act
			m, err := createMetadata(fakeMetaData)

			// assert
			assert.NoError(t, err)
			assert.Equal(t, fakeProperties[metadataHostKey], m.host)
			assert.Equal(t, fakeProperties[metadataConsumerIDKey], m.consumerID)
			assert.Equal(t, tt.expected, m.autoAck)
		})
	}

	for _, tt := range booleanFlagTests {
		t.Run(fmt.Sprintf("requeueInFailure value=%s", tt.in), func(t *testing.T) {
			fakeProperties := getFakeProperties()

			fakeMetaData := pubsub.Metadata{
				Properties: fakeProperties,
			}
			fakeMetaData.Properties[metadataRequeueInFailureKey] = tt.in

			// act
			m, err := createMetadata(fakeMetaData)

			// assert
			assert.NoError(t, err)
			assert.Equal(t, fakeProperties[metadataHostKey], m.host)
			assert.Equal(t, fakeProperties[metadataConsumerIDKey], m.consumerID)
			assert.Equal(t, tt.expected, m.requeueInFailure)
		})
	}

	for _, tt := range booleanFlagTests {
		t.Run(fmt.Sprintf("deleteWhenUnused value=%s", tt.in), func(t *testing.T) {
			fakeProperties := getFakeProperties()

			fakeMetaData := pubsub.Metadata{
				Properties: fakeProperties,
			}
			fakeMetaData.Properties[metadataDeleteWhenUnusedKey] = tt.in

			// act
			m, err := createMetadata(fakeMetaData)

			// assert
			assert.NoError(t, err)
			assert.Equal(t, fakeProperties[metadataHostKey], m.host)
			assert.Equal(t, fakeProperties[metadataConsumerIDKey], m.consumerID)
			assert.Equal(t, tt.expected, m.deleteWhenUnused)
		})
	}

	for _, tt := range booleanFlagTests {
		t.Run(fmt.Sprintf("durable value=%s", tt.in), func(t *testing.T) {
			fakeProperties := getFakeProperties()

			fakeMetaData := pubsub.Metadata{
				Properties: fakeProperties,
			}

			// act
			m, err := createMetadata(fakeMetaData)

			// assert
			assert.NoError(t, err)
			assert.Equal(t, fakeProperties[metadataHostKey], m.host)
			assert.Equal(t, fakeProperties[metadataConsumerIDKey], m.consumerID)
		})
	}
}
