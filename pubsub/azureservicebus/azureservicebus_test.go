// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package azureservicebus

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/dapr/components-contrib/pubsub"
)

const (
	invalidNumber = "invalid_number"
)

func getFakeProperties() map[string]string {
	return map[string]string{
		connectionString:              "fakeConnectionString",
		consumerID:                    "fakeConId",
		disableEntityManagement:       "true",
		timeoutInSec:                  "90",
		maxDeliveryCount:              "10",
		autoDeleteOnIdleInSec:         "240",
		defaultMessageTimeToLiveInSec: "2400",
		lockDurationInSec:             "120",
	}
}

func TestParseServiceBusMetadata(t *testing.T) {
	t.Run("metadata is correct", func(t *testing.T) {
		fakeProperties := getFakeProperties()

		fakeMetaData := pubsub.Metadata{
			Properties: fakeProperties,
		}

		// act
		m, err := parseAzureServiceBusMetadata(fakeMetaData)

		// assert
		assert.NoError(t, err)
		assert.Equal(t, fakeProperties[connectionString], m.ConnectionString)
		assert.Equal(t, fakeProperties[consumerID], m.ConsumerID)

		assert.Equal(t, 90, m.TimeoutInSec)
		assert.Equal(t, true, m.DisableEntityManagement)

		assert.NotNil(t, m.AutoDeleteOnIdleInSec)
		assert.Equal(t, 240, *m.AutoDeleteOnIdleInSec)
		assert.NotNil(t, m.MaxDeliveryCount)
		assert.Equal(t, 10, *m.MaxDeliveryCount)
		assert.NotNil(t, m.DefaultMessageTimeToLiveInSec)
		assert.Equal(t, 2400, *m.DefaultMessageTimeToLiveInSec)
		assert.NotNil(t, m.LockDurationInSec)
		assert.Equal(t, 120, *m.LockDurationInSec)
	})

	t.Run("missing required connectionString", func(t *testing.T) {
		fakeProperties := getFakeProperties()

		fakeMetaData := pubsub.Metadata{
			Properties: fakeProperties,
		}
		fakeMetaData.Properties[connectionString] = ""

		// act
		m, err := parseAzureServiceBusMetadata(fakeMetaData)

		// assert
		assert.Error(t, err)
		assert.Empty(t, m.ConnectionString)
	})

	t.Run("missing required consumerID", func(t *testing.T) {
		fakeProperties := getFakeProperties()

		fakeMetaData := pubsub.Metadata{
			Properties: fakeProperties,
		}
		fakeMetaData.Properties[consumerID] = ""

		// act
		m, err := parseAzureServiceBusMetadata(fakeMetaData)

		// assert
		assert.Error(t, err)
		assert.Empty(t, m.ConsumerID)
	})

	t.Run("missing optional timeoutInSec", func(t *testing.T) {
		fakeProperties := getFakeProperties()

		fakeMetaData := pubsub.Metadata{
			Properties: fakeProperties,
		}
		fakeMetaData.Properties[timeoutInSec] = ""

		// act
		m, err := parseAzureServiceBusMetadata(fakeMetaData)

		// assert
		assert.Equal(t, m.TimeoutInSec, 60)
		assert.Nil(t, err)
	})

	t.Run("invalid optional timeoutInSec", func(t *testing.T) {
		fakeProperties := getFakeProperties()

		fakeMetaData := pubsub.Metadata{
			Properties: fakeProperties,
		}
		fakeMetaData.Properties[timeoutInSec] = invalidNumber

		// act
		_, err := parseAzureServiceBusMetadata(fakeMetaData)

		// assert
		assert.Error(t, err)
	})

	t.Run("missing optional disableEntityManagement", func(t *testing.T) {
		fakeProperties := getFakeProperties()

		fakeMetaData := pubsub.Metadata{
			Properties: fakeProperties,
		}
		fakeMetaData.Properties[disableEntityManagement] = ""

		// act
		m, err := parseAzureServiceBusMetadata(fakeMetaData)

		// assert
		assert.Equal(t, m.DisableEntityManagement, false)
		assert.Nil(t, err)
	})

	t.Run("invalid optional disableEntityManagement", func(t *testing.T) {
		fakeProperties := getFakeProperties()

		fakeMetaData := pubsub.Metadata{
			Properties: fakeProperties,
		}
		fakeMetaData.Properties[disableEntityManagement] = "invalid_bool"

		// act
		_, err := parseAzureServiceBusMetadata(fakeMetaData)

		// assert
		assert.Error(t, err)
	})

	t.Run("missing nullable maxDeliveryCount", func(t *testing.T) {
		fakeProperties := getFakeProperties()

		fakeMetaData := pubsub.Metadata{
			Properties: fakeProperties,
		}
		fakeMetaData.Properties[maxDeliveryCount] = ""

		// act
		m, err := parseAzureServiceBusMetadata(fakeMetaData)

		// assert
		assert.Nil(t, m.MaxDeliveryCount)
		assert.Nil(t, err)
	})

	t.Run("invalid nullable maxDeliveryCount", func(t *testing.T) {
		fakeProperties := getFakeProperties()

		fakeMetaData := pubsub.Metadata{
			Properties: fakeProperties,
		}
		fakeMetaData.Properties[maxDeliveryCount] = invalidNumber

		// act
		_, err := parseAzureServiceBusMetadata(fakeMetaData)

		// assert
		assert.Error(t, err)
	})

	t.Run("missing nullable defaultMessageTimeToLiveInSec", func(t *testing.T) {
		fakeProperties := getFakeProperties()

		fakeMetaData := pubsub.Metadata{
			Properties: fakeProperties,
		}
		fakeMetaData.Properties[defaultMessageTimeToLiveInSec] = ""

		// act
		m, err := parseAzureServiceBusMetadata(fakeMetaData)

		// assert
		assert.Nil(t, m.DefaultMessageTimeToLiveInSec)
		assert.Nil(t, err)
	})

	t.Run("invalid nullable defaultMessageTimeToLiveInSec", func(t *testing.T) {
		fakeProperties := getFakeProperties()

		fakeMetaData := pubsub.Metadata{
			Properties: fakeProperties,
		}
		fakeMetaData.Properties[defaultMessageTimeToLiveInSec] = invalidNumber

		// act
		_, err := parseAzureServiceBusMetadata(fakeMetaData)

		// assert
		assert.Error(t, err)
	})

	t.Run("missing nullable autoDeleteOnIdleInSec", func(t *testing.T) {
		fakeProperties := getFakeProperties()

		fakeMetaData := pubsub.Metadata{
			Properties: fakeProperties,
		}
		fakeMetaData.Properties[autoDeleteOnIdleInSec] = ""

		// act
		m, err := parseAzureServiceBusMetadata(fakeMetaData)

		// assert
		assert.Nil(t, m.AutoDeleteOnIdleInSec)
		assert.Nil(t, err)
	})

	t.Run("invalid nullable autoDeleteOnIdleInSec", func(t *testing.T) {
		fakeProperties := getFakeProperties()

		fakeMetaData := pubsub.Metadata{
			Properties: fakeProperties,
		}
		fakeMetaData.Properties[autoDeleteOnIdleInSec] = invalidNumber

		// act
		_, err := parseAzureServiceBusMetadata(fakeMetaData)

		// assert
		assert.Error(t, err)
	})

	t.Run("missing nullable lockDurationInSec", func(t *testing.T) {
		fakeProperties := getFakeProperties()

		fakeMetaData := pubsub.Metadata{
			Properties: fakeProperties,
		}
		fakeMetaData.Properties[lockDurationInSec] = ""

		// act
		m, err := parseAzureServiceBusMetadata(fakeMetaData)

		// assert
		assert.Nil(t, m.LockDurationInSec)
		assert.Nil(t, err)
	})

	t.Run("invalid nullable lockDurationInSec", func(t *testing.T) {
		fakeProperties := getFakeProperties()

		fakeMetaData := pubsub.Metadata{
			Properties: fakeProperties,
		}
		fakeMetaData.Properties[lockDurationInSec] = invalidNumber

		// act
		_, err := parseAzureServiceBusMetadata(fakeMetaData)

		// assert
		assert.Error(t, err)
	})
}
