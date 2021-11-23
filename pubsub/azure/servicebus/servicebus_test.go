// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package servicebus

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
		connectionString:               "fakeConnectionString",
		namespaceName:                  "",
		consumerID:                     "fakeConId",
		disableEntityManagement:        "true",
		timeoutInSec:                   "90",
		handlerTimeoutInSec:            "30",
		maxDeliveryCount:               "10",
		autoDeleteOnIdleInSec:          "240",
		defaultMessageTimeToLiveInSec:  "2400",
		lockDurationInSec:              "120",
		lockRenewalInSec:               "15",
		maxConcurrentHandlers:          "1",
		prefetchCount:                  "10",
		maxActiveMessages:              "100",
		maxActiveMessagesRecoveryInSec: "5",
		maxReconnectionAttempts:        "30",
		connectionRecoveryInSec:        "5",
	}
}

func TestParseServiceBusMetadata(t *testing.T) {
	t.Run("metadata is correct", func(t *testing.T) {
		fakeProperties := getFakeProperties()

		fakeMetaData := pubsub.Metadata{
			Properties: fakeProperties,
		}

		// act.
		m, err := parseAzureServiceBusMetadata(fakeMetaData)

		// assert.
		assert.NoError(t, err)
		assert.Equal(t, fakeProperties[connectionString], m.ConnectionString)
		assert.Equal(t, fakeProperties[consumerID], m.ConsumerID)

		assert.Equal(t, 90, m.TimeoutInSec)
		assert.Equal(t, true, m.DisableEntityManagement)
		assert.Equal(t, 30, m.HandlerTimeoutInSec)
		assert.NotNil(t, m.LockRenewalInSec)
		assert.Equal(t, 15, m.LockRenewalInSec)
		assert.NotNil(t, m.MaxActiveMessages)
		assert.Equal(t, 100, m.MaxActiveMessages)
		assert.NotNil(t, m.MaxActiveMessagesRecoveryInSec)
		assert.Equal(t, 5, m.MaxActiveMessagesRecoveryInSec)
		assert.NotNil(t, m.MaxReconnectionAttempts)
		assert.Equal(t, 30, m.MaxReconnectionAttempts)
		assert.NotNil(t, m.ConnectionRecoveryInSec)
		assert.Equal(t, 5, m.ConnectionRecoveryInSec)

		assert.NotNil(t, m.AutoDeleteOnIdleInSec)
		assert.Equal(t, 240, *m.AutoDeleteOnIdleInSec)
		assert.NotNil(t, m.MaxDeliveryCount)
		assert.Equal(t, 10, *m.MaxDeliveryCount)
		assert.NotNil(t, m.DefaultMessageTimeToLiveInSec)
		assert.Equal(t, 2400, *m.DefaultMessageTimeToLiveInSec)
		assert.NotNil(t, m.LockDurationInSec)
		assert.Equal(t, 120, *m.LockDurationInSec)
		assert.NotNil(t, m.MaxConcurrentHandlers)
		assert.Equal(t, 1, *m.MaxConcurrentHandlers)
		assert.NotNil(t, m.PrefetchCount)
		assert.Equal(t, 10, *m.PrefetchCount)
	})

	t.Run("missing required connectionString and namespaceName", func(t *testing.T) {
		fakeProperties := getFakeProperties()

		fakeMetaData := pubsub.Metadata{
			Properties: fakeProperties,
		}
		fakeMetaData.Properties[connectionString] = ""
		fakeMetaData.Properties[namespaceName] = ""

		// act.
		m, err := parseAzureServiceBusMetadata(fakeMetaData)

		// assert.
		assert.Error(t, err)
		assertValidErrorMessage(t, err)
		assert.Empty(t, m.ConnectionString)
	})

	t.Run("connectionString makes namespace optional", func(t *testing.T) {
		fakeProperties := getFakeProperties()

		fakeMetaData := pubsub.Metadata{
			Properties: fakeProperties,
		}
		fakeMetaData.Properties[namespaceName] = ""

		// act.
		m, err := parseAzureServiceBusMetadata(fakeMetaData)

		// assert.
		assert.NoError(t, err)
		assert.Equal(t, "fakeConnectionString", m.ConnectionString)
	})

	t.Run("namespace makes conectionString optional", func(t *testing.T) {
		fakeProperties := getFakeProperties()

		fakeMetaData := pubsub.Metadata{
			Properties: fakeProperties,
		}
		fakeMetaData.Properties[namespaceName] = "fakeNamespace"
		fakeMetaData.Properties[connectionString] = ""

		// act.
		m, err := parseAzureServiceBusMetadata(fakeMetaData)

		// assert.
		assert.NoError(t, err)
		assert.Equal(t, "fakeNamespace", m.NamespaceName)
	})

	t.Run("connectionString and namespace are mutually exclusive", func(t *testing.T) {
		fakeProperties := getFakeProperties()

		fakeMetaData := pubsub.Metadata{
			Properties: fakeProperties,
		}

		fakeMetaData.Properties[namespaceName] = "fakeNamespace"

		// act.
		_, err := parseAzureServiceBusMetadata(fakeMetaData)

		// assert.
		assert.Error(t, err)
		assertValidErrorMessage(t, err)
	})

	t.Run("missing required consumerID", func(t *testing.T) {
		fakeProperties := getFakeProperties()

		fakeMetaData := pubsub.Metadata{
			Properties: fakeProperties,
		}
		fakeMetaData.Properties[consumerID] = ""

		// act.
		m, err := parseAzureServiceBusMetadata(fakeMetaData)

		// assert.
		assert.Error(t, err)
		assertValidErrorMessage(t, err)
		assert.Empty(t, m.ConsumerID)
	})

	t.Run("missing optional timeoutInSec", func(t *testing.T) {
		fakeProperties := getFakeProperties()

		fakeMetaData := pubsub.Metadata{
			Properties: fakeProperties,
		}
		fakeMetaData.Properties[timeoutInSec] = ""

		// act.
		m, err := parseAzureServiceBusMetadata(fakeMetaData)

		// assert.
		assert.Equal(t, 60, m.TimeoutInSec)
		assert.Nil(t, err)
	})

	t.Run("invalid optional timeoutInSec", func(t *testing.T) {
		fakeProperties := getFakeProperties()

		fakeMetaData := pubsub.Metadata{
			Properties: fakeProperties,
		}
		fakeMetaData.Properties[timeoutInSec] = invalidNumber

		// act.
		_, err := parseAzureServiceBusMetadata(fakeMetaData)

		// assert.
		assert.Error(t, err)
		assertValidErrorMessage(t, err)
	})

	t.Run("missing optional disableEntityManagement", func(t *testing.T) {
		fakeProperties := getFakeProperties()

		fakeMetaData := pubsub.Metadata{
			Properties: fakeProperties,
		}
		fakeMetaData.Properties[disableEntityManagement] = ""

		// act.
		m, err := parseAzureServiceBusMetadata(fakeMetaData)

		// assert.
		assert.Equal(t, false, m.DisableEntityManagement)
		assert.Nil(t, err)
	})

	t.Run("invalid optional disableEntityManagement", func(t *testing.T) {
		fakeProperties := getFakeProperties()

		fakeMetaData := pubsub.Metadata{
			Properties: fakeProperties,
		}
		fakeMetaData.Properties[disableEntityManagement] = "invalid_bool"

		// act.
		_, err := parseAzureServiceBusMetadata(fakeMetaData)

		// assert.
		assert.Error(t, err)
		assertValidErrorMessage(t, err)
	})

	t.Run("missing optional handlerTimeoutInSec", func(t *testing.T) {
		fakeProperties := getFakeProperties()

		fakeMetaData := pubsub.Metadata{
			Properties: fakeProperties,
		}
		fakeMetaData.Properties[handlerTimeoutInSec] = ""

		// act.
		m, err := parseAzureServiceBusMetadata(fakeMetaData)

		// assert.
		assert.Equal(t, 60, m.HandlerTimeoutInSec)
		assert.Nil(t, err)
	})

	t.Run("invalid optional handlerTimeoutInSec", func(t *testing.T) {
		fakeProperties := getFakeProperties()

		fakeMetaData := pubsub.Metadata{
			Properties: fakeProperties,
		}
		fakeMetaData.Properties[handlerTimeoutInSec] = invalidNumber

		// act.
		_, err := parseAzureServiceBusMetadata(fakeMetaData)

		// assert.
		assert.Error(t, err)
		assertValidErrorMessage(t, err)
	})

	t.Run("missing optional lockRenewalInSec", func(t *testing.T) {
		fakeProperties := getFakeProperties()

		fakeMetaData := pubsub.Metadata{
			Properties: fakeProperties,
		}
		fakeMetaData.Properties[lockRenewalInSec] = ""

		// act.
		m, err := parseAzureServiceBusMetadata(fakeMetaData)

		// assert.
		assert.Equal(t, 20, m.LockRenewalInSec)
		assert.Nil(t, err)
	})

	t.Run("invalid optional lockRenewalInSec", func(t *testing.T) {
		fakeProperties := getFakeProperties()

		fakeMetaData := pubsub.Metadata{
			Properties: fakeProperties,
		}
		fakeMetaData.Properties[lockRenewalInSec] = invalidNumber

		// act.
		_, err := parseAzureServiceBusMetadata(fakeMetaData)

		// assert.
		assert.Error(t, err)
		assertValidErrorMessage(t, err)
	})

	t.Run("missing optional maxActiveMessages", func(t *testing.T) {
		fakeProperties := getFakeProperties()

		fakeMetaData := pubsub.Metadata{
			Properties: fakeProperties,
		}
		fakeMetaData.Properties[maxActiveMessages] = ""

		// act.
		m, err := parseAzureServiceBusMetadata(fakeMetaData)

		// assert.
		assert.Equal(t, 10000, m.MaxActiveMessages)
		assert.Nil(t, err)
	})

	t.Run("invalid optional maxActiveMessages", func(t *testing.T) {
		fakeProperties := getFakeProperties()

		fakeMetaData := pubsub.Metadata{
			Properties: fakeProperties,
		}
		fakeMetaData.Properties[maxActiveMessages] = invalidNumber

		// act.
		_, err := parseAzureServiceBusMetadata(fakeMetaData)

		// assert.
		assert.Error(t, err)
		assertValidErrorMessage(t, err)
	})

	t.Run("missing optional maxActiveMessagesRecoveryInSec", func(t *testing.T) {
		fakeProperties := getFakeProperties()

		fakeMetaData := pubsub.Metadata{
			Properties: fakeProperties,
		}
		fakeMetaData.Properties[maxActiveMessagesRecoveryInSec] = ""

		// act.
		m, err := parseAzureServiceBusMetadata(fakeMetaData)

		// assert.
		assert.Equal(t, 2, m.MaxActiveMessagesRecoveryInSec)
		assert.Nil(t, err)
	})

	t.Run("invalid optional maxActiveMessagesRecoveryInSec", func(t *testing.T) {
		fakeProperties := getFakeProperties()

		fakeMetaData := pubsub.Metadata{
			Properties: fakeProperties,
		}
		fakeMetaData.Properties[maxActiveMessagesRecoveryInSec] = invalidNumber

		// act.
		_, err := parseAzureServiceBusMetadata(fakeMetaData)

		// assert.
		assert.Error(t, err)
		assertValidErrorMessage(t, err)
	})

	t.Run("missing optional maxReconnectionAttempts", func(t *testing.T) {
		fakeProperties := getFakeProperties()

		fakeMetaData := pubsub.Metadata{
			Properties: fakeProperties,
		}
		fakeMetaData.Properties[maxReconnectionAttempts] = ""

		// act.
		m, err := parseAzureServiceBusMetadata(fakeMetaData)

		// assert.
		assert.Equal(t, 30, m.MaxReconnectionAttempts)
		assert.Nil(t, err)
	})

	t.Run("invalid optional maxReconnectionAttempts", func(t *testing.T) {
		fakeProperties := getFakeProperties()

		fakeMetaData := pubsub.Metadata{
			Properties: fakeProperties,
		}
		fakeMetaData.Properties[maxReconnectionAttempts] = invalidNumber

		// act.
		_, err := parseAzureServiceBusMetadata(fakeMetaData)

		// assert.
		assert.Error(t, err)
		assertValidErrorMessage(t, err)
	})

	t.Run("missing optional connectionRecoveryInSec", func(t *testing.T) {
		fakeProperties := getFakeProperties()

		fakeMetaData := pubsub.Metadata{
			Properties: fakeProperties,
		}
		fakeMetaData.Properties[connectionRecoveryInSec] = ""

		// act.
		m, err := parseAzureServiceBusMetadata(fakeMetaData)

		// assert.
		assert.Equal(t, 2, m.ConnectionRecoveryInSec)
		assert.Nil(t, err)
	})

	t.Run("invalid optional connectionRecoveryInSec", func(t *testing.T) {
		fakeProperties := getFakeProperties()

		fakeMetaData := pubsub.Metadata{
			Properties: fakeProperties,
		}
		fakeMetaData.Properties[connectionRecoveryInSec] = invalidNumber

		// act.
		_, err := parseAzureServiceBusMetadata(fakeMetaData)

		// assert.
		assert.Error(t, err)
		assertValidErrorMessage(t, err)
	})

	t.Run("missing nullable maxDeliveryCount", func(t *testing.T) {
		fakeProperties := getFakeProperties()

		fakeMetaData := pubsub.Metadata{
			Properties: fakeProperties,
		}
		fakeMetaData.Properties[maxDeliveryCount] = ""

		// act.
		m, err := parseAzureServiceBusMetadata(fakeMetaData)

		// assert.
		assert.Nil(t, m.MaxDeliveryCount)
		assert.Nil(t, err)
	})

	t.Run("invalid nullable maxDeliveryCount", func(t *testing.T) {
		fakeProperties := getFakeProperties()

		fakeMetaData := pubsub.Metadata{
			Properties: fakeProperties,
		}
		fakeMetaData.Properties[maxDeliveryCount] = invalidNumber

		// act.
		_, err := parseAzureServiceBusMetadata(fakeMetaData)

		// assert.
		assert.Error(t, err)
		assertValidErrorMessage(t, err)
	})

	t.Run("missing nullable defaultMessageTimeToLiveInSec", func(t *testing.T) {
		fakeProperties := getFakeProperties()

		fakeMetaData := pubsub.Metadata{
			Properties: fakeProperties,
		}
		fakeMetaData.Properties[defaultMessageTimeToLiveInSec] = ""

		// act.
		m, err := parseAzureServiceBusMetadata(fakeMetaData)

		// assert.
		assert.Nil(t, m.DefaultMessageTimeToLiveInSec)
		assert.Nil(t, err)
	})

	t.Run("invalid nullable defaultMessageTimeToLiveInSec", func(t *testing.T) {
		fakeProperties := getFakeProperties()

		fakeMetaData := pubsub.Metadata{
			Properties: fakeProperties,
		}
		fakeMetaData.Properties[defaultMessageTimeToLiveInSec] = invalidNumber

		// act.
		_, err := parseAzureServiceBusMetadata(fakeMetaData)

		// assert.
		assert.Error(t, err)
		assertValidErrorMessage(t, err)
	})

	t.Run("missing nullable autoDeleteOnIdleInSec", func(t *testing.T) {
		fakeProperties := getFakeProperties()

		fakeMetaData := pubsub.Metadata{
			Properties: fakeProperties,
		}
		fakeMetaData.Properties[autoDeleteOnIdleInSec] = ""

		// act.
		m, err := parseAzureServiceBusMetadata(fakeMetaData)

		// assert.
		assert.Nil(t, m.AutoDeleteOnIdleInSec)
		assert.Nil(t, err)
	})

	t.Run("invalid nullable autoDeleteOnIdleInSec", func(t *testing.T) {
		fakeProperties := getFakeProperties()

		fakeMetaData := pubsub.Metadata{
			Properties: fakeProperties,
		}
		fakeMetaData.Properties[autoDeleteOnIdleInSec] = invalidNumber

		// act.
		_, err := parseAzureServiceBusMetadata(fakeMetaData)

		// assert.
		assert.Error(t, err)
		assertValidErrorMessage(t, err)
	})

	t.Run("missing nullable lockDurationInSec", func(t *testing.T) {
		fakeProperties := getFakeProperties()

		fakeMetaData := pubsub.Metadata{
			Properties: fakeProperties,
		}
		fakeMetaData.Properties[lockDurationInSec] = ""

		// act.
		m, err := parseAzureServiceBusMetadata(fakeMetaData)

		// assert.
		assert.Nil(t, m.LockDurationInSec)
		assert.Nil(t, err)
	})

	t.Run("invalid nullable lockDurationInSec", func(t *testing.T) {
		fakeProperties := getFakeProperties()

		fakeMetaData := pubsub.Metadata{
			Properties: fakeProperties,
		}
		fakeMetaData.Properties[lockDurationInSec] = invalidNumber

		// act.
		_, err := parseAzureServiceBusMetadata(fakeMetaData)

		// assert.
		assert.Error(t, err)
		assertValidErrorMessage(t, err)
	})

	t.Run("missing nullable maxConcurrentHandlers", func(t *testing.T) {
		fakeProperties := getFakeProperties()

		fakeMetaData := pubsub.Metadata{
			Properties: fakeProperties,
		}
		fakeMetaData.Properties[maxConcurrentHandlers] = ""

		// act.
		m, err := parseAzureServiceBusMetadata(fakeMetaData)

		// assert.
		assert.Nil(t, m.MaxConcurrentHandlers)
		assert.Nil(t, err)
	})

	t.Run("invalid nullable maxConcurrentHandlers", func(t *testing.T) {
		fakeProperties := getFakeProperties()

		fakeMetaData := pubsub.Metadata{
			Properties: fakeProperties,
		}
		fakeMetaData.Properties[maxConcurrentHandlers] = invalidNumber

		// act.
		_, err := parseAzureServiceBusMetadata(fakeMetaData)

		// assert.
		assert.Error(t, err)
		assertValidErrorMessage(t, err)
	})

	t.Run("missing nullable prefetchCount", func(t *testing.T) {
		fakeProperties := getFakeProperties()

		fakeMetaData := pubsub.Metadata{
			Properties: fakeProperties,
		}
		fakeMetaData.Properties[prefetchCount] = ""

		// act.
		m, err := parseAzureServiceBusMetadata(fakeMetaData)

		// assert.
		assert.Nil(t, m.PrefetchCount)
		assert.Nil(t, err)
	})

	t.Run("invalid nullable prefetchCount", func(t *testing.T) {
		fakeProperties := getFakeProperties()

		fakeMetaData := pubsub.Metadata{
			Properties: fakeProperties,
		}
		fakeMetaData.Properties[prefetchCount] = invalidNumber

		// act.
		_, err := parseAzureServiceBusMetadata(fakeMetaData)

		// assert.
		assert.Error(t, err)
		assertValidErrorMessage(t, err)
	})
}

func assertValidErrorMessage(t *testing.T, err error) {
	assert.Contains(t, err.Error(), errorMessagePrefix)
}
