// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package eventhubs

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/kit/logger"
)

var testLogger = logger.NewLogger("test")

func TestParseEventHubsMetadata(t *testing.T) {
	t.Run("test valid connectionString configuration", func(t *testing.T) {
		props := map[string]string{"connectionString": "fake"}

		metadata := pubsub.Metadata{Properties: props}
		m, err := parseEventHubsMetadata(metadata)

		assert.NoError(t, err)
		assert.Equal(t, "fake", m.ConnectionString)
	})

	t.Run("test namespace given", func(t *testing.T) {
		props := map[string]string{"eventHubNamespace": "fake"}

		metadata := pubsub.Metadata{Properties: props}
		m, err := parseEventHubsMetadata(metadata)

		assert.NoError(t, err)
		assert.Equal(t, "fake", m.EventHubNamespace)
	})

	t.Run("test both connectionString and eventHubNamespace given", func(t *testing.T) {
		props := map[string]string{"connectionString": "fake", "eventHubNamespace": "fake"}

		metadata := pubsub.Metadata{Properties: props}
		_, err := parseEventHubsMetadata(metadata)

		assert.Error(t, err)
		assert.Equal(t, bothConnectionStringNamespaceErrorMsg, err.Error())
	})

	t.Run("test missing metadata", func(t *testing.T) {
		props := map[string]string{}

		metadata := pubsub.Metadata{Properties: props}
		_, err := parseEventHubsMetadata(metadata)

		assert.Error(t, err)
		assert.Equal(t, missingConnectionStringNamespaceErrorMsg, err.Error())
	})
}

func TestValidateSubscriptionAttributes(t *testing.T) {
	t.Run("test valid configuration", func(t *testing.T) {
		props := map[string]string{"connectionString": "fake", "consumerID": "fake", "storageAccountName": "account", "storageAccountKey": "key", "storageContainerName": "container"}

		metadata := pubsub.Metadata{Properties: props}
		m, err := parseEventHubsMetadata(metadata)

		assert.NoError(t, err)
		aeh := &AzureEventHubs{logger: testLogger, metadata: m}
		assert.Equal(t, m.ConnectionString, "fake")
		assert.Equal(t, m.StorageAccountName, "account")
		assert.Equal(t, m.StorageAccountKey, "key")
		assert.Equal(t, m.StorageContainerName, "container")
		assert.Equal(t, m.ConsumerGroup, "fake")

		err = aeh.validateSubscriptionAttributes()

		assert.NoError(t, err)
	})

	type invalidConfigTestCase struct {
		name   string
		config map[string]string
		errMsg string
	}
	invalidConfigTestCases := []invalidConfigTestCase{
		{
			"missing consumerID",
			map[string]string{"connectionString": "fake", "storageAccountName": "account", "storageAccountKey": "key", "storageContainerName": "container"},
			missingConsumerIDErrorMsg,
		},
		{
			"missing storageAccountName",
			map[string]string{"consumerID": "fake", "connectionString": "fake", "storageAccountKey": "key", "storageContainerName": "container"},
			missingStorageAccountNameErrorMsg,
		},
		{
			"missing storageAccountKey",
			map[string]string{"consumerID": "fake", "connectionString": "fake", "storageAccountName": "name", "storageContainerName": "container"},
			missingStorageAccountKeyErrorMsg,
		},
		{
			"missing storageContainerName",
			map[string]string{"consumerID": "fake", "connectionString": "fake", "storageAccountName": "name", "storageAccountKey": "key"},
			missingStorageContainerNameErrorMsg,
		},
	}

	for _, c := range invalidConfigTestCases {
		t.Run(c.name, func(t *testing.T) {
			metadata := pubsub.Metadata{Properties: c.config}
			m, err := parseEventHubsMetadata(metadata)
			aeh := &AzureEventHubs{logger: testLogger, metadata: m}
			require.NoError(t, err)
			err = aeh.validateSubscriptionAttributes()
			assert.Error(t, err)
			assert.Equal(t, c.errMsg, err.Error())
		})
	}
}

func TestValidateEnitityManagementMetadata(t *testing.T) {
	t.Run("test valid configuration", func(t *testing.T) {
		props := map[string]string{"eventHubNamespace": "fake", "messageRetentionInDays": "2", "partitionCount": "3", "resourceGroupName": "rg", "subscriptionID": "id"}

		metadata := pubsub.Metadata{Properties: props}
		m, err := parseEventHubsMetadata(metadata)

		require.NoError(t, err)
		aeh := &AzureEventHubs{logger: testLogger, metadata: m}
		assert.Equal(t, "fake", m.EventHubNamespace)
		assert.Equal(t, int32(2), m.MessageRetentionInDays)
		assert.Equal(t, int32(3), m.PartitionCount)

		err = aeh.validateEnitityManagementMetadata()
		assert.NoError(t, err)
		assert.Equal(t, int32(2), m.MessageRetentionInDays)
		assert.Equal(t, int32(3), m.PartitionCount)
		assert.Equal(t, "rg", m.ResourceGroupName)
		assert.Equal(t, "id", m.SubscriptionID)
	})

	t.Run("test valid configuration", func(t *testing.T) {
		props := map[string]string{"eventHubNamespace": "fake", "resourceGroupName": "rg", "subscriptionID": "id"}

		metadata := pubsub.Metadata{Properties: props}
		m, err := parseEventHubsMetadata(metadata)

		require.NoError(t, err)
		aeh := &AzureEventHubs{logger: testLogger, metadata: m}
		assert.Equal(t, "fake", m.EventHubNamespace)
		assert.Equal(t, int32(0), m.MessageRetentionInDays)
		assert.Equal(t, int32(0), m.PartitionCount)

		err = aeh.validateEnitityManagementMetadata()
		assert.NoError(t, err)
		assert.Equal(t, int32(1), m.MessageRetentionInDays)
		assert.Equal(t, int32(1), m.PartitionCount)
		assert.Equal(t, "rg", m.ResourceGroupName)
		assert.Equal(t, "id", m.SubscriptionID)
	})

	type invalidConfigTestCase struct {
		name                   string
		config                 map[string]string
		messageRetentionInDays int32
		partitionCount         int32
		errMsg                 string
	}
	invalidConfigTestCases := []invalidConfigTestCase{
		{
			"negative message rentention days",
			map[string]string{"eventHubNamespace": "fake", "messageRetentionInDays": "-2", "resourceGroupName": "rg", "subscriptionID": "id"},
			defaultMessageRetentionInDays,
			defaultPartitionCount,
			"",
		},
		{
			"more than max message rentention days",
			map[string]string{"eventHubNamespace": "fake", "messageRetentionInDays": "91", "resourceGroupName": "rg", "subscriptionID": "id"},
			defaultMessageRetentionInDays,
			defaultPartitionCount,
			"",
		},
		{
			"negative partition count",
			map[string]string{"eventHubNamespace": "fake", "partitionCount": "-2", "resourceGroupName": "rg", "subscriptionID": "id"},
			defaultMessageRetentionInDays,
			defaultPartitionCount,
			"",
		},
		{
			"more than max partition count",
			map[string]string{"eventHubNamespace": "fake", "partitionCount": "1030", "resourceGroupName": "rg", "subscriptionID": "id"},
			defaultMessageRetentionInDays,
			defaultPartitionCount,
			"",
		},
		{
			"missingResourceGroupName",
			map[string]string{"eventHubNamespace": "fake", "subscriptionID": "id"},
			defaultMessageRetentionInDays,
			defaultPartitionCount,
			missingResourceGroupNameMsg,
		},
		{
			"missingSubscriptionID",
			map[string]string{"eventHubNamespace": "fake", "resourceGroupName": "id"},
			defaultMessageRetentionInDays,
			defaultPartitionCount,
			missingSubscriptionIDMsg,
		},
	}

	for _, c := range invalidConfigTestCases {
		t.Run(c.name, func(t *testing.T) {
			metadata := pubsub.Metadata{Properties: c.config}
			m, err := parseEventHubsMetadata(metadata)
			aeh := &AzureEventHubs{logger: testLogger, metadata: m}
			require.NoError(t, err)
			err = aeh.validateEnitityManagementMetadata()
			if c.errMsg != "" {
				assert.Error(t, err)
				assert.Equal(t, c.errMsg, err.Error())
			} else {
				assert.NoError(t, err)
			}
			assert.Equal(t, c.messageRetentionInDays, aeh.metadata.MessageRetentionInDays)
			assert.Equal(t, c.partitionCount, aeh.metadata.PartitionCount)
		})
	}
}

func TestGetStoragePrefixString(t *testing.T) {
	props := map[string]string{"connectionString": "fake", "consumerID": "test"}

	metadata := pubsub.Metadata{Properties: props}
	m, err := parseEventHubsMetadata(metadata)

	require.NoError(t, err)

	aeh := &AzureEventHubs{logger: testLogger, metadata: m}

	actual := aeh.getStoragePrefixString("topic")

	assert.Equal(t, "dapr-topic-test-", actual)
}

func TestValidateAndGetHubName(t *testing.T) {
	t.Run("valid connectionString with hub name", func(t *testing.T) {
		connectionString := "Endpoint=sb://fake.servicebus.windows.net/;SharedAccessKeyName=fakeKey;SharedAccessKey=key;EntityPath=testHub"
		h, err := validateAndGetHubName(connectionString)
		assert.NoError(t, err)
		assert.Equal(t, "testHub", h)
	})

	t.Run("valid connectionString without hub name", func(t *testing.T) {
		connectionString := "Endpoint=sb://fake.servicebus.windows.net/;SharedAccessKeyName=fakeKey;SharedAccessKey=key"
		h, err := validateAndGetHubName(connectionString)
		assert.NoError(t, err)
		assert.Empty(t, h)
	})

	t.Run("invalid connectionString ", func(t *testing.T) {
		connectionString := "Endpoint=sb://fake.servicebus.windows.net/;ShareKeyName=fakeKey;SharedAccessKey=key"
		_, err := validateAndGetHubName(connectionString)
		assert.Error(t, err)
	})
}

func TestConstructConnectionStringFromTopic(t *testing.T) {
	t.Run("valid connectionString without hub name", func(t *testing.T) {
		connectionString := "Endpoint=sb://fake.servicebus.windows.net/;SharedAccessKeyName=fakeKey;SharedAccessKey=key"
		topic := "testHub"

		aeh := &AzureEventHubs{logger: testLogger, metadata: &azureEventHubsMetadata{ConnectionString: connectionString}}

		c, err := aeh.constructConnectionStringFromTopic(topic)
		assert.NoError(t, err)
		assert.Equal(t, connectionString+";EntityPath=testHub", c)
	})
	t.Run("valid connectionString with hub name", func(t *testing.T) {
		connectionString := "Endpoint=sb://fake.servicebus.windows.net/;SharedAccessKeyName=fakeKey;SharedAccessKey=key;EntityPath=testHub"
		topic := "testHub"

		aeh := &AzureEventHubs{logger: testLogger, metadata: &azureEventHubsMetadata{ConnectionString: connectionString}}

		c, err := aeh.constructConnectionStringFromTopic(topic)
		assert.NoError(t, err)
		assert.Equal(t, connectionString, c)
	})
	t.Run("invalid connectionString with hub name", func(t *testing.T) {
		connectionString := "Endpoint=sb://fake.servicebus.windows.net/;SharedAccessKeyName=fakeKey;ShareKey=key;EntityPath=testHub"
		topic := "testHub"

		aeh := &AzureEventHubs{logger: testLogger, metadata: &azureEventHubsMetadata{ConnectionString: connectionString}}

		c, err := aeh.constructConnectionStringFromTopic(topic)
		assert.Error(t, err)
		assert.Equal(t, "", c)
	})
	t.Run("valid connectionString with different hub name", func(t *testing.T) {
		connectionString := "Endpoint=sb://fake.servicebus.windows.net/;SharedAccessKeyName=fakeKey;SharedAccessKey=key;EntityPath=testHub"
		topic := "differentHub"

		aeh := &AzureEventHubs{logger: testLogger, metadata: &azureEventHubsMetadata{ConnectionString: connectionString}}

		c, err := aeh.constructConnectionStringFromTopic(topic)
		assert.Error(t, err)
		assert.Equal(t, (fmt.Sprintf(differentTopicConnectionStringErrorTmpl, topic)), err.Error())
		assert.Equal(t, "", c)
	})
}
