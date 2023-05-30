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

package servicebus

import (
	"testing"

	azservicebus "github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	"github.com/stretchr/testify/assert"
)

const invalidNumber = "invalid_number"

func getFakeProperties() map[string]string {
	return map[string]string{
		keyConnectionString:              "fakeConnectionString",
		keyNamespaceName:                 "",
		keyConsumerID:                    "fakeConId", // For topics only
		keyDisableEntityManagement:       "true",
		keyTimeoutInSec:                  "90",
		keyHandlerTimeoutInSec:           "30",
		keyMaxDeliveryCount:              "10",
		keyAutoDeleteOnIdleInSec:         "240",
		keyDefaultMessageTimeToLiveInSec: "2400",
		keyLockDurationInSec:             "120",
		keyLockRenewalInSec:              "15",
		keyMaxConcurrentHandlers:         "1",
		keyMaxActiveMessages:             "100",
		keyMinConnectionRecoveryInSec:    "5",
		keyMaxConnectionRecoveryInSec:    "600",
		keyMaxRetriableErrorsPerSec:      "50",
		keyQueueName:                     "myqueue", // For queue bindings only
	}
}

func TestParseServiceBusMetadata(t *testing.T) {
	t.Run("metadata is correct for pubsub topics", func(t *testing.T) {
		fakeProperties := getFakeProperties()

		// act.
		m, err := ParseMetadata(fakeProperties, nil, MetadataModeTopics)

		// assert.
		assert.NoError(t, err)
		assert.Equal(t, fakeProperties[keyConnectionString], m.ConnectionString)
		assert.Equal(t, fakeProperties[keyConsumerID], m.ConsumerID)

		assert.Equal(t, 90, m.TimeoutInSec)
		assert.Equal(t, true, m.DisableEntityManagement)
		assert.Equal(t, 30, m.HandlerTimeoutInSec)
		assert.NotNil(t, m.LockRenewalInSec)
		assert.Equal(t, 15, m.LockRenewalInSec)
		assert.NotNil(t, m.MaxActiveMessages)
		assert.Equal(t, 100, m.MaxActiveMessages)
		assert.NotNil(t, m.MinConnectionRecoveryInSec)
		assert.Equal(t, 5, m.MinConnectionRecoveryInSec)
		assert.NotNil(t, m.MaxConnectionRecoveryInSec)
		assert.Equal(t, 600, m.MaxConnectionRecoveryInSec)
		assert.Equal(t, 50, m.MaxRetriableErrorsPerSec)

		assert.NotNil(t, m.AutoDeleteOnIdleInSec)
		assert.Equal(t, 240, *m.AutoDeleteOnIdleInSec)
		assert.NotNil(t, m.MaxDeliveryCount)
		assert.Equal(t, int32(10), *m.MaxDeliveryCount)
		assert.NotNil(t, m.DefaultMessageTimeToLiveInSec)
		assert.Equal(t, 2400, *m.DefaultMessageTimeToLiveInSec)
		assert.NotNil(t, m.LockDurationInSec)
		assert.Equal(t, 120, *m.LockDurationInSec)
		assert.NotNil(t, m.MaxConcurrentHandlers)
		assert.Equal(t, 1, m.MaxConcurrentHandlers)
	})

	t.Run("metadata is correct for pubsub queues", func(t *testing.T) {
		fakeProperties := getFakeProperties()

		// act.
		m, err := ParseMetadata(fakeProperties, nil, 0)

		// assert.
		assert.NoError(t, err)
		assert.Equal(t, fakeProperties[keyConnectionString], m.ConnectionString)

		assert.Equal(t, 90, m.TimeoutInSec)
		assert.Equal(t, true, m.DisableEntityManagement)
		assert.Equal(t, 30, m.HandlerTimeoutInSec)
		assert.NotNil(t, m.LockRenewalInSec)
		assert.Equal(t, 15, m.LockRenewalInSec)
		assert.NotNil(t, m.MaxActiveMessages)
		assert.Equal(t, 100, m.MaxActiveMessages)
		assert.NotNil(t, m.MinConnectionRecoveryInSec)
		assert.Equal(t, 5, m.MinConnectionRecoveryInSec)
		assert.NotNil(t, m.MaxConnectionRecoveryInSec)
		assert.Equal(t, 600, m.MaxConnectionRecoveryInSec)
		assert.Equal(t, 50, m.MaxRetriableErrorsPerSec)

		assert.NotNil(t, m.AutoDeleteOnIdleInSec)
		assert.Equal(t, 240, *m.AutoDeleteOnIdleInSec)
		assert.NotNil(t, m.MaxDeliveryCount)
		assert.Equal(t, int32(10), *m.MaxDeliveryCount)
		assert.NotNil(t, m.DefaultMessageTimeToLiveInSec)
		assert.Equal(t, 2400, *m.DefaultMessageTimeToLiveInSec)
		assert.NotNil(t, m.LockDurationInSec)
		assert.Equal(t, 120, *m.LockDurationInSec)
		assert.NotNil(t, m.MaxConcurrentHandlers)
		assert.Equal(t, 1, m.MaxConcurrentHandlers)
	})

	t.Run("metadata is correct for binding queues", func(t *testing.T) {
		fakeProperties := getFakeProperties()

		// act.
		m, err := ParseMetadata(fakeProperties, nil, MetadataModeBinding)

		// assert.
		assert.NoError(t, err)
		assert.Equal(t, fakeProperties[keyConnectionString], m.ConnectionString)
		assert.Equal(t, fakeProperties[keyQueueName], m.QueueName)

		assert.Equal(t, 90, m.TimeoutInSec)
		assert.Equal(t, true, m.DisableEntityManagement)
		assert.Equal(t, 30, m.HandlerTimeoutInSec)
		assert.NotNil(t, m.LockRenewalInSec)
		assert.Equal(t, 15, m.LockRenewalInSec)
		assert.NotNil(t, m.MaxActiveMessages)
		assert.Equal(t, 100, m.MaxActiveMessages)
		assert.NotNil(t, m.MinConnectionRecoveryInSec)
		assert.Equal(t, 5, m.MinConnectionRecoveryInSec)
		assert.NotNil(t, m.MaxConnectionRecoveryInSec)
		assert.Equal(t, 600, m.MaxConnectionRecoveryInSec)
		assert.Equal(t, 50, m.MaxRetriableErrorsPerSec)

		assert.NotNil(t, m.AutoDeleteOnIdleInSec)
		assert.Equal(t, 240, *m.AutoDeleteOnIdleInSec)
		assert.NotNil(t, m.MaxDeliveryCount)
		assert.Equal(t, int32(10), *m.MaxDeliveryCount)
		assert.NotNil(t, m.DefaultMessageTimeToLiveInSec)
		assert.Equal(t, 2400, *m.DefaultMessageTimeToLiveInSec)
		assert.NotNil(t, m.LockDurationInSec)
		assert.Equal(t, 120, *m.LockDurationInSec)
		assert.NotNil(t, m.MaxConcurrentHandlers)
		assert.Equal(t, 1, m.MaxConcurrentHandlers)
	})

	t.Run("missing required connectionString or namespaceName", func(t *testing.T) {
		fakeProperties := getFakeProperties()
		fakeProperties[keyConnectionString] = ""
		fakeProperties[keyNamespaceName] = ""

		// act.
		m, err := ParseMetadata(fakeProperties, nil, 0)

		// assert.
		assert.Error(t, err)
		assert.Empty(t, m.ConnectionString)
	})

	t.Run("connectionString makes namespace optional", func(t *testing.T) {
		fakeProperties := getFakeProperties()
		fakeProperties[keyNamespaceName] = ""

		// act.
		m, err := ParseMetadata(fakeProperties, nil, 0)

		// assert.
		assert.NoError(t, err)
		assert.Equal(t, "fakeConnectionString", m.ConnectionString)
	})

	t.Run("namespace makes conectionString optional", func(t *testing.T) {
		fakeProperties := getFakeProperties()
		fakeProperties[keyNamespaceName] = "fakeNamespace"
		fakeProperties[keyConnectionString] = ""

		// act.
		m, err := ParseMetadata(fakeProperties, nil, 0)

		// assert.
		assert.NoError(t, err)
		assert.Equal(t, "fakeNamespace", m.NamespaceName)
	})

	t.Run("connectionString and namespace are mutually exclusive", func(t *testing.T) {
		fakeProperties := getFakeProperties()

		fakeProperties[keyNamespaceName] = "fakeNamespace"

		// act.
		_, err := ParseMetadata(fakeProperties, nil, 0)

		// assert.
		assert.Error(t, err)
	})

	t.Run("missing required consumerID in topics", func(t *testing.T) {
		fakeProperties := getFakeProperties()
		fakeProperties[keyConsumerID] = ""

		// act.
		m, err := ParseMetadata(fakeProperties, nil, MetadataModeTopics)

		// assert.
		assert.Error(t, err)
		assert.Empty(t, m.ConsumerID)
	})

	t.Run("missing required queueName in queue binding", func(t *testing.T) {
		fakeProperties := getFakeProperties()
		fakeProperties[keyQueueName] = ""

		// act.
		m, err := ParseMetadata(fakeProperties, nil, MetadataModeBinding)

		// assert.
		assert.Error(t, err)
		assert.Empty(t, m.QueueName)
	})

	t.Run("missing optional timeoutInSec", func(t *testing.T) {
		fakeProperties := getFakeProperties()
		delete(fakeProperties, keyTimeoutInSec)

		// act.
		m, err := ParseMetadata(fakeProperties, nil, 0)

		// assert.
		assert.Equal(t, defaultTimeoutInSec, m.TimeoutInSec)
		assert.Nil(t, err)
	})

	t.Run("invalid optional timeoutInSec", func(t *testing.T) {
		fakeProperties := getFakeProperties()
		fakeProperties[keyTimeoutInSec] = invalidNumber

		// act.
		_, err := ParseMetadata(fakeProperties, nil, 0)

		// assert.
		assert.Error(t, err)
	})

	t.Run("missing optional disableEntityManagement", func(t *testing.T) {
		fakeProperties := getFakeProperties()
		fakeProperties[keyDisableEntityManagement] = ""

		// act.
		m, err := ParseMetadata(fakeProperties, nil, 0)

		// assert.
		assert.Equal(t, false, m.DisableEntityManagement)
		assert.Nil(t, err)
	})

	t.Run("invalid optional disableEntityManagement", func(t *testing.T) {
		fakeProperties := getFakeProperties()
		fakeProperties[keyDisableEntityManagement] = "invalid_bool"

		// act.
		m, err := ParseMetadata(fakeProperties, nil, 0)

		// assert.
		assert.Equal(t, false, m.DisableEntityManagement)
		assert.Nil(t, err)
	})

	t.Run("missing optional handlerTimeoutInSec binding", func(t *testing.T) {
		fakeProperties := getFakeProperties()
		fakeProperties[keyHandlerTimeoutInSec] = ""

		// act.
		m, err := ParseMetadata(fakeProperties, nil, MetadataModeBinding)

		// assert.
		assert.Equal(t, defaultHandlerTimeoutInSecBinding, m.HandlerTimeoutInSec)
		assert.Nil(t, err)
	})

	t.Run("missing optional handlerTimeoutInSec pubsub", func(t *testing.T) {
		fakeProperties := getFakeProperties()
		delete(fakeProperties, keyHandlerTimeoutInSec)

		// act.
		m, err := ParseMetadata(fakeProperties, nil, 0)

		// assert.
		assert.Equal(t, defaultHandlerTimeoutInSecPubSub, m.HandlerTimeoutInSec)
		assert.Nil(t, err)
	})

	t.Run("invalid optional handlerTimeoutInSec", func(t *testing.T) {
		fakeProperties := getFakeProperties()
		fakeProperties[keyHandlerTimeoutInSec] = invalidNumber

		// act.
		_, err := ParseMetadata(fakeProperties, nil, 0)

		// assert.
		assert.Error(t, err)
	})

	t.Run("missing optional lockRenewalInSec", func(t *testing.T) {
		fakeProperties := getFakeProperties()
		delete(fakeProperties, keyLockRenewalInSec)

		// act.
		m, err := ParseMetadata(fakeProperties, nil, 0)

		// assert.
		assert.Equal(t, defaultLockRenewalInSec, m.LockRenewalInSec)
		assert.Nil(t, err)
	})

	t.Run("invalid optional lockRenewalInSec", func(t *testing.T) {
		fakeProperties := getFakeProperties()
		fakeProperties[keyLockRenewalInSec] = invalidNumber

		// act.
		_, err := ParseMetadata(fakeProperties, nil, 0)

		// assert.
		assert.Error(t, err)
	})

	t.Run("missing optional maxRetriableErrorsPerSec", func(t *testing.T) {
		fakeProperties := getFakeProperties()
		delete(fakeProperties, keyMaxRetriableErrorsPerSec)

		// act.
		m, err := ParseMetadata(fakeProperties, nil, 0)

		// assert.
		assert.Equal(t, defaultMaxRetriableErrorsPerSec, m.MaxRetriableErrorsPerSec)
		assert.Nil(t, err)
	})

	t.Run("invalid optional maxRetriableErrorsPerSec", func(t *testing.T) {
		// Negative number
		fakeProperties := getFakeProperties()
		fakeProperties[keyMaxRetriableErrorsPerSec] = "-1"

		// act.
		_, err := ParseMetadata(fakeProperties, nil, 0)

		// assert.
		assert.Error(t, err)
	})

	t.Run("missing optional maxActiveMessages binding", func(t *testing.T) {
		fakeProperties := getFakeProperties()
		delete(fakeProperties, keyMaxActiveMessages)

		// act.
		m, err := ParseMetadata(fakeProperties, nil, MetadataModeBinding)

		// assert.
		assert.Equal(t, defaultMaxActiveMessagesBinding, m.MaxActiveMessages)
		assert.Nil(t, err)
	})

	t.Run("missing optional maxActiveMessages pubsub", func(t *testing.T) {
		fakeProperties := getFakeProperties()
		delete(fakeProperties, keyMaxActiveMessages)

		// act.
		m, err := ParseMetadata(fakeProperties, nil, 0)

		// assert.
		assert.Equal(t, defaultMaxActiveMessagesPubSub, m.MaxActiveMessages)
		assert.Nil(t, err)
	})

	t.Run("invalid optional maxActiveMessages", func(t *testing.T) {
		fakeProperties := getFakeProperties()
		fakeProperties[keyMaxActiveMessages] = invalidNumber

		// act.
		_, err := ParseMetadata(fakeProperties, nil, 0)

		// assert.
		assert.Error(t, err)
	})

	t.Run("missing optional maxConnectionRecoveryInSec", func(t *testing.T) {
		fakeProperties := getFakeProperties()
		delete(fakeProperties, keyMaxConnectionRecoveryInSec)

		// act.
		m, err := ParseMetadata(fakeProperties, nil, 0)

		// assert.
		assert.Equal(t, defaultMaxConnectionRecoveryInSec, m.MaxConnectionRecoveryInSec)
		assert.Nil(t, err)
	})

	t.Run("invalid optional maxConnectionRecoveryInSec", func(t *testing.T) {
		fakeProperties := getFakeProperties()
		fakeProperties[keyMaxConnectionRecoveryInSec] = invalidNumber

		// act.
		_, err := ParseMetadata(fakeProperties, nil, 0)

		// assert.
		assert.Error(t, err)
	})

	t.Run("missing optional minConnectionRecoveryInSec", func(t *testing.T) {
		fakeProperties := getFakeProperties()
		delete(fakeProperties, keyMinConnectionRecoveryInSec)

		// act.
		m, err := ParseMetadata(fakeProperties, nil, 0)

		// assert.
		assert.Equal(t, defaultMinConnectionRecoveryInSec, m.MinConnectionRecoveryInSec)
		assert.Nil(t, err)
	})

	t.Run("invalid optional minConnectionRecoveryInSec", func(t *testing.T) {
		fakeProperties := getFakeProperties()
		fakeProperties[keyMinConnectionRecoveryInSec] = invalidNumber

		// act.
		_, err := ParseMetadata(fakeProperties, nil, 0)

		// assert.
		assert.Error(t, err)
	})

	t.Run("missing optional maxConcurrentHandlers", func(t *testing.T) {
		fakeProperties := getFakeProperties()
		delete(fakeProperties, keyMaxConcurrentHandlers)

		// act.
		m, err := ParseMetadata(fakeProperties, nil, 0)

		// assert.
		assert.Equal(t, 0, m.MaxConcurrentHandlers)
		assert.Nil(t, err)
	})

	t.Run("invalid optional maxConcurrentHandlers", func(t *testing.T) {
		fakeProperties := getFakeProperties()
		fakeProperties[keyMaxConcurrentHandlers] = invalidNumber

		// act.
		_, err := ParseMetadata(fakeProperties, nil, 0)

		// assert.
		assert.Error(t, err)
	})

	t.Run("missing nullable maxDeliveryCount", func(t *testing.T) {
		fakeProperties := getFakeProperties()
		delete(fakeProperties, keyMaxDeliveryCount)

		// act.
		m, err := ParseMetadata(fakeProperties, nil, 0)

		// assert.
		assert.Nil(t, m.MaxDeliveryCount)
		assert.Nil(t, err)
	})

	t.Run("missing nullable defaultMessageTimeToLiveInSec", func(t *testing.T) {
		fakeProperties := getFakeProperties()
		delete(fakeProperties, keyDefaultMessageTimeToLiveInSec)

		// act.
		m, err := ParseMetadata(fakeProperties, nil, 0)

		// assert.
		assert.Nil(t, m.DefaultMessageTimeToLiveInSec)
		assert.Nil(t, err)
	})

	t.Run("missing nullable autoDeleteOnIdleInSec", func(t *testing.T) {
		fakeProperties := getFakeProperties()
		delete(fakeProperties, keyAutoDeleteOnIdleInSec)

		// act.
		m, err := ParseMetadata(fakeProperties, nil, 0)

		// assert.
		assert.Nil(t, m.AutoDeleteOnIdleInSec)
		assert.Nil(t, err)
	})

	t.Run("missing nullable lockDurationInSec", func(t *testing.T) {
		fakeProperties := getFakeProperties()
		delete(fakeProperties, keyLockDurationInSec)

		// act.
		m, err := ParseMetadata(fakeProperties, nil, 0)

		// assert.
		assert.Nil(t, m.LockDurationInSec)
		assert.Nil(t, err)
	})

	t.Run("Test add system metadata: ScheduledEnqueueTimeUtc", func(t *testing.T) {
		msg := azservicebus.Message{}
		metadata := map[string]string{
			MessageKeyScheduledEnqueueTimeUtc: "2024-06-15T13:45:30.00000000Z",
		}
		parseErr := addMetadataToMessage(&msg, metadata)
		assert.NoError(t, parseErr)
		assert.Equal(t, int64(1718459130000000), msg.ScheduledEnqueueTime.UnixMicro())

		msg2 := azservicebus.Message{}
		metadata2 := map[string]string{
			MessageKeyScheduledEnqueueTimeUtc: "Sat, 15 Jun 2024 13:45:30 GMT",
		}
		parseErr2 := addMetadataToMessage(&msg2, metadata2)
		assert.NoError(t, parseErr2)
		assert.Equal(t, int64(1718459130000000), msg2.ScheduledEnqueueTime.UnixMicro())

		msg3 := azservicebus.Message{}
		metadata3 := map[string]string{
			MessageKeyScheduledEnqueueTimeUtc: "Sat 2024-06-15 12:13:14 UTC+4",
		}
		parseErr3 := addMetadataToMessage(&msg3, metadata3)
		assert.Error(t, parseErr3)
	})
}
