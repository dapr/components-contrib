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
		fakeProperties[keyTimeoutInSec] = ""

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
		fakeProperties[keyHandlerTimeoutInSec] = ""

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
		fakeProperties[keyLockRenewalInSec] = ""

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
		fakeProperties[keyMaxRetriableErrorsPerSec] = ""

		// act.
		m, err := ParseMetadata(fakeProperties, nil, 0)

		// assert.
		assert.Equal(t, defaultMaxRetriableErrorsPerSec, m.MaxRetriableErrorsPerSec)
		assert.Nil(t, err)
	})

	t.Run("invalid optional maxRetriableErrorsPerSec", func(t *testing.T) {
		// NaN: Not a Number
		fakeProperties := getFakeProperties()
		fakeProperties[keyMaxRetriableErrorsPerSec] = invalidNumber

		// act.
		_, err := ParseMetadata(fakeProperties, nil, 0)

		// assert.
		assert.Error(t, err)

		// Negative number
		fakeProperties = getFakeProperties()
		fakeProperties[keyMaxRetriableErrorsPerSec] = "-1"

		// act.
		_, err = ParseMetadata(fakeProperties, nil, 0)

		// assert.
		assert.Error(t, err)
	})

	t.Run("missing optional maxActiveMessages binding", func(t *testing.T) {
		fakeProperties := getFakeProperties()
		fakeProperties[keyMaxActiveMessages] = ""

		// act.
		m, err := ParseMetadata(fakeProperties, nil, MetadataModeBinding)

		// assert.
		assert.Equal(t, defaultMaxActiveMessagesBinding, m.MaxActiveMessages)
		assert.Nil(t, err)
	})

	t.Run("missing optional maxActiveMessages pubsub", func(t *testing.T) {
		fakeProperties := getFakeProperties()
		fakeProperties[keyMaxActiveMessages] = ""

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
		fakeProperties[keyMaxConnectionRecoveryInSec] = ""

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
		fakeProperties[keyMinConnectionRecoveryInSec] = ""

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
		fakeProperties[keyMaxConcurrentHandlers] = ""

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
		fakeProperties[keyMaxDeliveryCount] = ""

		// act.
		m, err := ParseMetadata(fakeProperties, nil, 0)

		// assert.
		assert.Nil(t, m.MaxDeliveryCount)
		assert.Nil(t, err)
	})

	t.Run("invalid nullable maxDeliveryCount", func(t *testing.T) {
		fakeProperties := getFakeProperties()
		fakeProperties[keyMaxDeliveryCount] = invalidNumber

		// act.
		_, err := ParseMetadata(fakeProperties, nil, 0)

		// assert.
		assert.Error(t, err)
	})

	t.Run("missing nullable defaultMessageTimeToLiveInSec", func(t *testing.T) {
		fakeProperties := getFakeProperties()
		fakeProperties[keyDefaultMessageTimeToLiveInSec] = ""

		// act.
		m, err := ParseMetadata(fakeProperties, nil, 0)

		// assert.
		assert.Nil(t, m.DefaultMessageTimeToLiveInSec)
		assert.Nil(t, err)
	})

	t.Run("invalid nullable defaultMessageTimeToLiveInSec", func(t *testing.T) {
		fakeProperties := getFakeProperties()
		fakeProperties[keyDefaultMessageTimeToLiveInSec] = invalidNumber

		// act.
		_, err := ParseMetadata(fakeProperties, nil, 0)

		// assert.
		assert.Error(t, err)
	})

	t.Run("missing nullable autoDeleteOnIdleInSec", func(t *testing.T) {
		fakeProperties := getFakeProperties()
		fakeProperties[keyAutoDeleteOnIdleInSec] = ""

		// act.
		m, err := ParseMetadata(fakeProperties, nil, 0)

		// assert.
		assert.Nil(t, m.AutoDeleteOnIdleInSec)
		assert.Nil(t, err)
	})

	t.Run("invalid nullable autoDeleteOnIdleInSec", func(t *testing.T) {
		fakeProperties := getFakeProperties()
		fakeProperties[keyAutoDeleteOnIdleInSec] = invalidNumber

		// act.
		_, err := ParseMetadata(fakeProperties, nil, 0)

		// assert.
		assert.Error(t, err)
	})

	t.Run("missing nullable lockDurationInSec", func(t *testing.T) {
		fakeProperties := getFakeProperties()
		fakeProperties[keyLockDurationInSec] = ""

		// act.
		m, err := ParseMetadata(fakeProperties, nil, 0)

		// assert.
		assert.Nil(t, m.LockDurationInSec)
		assert.Nil(t, err)
	})

	t.Run("invalid nullable lockDurationInSec", func(t *testing.T) {
		fakeProperties := getFakeProperties()
		fakeProperties[keyLockDurationInSec] = invalidNumber

		// act.
		_, err := ParseMetadata(fakeProperties, nil, 0)

		// assert.
		assert.Error(t, err)
	})
}
