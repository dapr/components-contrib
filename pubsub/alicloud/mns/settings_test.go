package mns

import (
	"testing"

	ali_mns "github.com/aliyun/aliyun-mns-go-sdk"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSettingsDecode(t *testing.T) { //nolint:paralleltest
	props := map[string]string{
		"url":             "http://mns.test",
		"accessKeyId":     "testID",
		"accessKeySecret": "testKeySecret",
		"mnsMode":         MnsModeQueue,
		"token":           "test token",
		"timeoutSecond":   "202020",
		"contentType":     "application/octet-stream",
	}

	var settings Settings
	err := settings.Decode(props)
	require.NoError(t, err)

	assert.Equal(t, "http://mns.test", settings.Url)
	assert.Equal(t, "testID", settings.AccessKeyId)
	assert.Equal(t, "testKeySecret", settings.AccessKeySecret)
	assert.Equal(t, MnsModeQueue, settings.MnsMode)
	assert.Equal(t, "test token", settings.Token)
	assert.Equal(t, int64(202020), settings.TimeoutSecond)
	assert.Equal(t, "application/octet-stream", settings.ContentType)
}

func TestRequestMetaDataDecode(t *testing.T) { //nolint:paralleltest
	props := map[string]string{
		"queueName":                     "test",
		"queueDelaySeconds":             "10",
		"queueMaxMessageSize":           "256",
		"queueMessageRetentionPeriod":   "500",
		"queueVisibilityTimeout":        "10",
		"queuePollingWaitSeconds":       "20",
		"queueSlices":                   "3",
		"topicName":                     "test",
		"topicMaxMessageSize":           "256",
		"topicLoggingEnabled":           "true",
		"subscriptionName":              "subscriptionTest",
		"subscriptionNotifyContentType": string(ali_mns.SIMPLIFIED),
	}

	var metaData RequestMetaData
	err := metaData.Decode(props)
	require.NoError(t, err)

	assert.Equal(t, "test", metaData.QueueName)
	assert.Equal(t, int32(10), metaData.QueueDelaySeconds)
	assert.Equal(t, int32(256), metaData.QueueMaxMessageSize)
	assert.Equal(t, int32(500), metaData.QueueMessageRetentionPeriod)
	assert.Equal(t, int32(10), metaData.QueueVisibilityTimeout)
	assert.Equal(t, int32(20), metaData.QueuePollingWaitSeconds)
	assert.Equal(t, int32(3), metaData.QueueSlices)
	assert.Equal(t, int32(256), metaData.TopicMaxMessageSize)
	assert.Equal(t, true, metaData.TopicLoggingEnabled)
	assert.Equal(t, "subscriptionTest", metaData.SubscriptionName)
	assert.Equal(t, ali_mns.SIMPLIFIED, metaData.SubscriptionNotifyContentFormat)
}

func TestSettingsDecodeDefault(t *testing.T) { //nolint:paralleltest
	props := map[string]string{}

	var settings Settings
	err := settings.Decode(props)
	require.NoError(t, err)

	assert.Equal(t, int64(35), settings.TimeoutSecond)
}

func TestRequestMetaDataDecodeDefault(t *testing.T) { //nolint:paralleltest
	props := map[string]string{}

	var metaData RequestMetaData
	err := metaData.Decode(props)
	require.NoError(t, err)

	assert.Equal(t, "defaultMnsQueue", metaData.QueueName)
	assert.Equal(t, int32(0), metaData.QueueDelaySeconds)
	assert.Equal(t, int32(65536), metaData.QueueMaxMessageSize)
	assert.Equal(t, int32(345600), metaData.QueueMessageRetentionPeriod)
	assert.Equal(t, int32(30), metaData.QueueVisibilityTimeout)
	assert.Equal(t, int32(0), metaData.QueuePollingWaitSeconds)
	assert.Equal(t, int32(65536), metaData.TopicMaxMessageSize)
	assert.Equal(t, false, metaData.TopicLoggingEnabled)
	assert.Equal(t, ali_mns.SIMPLIFIED, metaData.SubscriptionNotifyContentFormat)
}
