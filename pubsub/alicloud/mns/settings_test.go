package mns

import (
	"testing"

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
		"queueDelaySeconds":           "10",
		"queueMaxMessageSize":         "256",
		"queueMessageRetentionPeriod": "500",
		"queueVisibilityTimeout":      "10",
		"queuePollingWaitSeconds":     "20",
		"queueSlices":                 "3",
		"TopicMaxMessageSize":         "256",
		"TopicLoggingEnabled":         "true",
	}

	var metaData RequestMetaData
	err := metaData.Decode(props)
	require.NoError(t, err)

	assert.Equal(t, int32(10), metaData.QueueDelaySeconds)
	assert.Equal(t, int32(256), metaData.QueueMaxMessageSize)
	assert.Equal(t, int32(500), metaData.QueueMessageRetentionPeriod)
	assert.Equal(t, int32(10), metaData.QueueVisibilityTimeout)
	assert.Equal(t, int32(20), metaData.QueuePollingWaitSeconds)
	assert.Equal(t, int32(3), metaData.QueueSlices)
	assert.Equal(t, int32(256), metaData.TopicMaxMessageSize)
	assert.Equal(t, true, metaData.TopicLoggingEnabled)
}

func TestSettingsDecodeDefault(t *testing.T) { //nolint:paralleltest
	props := map[string]string{}

	var settings Settings
	err := settings.Decode(props)
	require.NoError(t, err)

	assert.Equal(t, int64(35), settings.TimeoutSecond)
}

func TestRequestMetaDataDecodeDefault(t *testing.T) { //nolint:paralleltest
	props := map[string]string{
		"url":             "http://mns.test",
		"accessKeyId":     "testID",
		"accessKeySecret": "testKeySecret",
		"mnsMode":         MnsModeQueue,
		"queueName":       "test",
	}

	var metaData RequestMetaData
	err := metaData.Decode(props)
	require.NoError(t, err)

	assert.Equal(t, int32(0), metaData.QueueDelaySeconds)
	assert.Equal(t, int32(65536), metaData.QueueMaxMessageSize)
	assert.Equal(t, int32(345600), metaData.QueueMessageRetentionPeriod)
	assert.Equal(t, int32(30), metaData.QueueVisibilityTimeout)
	assert.Equal(t, int32(0), metaData.QueuePollingWaitSeconds)
	assert.Equal(t, int32(2), metaData.QueueSlices)
	assert.Equal(t, int32(65536), metaData.TopicMaxMessageSize)
	assert.Equal(t, false, metaData.TopicLoggingEnabled)
}
