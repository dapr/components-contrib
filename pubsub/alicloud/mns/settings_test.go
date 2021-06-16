package mns

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSettingsDecode(t *testing.T) { //nolint:paralleltest
	props := map[string]string{
		"url":                    "http://mns.test",
		"accessKeyId":            "testID",
		"accessKeySecret":        "testKeySecret",
		"mnsMode":                MnsModeQueue,
		"queueName":              "test",
		"delaySeconds":           "10",
		"maxMessageSize":         "256",
		"messageRetentionPeriod": "500",
		"visibilityTimeout":      "10",
		"pollingWaitSeconds":     "20",
		"slices":                 "3",
		"loggingEnabled":         "true",
		"contentType":            "application/octet-stream",
	}

	var settings Settings
	err := settings.Decode(props)
	require.NoError(t, err)

	assert.Equal(t, "http://mns.test", settings.Url)
	assert.Equal(t, "testID", settings.AccessKeyId)
	assert.Equal(t, "testKeySecret", settings.AccessKeySecret)
	assert.Equal(t, MnsModeQueue, settings.MnsMode)
	assert.Equal(t, "test", settings.QueueName)
	assert.Equal(t, int32(10), settings.DelaySeconds)
	assert.Equal(t, int32(256), settings.MaxMessageSize)
	assert.Equal(t, int32(500), settings.MessageRetentionPeriod)
	assert.Equal(t, int32(10), settings.VisibilityTimeout)
	assert.Equal(t, int32(20), settings.PollingWaitSeconds)
	assert.Equal(t, int32(3), settings.Slices)
	assert.Equal(t, true, settings.LoggingEnabled)
	assert.Equal(t, "application/octet-stream", settings.ContentType)
}

func TestSettingsDecodeDefault(t *testing.T) { //nolint:paralleltest
	props := map[string]string{
		"url":             "http://mns.test",
		"accessKeyId":     "testID",
		"accessKeySecret": "testKeySecret",
		"mnsMode":         MnsModeQueue,
		"queueName":       "test",
	}

	var settings Settings
	err := settings.Decode(props)
	require.NoError(t, err)

	assert.Equal(t, "http://mns.test", settings.Url)
	assert.Equal(t, "testID", settings.AccessKeyId)
	assert.Equal(t, "testKeySecret", settings.AccessKeySecret)
	assert.Equal(t, MnsModeQueue, settings.MnsMode)
	assert.Equal(t, "test", settings.QueueName)
	assert.Equal(t, int32(0), settings.DelaySeconds)
	assert.Equal(t, int32(65536), settings.MaxMessageSize)
	assert.Equal(t, int32(345600), settings.MessageRetentionPeriod)
	assert.Equal(t, int32(30), settings.VisibilityTimeout)
	assert.Equal(t, int32(0), settings.PollingWaitSeconds)
	assert.Equal(t, int32(2), settings.Slices)
	assert.Equal(t, false, settings.LoggingEnabled)
}
