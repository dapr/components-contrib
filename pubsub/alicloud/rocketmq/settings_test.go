// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package rocketmq

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSettingsDecode(t *testing.T) { //nolint:paralleltest
	props := map[string]string{
		"accessProto":   "http",
		"accessKey":     "**",
		"secretKey":     "***",
		"endpoint":      "http://test.endpoint",
		"nameServer":    "http://test.nameserver",
		"consumerGroup": "defaultGroup",
		"instanceId":    "defaultNamespace",
		"topics":        "defaultTopic",
	}

	var settings Settings
	err := settings.Decode(props)
	require.NoError(t, err)

	assert.Equal(t, "http", settings.AccessProto)
	assert.Equal(t, "**", settings.AccessKey)
	assert.Equal(t, "***", settings.SecretKey)
	assert.Equal(t, "http://test.endpoint", settings.Endpoint)
	assert.Equal(t, "defaultGroup", settings.ConsumerGroup)
	assert.Equal(t, "defaultNamespace", settings.InstanceID)
	assert.Equal(t, "defaultTopic", settings.Topics)
}

func TestParseCommonMetadata(t *testing.T) { //nolint:paralleltest
	props := map[string]string{
		"accessProto":   "http",
		"accessKey":     "**",
		"secretKey":     "***",
		"endpoint":      "http://test.endpoint",
		"nameServer":    "http://test.nameserver",
		"consumerGroup": "defaultGroup",
		"instanceId":    "defaultNamespace",
		"topics":        "defaultTopic",
	}

	var settings Settings
	err := settings.Decode(props)
	require.NoError(t, err)

	b := settings.ToRocketMQMetadata()

	assert.Equal(t, "http", b.AccessProto)
	assert.Equal(t, "**", b.AccessKey)
	assert.Equal(t, "***", b.SecretKey)
	assert.Equal(t, "http://test.endpoint", b.Endpoint)
	assert.Equal(t, "defaultGroup", b.ConsumerGroup)
	assert.Equal(t, "defaultNamespace", b.InstanceId)
	assert.Equal(t, "defaultTopic", b.Topics)
}
