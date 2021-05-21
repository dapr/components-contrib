// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package rocketmq_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/bindings/alicloud/rocketmq"
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
		"topics":        "defaultTopic1,defaultTopic2",
	}
	var settings rocketmq.Settings
	err := settings.Decode(props)
	require.NoError(t, err)

	assert.Equal(t, "http", settings.AccessProto)
	assert.Equal(t, "**", settings.AccessKey)
	assert.Equal(t, "***", settings.SecretKey)
	assert.Equal(t, "http://test.endpoint", settings.Endpoint)
	assert.Equal(t, "defaultGroup", settings.ConsumerGroup)
	assert.Equal(t, "defaultNamespace", settings.InstanceID)
	assert.Equal(t, "defaultTopic1,defaultTopic2", settings.Topics.ToString())
}
