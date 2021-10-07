// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package rocketmq

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/pubsub"
)

func TestMetaDataDecode(t *testing.T) {
	props := map[string]string{
		"accessProto":   "http",
		"accessKey":     "**",
		"secretKey":     "***",
		"nameServer":    "http://test.nameserver",
		"consumerGroup": "defaultGroup",
		"nameSpace":     "defaultNamespace",
	}
	pubsubMeta := pubsub.Metadata{Properties: props}
	metaData, err := parseRocketMQMetaData(pubsubMeta)
	require.NoError(t, err)
	assert.Equal(t, "http", metaData.AccessProto)
	assert.Equal(t, "**", metaData.AccessKey)
	assert.Equal(t, "***", metaData.SecretKey)
	assert.Equal(t, "defaultGroup", metaData.ConsumerGroup)
}
