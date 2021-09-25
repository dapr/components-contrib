package rocketmq

import (
	"github.com/dapr/components-contrib/pubsub"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestMetaDataDecode(t *testing.T) { //nolint:paralleltest
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