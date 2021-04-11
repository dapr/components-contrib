// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package rocketmq

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/dapr/pkg/logger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"os"
	"sync/atomic"
	"testing"
	"time"
)

func TestInit(t *testing.T) {
	t.Run("metadata is correct", func(t *testing.T) {
		m := pubsub.Metadata{}
		m.Properties = map[string]string{
			"accessProto":   "http",
			"accessKey":     "**",
			"secretKey":     "***",
			"endpoint":      "http://test.endpoint",
			"nameServer":    "http://test.nameserver",
			"consumerGroup": "defaultGroup",
			"instanceId":    "defaultNamespace",
			"topics":        "defaultTopic",
		}
		b, err := parseMetadata(m)
		assert.Nil(t, err)

		assert.Equal(t, "http", b.AccessProto)
		assert.Equal(t, "**", b.AccessKey)
		assert.Equal(t, "***", b.SecretKey)
		assert.Equal(t, "http://test.endpoint", b.Endpoint)
		assert.Equal(t, "defaultGroup", b.ConsumerGroup)
		assert.Equal(t, "defaultNamespace", b.InstanceId)
		assert.Equal(t, "defaultTopic", b.Topics)
	})
}

func TestPubSub(t *testing.T) {
	if !isLiveTest() {
		return
	}
	m := pubsub.Metadata{}
	m.Properties = getTestMetadata()
	r := NewRocketMQ(logger.NewLogger("test"))
	err := r.Init(m)
	require.NoError(t, err)

	var count int32
	handler := func(_ context.Context, msg *pubsub.NewMessage) error {
		type TopicEvent struct {
			Data interface{} `json:"data"`
		}
		var in TopicEvent
		err := json.NewDecoder(bytes.NewReader(msg.Data)).Decode(&in)
		require.NoError(t, err)

		require.Equal(t, "hello", in.Data.(string))

		atomic.AddInt32(&count, 1)
		return nil
	}

	err = r.Subscribe(pubsub.SubscribeRequest{Topic: "TOPIC_TEST"}, handler)
	require.NoError(t, err)

	time.Sleep(5 * time.Second)
	atomic.StoreInt32(&count, 0)
	err = r.Publish(&pubsub.PublishRequest{Topic: "TOPIC_TEST", Data: []byte("hello")})
	require.NoError(t, err)

	time.Sleep(10 * time.Second)
	for i := 0; i < 30; i++ {
		if atomic.LoadInt32(&count) > 0 {
			break
		}
		time.Sleep(time.Second)
	}
	assert.Equal(t, int32(1), atomic.LoadInt32(&count))
}

func isLiveTest() bool {
	return os.Getenv("RUN_LIVE_ROCKETMQ_TEST") == "true"
}

func getTestMetadata() map[string]string {
	return map[string]string{
		"accessProto":        "tcp",
		"nameServer":         "http://xx.mq-internet-access.mq-internet.aliyuncs.com:80",
		"consumerGroup":      "GID_DAPR-MQ-TCP",
		"topics":             "TOPIC_TEST",
		"accessKey":          "xx",
		"secretKey":          "xx",
		"instanceId":         "MQ_INST_xx",
		"consumerBatchSize":  "1",
		"consumerThreadNums": "2",
	}
}
