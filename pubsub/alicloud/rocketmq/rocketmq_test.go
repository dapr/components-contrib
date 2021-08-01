// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package rocketmq

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/kit/logger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPubSub(t *testing.T) { //nolint:paralleltest
	//if !isLiveTest() {
	//	return
	//}
	m := pubsub.Metadata{Properties: getTestMetadata()}

	r := NewRocketMQ(logger.NewLogger("test"))
	err := r.Init(m)
	require.NoError(t, err)

	var count int32
	handler := func(_ context.Context, msg *pubsub.NewMessage) error {
		type TopicEvent struct {
			Data interface{} `json:"data"`
		}
		var in TopicEvent
		err = json.NewDecoder(bytes.NewReader(msg.Data)).Decode(&in)
		require.NoError(t, err)

		require.Equal(t, "hello", in.Data.(string))

		atomic.AddInt32(&count, 1)

		return nil
	}

	err = r.Subscribe(pubsub.SubscribeRequest{Topic: "TOPIC_TEST", Metadata: map[string]string{}}, handler)
	require.NoError(t, err)

	time.Sleep(5 * time.Second)
	atomic.StoreInt32(&count, 0)
	err = r.Publish(&pubsub.PublishRequest{PubsubName: "test", Topic: "TOPIC_TEST", Metadata: map[string]string{}, Data: []byte("hello")})
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
		"nameServer":         "http://**.mq-internet-access.mq-internet.aliyuncs.com:80",
		"consumerGroup":      "GID_DAPR-MQ-TCP",
		"accessKey":          "**",
		"secretKey":          "**",
		"instanceId":         "MQ_INST_**",
		"consumerBatchSize":  "1",
		"consumerThreadNums": "2",
	}
}
