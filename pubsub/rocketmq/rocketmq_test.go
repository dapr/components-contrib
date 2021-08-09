package rocketmq

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/kit/logger"
	"github.com/stretchr/testify/require"
)

func getTestMetadata() map[string]string {
	return map[string]string{
		"nameServer":         "127.0.0.1:9876",
		"consumerGroup":      "dapr.rocketmq.producer",
		"accessKey":          "RocketMQ",
		"secretKey":          "12345",
		"consumerBatchSize":  "1",
		"consumerThreadNums": "2",
		"retries":            "2",
	}
}

func TestNewRocketMQ(t *testing.T) {
	m := pubsub.Metadata{Properties: getTestMetadata()}
	r := NewRocketMQ(logger.NewLogger("test"))
	err := r.Init(m)
	require.NoError(t, err)
	err = r.Publish(&pubsub.PublishRequest{
		Data:       []byte("hello rocketmq"),
		PubsubName: "rocketmq",
		Topic:      "dapr",
		Metadata: map[string]string{
			metadataRocketmqTag: "dapr",
		},
	})
	require.NoError(t, err)

	var count int32
	handler := func(_ context.Context, msg *pubsub.NewMessage) error {
		type TopicEvent struct {
			Data interface{} `json:"data"`
		}
		var in TopicEvent
		err = json.NewDecoder(bytes.NewReader(msg.Data)).Decode(&in)
		require.NoError(t, err)

		fmt.Println("recv: ", in.Data.(string))

		atomic.AddInt32(&count, 1)

		return nil
	}
	r.Subscribe(pubsub.SubscribeRequest{
		Topic:    "dapr",
		Metadata: map[string]string{},
	}, handler)
}
