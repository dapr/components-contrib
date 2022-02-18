/*
Copyright 2022 The Dapr Authors
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package rocketmq

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/kit/logger"
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

func TestParseRocketMQMetadata(t *testing.T) {
	t.Run("correct metadata", func(t *testing.T) {
		meta := getTestMetadata()
		_, err := parseRocketMQMetaData(pubsub.Metadata{Properties: meta})
		assert.Nil(t, err)
	})

	t.Run("correct init", func(t *testing.T) {
		meta := getTestMetadata()
		r := NewRocketMQ(logger.NewLogger("test"))
		err := r.Init(pubsub.Metadata{Properties: meta})
		assert.Nil(t, err)
	})

	t.Run("setup producer missing nameserver", func(t *testing.T) {
		meta := getTestMetadata()
		delete(meta, "nameServer")
		r := NewRocketMQ(logger.NewLogger("test"))
		err := r.Init(pubsub.Metadata{Properties: meta})
		assert.Nil(t, err)
		req := &pubsub.PublishRequest{
			Data:       []byte("hello"),
			PubsubName: "rocketmq",
			Topic:      "test",
			Metadata:   map[string]string{},
		}
		err = r.Publish(req)
		assert.NotNil(t, err)
	})

	t.Run("subscribe illegal type", func(t *testing.T) {
		meta := getTestMetadata()
		r := NewRocketMQ(logger.NewLogger("test"))
		err := r.Init(pubsub.Metadata{Properties: meta})
		assert.Nil(t, err)

		req := pubsub.SubscribeRequest{
			Topic: "test",
			Metadata: map[string]string{
				metadataRocketmqType: "incorrect type",
			},
		}
		handler := func(ctx context.Context, msg *pubsub.NewMessage) error {
			return nil
		}
		err = r.Subscribe(req, handler)
		assert.NotNil(t, err)
	})
}
