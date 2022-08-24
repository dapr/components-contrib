/*
Copyright 2021 The Dapr Authors
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
	"time"

	"github.com/apache/rocketmq-client-go/v2/rlog"

	"github.com/stretchr/testify/assert"

	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/kit/logger"
)

func getTestMetadata() map[string]string {
	return map[string]string{
		"nameServer":         "172.16.101.223:9876,172.16.101.224:9876",
		"consumerGroup":      "dapr.rocketmq.producer",
		"accessKey":          "RocketMQ",
		"secretKey":          "12345",
		"consumerBatchSize":  "1",
		"consumerThreadNums": "2",
		"retries":            "2",
		"sendMsgTimeout":     "30",
	}
}

func TestParseRocketMQMetadata(t *testing.T) {
	meta := getTestMetadata()
	_, err := parseRocketMQMetaData(pubsub.Metadata{Properties: meta})
	assert.Nil(t, err)
}

func TestRocketMQ_Init(t *testing.T) {
	meta := getTestMetadata()
	r := NewRocketMQ(logger.NewLogger("test"))
	err := r.Init(pubsub.Metadata{Properties: meta})
	assert.Nil(t, err)
}

func TestRocketMQ_Publish_Currently(t *testing.T) {
	meta := getTestMetadata()
	r := NewRocketMQ(logger.NewLogger("test"))
	err := r.Init(pubsub.Metadata{Properties: meta})
	assert.Nil(t, err)
	req := &pubsub.PublishRequest{
		Data:       []byte("{\"key\": 1, \"value\": \"1\"}"),
		PubsubName: "rocketmq",
		Topic:      "ZCY_ZHIXING_TEST_test",
		Metadata:   map[string]string{},
	}
	err = r.Publish(req)
	assert.Nil(t, err)

	req = &pubsub.PublishRequest{
		Data:       []byte("{\"key\": 2, \"value\": \"2\"}"),
		PubsubName: "rocketmq",
		Topic:      "ZCY_ZHIXING_TEST_test",
		Metadata: map[string]string{
			"rocketmq-tag":         "tag",
			"rocketmq-key":         "2",
			"rocketmq-shardingkey": "key",
			"traceId":              "4a09073987b148348ae0420435cddf5e",
		},
	}
	err = r.Publish(req)
	assert.Nil(t, err)

	req = &pubsub.PublishRequest{
		Data:       []byte("{\"key\": 3, \"value\": \"3\"}"),
		PubsubName: "rocketmq",
		Topic:      "ZCY_ZHIXING_TEST_test",
		Metadata: map[string]string{
			"rocketmq-tag":         "tag",
			"rocketmq-key":         "3",
			"rocketmq-shardingkey": "key",
		},
	}
	err = r.Publish(req)
	assert.Nil(t, err)

	req = &pubsub.PublishRequest{
		Data:       []byte("{\"key\": 4, \"value\": \"4\"}"),
		PubsubName: "rocketmq",
		Topic:      "ZCY_ZHIXING_TEST_test",
		Metadata: map[string]string{
			"rocketmq-tag":         "tag",
			"rocketmq-key":         "4",
			"rocketmq-shardingkey": "key",
		},
	}
	err = r.Publish(req)
	assert.Nil(t, err)
}

func TestRocketMQ_Publish_Orderly(t *testing.T) {
	meta := getTestMetadata()
	meta["consumeOrderly"] = "true"
	r := NewRocketMQ(logger.NewLogger("test"))
	err := r.Init(pubsub.Metadata{Properties: meta})
	assert.Nil(t, err)
	req := &pubsub.PublishRequest{
		Data:       []byte("{\"key\": 1, \"value\": \"1\", \"sKey\": \"sKeyHello\"}"),
		PubsubName: "rocketmq",
		Topic:      "ZCY_ZHIXING_TEST_ORDER_test",
		Metadata: map[string]string{
			"rocketmq-tag":         "tag",
			"rocketmq-key":         "1",
			"rocketmq-shardingkey": "sKey",
			"rocketmq-queue":       "2",
		},
	}
	err = r.Publish(req)
	assert.Nil(t, err)

	req = &pubsub.PublishRequest{
		Data:       []byte("{\"key\": 2, \"value\": \"2\", \"sKey\": \"sKeyHello\"}"),
		PubsubName: "rocketmq",
		Topic:      "ZCY_ZHIXING_TEST_ORDER_test",
		Metadata: map[string]string{
			"rocketmq-tag":         "tag",
			"rocketmq-key":         "2",
			"rocketmq-shardingkey": "sKey",
			"rocketmq-queue":       "3",
		},
	}
	err = r.Publish(req)
	assert.Nil(t, err)
	req = &pubsub.PublishRequest{
		Data:       []byte("{\"key\": 3, \"value\": \"3\", \"sKey\": \"sKeyHello\"}"),
		PubsubName: "rocketmq",
		Topic:      "ZCY_ZHIXING_TEST_ORDER_test",
		Metadata: map[string]string{
			"rocketmq-tag":         "tag",
			"rocketmq-key":         "3",
			"rocketmq-shardingkey": "sKey",
		},
	}
	err = r.Publish(req)
	assert.Nil(t, err)
}

func TestRocketMQ_Subscribe_Currently(t *testing.T) {
	meta := getTestMetadata()
	l := logger.NewLogger("test")
	r := NewRocketMQ(l)
	err := r.Init(pubsub.Metadata{Properties: meta})
	assert.Nil(t, err)

	req := pubsub.SubscribeRequest{
		Topic: "ZCY_ZHIXING_TEST_test",
	}
	handler := func(ctx context.Context, msg *pubsub.NewMessage) error {
		l.Info(string(msg.Data))
		return nil
	}
	err = r.Subscribe(context.Background(), req, handler)
	assert.Nil(t, err)
	time.Sleep(10 * time.Minute)
}

func TestRocketMQ_Subscribe_Orderly(t *testing.T) {
	rlog.SetLogLevel("warn")
	meta := getTestMetadata()
	l := logger.NewLogger("test")
	r := NewRocketMQ(l)
	err := r.Init(pubsub.Metadata{Properties: meta})
	assert.Nil(t, err)

	handler := func(ctx context.Context, msg *pubsub.NewMessage) error {
		l.Info(msg.Topic, string(msg.Data))
		return nil
	}
	req := pubsub.SubscribeRequest{
		Topic: "ZCY_ZHIXING_TEST_ORDER_test",
		Metadata: map[string]string{
			metadataRocketmqType:       "tag",
			metadataRocketmqExpression: "*",
		},
	}
	err = r.Subscribe(context.Background(), req, handler)

	req = pubsub.SubscribeRequest{
		Topic: "ZCY_ZHIXING_TEST_test",
		Metadata: map[string]string{
			metadataRocketmqType:       "tag",
			metadataRocketmqExpression: "*",
		},
	}
	err = r.Subscribe(context.Background(), req, handler)
	assert.Nil(t, err)
	time.Sleep(10 * time.Minute)
}
