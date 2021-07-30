package mns

import (
	"context"
	"os"
	"testing"
	"time"

	ali_mns "github.com/aliyun/aliyun-mns-go-sdk"
	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/kit/logger"
	"github.com/stretchr/testify/assert"
)

func TestMNSQueue(t *testing.T) { //nolint:paralleltest
	if !isLiveTest() {
		return
	}

	mns := NewMNS(logger.NewLogger("mns-test"))
	props := pubsub.Metadata{
		Properties: map[string]string{
			"url":             os.Getenv("MNS_URL"),
			"accessKeyId":     os.Getenv("MNS_ACCESS_KEY"),
			"accessKeySecret": os.Getenv("MNS_ACCESS_KEY_SECRET"),
			"mnsMode":         MNSModeQueue,
		},
	}

	t.Logf("%v", props)

	mns.Init(props)

	topicName := "dapr-test-topic"
	queueName := "dapr-test-queue"
	message := "dapr-test-msg"
	subscriptionName := "dapr-test-subscription"

	subscribeReq := pubsub.SubscribeRequest{
		Topic: topicName,
		Metadata: map[string]string{
			"queueName":                     queueName,
			"subscriptionName":              subscriptionName,
			"subscriptionNotifyContentType": string(ali_mns.SIMPLIFIED),
		},
	}

	messageCount := 0

	err := mns.Subscribe(
		subscribeReq,
		func(ctx context.Context, msg *pubsub.NewMessage) error {
			assert.Equal(t, message, string(msg.Data))
			messageCount++

			return nil
		},
	)
	if err != nil {
		t.Error(err)
	}

	publishReq := pubsub.PublishRequest{ // nolint:exhaustivestruct
		Topic: topicName,
		Data:  []byte(message),
		Metadata: map[string]string{
			"queueName":                     queueName,
			"subscriptionName":              subscriptionName,
			"subscriptionNotifyContentType": string(ali_mns.SIMPLIFIED),
			"priority":                      "8",
		},
	}

	err = mns.Publish(&publishReq)
	if err != nil {
		t.Error(err)
	}

	time.Sleep(5 * time.Second)
	if messageCount == 0 {
		t.Errorf("didn't receive any message")
	}
}

func TestMNSTopic(t *testing.T) { //nolint:paralleltest
	if !isLiveTest() {
		return
	}

	mns := NewMNS(logger.NewLogger("mns-test"))
	props := pubsub.Metadata{
		Properties: map[string]string{
			"url":             os.Getenv("MNS_URL"),
			"accessKeyId":     os.Getenv("MNS_ACCESS_KEY"),
			"accessKeySecret": os.Getenv("MNS_ACCESS_KEY_SECRET"),
			"mnsMode":         MNSModeTopic,
		},
	}

	t.Logf("%v", props)

	mns.Init(props)

	topicName := "dapr-test-topic"
	queueName := "dapr-test-queue"
	message := "dapr-test-msg"
	subscriptionName := "dapr-test-subscription"

	subscribeReq := pubsub.SubscribeRequest{
		Topic: topicName,
		Metadata: map[string]string{
			"queueName":                     queueName,
			"subscriptionName":              subscriptionName,
			"subscriptionNotifyContentType": string(ali_mns.SIMPLIFIED),
		},
	}

	messageCount := 0

	err := mns.Subscribe(
		subscribeReq,
		func(ctx context.Context, msg *pubsub.NewMessage) error {
			assert.Equal(t, message, string(msg.Data))
			messageCount++

			return nil
		},
	)
	if err != nil {
		t.Error(err)
	}

	publishReq := pubsub.PublishRequest{ // nolint:exhaustivestruct
		Topic: topicName,
		Data:  []byte(message),
		Metadata: map[string]string{
			"queueName":                     queueName,
			"subscriptionName":              subscriptionName,
			"subscriptionNotifyContentType": string(ali_mns.SIMPLIFIED),
			"subject":                       "AAA中文",
			"accountName":                   "BBB",
		},
	}

	err = mns.Publish(&publishReq)
	if err != nil {
		t.Error(err)
	}

	time.Sleep(5 * time.Second)
	if messageCount == 0 {
		t.Errorf("didn't receive any message")
	}
}

func isLiveTest() bool {
	if os.Getenv("MNS_URL") == "" ||
		os.Getenv("MNS_ACCESS_KEY") == "" ||
		os.Getenv("MNS_ACCESS_KEY_SECRET") == "" {
		return false
	}

	return true
}
