package main

import (
	"context"
	"fmt"
	"time"

	"github.com/dapr/dapr/pkg/runtime"
	"github.com/dapr/kit/logger"

	// Pub/Sub.
	"github.com/dapr/components-contrib/pubsub"
	pubsub_kafka "github.com/dapr/components-contrib/pubsub/kafka"
	pubsub_redis "github.com/dapr/components-contrib/pubsub/redis"
	pubsub_loader "github.com/dapr/dapr/pkg/components/pubsub"

	"github.com/dapr/components-contrib/tests/poc/pubsub/kafka/pkg/harness"

	// Go SDK
	"github.com/dapr/go-sdk/service/common"
)

func main() {
	harness.Run(&Test{})
}

type Test struct {
	harness.Base
	messages *harness.Watcher
}

func (t *Test) Options(log logger.Logger) []runtime.Option {
	return []runtime.Option{
		runtime.WithPubSubs(
			pubsub_loader.New("kafka", func() pubsub.PubSub {
				return pubsub_kafka.NewKafka(log)
			}),
			pubsub_loader.New("redis", func() pubsub.PubSub {
				return pubsub_redis.NewRedisStreams(log)
			}),
		),
	}
}

func (t *Test) Setup(ctx context.Context) error {
	t.messages = harness.NewWatcher()

	if err := t.AddTopicEventHandler(&common.Subscription{
		PubsubName: "messagebus",
		Topic:      "neworder",
		Route:      "/orders",
	}, t.eventHandler); err != nil {
		return err
	}

	return nil
}

func (t *Test) eventHandler(ctx context.Context, e *common.TopicEvent) (retry bool, err error) {
	t.messages.Observe(e.Data)
	t.Infof("Event - pubsub: %s, topic: %s, id: %s, data: %s", e.PubsubName, e.Topic, e.ID, e.Data)
	return false, nil
}

func (t *Test) Run(ctx context.Context) error {
	t.Info("Sending messages!")

	msg := "Hello World!"
	t.messages.Expect(msg)

	if err := t.PublishEventFromCustomContent(ctx, "messagebus", "neworder", msg); err != nil {
		return fmt.Errorf("error publishing message: %w", err)
	}

	return t.messages.WaitForResult(5 * time.Second)
}
