package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/dapr/dapr/pkg/runtime"
	"github.com/dapr/kit/logger"
	"github.com/gofrs/uuid"

	// Pub/Sub.
	"github.com/dapr/components-contrib/pubsub"
	pubsub_kafka "github.com/dapr/components-contrib/pubsub/kafka"
	pubsub_redis "github.com/dapr/components-contrib/pubsub/redis"
	pubsub_loader "github.com/dapr/dapr/pkg/components/pubsub"

	"github.com/dapr/components-contrib/tests/poc/pubsub/kafka/pkg/harness"

	// Go SDK
	"github.com/dapr/go-sdk/service/common"
)

type Test struct {
	harness.Base
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
	t.Infof("event - PubsubName: %s, Topic: %s, ID: %s, Data: %s", e.PubsubName, e.Topic, e.ID, e.Data)
	return false, nil
}

func (t *Test) Run(ctx context.Context) error {
	t.Info("Sending messages!")

	// Publish via daprd + SDK
	if err := t.PublishEventFromCustomContent(ctx, "messagebus", "neworder", "Hello World!"); err != nil {
		return fmt.Errorf("error publishing message: %w", err)
	}

	// Publish directly via component
	comp := t.PubSubs["messagebus"]
	if err := comp.Publish(&pubsub.PublishRequest{
		Data:       []byte(fmt.Sprintf(`{"id": "%s", "data": "Hello Again!"}`, uuid.Must(uuid.NewV4()))),
		PubsubName: "messagebus",
		Topic:      "neworder",
	}); err != nil {
		return fmt.Errorf("error publishing message: %w", err)
	}

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	<-c

	return nil
}

func main() {
	harness.Run(&Test{})
}
