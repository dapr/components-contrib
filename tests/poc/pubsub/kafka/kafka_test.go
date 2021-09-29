package kafka_test

import (
	"context"
	"testing"
	"time"

	// Pub/Sub.
	"github.com/dapr/components-contrib/pubsub"
	pubsub_kafka "github.com/dapr/components-contrib/pubsub/kafka"
	pubsub_redis "github.com/dapr/components-contrib/pubsub/redis"
	pubsub_loader "github.com/dapr/dapr/pkg/components/pubsub"
	"github.com/dapr/dapr/pkg/runtime"
	"github.com/dapr/go-sdk/service/common"
	"github.com/dapr/kit/logger"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/tests/poc/pubsub/kafka/pkg/flow"
	"github.com/dapr/components-contrib/tests/poc/pubsub/kafka/pkg/flow/app"
	"github.com/dapr/components-contrib/tests/poc/pubsub/kafka/pkg/flow/dockercompose"
	"github.com/dapr/components-contrib/tests/poc/pubsub/kafka/pkg/flow/network"
	"github.com/dapr/components-contrib/tests/poc/pubsub/kafka/pkg/flow/sidecar"
	"github.com/dapr/components-contrib/tests/poc/pubsub/kafka/pkg/flow/watcher"
)

const (
	sidecarName       = "dapr-1"
	applicationName   = "app-1"
	clusterName       = "kafka"
	dockerComposeYAML = "kafka-cluster.yaml"
)

func TestKafka(t *testing.T) {
	log := logger.NewLogger("dapr.components")
	messages := watcher.New()

	flow.New(t, "kafka comformance").
		Service(clusterName,
			dockercompose.Up(dockerComposeYAML),
			dockercompose.Down(dockerComposeYAML)).
		Step("wait for kafka readiness",
			network.WaitForAddresses(time.Minute, "localhost:9092")).
		Service(applicationName,
			app.Start(applicationName, ":8000",
				func(ctx flow.Context, s common.Service) error {
					if err := s.AddTopicEventHandler(&common.Subscription{
						PubsubName: "messagebus",
						Topic:      "neworder",
						Route:      "/orders",
					}, func(_ context.Context, e *common.TopicEvent) (retry bool, err error) {
						messages.Observe(e.Data)
						ctx.Logf("Event - pubsub: %s, topic: %s, id: %s, data: %s",
							e.PubsubName, e.Topic, e.ID, e.Data)
						return false, nil
					}); err != nil {
						return err
					}

					return nil
				}),
			app.Stop(applicationName)).
		Service(sidecarName,
			sidecar.Start(sidecarName, runtime.WithPubSubs(
				pubsub_loader.New("kafka", func() pubsub.PubSub {
					return pubsub_kafka.NewKafka(log)
				}),
				pubsub_loader.New("redis", func() pubsub.PubSub {
					return pubsub_redis.NewRedisStreams(log)
				}),
			)),
			sidecar.Stop(sidecarName)).
		Step("send and wait", func(ctx flow.Context) error {
			var client *sidecar.Client
			ctx.MustGet(sidecarName, &client)

			ctx.Log("Sending messages!")

			// Declare what is expected BEFORE performing any steps
			// that will satisfy the test.
			msgs := []string{"Hello, World!", "Hello Again!"}
			messages.ExpectStrings(msgs...)

			// Send events that the application above will observe.
			for _, msg := range msgs {
				ctx.Logf("Sending: %q", msg)
				err := client.PublishEventFromCustomContent(
					ctx, "messagebus", "neworder", msg)
				require.NoError(ctx, err, "error publishing message")
			}

			// Do the messages we observed match what we expect?
			messages.AssertResult(ctx, 5*time.Second)

			return nil
		}).
		Run()
}
