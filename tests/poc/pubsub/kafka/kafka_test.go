package kafka_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/multierr"

	// Pub/Sub.
	"github.com/dapr/components-contrib/pubsub"
	pubsub_kafka "github.com/dapr/components-contrib/pubsub/kafka"
	pubsub_redis "github.com/dapr/components-contrib/pubsub/redis"
	pubsub_loader "github.com/dapr/dapr/pkg/components/pubsub"
	"github.com/dapr/dapr/pkg/runtime"
	"github.com/dapr/go-sdk/service/common"
	"github.com/dapr/kit/logger"

	"github.com/dapr/components-contrib/tests/poc/pubsub/kafka/pkg/flow"
	"github.com/dapr/components-contrib/tests/poc/pubsub/kafka/pkg/flow/app"
	"github.com/dapr/components-contrib/tests/poc/pubsub/kafka/pkg/flow/dockercompose"
	"github.com/dapr/components-contrib/tests/poc/pubsub/kafka/pkg/flow/network"
	"github.com/dapr/components-contrib/tests/poc/pubsub/kafka/pkg/flow/sidecar"
	"github.com/dapr/components-contrib/tests/poc/pubsub/kafka/pkg/flow/watcher"
)

const (
	sidecarName       = "dapr-1"
	applicationID     = "app-1"
	clusterName       = "kafka"
	dockerComposeYAML = "docker-compose.yml"
)

func TestKafka(t *testing.T) {
	log := logger.NewLogger("dapr.components")
	messages := watcher.New()

	test := func(ctx flow.Context) error {
		client := sidecar.GetClient(ctx, sidecarName)

		// Declare what is expected BEFORE performing any steps
		// that will satisfy the test.
		msgs := []string{"Hello, World!", "Hello Again!"}
		messages.ExpectStrings(msgs...)

		// Send events that the application above will observe.
		ctx.Log("Sending messages!")
		for _, msg := range msgs {
			ctx.Logf("Sending: %q", msg)
			err := client.PublishEventFromCustomContent(
				ctx, "messagebus", "neworder", msg)
			require.NoError(ctx, err, "error publishing message")
		}

		// Do the messages we observed match what we expect?
		messages.Assert(ctx, 5*time.Second)

		return nil
	}

	server := func(ctx flow.Context, s common.Service) (err error) {
		err = multierr.Append(err,
			s.AddTopicEventHandler(&common.Subscription{
				PubsubName: "messagebus",
				Topic:      "neworder",
				Route:      "/orders",
			}, func(_ context.Context, e *common.TopicEvent) (retry bool, err error) {
				messages.Observe(e.Data)
				ctx.Logf("Event - pubsub: %s, topic: %s, id: %s, data: %s",
					e.PubsubName, e.Topic, e.ID, e.Data)
				return false, nil
			}))

		return err
	}

	flow.New(t, "kafka certification").
		Step(
			dockercompose.Step(clusterName, dockerComposeYAML)).
		Step("wait for kafka readiness",
			network.WaitForAddresses(5*time.Minute,
				"localhost:19092", "localhost:29092", "localhost:39092")).
		Step(app.Step(applicationID, ":8000", server)).
		Step(sidecar.Step(sidecarName, runtime.WithPubSubs(
			pubsub_loader.New("kafka", func() pubsub.PubSub {
				return pubsub_kafka.NewKafka(log)
			}),
			pubsub_loader.New("redis", func() pubsub.PubSub {
				return pubsub_redis.NewRedisStreams(log)
			}),
		))).
		Step("send and wait", test).
		Run()
}
