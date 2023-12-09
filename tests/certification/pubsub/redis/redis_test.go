package redispubsub_test

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	pubsub_redis "github.com/dapr/components-contrib/pubsub/redis"
	secretstore_env "github.com/dapr/components-contrib/secretstores/local/env"
	"github.com/dapr/components-contrib/tests/certification/embedded"
	"github.com/dapr/components-contrib/tests/certification/flow"
	"github.com/dapr/components-contrib/tests/certification/flow/app"
	"github.com/dapr/components-contrib/tests/certification/flow/dockercompose"
	"github.com/dapr/components-contrib/tests/certification/flow/retry"
	"github.com/dapr/components-contrib/tests/certification/flow/sidecar"
	"github.com/dapr/components-contrib/tests/certification/flow/watcher"
	pubsub_loader "github.com/dapr/dapr/pkg/components/pubsub"
	secretstores_loader "github.com/dapr/dapr/pkg/components/secretstores"
	"github.com/dapr/dapr/pkg/config/protocol"
	"github.com/dapr/dapr/pkg/runtime"
	dapr "github.com/dapr/go-sdk/client"
	"github.com/dapr/go-sdk/service/common"
	"github.com/dapr/kit/logger"
	"github.com/go-redis/redis/v8"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/multierr"
)

const (
	sidecarName1     = "dapr-1"
	sidecarName2     = "dapr-2"
	topicActiveName1 = "topic"

	appID1            = "app-1"
	appID2            = "app-2"
	dockerComposeYAML = "docker-compose.yml"
	appPort           = 8000
	portOffset        = 2
	pubsubName        = "messages"
)

func TestRedisStreamsOrdering(t *testing.T) {
	log := logger.NewLogger("dapr.components")
	consumerGroup1 := watcher.NewUnordered()
	consumerGroup2 := watcher.NewUnordered()
	numOfMsgsRecieved := 0

	resetMessages := func(ctx flow.Context) error {
		numOfMsgsRecieved = 0
		return nil
	}

	checkRedisConnection := func(ctx flow.Context) error {
		rdb := redis.NewClient(&redis.Options{
			Addr:     "localhost:6399", // host:port of the redis server
			Password: "",               // no password set
			DB:       0,                // use default DB
		})

		if err := rdb.Ping(ctx).Err(); err != nil {
			return nil
		} else {
			log.Info("Setup for Redis done")
		}

		err := rdb.Close()
		if err != nil {
			return err
		}
		return nil
	}

	// subscriber of the given topic
	subscriberApplication := func(appID string, topicName string, messagesWatcher *watcher.Watcher) app.SetupFn {
		return func(ctx flow.Context, s common.Service) error {
			// Setup the /orders event handler.
			return multierr.Combine(
				s.AddTopicEventHandler(&common.Subscription{
					PubsubName: pubsubName,
					Topic:      topicName,
					Route:      "/orders",
				}, func(_ context.Context, e *common.TopicEvent) (retry bool, err error) {
					messagesWatcher.Observe(e.Data)
					if e.Data != nil {
						numOfMsgsRecieved += 1
					}
					return false, nil
				}),
			)
		}
	}

	publishMessages := func(metadata map[string]string, numMessages int, sidecarName string, topicName string, messageWatchers ...*watcher.Watcher) flow.Runnable {
		return func(ctx flow.Context) error {
			// prepare the messages
			messages := make([]string, numOfMsgsRecieved+numMessages)
			for i := range messages {
				messages[i] = fmt.Sprintf("message for topic: %s, index: %03d, uniqueId: %s", topicName, i, uuid.New().String())
			}

			// add the messages as expectations to the watchers
			for _, messageWatcher := range messageWatchers {
				messageWatcher.ExpectStrings(messages...)
			}

			// get the sidecar (dapr) client
			client := sidecar.GetClient(ctx, sidecarName)

			// publish messages
			ctx.Logf("Publishing messages. sidecarName: %s, topicName: %s", sidecarName, topicName)

			var publishOptions dapr.PublishEventOption

			if metadata != nil {
				publishOptions = dapr.PublishEventWithMetadata(metadata)
			}

			for _, message := range messages {
				ctx.Logf("Publishing: %q", message)
				var err error

				if publishOptions != nil {
					err = client.PublishEvent(ctx, pubsubName, topicName, message, publishOptions)
				} else {
					err = client.PublishEvent(ctx, pubsubName, topicName, message)
				}
				require.NoError(ctx, err, "RedisPubSubBasic - error publishing message")
			}
			return nil
		}
	}

	verifyMessages := func(numMessages int) flow.Runnable {
		return func(ctx flow.Context) error {
			assert.Equal(t, numMessages, numOfMsgsRecieved)
			return nil
		}
	}

	// publish some events then connect with "0" as ID and check if you recieve all messages
	// then publish some events then connect with "$" as ID and check if you  dont recieve any messages since "$" means recieve message from when the consumer group was created
	flow.New(t, "Test Redis Streams messages ordering when ID is not configured ").
		// Run subscriberApplication app1
		Step(dockercompose.Run("redis", dockerComposeYAML)).
		Step("Waiting for Redis Readiness...", retry.Do(time.Second*3, 10, checkRedisConnection)).
		Step(sidecar.Run(sidecarName1,
			append(componentRuntimeOptions(),
				embedded.WithComponentsPath("components/standard"),
				embedded.WithAppProtocol(protocol.HTTPProtocol, strconv.Itoa(appPort)),
				embedded.WithDaprGRPCPort(strconv.Itoa(runtime.DefaultDaprAPIGRPCPort)),
				embedded.WithDaprHTTPPort(strconv.Itoa(runtime.DefaultDaprHTTPPort)),
			)...,
		)).
		Step("publish messages to topic ==> "+topicActiveName1, publishMessages(nil, 5, sidecarName1, topicActiveName1, consumerGroup1)).
		Step(app.Run(appID1, fmt.Sprintf(":%d", appPort),
			subscriberApplication(appID1, topicActiveName1, consumerGroup1))).
		// in this case we have ID set to "0" so no matter when we subscribe it will always recieve all messages
		Step("giving subscriber a bit time to consume", flow.Sleep(5*time.Second)).
		Step("verify messages", verifyMessages(5)).
		Step("reset", flow.Reset(consumerGroup1, consumerGroup2)).
		Step("reset messages", resetMessages).
		Run()

	flow.New(t, "Test Redis Streams messages ordering when ID is configured ").
		// Run subscriberApplication app1
		Step(dockercompose.Run("redis", dockerComposeYAML)).
		Step("Waiting for Redis Readiness...", retry.Do(time.Second*3, 10, checkRedisConnection)).
		Step(sidecar.Run(sidecarName2,
			append(componentRuntimeOptions(),
				embedded.WithComponentsPath("components/orderings"),
				embedded.WithAppProtocol(protocol.HTTPProtocol, strconv.Itoa(appPort)),
				embedded.WithDaprGRPCPort(strconv.Itoa(runtime.DefaultDaprAPIGRPCPort)),
				embedded.WithDaprHTTPPort(strconv.Itoa(runtime.DefaultDaprHTTPPort)),
			)...,
		)).
		Step("publish messages to topic first==> "+topicActiveName1, publishMessages(nil, 5, sidecarName2, topicActiveName1, consumerGroup2)).
		Step(app.Run(appID2, fmt.Sprintf(":%d", appPort),
			subscriberApplication(appID1, topicActiveName1, consumerGroup1))).
		// since we have id as $ we should not recieve any messages because at the time of consuming those messages by subscriber by creating that consumer group those messages were not there
		// sometimes waiting increases accuracy since the observation was messages are not consumed instantly on subscription
		Step("polling messages .......", flow.Sleep(5*time.Second)).
		Step("verify messages", verifyMessages(0)).
		Step("reset", flow.Reset(consumerGroup1, consumerGroup2)).
		Step("reset messages", resetMessages).
		Run()
}

func componentRuntimeOptions() []embedded.Option {
	log := logger.NewLogger("dapr.pubsub")

	pubsubRegistry := pubsub_loader.NewRegistry()
	pubsubRegistry.Logger = log
	pubsubRegistry.RegisterComponent(pubsub_redis.NewRedisStreams, "redis")

	secretstoreRegistry := secretstores_loader.NewRegistry()
	secretstoreRegistry.Logger = log
	secretstoreRegistry.RegisterComponent(secretstore_env.NewEnvSecretStore, "local.env")

	return []embedded.Option{
		embedded.WithPubSubs(pubsubRegistry),
		embedded.WithSecretStores(secretstoreRegistry),
	}
}
