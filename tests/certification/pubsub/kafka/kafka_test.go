// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package kafka_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/stretchr/testify/require"
	"go.uber.org/multierr"

	// Pub/Sub.
	"github.com/dapr/components-contrib/pubsub"
	pubsub_kafka "github.com/dapr/components-contrib/pubsub/kafka"
	pubsub_loader "github.com/dapr/dapr/pkg/components/pubsub"
	"github.com/dapr/dapr/pkg/runtime"
	"github.com/dapr/go-sdk/service/common"
	"github.com/dapr/kit/logger"

	"github.com/dapr/components-contrib/tests/certification/embedded"
	"github.com/dapr/components-contrib/tests/certification/flow"
	"github.com/dapr/components-contrib/tests/certification/flow/app"
	"github.com/dapr/components-contrib/tests/certification/flow/dockercompose"
	"github.com/dapr/components-contrib/tests/certification/flow/network"
	"github.com/dapr/components-contrib/tests/certification/flow/retry"
	"github.com/dapr/components-contrib/tests/certification/flow/sidecar"
	"github.com/dapr/components-contrib/tests/certification/flow/simulate"
	"github.com/dapr/components-contrib/tests/certification/flow/watcher"
)

const (
	sidecarName       = "dapr-1"
	applicationID     = "app-1"
	clusterName       = "kafkacertification"
	dockerComposeYAML = "docker-compose.yml"
	numMessages       = 1000
	appPort           = 8000
)

var brokers = []string{"localhost:19092", "localhost:29092", "localhost:39092"}

func TestKafka(t *testing.T) {
	log := logger.NewLogger("dapr.components")
	// For Kafka, we should ensure messages are received in order.
	messages := watcher.NewOrdered()

	// Test logic that sends messages to a topic and
	// verifies the application has received them.
	test := func(ctx flow.Context) error {
		client := sidecar.GetClient(ctx, sidecarName)

		// Declare what is expected BEFORE performing any steps
		// that will satisfy the test.
		msgs := make([]string, numMessages)
		for i := range msgs {
			msgs[i] = fmt.Sprintf("Hello, Messages %03d", i)
		}
		messages.ExpectStrings(msgs...)

		// Send events that the application above will observe.
		ctx.Log("Sending messages!")
		for _, msg := range msgs {
			ctx.Logf("Sending: %q", msg)
			err := client.PublishEvent(
				ctx, "messagebus", "neworder", msg)
			require.NoError(ctx, err, "error publishing message")
		}

		// Do the messages we observed match what we expect?
		messages.Assert(ctx, time.Minute)

		return nil
	}

	// Application logic that tracks messages from a topic.
	application := func(ctx flow.Context, s common.Service) (err error) {
		// Simulate periodic errors.
		sim := simulate.PeriodicError(ctx, 100)

		// Setup the /orders event handler.
		err = multierr.Append(err,
			s.AddTopicEventHandler(&common.Subscription{
				PubsubName: "messagebus",
				Topic:      "neworder",
				Route:      "/orders",
			}, func(_ context.Context, e *common.TopicEvent) (retry bool, err error) {
				if err := sim(); err != nil {
					return true, err
				}

				// Track/Observe the data of the event.
				messages.Observe(e.Data)
				ctx.Logf("Event - pubsub: %s, topic: %s, id: %s, data: %s",
					e.PubsubName, e.Topic, e.ID, e.Data)
				return false, nil
			}))

		return err
	}

	flow.New(t, "kafka certification").
		// Run Kafka using Docker Compose.
		Step(dockercompose.Run(clusterName, dockerComposeYAML)).
		Step("wait for broker sockets",
			network.WaitForAddresses(5*time.Minute, brokers...)).
		Step("wait for kafka readiness", retry.Do(time.Second, 30, func(ctx flow.Context) error {
			config := sarama.NewConfig()
			config.ClientID = "go-kafka-consumer"
			config.Consumer.Return.Errors = true

			// Create new consumer
			client, err := sarama.NewConsumer(brokers, config)
			if err != nil {
				return err
			}
			defer client.Close()

			// Ensure the brokers are ready by attempting to consume
			// a topic partition.
			_, err = client.ConsumePartition("myTopic", 0, sarama.OffsetOldest)

			return err
		})).
		// Run the application logic above.
		Step(app.Run(applicationID, fmt.Sprintf(":%d", appPort), application)).
		// Run the Dapr sidecar with the Kafka component.
		Step(sidecar.Run(sidecarName,
			embedded.WithAppProtocol(runtime.HTTPProtocol, appPort),
			embedded.WithDaprGRPCPort(runtime.DefaultDaprAPIGRPCPort),
			embedded.WithDaprHTTPPort(runtime.DefaultDaprHTTPPort),
			runtime.WithPubSubs(
				pubsub_loader.New("kafka", func() pubsub.PubSub {
					return pubsub_kafka.NewKafka(log)
				}),
			))).
		Step("send and wait", test).
		Run()
}
