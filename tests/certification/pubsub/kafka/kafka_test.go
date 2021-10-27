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
	"github.com/cenkalti/backoff/v4"
	"github.com/stretchr/testify/require"
	"go.uber.org/multierr"

	// Pub/Sub.
	"github.com/dapr/components-contrib/pubsub"
	pubsub_kafka "github.com/dapr/components-contrib/pubsub/kafka"
	pubsub_loader "github.com/dapr/dapr/pkg/components/pubsub"

	// Dapr runtime and Go-SDK
	"github.com/dapr/dapr/pkg/runtime"
	dapr "github.com/dapr/go-sdk/client"
	"github.com/dapr/go-sdk/service/common"
	"github.com/dapr/kit/logger"
	kit_retry "github.com/dapr/kit/retry"

	// Certification testing runnables
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

	// Set the partition key on all messages so they
	// are written to the same partition.
	// This allows for checking of ordered messages.
	metadata := map[string]string{
		"partitionKey": "test",
	}

	// Test logic that sends messages to a topic and
	// verifies the application has received them.
	basicTest := func(ctx flow.Context) error {
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
				ctx, "messagebus", "neworder", msg,
				dapr.PublishEventWithMetadata(metadata))
			require.NoError(ctx, err, "error publishing message")
		}

		// Do the messages we observed match what we expect?
		messages.Assert(ctx, time.Minute)

		return nil
	}

	// Application logic that tracks messages from a topic.
	application := func(ctx flow.Context, s common.Service) error {
		// Simulate periodic errors.
		sim := simulate.PeriodicError(ctx, 100)

		// Setup the /orders event handler.
		return multierr.Combine(
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
				return false, nil
			}))
	}

	// sendMessagesInBackground and assertMessages are
	// Runnables for testing publishing and consuming
	// messages reliably when infrastructure and network
	// interruptions occur.
	var cctx context.Context
	var cancel context.CancelFunc
	var done chan struct{}
	sendMessagesInBackground := func(ctx flow.Context) error {
		cctx, cancel = context.WithCancel(ctx)
		client := sidecar.GetClient(ctx, sidecarName)
		messages.Reset()

		go func() {
			done = make(chan struct{}, 1)
			t := time.NewTicker(100 * time.Millisecond)
			defer func() {
				t.Stop()
				close(done)
			}()

			counter := 1
			for {
				select {
				case <-cctx.Done():
					return
				case <-t.C:
					msg := fmt.Sprintf("Background message - %03d", counter)
					messages.Prepare(msg) // Track for observation

					// Publish with retries.
					bo := backoff.WithContext(backoff.NewConstantBackOff(time.Second), cctx)
					if err := kit_retry.NotifyRecover(func() error {
						return client.PublishEvent(
							cctx, "messagebus", "neworder", msg,
							dapr.PublishEventWithMetadata(metadata))
					}, bo, func(err error, t time.Duration) {
						ctx.Logf("Error publishing message, retrying in %s", t)
					}, func() {}); err == nil {
						messages.Add(msg) // Success
						counter++
					}
				}
			}
		}()

		return nil
	}
	assertMessages := func(ctx flow.Context) error {
		cancel() // Signal sendMessagesInBackground to stop.
		<-done   // Wait for sendMessagesInBackground to complete.
		messages.Assert(ctx, 5*time.Minute)

		return nil
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
		//
		// Run the application logic above.
		Step(app.Run(applicationID, fmt.Sprintf(":%d", appPort), application)).
		//
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
		Step("send and wait", basicTest).
		//
		// Gradually stop each broker.
		// This tests the components ability to handle reconnections
		// when brokers are shutdown cleanly.
		Step("steady flow of messages to publish", sendMessagesInBackground).
		Step("wait", flow.Sleep(5*time.Second)).
		Step("stop broker 1", dockercompose.Stop(clusterName, dockerComposeYAML, "kafka1")).
		Step("wait", flow.Sleep(5*time.Second)).
		//
		// Errors will likely start occurring here since quorum is lost.
		Step("stop broker 2", dockercompose.Stop(clusterName, dockerComposeYAML, "kafka2")).
		Step("wait", flow.Sleep(10*time.Second)).
		//
		// Errors will definitely occur here.
		Step("stop broker 3", dockercompose.Stop(clusterName, dockerComposeYAML, "kafka3")).
		Step("wait", flow.Sleep(30*time.Second)).
		Step("restart broker 3", dockercompose.Start(clusterName, dockerComposeYAML, "kafka3")).
		Step("restart broker 2", dockercompose.Start(clusterName, dockerComposeYAML, "kafka2")).
		Step("restart broker 1", dockercompose.Start(clusterName, dockerComposeYAML, "kafka1")).
		//
		// Component should recover at this point.
		Step("wait", flow.Sleep(30*time.Second)).
		Step("assert messages", assertMessages).
		//
		// Simulate a network interruption.
		// This tests the components ability to handle reconnections
		// when Dapr is disconnected abnormally.
		Step("steady flow of messages to publish", sendMessagesInBackground).
		Step("wait", flow.Sleep(5*time.Second)).
		//
		// Errors will occurring here.
		Step("interrupt network",
			network.InterruptNetwork(30*time.Second, nil, nil, "19092", "29092", "39092")).
		//
		// Component should recover at this point.
		Step("wait", flow.Sleep(30*time.Second)).
		Step("assert messages", assertMessages).
		Run()
}
