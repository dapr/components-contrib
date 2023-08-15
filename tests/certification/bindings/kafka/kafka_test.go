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

package kafka_test

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/cenkalti/backoff/v4"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"go.uber.org/multierr"

	// Pub/Sub.

	"github.com/dapr/components-contrib/bindings"
	bindings_kafka "github.com/dapr/components-contrib/bindings/kafka"
	bindings_loader "github.com/dapr/dapr/pkg/components/bindings"
	"github.com/dapr/dapr/pkg/config/protocol"

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
	sidecarName1          = "dapr-1"
	sidecarName2          = "dapr-2"
	sidecarName3          = "dapr-3"
	sidecarName4          = "dapr-4"
	appID1                = "app-1"
	appID2                = "app-2"
	appID3                = "app-3"
	appID4                = "app-4"
	clusterName           = "kafkacertification"
	dockerComposeYAML     = "docker-compose.yml"
	dockerComposeYAMLSasl = "sasl-docker/docker-compose.yml"
	numMessages           = 1000
	appPort               = 8000
	portOffset            = 2
	messageKey            = "partitionKey"

	bindingName = "messagebus"
	topicName   = "neworder"
)

var brokers = []string{"localhost:19092", "localhost:29092", "localhost:39092"}

func TestKafka_with_retry(t *testing.T) {
	// For Kafka, we should ensure messages are received in order.
	consumerGroup1 := watcher.NewOrdered()
	// This watcher is across multiple consumers in the same group
	// so exact ordering is not expected.
	consumerGroup2 := watcher.NewUnordered()

	// Application logic that tracks messages from a topic.
	application := func(appName string, watcher *watcher.Watcher) app.SetupFn {
		return func(ctx flow.Context, s common.Service) error {
			// Simulate periodic errors.
			sim := simulate.PeriodicError(ctx, 100)

			// Setup the /orders event handler.
			return multierr.Combine(
				s.AddBindingInvocationHandler(bindingName, func(_ context.Context, in *common.BindingEvent) (out []byte, err error) {
					if err := sim(); err != nil {
						return nil, err
					}
					// Track/Observe the data of the event.
					watcher.Observe(string(in.Data))
					ctx.Logf("======== %s received event: %s\n", appName, string(in.Data))
					return in.Data, nil
				}),
			)
		}
	}

	// Set the partition key on all messages so they
	// are written to the same partition.
	// This allows for checking of ordered messages.
	metadata := map[string]string{
		messageKey: "test",
	}

	// Test logic that sends messages to a topic and
	// verifies the application has received them.
	sendRecvTest := func(metadata map[string]string, watchers ...*watcher.Watcher) flow.Runnable {
		_, hasKey := metadata[messageKey]
		return func(ctx flow.Context) error {
			client := sidecar.GetClient(ctx, sidecarName1)

			// Declare what is expected BEFORE performing any steps
			// that will satisfy the test.
			msgs := make([]string, numMessages)
			for i := range msgs {
				msgs[i] = fmt.Sprintf("Hello, Messages %03d", i)
			}
			for _, m := range watchers {
				m.ExpectStrings(msgs...)
			}
			// If no key it provided, create a random one.
			// For Kafka, this will spread messages across
			// the topic's partitions.
			if !hasKey {
				metadata[messageKey] = uuid.NewString()
			}

			// Send events that the application above will observe.
			ctx.Log("Sending messages!")
			for _, msg := range msgs {
				ctx.Logf("Sending: %q", msg)
				err := client.InvokeOutputBinding(ctx, &dapr.InvokeBindingRequest{
					Name:      bindingName,
					Operation: string(bindings.CreateOperation),
					Data:      []byte(msg),
					Metadata:  metadata,
				})
				require.NoError(ctx, err, "error publishing message")
			}

			// Do the messages we observed match what we expect?
			for _, m := range watchers {
				m.Assert(ctx, time.Minute)
			}

			return nil
		}
	}

	simpleSendTest := func(metadata map[string]string) flow.Runnable {
		return func(ctx flow.Context) error {
			client := sidecar.GetClient(ctx, sidecarName4)
			err := client.InvokeOutputBinding(ctx, &dapr.InvokeBindingRequest{
				Name:      bindingName,
				Operation: string(bindings.CreateOperation),
				Data:      []byte("sasl password auth test message"),
				Metadata:  metadata,
			})
			require.NoError(ctx, err, "error publishing message")
			return nil
		}
	}

	// sendMessagesInBackground and assertMessages are
	// Runnables for testing publishing and consuming
	// messages reliably when infrastructure and network
	// interruptions occur.
	var task flow.AsyncTask
	sendMessagesInBackground := func(watchers ...*watcher.Watcher) flow.Runnable {
		return func(ctx flow.Context) error {
			client := sidecar.GetClient(ctx, sidecarName1)
			for _, m := range watchers {
				m.Reset()
			}

			t := time.NewTicker(100 * time.Millisecond)
			defer t.Stop()

			counter := 1
			for {
				select {
				case <-task.Done():
					return nil
				case <-t.C:
					msg := fmt.Sprintf("Background message - %03d", counter)
					for _, m := range watchers {
						m.Prepare(msg) // Track for observation
					}

					// Publish with retries.
					bo := backoff.WithContext(backoff.NewConstantBackOff(time.Second), task)
					if err := kit_retry.NotifyRecover(func() error {
						return client.InvokeOutputBinding(ctx, &dapr.InvokeBindingRequest{
							Name:      bindingName,
							Operation: string(bindings.CreateOperation),
							Data:      []byte(msg),
							Metadata:  metadata,
						})
					}, bo, func(err error, t time.Duration) {
						ctx.Logf("Error output binding message, retrying in %s", t)
					}, func() {}); err == nil {
						for _, m := range watchers {
							m.Add(msg) // Success
						}
						counter++
					} else {
						for _, m := range watchers {
							m.Remove(msg) // Remove from Tracking
						}
					}
				}
			}
		}
	}
	assertMessages := func(watchers ...*watcher.Watcher) flow.Runnable {
		return func(ctx flow.Context) error {
			// Signal sendMessagesInBackground to stop and wait for it to complete.
			task.CancelAndWait()
			for _, m := range watchers {
				m.Assert(ctx, 5*time.Minute)
			}

			return nil
		}
	}

	flow.New(t, "kafka certification with retry").
		// Run Kafka using Docker Compose.
		Step(dockercompose.Run(clusterName, dockerComposeYAML)).
		Step("wait for broker sockets",
			network.WaitForAddresses(5*time.Minute, brokers...)).
		Step("wait", flow.Sleep(5*time.Second)).
		Step("wait for kafka readiness", retry.Do(10*time.Second, 30, func(ctx flow.Context) error {
			config := sarama.NewConfig()
			config.ClientID = "test-consumer"
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
		Step(app.Run(appID1, fmt.Sprintf(":%d", appPort),
			application(appID1, consumerGroup1))).
		//
		// Run the Dapr sidecar with the Kafka component.
		Step(sidecar.Run(sidecarName1,
			append(componentRuntimeOptions(),
				embedded.WithResourcesPath("./components/consumer1"),
				embedded.WithAppProtocol(protocol.HTTPProtocol, strconv.Itoa(appPort)),
				embedded.WithDaprGRPCPort(strconv.Itoa(runtime.DefaultDaprAPIGRPCPort)),
				embedded.WithDaprHTTPPort(strconv.Itoa(runtime.DefaultDaprHTTPPort)),
			)...,
		)).
		//
		// Run the second application.
		Step(app.Run(appID2, fmt.Sprintf(":%d", appPort+portOffset),
			application(appID2, consumerGroup2))).
		//
		// Run the Dapr sidecar with the Kafka component.
		Step(sidecar.Run(sidecarName2,
			append(componentRuntimeOptions(),
				embedded.WithResourcesPath("./components/consumer2"),
				embedded.WithAppProtocol(protocol.HTTPProtocol, strconv.Itoa(appPort+portOffset)),
				embedded.WithDaprGRPCPort(strconv.Itoa(runtime.DefaultDaprAPIGRPCPort+portOffset)),
				embedded.WithDaprHTTPPort(strconv.Itoa(runtime.DefaultDaprHTTPPort+portOffset)),
				embedded.WithProfilePort(strconv.Itoa(runtime.DefaultProfilePort+portOffset)),
			)...,
		)).
		//
		// Send messages using the same metadata/message key so we can expect
		// in-order processing.
		Step("send and wait(in-order)", sendRecvTest(metadata, consumerGroup1, consumerGroup2)).
		//
		// Run the third application.
		Step(app.Run(appID3, fmt.Sprintf(":%d", appPort+portOffset*2),
			application(appID3, consumerGroup2))).
		//
		// Run the Dapr sidecar with the Kafka component.
		Step(sidecar.Run(sidecarName3,
			append(componentRuntimeOptions(),
				embedded.WithResourcesPath("./components/consumer2"),
				embedded.WithAppProtocol(protocol.HTTPProtocol, strconv.Itoa(appPort+portOffset*2)),
				embedded.WithDaprGRPCPort(strconv.Itoa(runtime.DefaultDaprAPIGRPCPort+portOffset*2)),
				embedded.WithDaprHTTPPort(strconv.Itoa(runtime.DefaultDaprHTTPPort+portOffset*2)),
				embedded.WithProfilePort(strconv.Itoa(runtime.DefaultProfilePort+portOffset*2)),
			)...,
		)).
		Step("reset", flow.Reset(consumerGroup2)).
		//
		// Send messages with random keys to test message consumption
		// across more than one consumer group and consumers per group.
		Step("send and wait(no-order)", sendRecvTest(map[string]string{}, consumerGroup2)).
		//
		// Gradually stop each broker.
		// This tests the components ability to handle reconnections
		// when brokers are shutdown cleanly.
		StepAsync("steady flow of messages to publish", &task,
			sendMessagesInBackground(consumerGroup1, consumerGroup2)).
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
		Step("assert messages(Component reconnect)", assertMessages(consumerGroup1, consumerGroup2)).
		//
		// Simulate a network interruption.
		// This tests the components ability to handle reconnections
		// when Dapr is disconnected abnormally.
		StepAsync("steady flow of messages to publish", &task,
			sendMessagesInBackground(consumerGroup1, consumerGroup2)).
		Step("wait", flow.Sleep(5*time.Second)).
		//
		// Errors will occurring here.
		Step("interrupt network",
			network.InterruptNetwork(30*time.Second, nil, nil, "19092", "29092", "39092")).
		//
		// Component should recover at this point.
		Step("wait", flow.Sleep(30*time.Second)).
		Step("assert messages(network interruption)", assertMessages(consumerGroup1, consumerGroup2)).
		//
		// Reset and test that all messages are received during a
		// consumer rebalance.
		Step("reset", flow.Reset(consumerGroup2)).
		StepAsync("steady flow of messages to publish", &task,
			sendMessagesInBackground(consumerGroup2)).
		Step("wait", flow.Sleep(15*time.Second)).
		Step("stop sidecar 2", sidecar.Stop(sidecarName2)).
		Step("wait", flow.Sleep(3*time.Second)).
		Step("stop app 2", app.Stop(appID2)).
		Step("wait", flow.Sleep(30*time.Second)).
		Step("assert messages(consumer rebalance)", assertMessages(consumerGroup2)).
		Run()

	flow.New(t, "kafka with sals password auth - no tls - wurstmeister").
		// Run Kafka using Docker Compose.
		Step(dockercompose.Run(clusterName, dockerComposeYAMLSasl)).
		Step("wait for broker sockets",
			network.WaitForAddresses(5*time.Minute, "localhost:9092")).
		Step("wait", flow.Sleep(20*time.Second)).
		// Run the Dapr sidecar with the Kafka component.
		Step(sidecar.Run(sidecarName4,
			append(componentRuntimeOptions(),
				embedded.WithResourcesPath("./components/sasl-password"),
				embedded.WithDaprGRPCPort(strconv.Itoa(runtime.DefaultDaprAPIGRPCPort+5*portOffset)),
				embedded.WithDaprHTTPPort(strconv.Itoa(runtime.DefaultDaprHTTPPort+5*portOffset)),
				embedded.WithoutApp(),
			)...,
		)).
		Step("simple send test", simpleSendTest(metadata)).
		Step("wait", flow.Sleep(10*time.Second)).
		Step("stop sidecar 1", sidecar.Stop(sidecarName4)).
		Run()
}

func componentRuntimeOptions() []embedded.Option {
	log := logger.NewLogger("dapr.components")

	bindingsRegistry := bindings_loader.NewRegistry()
	bindingsRegistry.Logger = log
	bindingsRegistry.RegisterInputBinding(func(l logger.Logger) bindings.InputBinding {
		return bindings_kafka.NewKafka(l)
	}, "kafka")
	bindingsRegistry.RegisterOutputBinding(func(l logger.Logger) bindings.OutputBinding {
		return bindings_kafka.NewKafka(l)
	}, "kafka")

	return []embedded.Option{
		embedded.WithBindings(bindingsRegistry),
	}
}
