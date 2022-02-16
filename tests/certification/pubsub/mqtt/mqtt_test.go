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

package mqtt_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/cenkalti/backoff"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	// "github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"go.uber.org/multierr"

	// Pub/Sub.

	"github.com/dapr/components-contrib/pubsub"
	pubsub_mqtt "github.com/dapr/components-contrib/pubsub/mqtt"
	pubsub_loader "github.com/dapr/dapr/pkg/components/pubsub"

	// Dapr runtime and Go-SDK
	"github.com/dapr/dapr/pkg/runtime"
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
	sidecarName1      = "dapr-1"
	sidecarName2      = "dapr-2"
	appID1            = "app-1"
	appID2            = "app-2"
	clusterName       = "mqttcertification"
	dockerComposeYAML = "docker-compose.yml"
	numMessages       = 1000
	appPort           = 8000
	portOffset        = 2
	messageKey        = "partitionKey"
	mqttURL           = "tcp://localhost:1884"

	pubsubName = "messagebus"
	topicName  = "neworder"
)

var brokers = []string{"localhost:1884"}

func mqttReady(url string) flow.Runnable {
	return func(ctx flow.Context) error {
		const defaultWait = 3 * time.Second
		opts := mqtt.NewClientOptions()
		opts.SetClientID("test")
		opts.AddBroker(url)

		client := mqtt.NewClient(opts)
		token := client.Connect()
		for !token.WaitTimeout(defaultWait) {
		}
		if err := token.Error(); err != nil {
			return err
		}
		client.Disconnect(0)

		return nil
	}
}

func TestMQTT(t *testing.T) {
	log := logger.NewLogger("dapr.components")
	component := pubsub_loader.New("mqtt", func() pubsub.PubSub {
		return pubsub_mqtt.NewMQTTPubSub(log)
	})

	//In-order processing not guaranteed
	consumerGroup1 := watcher.NewUnordered()
	consumerGroup2 := watcher.NewUnordered()

	// Application logic that tracks messages from a topic.
	application := func(messages *watcher.Watcher, appID string) app.SetupFn {
		return func(ctx flow.Context, s common.Service) error {
			// Simulate periodic errors.
			sim := simulate.PeriodicError(ctx, 100)
			// Setup the /orders event handler.
			return multierr.Combine(
				s.AddTopicEventHandler(&common.Subscription{
					PubsubName: pubsubName,
					Topic:      topicName,
					Route:      "/orders",
				}, func(_ context.Context, e *common.TopicEvent) (retry bool, err error) {
					if err := sim(); err != nil {
						return true, err
					}

					// Track/Observe the data of the event.
					messages.Observe(e.Data)
					ctx.Logf("%s Event - pubsub: %s, topic: %s, id: %s, data: %s", appID,
						e.PubsubName, e.Topic, e.ID, e.Data)
					return false, nil
				}),
			)
		}
	}

	// Test logic that sends messages to a topic and
	// verifies the application has received them.
	test := func(messages ...*watcher.Watcher) flow.Runnable {
		return func(ctx flow.Context) error {

			client := sidecar.GetClient(ctx, sidecarName1)

			// Declare what is expected BEFORE performing any steps
			// that will satisfy the test.
			msgs := make([]string, numMessages)
			for i := range msgs {
				msgs[i] = fmt.Sprintf("Hello, Messages %03d", i)
			}
			for _, m := range messages {
				m.ExpectStrings(msgs...)
			}

			// Send events that the application above will observe.
			ctx.Log("Sending messages!")
			for _, msg := range msgs {
				ctx.Logf("Sending: %q", msg)
				err := client.PublishEvent(
					ctx, pubsubName, topicName, msg)
				require.NoError(ctx, err, "error publishing message")
			}

			// Do the messages we observed match what we expect?
			for _, m := range messages {
				m.Assert(ctx, time.Minute)
			}

			return nil

		}
	}

	multiple_test := func(messages ...*watcher.Watcher) flow.Runnable {
		return func(ctx flow.Context) error {

			var wg sync.WaitGroup
			wg.Add(2)
			publish_msgs := func(sidecarName string) {
				defer wg.Done()
				client := sidecar.GetClient(ctx, sidecarName)
				msgs := make([]string, numMessages/2)
				for i := range msgs {
					msgs[i] = fmt.Sprintf("%s: Hello, Messages %03d", sidecarName, i)
				}
				for _, m := range messages {
					m.ExpectStrings(msgs...)
				}
				ctx.Log("Sending messages!")
				for _, msg := range msgs {
					ctx.Logf("Sending: %q", msg)
					err := client.PublishEvent(
						ctx, pubsubName, topicName, msg)
					require.NoError(ctx, err, "error publishing message")
				}
			}
			go publish_msgs(sidecarName1)
			go publish_msgs(sidecarName2)

			wg.Wait()
			// Do the messages we observed match what we expect?
			for _, m := range messages {
				m.Assert(ctx, time.Minute)
			}

			return nil
		}
	}
	// sendMessagesInBackground and assertMessages are
	// Runnables for testing publishing and consuming
	// messages reliably when infrastructure and network
	// interruptions occur.
	var task flow.AsyncTask
	sendMessagesInBackground := func(messages ...*watcher.Watcher) flow.Runnable {
		return func(ctx flow.Context) error {
			client := sidecar.GetClient(ctx, sidecarName1)
			for _, m := range messages {
				m.Reset()
			}

			t := time.NewTicker(200 * time.Millisecond)
			defer t.Stop()

			counter := 1
			for {
				select {
				case <-task.Done():
					return nil
				case <-t.C:
					msg := fmt.Sprintf("Background message - %03d", counter)
					for _, m := range messages {
						m.Prepare(msg) // Track for observation
					}

					// Publish with retries.
					bo := backoff.WithContext(backoff.NewConstantBackOff(time.Second), task)
					if err := kit_retry.NotifyRecover(func() error {
						return client.PublishEvent(
							// Using ctx instead of task here is deliberate.
							// We don't want cancelation to prevent adding
							// the message, only to interrupt between tries.
							ctx, pubsubName, topicName, msg)
					}, bo, func(err error, t time.Duration) {
						ctx.Logf("Error publishing message, retrying in %s", t)
					}, func() {}); err == nil {
						for _, m := range messages {
							m.Add(msg) // Success
						}
						counter++
					}
				}
			}
		}
	}

	assertMessages := func(messages ...*watcher.Watcher) flow.Runnable {
		return func(ctx flow.Context) error {
			// Signal sendMessagesInBackground to stop and wait for it to complete.
			task.CancelAndWait()
			for _, m := range messages {
				m.Assert(ctx, 1*time.Minute)
			}

			return nil
		}
	}

	flow.New(t, "mqtt certification").
		// Run MQTT using Docker Compose.
		Step(dockercompose.Run(clusterName, dockerComposeYAML)).
		Step("wait for broker sockets",
			network.WaitForAddresses(5*time.Minute, brokers...)).
		Step("wait for MQTT readiness",
			retry.Do(time.Second, 30, mqttReady(mqttURL))).
		//
		// Run the application logic above(App1)
		Step(app.Run(appID1, fmt.Sprintf(":%d", appPort),
			application(consumerGroup1, appID1))).
		// Run the Dapr sidecar with the MQTTPubSub component.
		Step(sidecar.Run(sidecarName1,
			embedded.WithComponentsPath("./components/consumer1"),
			embedded.WithAppProtocol(runtime.HTTPProtocol, appPort),
			embedded.WithDaprGRPCPort(runtime.DefaultDaprAPIGRPCPort),
			embedded.WithDaprHTTPPort(runtime.DefaultDaprHTTPPort),
			runtime.WithPubSubs(component))).
		//
		// Send messages and test
		Step("send and wait", test(consumerGroup1)).
		Step("reset", flow.Reset(consumerGroup1)).
		//
		//Run Second application App2
		Step(app.Run(appID2, fmt.Sprintf(":%d", appPort+portOffset),
			application(consumerGroup2, appID2))).
		// Run the Dapr sidecar with the MQTTPubSub component.
		Step(sidecar.Run(sidecarName2,
			embedded.WithComponentsPath("./components/consumer2"),
			embedded.WithAppProtocol(runtime.HTTPProtocol, appPort+portOffset),
			embedded.WithDaprGRPCPort(runtime.DefaultDaprAPIGRPCPort+portOffset),
			embedded.WithDaprHTTPPort(runtime.DefaultDaprHTTPPort+portOffset),
			embedded.WithProfilePort(runtime.DefaultProfilePort+portOffset),
			runtime.WithPubSubs(component))).
		//
		// Send messages and test
		Step("multiple send and wait", multiple_test(consumerGroup1, consumerGroup2)).
		Step("reset", flow.Reset(consumerGroup1, consumerGroup2)).
		//
		// Infra test
		StepAsync("steady flow of messages to publish", &task,
			sendMessagesInBackground(consumerGroup1, consumerGroup2)).
		Step("wait", flow.Sleep(5*time.Second)).
		Step("stop sidecar 2", sidecar.Stop(sidecarName2)).
		Step("wait", flow.Sleep(5*time.Second)).
		Step("stop sidecar 1", sidecar.Stop(sidecarName1)).
		Step("wait", flow.Sleep(5*time.Second)).
		Step(sidecar.Run(sidecarName2,
			embedded.WithComponentsPath("./components/consumer2"),
			embedded.WithAppProtocol(runtime.HTTPProtocol, appPort+portOffset),
			embedded.WithDaprGRPCPort(runtime.DefaultDaprAPIGRPCPort+portOffset),
			embedded.WithDaprHTTPPort(runtime.DefaultDaprHTTPPort+portOffset),
			embedded.WithProfilePort(runtime.DefaultProfilePort+portOffset),
			runtime.WithPubSubs(component))).
		Step("wait", flow.Sleep(5*time.Second)).
		Step(sidecar.Run(sidecarName1,
			embedded.WithComponentsPath("./components/consumer1"),
			embedded.WithAppProtocol(runtime.HTTPProtocol, appPort),
			embedded.WithDaprGRPCPort(runtime.DefaultDaprAPIGRPCPort),
			embedded.WithDaprHTTPPort(runtime.DefaultDaprHTTPPort),
			runtime.WithPubSubs(component))).
		Step("wait", flow.Sleep(5*time.Second)).
		Step("assert messages", assertMessages(consumerGroup1, consumerGroup2)).
		Step("reset", flow.Reset(consumerGroup1, consumerGroup2)).
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
			network.InterruptNetwork(5*time.Second, nil, nil, "18084")).
		//
		// Component should recover at this point.
		Step("wait", flow.Sleep(5*time.Second)).
		Step("assert messages", assertMessages(consumerGroup1, consumerGroup2)).
		Run()
}
