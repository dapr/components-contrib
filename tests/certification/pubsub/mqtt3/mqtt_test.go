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
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cenkalti/backoff/v4"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/stretchr/testify/require"
	"go.uber.org/multierr"

	// Pub/Sub.

	pubsub_mqtt "github.com/dapr/components-contrib/pubsub/mqtt3"
	pubsub_loader "github.com/dapr/dapr/pkg/components/pubsub"
	"github.com/dapr/dapr/pkg/config/protocol"

	// Dapr runtime and Go-SDK
	"github.com/dapr/dapr/pkg/runtime"
	"github.com/dapr/go-sdk/service/common"
	"github.com/dapr/kit/logger"

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
	sidecarName3      = "dapr-3"
	appID1            = "app-1"
	appID2            = "app-2"
	appID3            = "app-3"
	clusterName       = "mqttcertification"
	dockerComposeYAML = "docker-compose.yml"
	numMessages       = 1000
	appPort           = 8000
	portOffset        = 2
	messageKey        = "partitionKey"
	mqttURL           = "tcp://localhost:1884"

	pubsubName             = "messagebus"
	topicName              = "neworder"
	wildcardTopicSubscribe = "orders/#"
	wildcardTopicPublish   = "orders/%s"
	sharedTopicSubscribe   = "$share/mygroup/mytopic/+/hello"
	sharedTopicPublish     = "mytopic/%s/hello"
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
	logger.ApplyOptionsToLoggers(&logger.Options{
		OutputLevel: "debug",
	})

	// In-order processing not guaranteed
	consumerGroup1 := watcher.NewUnordered()
	consumerGroup2 := watcher.NewUnordered()
	consumerGroupMultiWildcard := watcher.NewUnordered()
	consumerGroupMultiShared := watcher.NewUnordered()

	// Application logic that tracks messages from a topic.
	application := func(messages *watcher.Watcher, appID string, topicName string) app.SetupFn {
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
					ctx.Logf("%s Event - pubsub: %s, topic: %s, id: %s, data: %s",
						appID, e.PubsubName, e.Topic, e.ID, e.Data)
					return false, nil
				}),
			)
		}
	}

	// Application logic that subscribes to multiple topics
	applicationMultiTopic := func(appID string, subs ...topicSubscription) app.SetupFn {
		return func(ctx flow.Context, s common.Service) (err error) {
			handlerGen := func(name string, messages *watcher.Watcher) func(_ context.Context, e *common.TopicEvent) (retry bool, err error) {
				return func(_ context.Context, e *common.TopicEvent) (retry bool, err error) {
					messages.Observe(e.Data)
					ctx.Logf("%s/%s Event - pubsub: %s, topic: %s, id: %s, data: %s",
						appID, name, e.PubsubName, e.Topic, e.ID, e.Data)
					return false, nil
				}
			}
			for _, sub := range subs {
				err = s.AddTopicEventHandler(
					&common.Subscription{
						PubsubName: pubsubName,
						Topic:      sub.name,
						Route:      sub.route,
					}, handlerGen(sub.name, sub.messages),
				)
				if err != nil {
					return err
				}
			}

			return nil
		}
	}

	// Test logic that sends messages to a topic and
	// verifies the application has received them.
	test := func(topicName string, messages ...*watcher.Watcher) flow.Runnable {
		return func(ctx flow.Context) error {
			client := sidecar.GetClient(ctx, sidecarName1)

			// Declare what is expected BEFORE performing any steps
			// that will satisfy the test.
			msgs := make([]string, numMessages)
			for i := range msgs {
				msgs[i] = fmt.Sprintf("Hello, Messages %s#%03d", topicName, i)
			}
			for _, m := range messages {
				m.ExpectStrings(msgs...)
			}

			// Send events that the application above will observe.
			ctx.Log("Sending messages!")
			for _, msg := range msgs {
				// If topicName has a %s, this will add some randomness (if not, it won't be changed)
				tn := topicName
				if strings.Contains(tn, "%s") {
					tn = fmt.Sprintf(tn, randomStr())
				}
				ctx.Logf("Sending '%q' to topic '%s'", msg, tn)
				err := client.PublishEvent(ctx, pubsubName, tn, msg)
				require.NoError(ctx, err, "error publishing message")
			}

			// Do the messages we observed match what we expect?
			for _, m := range messages {
				m.Assert(ctx, time.Minute)
			}

			return nil
		}
	}

	multipleTest := func(messages ...*watcher.Watcher) flow.Runnable {
		return func(ctx flow.Context) error {
			var wg sync.WaitGroup
			wg.Add(2)
			publishMsgs := func(sidecarName string) {
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
					err := client.PublishEvent(ctx, pubsubName, topicName, msg)
					require.NoError(ctx, err, "error publishing message")
				}
			}
			go publishMsgs(sidecarName1)
			go publishMsgs(sidecarName2)

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
	counter := &atomic.Int64{}
	counter.Store(1)
	sendMessagesInBackground := func(messages ...*watcher.Watcher) flow.Runnable {
		return func(ctx flow.Context) error {
			client := sidecar.GetClient(ctx, sidecarName1)
			for _, m := range messages {
				m.Reset()
			}

			t := time.NewTicker(200 * time.Millisecond)
			defer t.Stop()

			for {
				select {
				case <-task.Done():
					return nil
				case <-t.C:
					msg := fmt.Sprintf("Background message - %03d", counter.Load())
					for _, m := range messages {
						m.Prepare(msg) // Track for observation
					}

					// Publish with retries.
					err := backoff.RetryNotify(
						func() error {
							ctx.Logf("Sending '%q' to topic '%s'", msg, topicName)
							// Using ctx instead of task here is deliberate.
							// We don't want cancelation to prevent adding
							// the message, only to interrupt between tries.
							return client.PublishEvent(ctx, pubsubName, topicName, msg)
						},
						backoff.WithContext(backoff.NewConstantBackOff(time.Second), task),
						func(err error, t time.Duration) {
							ctx.Logf("Error publishing message '%s', retrying in %s", msg, t)
						},
					)
					if err == nil {
						for _, m := range messages {
							m.Add(msg) // Success
						}
						counter.Add(1)
					} else {
						for _, m := range messages {
							m.Remove(msg) // Remove from Tracking
						}
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
			application(consumerGroup1, appID1, topicName))).
		// Run the Dapr sidecar with the MQTTPubSub component.
		Step(sidecar.Run(sidecarName1,
			append(componentRuntimeOptions(),
				embedded.WithComponentsPath("./components/consumer1"),
				embedded.WithAppProtocol(protocol.HTTPProtocol, strconv.Itoa(appPort)),
				embedded.WithDaprGRPCPort(strconv.Itoa(runtime.DefaultDaprAPIGRPCPort)),
				embedded.WithDaprHTTPPort(strconv.Itoa(runtime.DefaultDaprHTTPPort)),
				embedded.WithGracefulShutdownDuration(0),
			)...,
		)).
		//
		// Send messages and test
		Step("send and wait 1", test(topicName, consumerGroup1)).
		Step("reset 1", flow.Reset(consumerGroup1)).
		//
		//Run Second application App2
		Step(app.Run(appID2, fmt.Sprintf(":%d", appPort+portOffset),
			application(consumerGroup2, appID2, topicName))).
		// Run the Dapr sidecar with the MQTTPubSub component.
		Step(sidecar.Run(sidecarName2,
			append(componentRuntimeOptions(),
				embedded.WithComponentsPath("./components/consumer2"),
				embedded.WithAppProtocol(protocol.HTTPProtocol, strconv.Itoa(appPort+portOffset)),
				embedded.WithDaprGRPCPort(strconv.Itoa(runtime.DefaultDaprAPIGRPCPort+portOffset)),
				embedded.WithDaprHTTPPort(strconv.Itoa(runtime.DefaultDaprHTTPPort+portOffset)),
				embedded.WithProfilePort(strconv.Itoa(runtime.DefaultProfilePort+portOffset)),
				embedded.WithGracefulShutdownDuration(0),
			)...,
		)).
		//
		// Send messages and test
		Step("multiple send and wait", multipleTest(consumerGroup1, consumerGroup2)).
		Step("reset 2", flow.Reset(consumerGroup1, consumerGroup2)).
		//
		// Test multiple topics and wildcards
		Step(
			app.Run(
				appID3,
				fmt.Sprintf(":%d", appPort+(portOffset*3)),
				applicationMultiTopic(
					appID3,
					topicSubscription{messages: consumerGroupMultiWildcard, name: wildcardTopicSubscribe, route: "/wildcard"},
					topicSubscription{messages: consumerGroupMultiShared, name: sharedTopicSubscribe, route: "/shared"},
				),
			),
		).
		Step(sidecar.Run(sidecarName3,
			append(componentRuntimeOptions(),
				embedded.WithComponentsPath("./components/consumer3"),
				embedded.WithAppProtocol(protocol.HTTPProtocol, strconv.Itoa(appPort+(portOffset*3))),
				embedded.WithDaprGRPCPort(strconv.Itoa(runtime.DefaultDaprAPIGRPCPort+(portOffset*3))),
				embedded.WithDaprHTTPPort(strconv.Itoa(runtime.DefaultDaprHTTPPort+(portOffset*3))),
				embedded.WithProfilePort(strconv.Itoa(runtime.DefaultProfilePort+(portOffset*3))),
				embedded.WithGracefulShutdownDuration(0),
			)...,
		)).
		Step("send and wait wildcard", test(wildcardTopicPublish, consumerGroupMultiWildcard)).
		Step("send and wait shared", test(sharedTopicPublish, consumerGroupMultiShared)).
		//
		// Infra test
		StepAsync("steady flow of messages to publish", &task,
			sendMessagesInBackground(consumerGroup1, consumerGroup2)).
		Step("wait before stopping sidecar 2", flow.Sleep(5*time.Second)).
		Step("stop sidecar 2", sidecar.Stop(sidecarName2)).
		Step("wait before stopping sidecar 1", flow.Sleep(5*time.Second)).
		Step("stop sidecar 1", sidecar.Stop(sidecarName1)).
		Step("wait 1", flow.Sleep(5*time.Second)).
		Step(sidecar.Run(sidecarName2,
			append(componentRuntimeOptions(),
				embedded.WithComponentsPath("./components/consumer2"),
				embedded.WithAppProtocol(protocol.HTTPProtocol, strconv.Itoa(appPort+portOffset)),
				embedded.WithDaprGRPCPort(strconv.Itoa(runtime.DefaultDaprAPIGRPCPort+portOffset)),
				embedded.WithDaprHTTPPort(strconv.Itoa(runtime.DefaultDaprHTTPPort+portOffset)),
				embedded.WithProfilePort(strconv.Itoa(runtime.DefaultProfilePort+portOffset)),
				embedded.WithGracefulShutdownDuration(0),
			)...,
		)).
		Step("wait 2", flow.Sleep(5*time.Second)).
		Step(sidecar.Run(sidecarName1,
			append(componentRuntimeOptions(),
				embedded.WithComponentsPath("./components/consumer1"),
				embedded.WithAppProtocol(protocol.HTTPProtocol, strconv.Itoa(appPort)),
				embedded.WithDaprGRPCPort(strconv.Itoa(runtime.DefaultDaprAPIGRPCPort)),
				embedded.WithDaprHTTPPort(strconv.Itoa(runtime.DefaultDaprHTTPPort)),
				embedded.WithGracefulShutdownDuration(0),
			)...,
		)).
		Step("wait 3", flow.Sleep(5*time.Second)).
		Step("assert messages 1", assertMessages(consumerGroup1, consumerGroup2)).
		Step("reset 3", flow.Reset(consumerGroup1, consumerGroup2)).
		//
		// Simulate a network interruption.
		// This tests the components ability to handle reconnections
		// when Dapr is disconnected abnormally.
		StepAsync("steady flow of messages to publish", &task,
			sendMessagesInBackground(consumerGroup1, consumerGroup2)).
		Step("wait 4", flow.Sleep(5*time.Second)).
		//
		// Errors will occurring here.
		Step("interrupt network",
			network.InterruptNetwork(5*time.Second, nil, nil, "18084")).
		//
		// Component should recover at this point.
		Step("wait 5", flow.Sleep(5*time.Second)).
		Step("assert messages 2", assertMessages(consumerGroup1, consumerGroup2)).
		Run()
}

type topicSubscription struct {
	messages *watcher.Watcher
	name     string
	route    string
}

func componentRuntimeOptions() []embedded.Option {
	log := logger.NewLogger("dapr.components")

	pubsubRegistry := pubsub_loader.NewRegistry()
	pubsubRegistry.Logger = log
	pubsubRegistry.RegisterComponent(pubsub_mqtt.NewMQTTPubSub, "mqtt3")

	return []embedded.Option{
		embedded.WithPubSubs(pubsubRegistry),
	}
}

func randomStr() string {
	buf := make([]byte, 4)
	_, _ = io.ReadFull(rand.Reader, buf)
	return hex.EncodeToString(buf)
}
