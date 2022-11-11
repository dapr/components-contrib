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

package rabbitmq_test

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/cenkalti/backoff/v4"
	daprClient "github.com/dapr/go-sdk/client"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/require"
	"go.uber.org/multierr"

	// Pub/Sub.

	pubsub_loader "github.com/dapr/dapr/pkg/components/pubsub"
	"github.com/dapr/dapr/pkg/runtime"
	"github.com/dapr/go-sdk/service/common"
	"github.com/dapr/kit/logger"
	kit_retry "github.com/dapr/kit/retry"

	pubsub_rabbitmq "github.com/dapr/components-contrib/pubsub/rabbitmq"

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
	sidecarName1         = "dapr-1"
	sidecarName2         = "dapr-2"
	sidecarName3         = "dapr-3"
	sidecarNameTTLClient = "dapr-ttl-client"
	appID1               = "app-1"
	appID2               = "app-2"
	appID3               = "app-3"
	clusterName          = "rabbitmqcertification"
	dockerComposeYAML    = "docker-compose.yml"
	numMessages          = 1000
	errFrequency         = 100
	appPort              = 8000

	rabbitMQURL = "amqp://test:test@localhost:5672"

	pubsubAlpha          = "mq-alpha"
	pubsubBeta           = "mq-beta"
	pubsubMessageOnlyTTL = "msg-ttl-pubsub"
	pubsubQueueOnlyTTL   = "overwrite-ttl-pubsub"
	pubsubOverwriteTTL   = "queue-ttl-pubsub"

	topicRed   = "red"
	topicBlue  = "blue"
	topicGreen = "green"

	topicTTL1 = "ttl1"
	topicTTL2 = "ttl2"
	topicTTL3 = "ttl3"
)

type Consumer struct {
	pubsub   string
	messages map[string]*watcher.Watcher
}

func amqpReady(url string) flow.Runnable {
	return func(ctx flow.Context) error {
		conn, err := amqp.Dial(url)
		if err != nil {
			return err
		}
		defer conn.Close()

		ch, err := conn.Channel()
		if err != nil {
			return err
		}
		defer ch.Close()

		return nil
	}
}

func TestRabbitMQ(t *testing.T) {
	rand.Seed(time.Now().UTC().UnixNano())
	log := logger.NewLogger("dapr.components")
	// log.SetOutputLevel(logger.DebugLevel)

	pubTopics := []string{topicRed, topicBlue, topicGreen}
	subTopics := []string{topicRed, topicBlue}

	alpha := &Consumer{pubsub: pubsubAlpha, messages: make(map[string]*watcher.Watcher)}
	beta := &Consumer{pubsub: pubsubBeta, messages: make(map[string]*watcher.Watcher)}
	consumers := []*Consumer{alpha, beta}

	for _, consumer := range consumers {
		for _, topic := range pubTopics {
			// In RabbitMQ, messages might not come in order.
			consumer.messages[topic] = watcher.NewUnordered()
		}
	}

	// subscribed is used to synchronize between publisher and subscriber
	subscribed := make(chan struct{}, 1)

	// Test logic that sends messages to topics and
	// verifies the two consumers with different IDs have received them.
	test := func(ctx flow.Context) error {
		// Declare what is expected BEFORE performing any steps
		// that will satisfy the test.
		msgs := make([]string, numMessages)
		for i := range msgs {
			msgs[i] = fmt.Sprintf("Hello, Messages %03d", i)
		}

		for _, consumer := range consumers {
			for _, topic := range subTopics {
				consumer.messages[topic].ExpectStrings(msgs...)
			}
		}

		<-subscribed

		// sidecar client array []{sidecar client, pubsub component name}
		sidecars := []struct {
			client *sidecar.Client
			pubsub string
		}{
			{sidecar.GetClient(ctx, sidecarName1), pubsubAlpha},
			{sidecar.GetClient(ctx, sidecarName2), pubsubBeta},
			{sidecar.GetClient(ctx, sidecarName3), pubsubBeta},
		}

		var wg sync.WaitGroup
		wg.Add(len(pubTopics))
		for _, topic := range pubTopics {
			go func(topic string) {
				defer wg.Done()

				// Send events that the application above will observe.
				log.Infof("Sending messages on topic '%s'", topic)

				for _, msg := range msgs {
					// randomize publishers
					indx := rand.Intn(len(sidecars))
					log.Debugf("Sending: '%s' on topic '%s'", msg, topic)
					err := sidecars[indx].client.PublishEvent(ctx, sidecars[indx].pubsub, topic, msg)
					require.NoError(ctx, err, "error publishing message")
				}
			}(topic)
		}
		wg.Wait()
		log.Debugf("All messages sent - start receiving messages")

		// Do the messages we observed match what we expect?
		wg.Add(len(consumers) * len(pubTopics))
		for _, topic := range pubTopics {
			for _, consumer := range consumers {
				go func(topic string, consumer *Consumer) {
					defer wg.Done()
					consumer.messages[topic].Assert(ctx, 3*time.Minute)
				}(topic, consumer)
			}
		}
		wg.Wait()

		return nil
	}

	// Application logic that tracks messages from a topic.
	application := func(consumer *Consumer, routeIndex int) app.SetupFn {
		return func(ctx flow.Context, s common.Service) (err error) {
			// Simulate periodic errors.
			sim := simulate.PeriodicError(ctx, errFrequency)

			for _, topic := range subTopics {
				// Setup the /orders event handler.
				err = multierr.Combine(
					err,
					s.AddTopicEventHandler(&common.Subscription{
						PubsubName: consumer.pubsub,
						Topic:      topic,
						Route:      fmt.Sprintf("/%s-%d", topic, routeIndex),
					}, func(_ context.Context, e *common.TopicEvent) (retry bool, err error) {
						if err := sim(); err != nil {
							log.Debugf("Simulated error - consumer: %s, pubsub: %s, topic: %s, id: %s, data: %s", consumer.pubsub, e.PubsubName, e.Topic, e.ID, e.Data)
							return true, err
						}

						// Track/Observe the data of the event.
						consumer.messages[e.Topic].Observe(e.Data)
						log.Debugf("Event - consumer: %s, pubsub: %s, topic: %s, id: %s, data: %s", consumer.pubsub, e.PubsubName, e.Topic, e.ID, e.Data)
						return false, nil
					}),
				)
			}
			return err
		}
	}

	// sendMessagesInBackground and assertMessages are
	// Runnables for testing publishing and consuming
	// messages reliably when infrastructure and network
	// interruptions occur.
	var task flow.AsyncTask
	sendMessagesInBackground := func(consumer *Consumer) flow.Runnable {
		return func(ctx flow.Context) error {
			client := sidecar.GetClient(ctx, sidecarName1)
			for _, topic := range subTopics {
				consumer.messages[topic].Reset()
			}
			t := time.NewTicker(100 * time.Millisecond)
			defer t.Stop()

			counter := 1
			for {
				select {
				case <-task.Done():
					return nil
				case <-t.C:
					for _, topic := range subTopics {
						msg := fmt.Sprintf("Background message - %03d", counter)
						consumer.messages[topic].Prepare(msg) // Track for observation

						// Publish with retries.
						bo := backoff.WithContext(backoff.NewConstantBackOff(time.Second), task)
						if err := kit_retry.NotifyRecover(func() error {
							return client.PublishEvent(
								// Using ctx instead of task here is deliberate.
								// We don't want cancelation to prevent adding
								// the message, only to interrupt between tries.
								ctx, consumer.pubsub, topic, msg)
						}, bo, func(err error, t time.Duration) {
							ctx.Logf("Error publishing message, retrying in %s", t)
						}, func() {}); err == nil {
							consumer.messages[topic].Add(msg) // Success
							counter++
						}
					}
				}
			}
		}
	}
	assertMessages := func(consumer *Consumer) flow.Runnable {
		return func(ctx flow.Context) error {
			// Signal sendMessagesInBackground to stop and wait for it to complete.
			task.CancelAndWait()
			for _, topic := range subTopics {
				consumer.messages[topic].Assert(ctx, 5*time.Minute)
			}

			return nil
		}
	}

	flow.New(t, "rabbitmq certification").
		// Run RabbitMQ using Docker Compose.
		Step(dockercompose.Run(clusterName, dockerComposeYAML)).
		Step("wait for rabbitmq readiness",
			retry.Do(time.Second, 30, amqpReady(rabbitMQURL))).
		// Run the application1 logic above.
		Step(app.Run(appID1, fmt.Sprintf(":%d", appPort),
			application(alpha, 1))).
		// Run the Dapr sidecar with the RabbitMQ component.
		Step(sidecar.Run(sidecarName1,
			embedded.WithComponentsPath("./components/alpha"),
			embedded.WithAppProtocol(runtime.HTTPProtocol, appPort),
			embedded.WithDaprGRPCPort(runtime.DefaultDaprAPIGRPCPort),
			embedded.WithDaprHTTPPort(runtime.DefaultDaprHTTPPort),
			embedded.WithProfilePort(runtime.DefaultProfilePort),
			componentRuntimeOptions(),
		)).
		// Run the application2 logic above.
		Step(app.Run(appID2, fmt.Sprintf(":%d", appPort+2),
			application(beta, 2))).
		// Run the Dapr sidecar with the RabbitMQ component.
		Step(sidecar.Run(sidecarName2,
			embedded.WithComponentsPath("./components/beta"),
			embedded.WithAppProtocol(runtime.HTTPProtocol, appPort+2),
			embedded.WithDaprGRPCPort(runtime.DefaultDaprAPIGRPCPort+2),
			embedded.WithDaprHTTPPort(runtime.DefaultDaprHTTPPort+2),
			embedded.WithProfilePort(runtime.DefaultProfilePort+2),
			componentRuntimeOptions(),
		)).
		// Run the application3 logic above.
		Step(app.Run(appID3, fmt.Sprintf(":%d", appPort+4),
			application(beta, 3))).
		// Run the Dapr sidecar with the RabbitMQ component.
		Step(sidecar.Run(sidecarName3,
			embedded.WithComponentsPath("./components/beta"),
			embedded.WithAppProtocol(runtime.HTTPProtocol, appPort+4),
			embedded.WithDaprGRPCPort(runtime.DefaultDaprAPIGRPCPort+4),
			embedded.WithDaprHTTPPort(runtime.DefaultDaprHTTPPort+4),
			embedded.WithProfilePort(runtime.DefaultProfilePort+4),
			componentRuntimeOptions(),
		)).
		Step("wait", flow.Sleep(5*time.Second)).
		Step("signal subscribed", flow.MustDo(func() {
			close(subscribed)
		})).
		Step("send and wait", test).
		// Simulate a network interruption.
		// This tests the components ability to handle reconnections
		// when Dapr is disconnected abnormally.
		StepAsync("steady flow of messages to publish", &task, sendMessagesInBackground(alpha)).
		Step("wait", flow.Sleep(5*time.Second)).
		//
		// Errors will occurring here.
		Step("interrupt network", network.InterruptNetwork(30*time.Second, nil, nil, "5672")).
		//
		// Component should recover at this point.
		Step("wait", flow.Sleep(30*time.Second)).
		Step("assert messages", assertMessages(alpha)).
		Run()
}

func TestRabbitMQTTL(t *testing.T) {
	rand.Seed(time.Now().UTC().UnixNano())
	log := logger.NewLogger("dapr.components")
	// log.SetOutputLevel(logger.DebugLevel)

	MessageOnlyMessages, QueueOnlyMessages, OverwriteMessages := watcher.NewUnordered(), watcher.NewUnordered(), watcher.NewUnordered()
	fullMessages := watcher.NewUnordered()
	// Application logic that tracks messages from a topic.
	application := func(pubsubName, topic string, w *watcher.Watcher) app.SetupFn {
		return func(ctx flow.Context, s common.Service) (err error) {
			// Simulate periodic errors.
			sim := simulate.PeriodicError(ctx, errFrequency)
			err = multierr.Combine(
				err,
				s.AddTopicEventHandler(&common.Subscription{
					PubsubName: pubsubName,
					Topic:      topic,
					Route:      fmt.Sprintf("/%s", topic),
				}, func(_ context.Context, e *common.TopicEvent) (retry bool, err error) {
					if err := sim(); err != nil {
						log.Debugf("Simulated error - pubsub: %s, topic: %s, id: %s, data: %s", e.PubsubName, e.Topic, e.ID, e.Data)
						return true, err
					}

					// Track/Observe the data of the event.
					w.Observe(e.Data)
					log.Debugf("Event - pubsub: %s, topic: %s, id: %s, data: %s", e.PubsubName, e.Topic, e.ID, e.Data)
					return false, nil
				}),
			)

			return err
		}
	}

	sendMessage := func(sidecarName, pubsubName, topic string, ttlInSeconds int) func(ctx flow.Context) error {
		fullMessages.Reset()
		return func(ctx flow.Context) error {
			// Declare what is expected BEFORE performing any steps
			// that will satisfy the test.
			msgs := make([]string, numMessages)
			for i := range msgs {
				msgs[i] = fmt.Sprintf("Hello, Messages %03d", i)
			}

			sc := sidecar.GetClient(ctx, sidecarName)

			// Send events that the application above will observe.
			log.Infof("Sending messages on topic '%s'", topic)

			var err error
			for _, msg := range msgs {
				log.Debugf("Sending: '%s' on topic '%s'", msg, topic)
				if ttlInSeconds > 0 {
					err = sc.PublishEvent(ctx, pubsubName, topic, msg, daprClient.PublishEventWithMetadata(map[string]string{"ttlInSeconds": strconv.Itoa(ttlInSeconds)}))
				} else {
					err = sc.PublishEvent(ctx, pubsubName, topic, msg)
				}
				require.NoError(ctx, err, "error publishing message")
				fullMessages.Add(msg)
				fullMessages.Prepare(msg)
			}

			return nil
		}
	}

	flow.New(t, "rabbitmq ttl certification").
		// Run RabbitMQ using Docker Compose.
		Step(dockercompose.Run(clusterName, dockerComposeYAML)).
		Step("wait for rabbitmq readiness",
			retry.Do(time.Second, 30, amqpReady(rabbitMQURL))).
		// Start dapr and app to precreate all 3 queue in rabbitmq,
		// if topic is not subscribed, then the message will be lost.
		// Sidecar will block to wait app, so we need to start app first.
		Step(app.Run(appID1, fmt.Sprintf(":%d", appPort+1),
			application(pubsubMessageOnlyTTL, topicTTL1, fullMessages))).
		Step(sidecar.Run(sidecarName1,
			embedded.WithComponentsPath("./components/ttl"),
			embedded.WithAppProtocol(runtime.HTTPProtocol, appPort+1),
			embedded.WithDaprGRPCPort(runtime.DefaultDaprAPIGRPCPort+1),
			embedded.WithDaprHTTPPort(runtime.DefaultDaprHTTPPort+1),
			embedded.WithProfilePort(runtime.DefaultProfilePort+1),
			componentRuntimeOptions(),
		)).
		Step(app.Run(appID2, fmt.Sprintf(":%d", appPort+2),
			application(pubsubQueueOnlyTTL, topicTTL2, QueueOnlyMessages))).
		Step(sidecar.Run(sidecarName2,
			embedded.WithComponentsPath("./components/ttl"),
			embedded.WithAppProtocol(runtime.HTTPProtocol, appPort+2),
			embedded.WithDaprGRPCPort(runtime.DefaultDaprAPIGRPCPort+2),
			embedded.WithDaprHTTPPort(runtime.DefaultDaprHTTPPort+2),
			embedded.WithProfilePort(runtime.DefaultProfilePort+2),
			componentRuntimeOptions(),
		)).
		Step(app.Run(appID3, fmt.Sprintf(":%d", appPort+4),
			application(pubsubOverwriteTTL, topicTTL3, OverwriteMessages))).
		Step(sidecar.Run(sidecarName3,
			embedded.WithComponentsPath("./components/ttl"),
			embedded.WithAppProtocol(runtime.HTTPProtocol, appPort+4),
			embedded.WithDaprGRPCPort(runtime.DefaultDaprAPIGRPCPort+4),
			embedded.WithDaprHTTPPort(runtime.DefaultDaprHTTPPort+4),
			embedded.WithProfilePort(runtime.DefaultProfilePort+4),
			componentRuntimeOptions(),
		)).
		// Wait for all queue crated and then stop.
		Step("wait", flow.Sleep(10*time.Second)).
		Step("stop sidecar1", sidecar.Stop(sidecarName1)).
		Step("stop app1", app.Stop(appID1)).
		Step("stop sidecar2", sidecar.Stop(sidecarName2)).
		Step("stop app2", app.Stop(appID2)).
		Step("stop sidecar3", sidecar.Stop(sidecarName3)).
		Step("stop app3", app.Stop(appID3)).
		// Run publishing sidecars and send to RabbitMQ.
		Step(sidecar.Run(sidecarNameTTLClient,
			embedded.WithComponentsPath("./components/ttl"),
			embedded.WithAppProtocol(runtime.HTTPProtocol, 0),
			embedded.WithDaprGRPCPort(runtime.DefaultDaprAPIGRPCPort),
			embedded.WithDaprHTTPPort(runtime.DefaultDaprHTTPPort),
			embedded.WithProfilePort(runtime.DefaultProfilePort),
			componentRuntimeOptions(),
		)).
		Step("wait", flow.Sleep(5*time.Second)).
		// base test case, send message with large ttl and check messages are received.
		Step("send message only ttl messages", sendMessage(sidecarNameTTLClient, pubsubMessageOnlyTTL, topicTTL1, 100)).
		Step("wait", flow.Sleep(5*time.Second)).
		// Run the application1 logic above.
		Step(app.Run(appID1, fmt.Sprintf(":%d", appPort+1),
			application(pubsubMessageOnlyTTL, topicTTL1, fullMessages))).
		// Run the Dapr sidecar with the RabbitMQ component.
		Step(sidecar.Run(sidecarName1,
			embedded.WithComponentsPath("./components/ttl"),
			embedded.WithAppProtocol(runtime.HTTPProtocol, appPort+1),
			embedded.WithDaprGRPCPort(runtime.DefaultDaprAPIGRPCPort+1),
			embedded.WithDaprHTTPPort(runtime.DefaultDaprHTTPPort+1),
			embedded.WithProfilePort(runtime.DefaultProfilePort+1),
			componentRuntimeOptions(),
		)).
		Step("verify full messages", func(ctx flow.Context) error {
			// Assertion on the data.
			fullMessages.Assert(t, 3*time.Minute)
			return nil
		}).
		Step("stop sidecar", sidecar.Stop(sidecarName1)).
		Step("stop app", app.Stop(appID1)).
		// test case 1, send message with ttl and check messages are expired.
		Step("send message only ttl messages", sendMessage(sidecarNameTTLClient, pubsubMessageOnlyTTL, topicTTL1, 10)).
		Step("wait", flow.Sleep(10*time.Second)).
		Step(app.Run(appID1, fmt.Sprintf(":%d", appPort+1),
			application(pubsubMessageOnlyTTL, topicTTL1, MessageOnlyMessages))).
		Step(sidecar.Run(sidecarName1,
			embedded.WithComponentsPath("./components/ttl"),
			embedded.WithAppProtocol(runtime.HTTPProtocol, appPort+1),
			embedded.WithDaprGRPCPort(runtime.DefaultDaprAPIGRPCPort+1),
			embedded.WithDaprHTTPPort(runtime.DefaultDaprHTTPPort+1),
			embedded.WithProfilePort(runtime.DefaultProfilePort+1),
			componentRuntimeOptions(),
		)).
		Step("verify messages only ttl", func(ctx flow.Context) error {
			// Assertion on the data.
			MessageOnlyMessages.Assert(t, 3*time.Minute)
			return nil
		}).
		// test case 2, send message without ttl to a queue with ttl, check messages are expired.
		Step("send message without ttl to queue with ttl", sendMessage(sidecarNameTTLClient, pubsubQueueOnlyTTL, topicTTL2, 0)).
		Step("wait", flow.Sleep(10*time.Second)).
		// Run the application2 logic above.
		Step(app.Run(appID2, fmt.Sprintf(":%d", appPort+2),
			application(pubsubQueueOnlyTTL, topicTTL2, QueueOnlyMessages))).
		// Run the Dapr sidecar with the RabbitMQ component.
		Step(sidecar.Run(sidecarName2,
			embedded.WithComponentsPath("./components/ttl"),
			embedded.WithAppProtocol(runtime.HTTPProtocol, appPort+2),
			embedded.WithDaprGRPCPort(runtime.DefaultDaprAPIGRPCPort+2),
			embedded.WithDaprHTTPPort(runtime.DefaultDaprHTTPPort+2),
			embedded.WithProfilePort(runtime.DefaultProfilePort+2),
			componentRuntimeOptions(),
		)).
		Step("verify messages only ttl", func(ctx flow.Context) error {
			// Assertion on the data.
			QueueOnlyMessages.Assert(t, 3*time.Minute)
			return nil
		}).
		Step("send message with ttl 10 to queue with ttl 30",
			sendMessage(sidecarNameTTLClient, pubsubOverwriteTTL, topicTTL3, 10)).
		Step("wait", flow.Sleep(10*time.Second)).
		// test case 3, send message with ttl 10s to a queue with ttl 30s, wait for 10s, check messages are expired.
		Step(app.Run(appID3, fmt.Sprintf(":%d", appPort+4),
			application(pubsubOverwriteTTL, topicTTL3, OverwriteMessages))).
		// Run the Dapr sidecar with the RabbitMQ component.
		Step(sidecar.Run(sidecarName3,
			embedded.WithComponentsPath("./components/ttl"),
			embedded.WithAppProtocol(runtime.HTTPProtocol, appPort+4),
			embedded.WithDaprGRPCPort(runtime.DefaultDaprAPIGRPCPort+4),
			embedded.WithDaprHTTPPort(runtime.DefaultDaprHTTPPort+4),
			embedded.WithProfilePort(runtime.DefaultProfilePort+4),
			componentRuntimeOptions(),
		)).
		Step("verify messages only ttl", func(ctx flow.Context) error {
			// Assertion on the data.
			OverwriteMessages.Assert(t, 3*time.Minute)
			return nil
		}).
		Run()
}

func componentRuntimeOptions() []runtime.Option {
	log := logger.NewLogger("dapr.components")

	pubsubRegistry := pubsub_loader.NewRegistry()
	pubsubRegistry.Logger = log
	pubsubRegistry.RegisterComponent(pubsub_rabbitmq.NewRabbitMQ, "rabbitmq")

	return []runtime.Option{
		runtime.WithPubSubs(pubsubRegistry),
	}
}
