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

package servicebusqueues_test

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"go.uber.org/multierr"

	// Pub-Sub.
	pubsub_servicebus "github.com/dapr/components-contrib/pubsub/azure/servicebus/queues"
	secretstore_env "github.com/dapr/components-contrib/secretstores/local/env"
	pubsub_loader "github.com/dapr/dapr/pkg/components/pubsub"
	secretstores_loader "github.com/dapr/dapr/pkg/components/secretstores"
	"github.com/dapr/dapr/pkg/config/protocol"
	"github.com/dapr/kit/logger"

	"github.com/dapr/dapr/pkg/runtime"
	dapr "github.com/dapr/go-sdk/client"
	"github.com/dapr/go-sdk/service/common"

	// Certification testing runnables
	"github.com/dapr/components-contrib/tests/certification/embedded"
	"github.com/dapr/components-contrib/tests/certification/flow"
	"github.com/dapr/components-contrib/tests/certification/flow/app"
	"github.com/dapr/components-contrib/tests/certification/flow/network"
	"github.com/dapr/components-contrib/tests/certification/flow/sidecar"
	"github.com/dapr/components-contrib/tests/certification/flow/simulate"
	"github.com/dapr/components-contrib/tests/certification/flow/watcher"
)

const (
	sidecarName1 = "dapr-1"
	sidecarName2 = "dapr-2"

	appID1 = "app-1"
	appID2 = "app-2"

	numMessages      = 10
	appPort          = 8000
	portOffset       = 2
	messageKey       = "partitionKey"
	pubsubName       = "messagebus"
	queueActiveName  = "certification-pubsub-queue-active"
	queuePassiveName = "certification-pubsub-queue-passive"
	queueToBeCreated = "certification-queue-per-test-run"
	queueDefaultName = "certification-queue-default"
	partition0       = "partition-0"
	partition1       = "partition-1"
)

func TestServicebusQueues(t *testing.T) {
	// For queues, messages are competing between consumers - each message is received by only ONE consumer.
	// Use a single shared watcher that both apps will feed into.
	sharedWatcher := watcher.NewUnordered()

	// subscriber of the given queue
	subscriberApplication := func(appID string, queueName string, messagesWatcher *watcher.Watcher) app.SetupFn {
		return func(ctx flow.Context, s common.Service) error {
			// Simulate periodic errors.
			sim := simulate.PeriodicError(ctx, 100)
			// Setup the /orders event handler.
			return multierr.Combine(
				s.AddTopicEventHandler(&common.Subscription{
					PubsubName: pubsubName,
					Topic:      queueName,
					Route:      "/orders",
				}, func(_ context.Context, e *common.TopicEvent) (retry bool, err error) {
					if err := sim(); err != nil {
						return true, err
					}

					// Track/Observe the data of the event.
					messagesWatcher.Observe(e.Data)
					ctx.Logf("Message Received appID: %s, pubsub: %s, queue: %s, id: %s, data: %s", appID, e.PubsubName, e.Topic, e.ID, e.Data)
					return false, nil
				}),
			)
		}
	}

	publishMessages := func(metadata map[string]string, sidecarName string, queueName string, messageWatchers ...*watcher.Watcher) flow.Runnable {
		return func(ctx flow.Context) error {
			// prepare the messages
			messages := make([]string, numMessages)
			for i := range messages {
				messages[i] = fmt.Sprintf("partitionKey: %s, message for queue: %s, index: %03d, uniqueId: %s", metadata[messageKey], queueName, i, uuid.New().String())
			}

			// add the messages as expectations to the watchers
			for _, messageWatcher := range messageWatchers {
				messageWatcher.ExpectStrings(messages...)
			}

			// get the sidecar (dapr) client
			client := sidecar.GetClient(ctx, sidecarName)

			// publish messages
			ctx.Logf("Publishing messages. sidecarName: %s, queueName: %s", sidecarName, queueName)

			var publishOptions dapr.PublishEventOption

			if metadata != nil {
				publishOptions = dapr.PublishEventWithMetadata(metadata)
			}

			for _, message := range messages {
				ctx.Logf("Publishing: %q", message)
				var err error

				if publishOptions != nil {
					err = client.PublishEvent(ctx, pubsubName, queueName, message, publishOptions)
				} else {
					err = client.PublishEvent(ctx, pubsubName, queueName, message)
				}
				require.NoError(ctx, err, "error publishing message")
			}
			return nil
		}
	}

	assertMessages := func(timeout time.Duration, messageWatchers ...*watcher.Watcher) flow.Runnable {
		return func(ctx flow.Context) error {
			// assert for messages
			for _, m := range messageWatchers {
				m.Assert(ctx, 25*timeout)
			}

			return nil
		}
	}

	flow.New(t, "servicebus queues certification basic test").

		// Run subscriberApplication app1 - both apps use the same sharedWatcher
		Step(app.Run(appID1, fmt.Sprintf(":%d", appPort),
			subscriberApplication(appID1, queueActiveName, sharedWatcher))).

		// Run the Dapr sidecar with the servicebus queues component 1
		Step(sidecar.Run(sidecarName1,
			append(componentRuntimeOptions(),
				embedded.WithComponentsPath("./components/consumer_one"),
				embedded.WithAppProtocol(protocol.HTTPProtocol, strconv.Itoa(appPort)),
				embedded.WithDaprGRPCPort(strconv.Itoa(runtime.DefaultDaprAPIGRPCPort)),
				embedded.WithDaprHTTPPort(strconv.Itoa(runtime.DefaultDaprHTTPPort)),
			)...,
		)).

		// Run subscriberApplication app2 - both apps use the same sharedWatcher
		Step(app.Run(appID2, fmt.Sprintf(":%d", appPort+portOffset),
			subscriberApplication(appID2, queueActiveName, sharedWatcher))).

		// Run the Dapr sidecar with the component 2.
		Step(sidecar.Run(sidecarName2,
			append(componentRuntimeOptions(),
				embedded.WithComponentsPath("./components/consumer_two"),
				embedded.WithAppProtocol(protocol.HTTPProtocol, strconv.Itoa(appPort+portOffset)),
				embedded.WithDaprGRPCPort(strconv.Itoa(runtime.DefaultDaprAPIGRPCPort+portOffset)),
				embedded.WithDaprHTTPPort(strconv.Itoa(runtime.DefaultDaprHTTPPort+portOffset)),
				embedded.WithProfilePort(strconv.Itoa(runtime.DefaultProfilePort+portOffset)),
			)...,
		)).
		// For queues, only register expectations once since messages compete between consumers
		Step("publish messages to queue", publishMessages(nil, sidecarName1, queueActiveName, sharedWatcher)).
		Step("publish messages to unused queue", publishMessages(nil, sidecarName1, queuePassiveName)).
		Step("verify all messages received by competing consumers", assertMessages(10*time.Second, sharedWatcher)).
		Step("reset", flow.Reset(sharedWatcher)).
		Run()
}

func TestServicebusQueuesMultipleSubsSameApp(t *testing.T) {
	consumerGroup1 := watcher.NewUnordered()

	// Set the partition key on all messages so they are written to the same partition.
	metadata := map[string]string{
		messageKey: partition0,
	}

	// subscriber of the given queue
	subscriberApplication := func(appID string, queueName string, messagesWatcher *watcher.Watcher) app.SetupFn {
		return func(ctx flow.Context, s common.Service) error {
			// Simulate periodic errors.
			sim := simulate.PeriodicError(ctx, 100)
			// Setup the /orders event handler.
			return multierr.Combine(
				s.AddTopicEventHandler(&common.Subscription{
					PubsubName: pubsubName,
					Topic:      queueName,
					Route:      "/orders",
				}, func(_ context.Context, e *common.TopicEvent) (retry bool, err error) {
					if err := sim(); err != nil {
						return true, err
					}

					// Track/Observe the data of the event.
					messagesWatcher.Observe(e.Data)
					ctx.Logf("Message Received appID: %s, pubsub: %s, queue: %s, id: %s, data: %s", appID, e.PubsubName, e.Topic, e.ID, e.Data)
					return false, nil
				}),
			)
		}
	}

	publishMessages := func(metadata map[string]string, sidecarName string, queueName string, messageWatchers ...*watcher.Watcher) flow.Runnable {
		return func(ctx flow.Context) error {
			// prepare the messages
			messages := make([]string, numMessages)
			for i := range messages {
				messages[i] = fmt.Sprintf("partitionKey: %s, message for queue: %s, index: %03d, uniqueId: %s", metadata[messageKey], queueName, i, uuid.New().String())
			}

			// add the messages as expectations to the watchers
			for _, messageWatcher := range messageWatchers {
				messageWatcher.ExpectStrings(messages...)
			}

			// get the sidecar (dapr) client
			client := sidecar.GetClient(ctx, sidecarName)

			// publish messages
			ctx.Logf("Publishing messages. sidecarName: %s, queueName: %s", sidecarName, queueName)

			var publishOptions dapr.PublishEventOption

			if metadata != nil {
				publishOptions = dapr.PublishEventWithMetadata(metadata)
			}

			for _, message := range messages {
				ctx.Logf("Publishing: %q", message)
				var err error

				if publishOptions != nil {
					err = client.PublishEvent(ctx, pubsubName, queueName, message, publishOptions)
				} else {
					err = client.PublishEvent(ctx, pubsubName, queueName, message)
				}
				require.NoError(ctx, err, "error publishing message")
			}
			return nil
		}
	}

	assertMessages := func(timeout time.Duration, messageWatchers ...*watcher.Watcher) flow.Runnable {
		return func(ctx flow.Context) error {
			// assert for messages
			for _, m := range messageWatchers {
				m.Assert(ctx, 25*timeout)
			}

			return nil
		}
	}

	flow.New(t, "servicebus queues certification - multiple subscribers same app").

		// Run subscriberApplication app1
		Step(app.Run(appID1, fmt.Sprintf(":%d", appPort),
			subscriberApplication(appID1, queueActiveName, consumerGroup1))).

		// Run the Dapr sidecar with the servicebus queues component 1
		Step(sidecar.Run(sidecarName1,
			append(componentRuntimeOptions(),
				embedded.WithComponentsPath("./components/consumer_one"),
				embedded.WithAppProtocol(protocol.HTTPProtocol, strconv.Itoa(appPort)),
				embedded.WithDaprGRPCPort(strconv.Itoa(runtime.DefaultDaprAPIGRPCPort)),
				embedded.WithDaprHTTPPort(strconv.Itoa(runtime.DefaultDaprHTTPPort)),
			)...,
		)).
		Step("publish messages to queue", publishMessages(metadata, sidecarName1, queueActiveName, consumerGroup1)).
		Step("verify if app1 has received messages published to queue", assertMessages(10*time.Second, consumerGroup1)).
		Step("reset", flow.Reset(consumerGroup1)).
		Run()
}

func TestServicebusQueuesNonexistingQueue(t *testing.T) {
	consumerGroup1 := watcher.NewUnordered()

	// Set the partition key on all messages so they are written to the same partition.
	metadata := map[string]string{
		messageKey: partition0,
	}

	// subscriber of the given queue
	subscriberApplication := func(appID string, queueName string, messagesWatcher *watcher.Watcher) app.SetupFn {
		return func(ctx flow.Context, s common.Service) error {
			// Simulate periodic errors.
			sim := simulate.PeriodicError(ctx, 100)
			// Setup the /orders event handler.
			return multierr.Combine(
				s.AddTopicEventHandler(&common.Subscription{
					PubsubName: pubsubName,
					Topic:      queueName,
					Route:      "/orders",
				}, func(_ context.Context, e *common.TopicEvent) (retry bool, err error) {
					if err := sim(); err != nil {
						return true, err
					}

					// Track/Observe the data of the event.
					messagesWatcher.Observe(e.Data)
					ctx.Logf("Message Received appID: %s, pubsub: %s, queue: %s, id: %s, data: %s", appID, e.PubsubName, e.Topic, e.ID, e.Data)
					return false, nil
				}),
			)
		}
	}

	publishMessages := func(metadata map[string]string, sidecarName string, queueName string, messageWatchers ...*watcher.Watcher) flow.Runnable {
		return func(ctx flow.Context) error {
			// prepare the messages
			messages := make([]string, numMessages)
			for i := range messages {
				messages[i] = fmt.Sprintf("partitionKey: %s, message for queue: %s, index: %03d, uniqueId: %s", metadata[messageKey], queueName, i, uuid.New().String())
			}

			// add the messages as expectations to the watchers
			for _, messageWatcher := range messageWatchers {
				messageWatcher.ExpectStrings(messages...)
			}

			// get the sidecar (dapr) client
			client := sidecar.GetClient(ctx, sidecarName)

			// publish messages
			ctx.Logf("Publishing messages. sidecarName: %s, queueName: %s", sidecarName, queueName)

			var publishOptions dapr.PublishEventOption

			if metadata != nil {
				publishOptions = dapr.PublishEventWithMetadata(metadata)
			}

			for _, message := range messages {
				ctx.Logf("Publishing: %q", message)
				var err error

				if publishOptions != nil {
					err = client.PublishEvent(ctx, pubsubName, queueName, message, publishOptions)
				} else {
					err = client.PublishEvent(ctx, pubsubName, queueName, message)
				}
				require.NoError(ctx, err, "error publishing message")
			}
			return nil
		}
	}

	assertMessages := func(timeout time.Duration, messageWatchers ...*watcher.Watcher) flow.Runnable {
		return func(ctx flow.Context) error {
			// assert for messages
			for _, m := range messageWatchers {
				m.Assert(ctx, 25*timeout)
			}

			return nil
		}
	}

	flow.New(t, "servicebus queues certification - non-existing queue").

		// Run subscriberApplication app1
		Step(app.Run(appID1, fmt.Sprintf(":%d", appPort+portOffset*3),
			subscriberApplication(appID1, queueToBeCreated, consumerGroup1))).

		// Run the Dapr sidecar with the component entitymanagement
		Step(sidecar.Run(sidecarName1,
			append(componentRuntimeOptions(),
				embedded.WithComponentsPath("./components/consumer_one"),
				embedded.WithAppProtocol(protocol.HTTPProtocol, strconv.Itoa(appPort+portOffset*3)),
				embedded.WithDaprGRPCPort(strconv.Itoa(runtime.DefaultDaprAPIGRPCPort+portOffset*3)),
				embedded.WithDaprHTTPPort(strconv.Itoa(runtime.DefaultDaprHTTPPort+portOffset*3)),
				embedded.WithProfilePort(strconv.Itoa(runtime.DefaultProfilePort+portOffset*3)),
			)...,
		)).
		Step(fmt.Sprintf("publish messages to queueToBeCreated: %s", queueToBeCreated), publishMessages(metadata, sidecarName1, queueToBeCreated, consumerGroup1)).
		Step("wait", flow.Sleep(30*time.Second)).
		Step("verify if app1 has received messages published to newly created queue", assertMessages(10*time.Second, consumerGroup1)).
		Run()
}

func TestServicebusQueuesNetworkInterruption(t *testing.T) {
	consumerGroup1 := watcher.NewUnordered()

	// Set the partition key on all messages so they are written to the same partition.
	metadata := map[string]string{
		messageKey: partition0,
	}

	// subscriber of the given queue
	subscriberApplication := func(appID string, queueName string, messagesWatcher *watcher.Watcher) app.SetupFn {
		return func(ctx flow.Context, s common.Service) error {
			// Simulate periodic errors.
			sim := simulate.PeriodicError(ctx, 100)
			// Setup the /orders event handler.
			return multierr.Combine(
				s.AddTopicEventHandler(&common.Subscription{
					PubsubName: pubsubName,
					Topic:      queueName,
					Route:      "/orders",
				}, func(_ context.Context, e *common.TopicEvent) (retry bool, err error) {
					if err := sim(); err != nil {
						return true, err
					}

					// Track/Observe the data of the event.
					messagesWatcher.Observe(e.Data)
					ctx.Logf("Message Received appID: %s, pubsub: %s, queue: %s, id: %s, data: %s", appID, e.PubsubName, e.Topic, e.ID, e.Data)
					return false, nil
				}),
			)
		}
	}

	publishMessages := func(metadata map[string]string, sidecarName string, queueName string, messageWatchers ...*watcher.Watcher) flow.Runnable {
		return func(ctx flow.Context) error {
			// prepare the messages
			messages := make([]string, numMessages)
			for i := range messages {
				messages[i] = fmt.Sprintf("partitionKey: %s, message for queue: %s, index: %03d, uniqueId: %s", metadata[messageKey], queueName, i, uuid.New().String())
			}

			// add the messages as expectations to the watchers
			for _, messageWatcher := range messageWatchers {
				messageWatcher.ExpectStrings(messages...)
			}

			// get the sidecar (dapr) client
			client := sidecar.GetClient(ctx, sidecarName)

			// publish messages
			ctx.Logf("Publishing messages. sidecarName: %s, queueName: %s", sidecarName, queueName)

			var publishOptions dapr.PublishEventOption

			if metadata != nil {
				publishOptions = dapr.PublishEventWithMetadata(metadata)
			}

			for _, message := range messages {
				ctx.Logf("Publishing: %q", message)
				var err error

				if publishOptions != nil {
					err = client.PublishEvent(ctx, pubsubName, queueName, message, publishOptions)
				} else {
					err = client.PublishEvent(ctx, pubsubName, queueName, message)
				}
				require.NoError(ctx, err, "error publishing message")
			}
			return nil
		}
	}

	assertMessages := func(timeout time.Duration, messageWatchers ...*watcher.Watcher) flow.Runnable {
		return func(ctx flow.Context) error {
			// assert for messages
			for _, m := range messageWatchers {
				m.Assert(ctx, 25*timeout)
			}

			return nil
		}
	}

	flow.New(t, "servicebus queues certification - network interruption").

		// Run subscriberApplication app1
		Step(app.Run(appID1, fmt.Sprintf(":%d", appPort+portOffset),
			subscriberApplication(appID1, queueActiveName, consumerGroup1))).

		// Run the Dapr sidecar with the component entitymanagement
		Step(sidecar.Run(sidecarName1,
			append(componentRuntimeOptions(),
				embedded.WithComponentsPath("./components/consumer_one"),
				embedded.WithAppProtocol(protocol.HTTPProtocol, strconv.Itoa(appPort+portOffset)),
				embedded.WithDaprGRPCPort(strconv.Itoa(runtime.DefaultDaprAPIGRPCPort+portOffset)),
				embedded.WithDaprHTTPPort(strconv.Itoa(runtime.DefaultDaprHTTPPort+portOffset)),
				embedded.WithProfilePort(strconv.Itoa(runtime.DefaultProfilePort+portOffset)),
			)...,
		)).
		Step(fmt.Sprintf("publish messages to queue: %s", queueActiveName), publishMessages(metadata, sidecarName1, queueActiveName, consumerGroup1)).
		Step("interrupt network", network.InterruptNetwork(time.Minute, []string{}, []string{}, "5671", "5672")).
		Step("wait", flow.Sleep(30*time.Second)).
		Step("verify if app1 has received messages published to queue", assertMessages(10*time.Second, consumerGroup1)).
		Run()
}

func TestServicebusQueuesEntityManagement(t *testing.T) {
	consumerGroup1 := watcher.NewUnordered()

	// Use a unique queue name that does NOT exist in Azure
	// This test verifies that publishing fails when entity management is disabled
	// and the queue doesn't exist
	nonExistentQueue := fmt.Sprintf("non-existent-queue-%s", uuid.New().String())

	// Set the partition key on all messages so they are written to the same partition.
	metadata := map[string]string{
		messageKey: partition0,
	}

	subscriberApplication := func(appID string, queueName string, messagesWatcher *watcher.Watcher) app.SetupFn {
		return func(ctx flow.Context, s common.Service) error {
			// Setup the /orders event handler.
			return multierr.Combine(
				s.AddTopicEventHandler(&common.Subscription{
					PubsubName: pubsubName,
					Topic:      queueName,
					Route:      "/orders",
				}, func(_ context.Context, e *common.TopicEvent) (retry bool, err error) {
					// Track/Observe the data of the event.
					messagesWatcher.Observe(e.Data)
					ctx.Logf("Message Received appID: %s, pubsub: %s, queue: %s, id: %s, data: %s", appID, e.PubsubName, e.Topic, e.ID, e.Data)
					return false, nil
				}),
			)
		}
	}

	publishMessages := func(metadata map[string]string, sidecarName string, queueName string, messageWatchers ...*watcher.Watcher) flow.Runnable {
		return func(ctx flow.Context) error {
			// prepare the messages
			messages := make([]string, numMessages)
			for i := range messages {
				messages[i] = fmt.Sprintf("partitionKey: %s, message for queue: %s, index: %03d, uniqueId: %s", metadata[messageKey], queueName, i, uuid.New().String())
			}

			// add the messages as expectations to the watchers
			for _, messageWatcher := range messageWatchers {
				messageWatcher.ExpectStrings(messages...)
			}

			// get the sidecar (dapr) client
			client := sidecar.GetClient(ctx, sidecarName)

			// publish messages
			ctx.Logf("Publishing messages. sidecarName: %s, queueName: %s", sidecarName, queueName)

			var publishOptions dapr.PublishEventOption

			if metadata != nil {
				publishOptions = dapr.PublishEventWithMetadata(metadata)
			}

			for _, message := range messages {
				ctx.Logf("Publishing: %q", message)
				var err error

				if publishOptions != nil {
					err = client.PublishEvent(ctx, pubsubName, queueName, message, publishOptions)
				} else {
					err = client.PublishEvent(ctx, pubsubName, queueName, message)
				}
				// Error is expected as the queue does not exist
				require.Error(ctx, err, "error publishing message")
			}
			return nil
		}
	}

	flow.New(t, "servicebus queues certification - entity management disabled").

		// Run subscriberApplication app1
		Step(app.Run(appID1, fmt.Sprintf(":%d", appPort+portOffset),
			subscriberApplication(appID1, queueActiveName, consumerGroup1))).
		Step(sidecar.Run(sidecarName1,
			append(componentRuntimeOptions(),
				embedded.WithComponentsPath("./components/entity_mgmt"),
				embedded.WithAppProtocol(protocol.HTTPProtocol, strconv.Itoa(appPort+portOffset)),
				embedded.WithDaprGRPCPort(strconv.Itoa(runtime.DefaultDaprAPIGRPCPort+portOffset)),
				embedded.WithDaprHTTPPort(strconv.Itoa(runtime.DefaultDaprHTTPPort+portOffset)),
				embedded.WithProfilePort(strconv.Itoa(runtime.DefaultProfilePort+portOffset)),
			)...,
		)).
		Step(fmt.Sprintf("publish messages to non-existent queue: %s", nonExistentQueue), publishMessages(metadata, sidecarName1, nonExistentQueue, consumerGroup1)).
		Run()
}

func TestServicebusQueuesDefaultTtl(t *testing.T) {
	consumerGroup1 := watcher.NewUnordered()

	// Set the partition key on all messages so they are written to the same partition.
	metadata := map[string]string{
		messageKey: partition0,
	}

	// subscriber of the given queue
	subscriberApplication := func(appID string, queueName string, messagesWatcher *watcher.Watcher) app.SetupFn {
		return func(ctx flow.Context, s common.Service) error {
			// Setup the /orders event handler.
			return multierr.Combine(
				s.AddTopicEventHandler(&common.Subscription{
					PubsubName: pubsubName,
					Topic:      queueName,
					Route:      "/orders",
				}, func(_ context.Context, e *common.TopicEvent) (retry bool, err error) {
					ctx.Logf("Got message: %s", e.Data)
					messagesWatcher.FailIfNotExpected(t, e.Data)
					return false, nil
				}),
			)
		}
	}

	testTtlPublishMessages := func(metadata map[string]string, sidecarName string, queueName string, messageWatchers ...*watcher.Watcher) flow.Runnable {
		return func(ctx flow.Context) error {
			// prepare the messages
			messages := make([]string, numMessages)
			for i := range messages {
				messages[i] = fmt.Sprintf("partitionKey: %s, message for queue: %s, index: %03d, uniqueId: %s", metadata[messageKey], queueName, i, uuid.New().String())
			}

			// get the sidecar (dapr) client
			client := sidecar.GetClient(ctx, sidecarName)

			// publish messages
			ctx.Logf("Publishing messages. sidecarName: %s, queueName: %s", sidecarName, queueName)

			var publishOptions dapr.PublishEventOption

			if metadata != nil {
				publishOptions = dapr.PublishEventWithMetadata(metadata)
			}

			for _, message := range messages {
				ctx.Logf("Publishing: %q", message)
				var err error

				if publishOptions != nil {
					err = client.PublishEvent(ctx, pubsubName, queueName, message, publishOptions)
				} else {
					err = client.PublishEvent(ctx, pubsubName, queueName, message)
				}
				require.NoError(ctx, err, "error publishing message")
			}
			return nil
		}
	}

	assertMessages := func(timeout time.Duration, messageWatchers ...*watcher.Watcher) flow.Runnable {
		return func(ctx flow.Context) error {
			// assert for messages
			for _, m := range messageWatchers {
				m.Assert(ctx, 25*timeout)
			}

			return nil
		}
	}

	flow.New(t, "servicebus queues certification - default ttl attribute").

		// Run subscriberApplication app1
		Step(app.Run(appID1, fmt.Sprintf(":%d", appPort+portOffset),
			subscriberApplication(appID1, queueActiveName, consumerGroup1))).

		// Run the Dapr sidecar with the component default_ttl
		Step(sidecar.Run("initialSidecar",
			append(componentRuntimeOptions(),
				embedded.WithComponentsPath("./components/default_ttl"),
				embedded.WithoutApp(),
				embedded.WithDaprGRPCPort(strconv.Itoa(runtime.DefaultDaprAPIGRPCPort+portOffset)),
				embedded.WithDaprHTTPPort(strconv.Itoa(runtime.DefaultDaprHTTPPort+portOffset)),
				embedded.WithProfilePort(strconv.Itoa(runtime.DefaultProfilePort+portOffset)),
			)...,
		)).
		Step(fmt.Sprintf("publish messages to queue: %s", queueActiveName), testTtlPublishMessages(metadata, "initialSidecar", queueActiveName, consumerGroup1)).
		Step("stop initial sidecar", sidecar.Stop("initialSidecar")).
		Step("wait", flow.Sleep(20*time.Second)).
		Step(sidecar.Run(sidecarName1,
			append(componentRuntimeOptions(),
				embedded.WithComponentsPath("./components/default_ttl"),
				embedded.WithAppProtocol(protocol.HTTPProtocol, strconv.Itoa(appPort+portOffset)),
				embedded.WithDaprGRPCPort(strconv.Itoa(runtime.DefaultDaprAPIGRPCPort+portOffset*2)),
				embedded.WithDaprHTTPPort(strconv.Itoa(runtime.DefaultDaprHTTPPort+portOffset*2)),
				embedded.WithProfilePort(strconv.Itoa(runtime.DefaultProfilePort+portOffset*2)),
			)...,
		)).
		Step("verify if app1 has received messages published to queue", assertMessages(10*time.Second, consumerGroup1)).
		Run()
}

func TestServicebusQueuesAuthentication(t *testing.T) {
	consumerGroup1 := watcher.NewUnordered()

	// Set the partition key on all messages so they are written to the same partition.
	metadata := map[string]string{
		messageKey: partition0,
	}

	// subscriber of the given queue
	subscriberApplication := func(appID string, queueName string, messagesWatcher *watcher.Watcher) app.SetupFn {
		return func(ctx flow.Context, s common.Service) error {
			// Simulate periodic errors.
			sim := simulate.PeriodicError(ctx, 100)
			// Setup the /orders event handler.
			return multierr.Combine(
				s.AddTopicEventHandler(&common.Subscription{
					PubsubName: pubsubName,
					Topic:      queueName,
					Route:      "/orders",
				}, func(_ context.Context, e *common.TopicEvent) (retry bool, err error) {
					if err := sim(); err != nil {
						return true, err
					}

					// Track/Observe the data of the event.
					messagesWatcher.Observe(e.Data)
					ctx.Logf("Message Received appID: %s, pubsub: %s, queue: %s, id: %s, data: %s", appID, e.PubsubName, e.Topic, e.ID, e.Data)
					return false, nil
				}),
			)
		}
	}

	publishMessages := func(metadata map[string]string, sidecarName string, queueName string, messageWatchers ...*watcher.Watcher) flow.Runnable {
		return func(ctx flow.Context) error {
			// prepare the messages
			messages := make([]string, numMessages)
			for i := range messages {
				messages[i] = fmt.Sprintf("partitionKey: %s, message for queue: %s, index: %03d, uniqueId: %s", metadata[messageKey], queueName, i, uuid.New().String())
			}

			// add the messages as expectations to the watchers
			for _, messageWatcher := range messageWatchers {
				messageWatcher.ExpectStrings(messages...)
			}

			// get the sidecar (dapr) client
			client := sidecar.GetClient(ctx, sidecarName)

			// publish messages
			ctx.Logf("Publishing messages. sidecarName: %s, queueName: %s", sidecarName, queueName)

			var publishOptions dapr.PublishEventOption

			if metadata != nil {
				publishOptions = dapr.PublishEventWithMetadata(metadata)
			}

			for _, message := range messages {
				ctx.Logf("Publishing: %q", message)
				var err error

				if publishOptions != nil {
					err = client.PublishEvent(ctx, pubsubName, queueName, message, publishOptions)
				} else {
					err = client.PublishEvent(ctx, pubsubName, queueName, message)
				}
				require.NoError(ctx, err, "error publishing message")
			}
			return nil
		}
	}

	assertMessages := func(timeout time.Duration, messageWatchers ...*watcher.Watcher) flow.Runnable {
		return func(ctx flow.Context) error {
			// assert for messages
			for _, m := range messageWatchers {
				m.Assert(ctx, 25*timeout)
			}

			return nil
		}
	}

	flow.New(t, "servicebus queues certification - authentication").

		// Run subscriberApplication app1
		Step(app.Run(appID1, fmt.Sprintf(":%d", appPort+portOffset),
			subscriberApplication(appID1, queueActiveName, consumerGroup1))).

		// Run the Dapr sidecar with the authentication component
		Step(sidecar.Run(sidecarName1,
			append(componentRuntimeOptions(),
				embedded.WithComponentsPath("./components/authentication"),
				embedded.WithAppProtocol(protocol.HTTPProtocol, strconv.Itoa(appPort+portOffset)),
				embedded.WithDaprGRPCPort(strconv.Itoa(runtime.DefaultDaprAPIGRPCPort+portOffset)),
				embedded.WithDaprHTTPPort(strconv.Itoa(runtime.DefaultDaprHTTPPort+portOffset)),
				embedded.WithProfilePort(strconv.Itoa(runtime.DefaultProfilePort+portOffset)),
			)...,
		)).
		Step("wait for sidecar to be ready", flow.Sleep(10*time.Second)).
		Step(fmt.Sprintf("publish messages to queue: %s", queueActiveName), publishMessages(metadata, sidecarName1, queueActiveName, consumerGroup1)).
		Step("wait", flow.Sleep(30*time.Second)).
		Step("verify if app1 has received messages published to queue", assertMessages(10*time.Second, consumerGroup1)).
		Run()
}

// TestServicebusQueuesMessageMetadata tests that message metadata is correctly passed
func TestServicebusQueuesMessageMetadata(t *testing.T) {
	consumerGroup1 := watcher.NewUnordered()

	metadata := map[string]string{
		messageKey: partition0,
	}

	subscriberApplication := func(appID string, queueName string, messagesWatcher *watcher.Watcher) app.SetupFn {
		return func(ctx flow.Context, s common.Service) error {
			return multierr.Combine(
				s.AddTopicEventHandler(&common.Subscription{
					PubsubName: pubsubName,
					Topic:      queueName,
					Route:      "/orders",
				}, func(_ context.Context, e *common.TopicEvent) (retry bool, err error) {
					messagesWatcher.Observe(e.Data)
					ctx.Logf("Message Received appID: %s, pubsub: %s, queue: %s, id: %s, data: %s", appID, e.PubsubName, e.Topic, e.ID, e.Data)
					return false, nil
				}),
			)
		}
	}

	publishMessages := func(metadata map[string]string, sidecarName string, queueName string, messageWatchers ...*watcher.Watcher) flow.Runnable {
		return func(ctx flow.Context) error {
			messages := make([]string, numMessages)
			for i := range messages {
				messages[i] = fmt.Sprintf("partitionKey: %s, message for queue: %s, index: %03d, uniqueId: %s", metadata[messageKey], queueName, i, uuid.New().String())
			}

			for _, messageWatcher := range messageWatchers {
				messageWatcher.ExpectStrings(messages...)
			}

			client := sidecar.GetClient(ctx, sidecarName)
			ctx.Logf("Publishing messages with metadata. sidecarName: %s, queueName: %s", sidecarName, queueName)

			publishOptions := dapr.PublishEventWithMetadata(metadata)

			for _, message := range messages {
				ctx.Logf("Publishing: %q", message)
				err := client.PublishEvent(ctx, pubsubName, queueName, message, publishOptions)
				require.NoError(ctx, err, "error publishing message")
			}
			return nil
		}
	}

	assertMessages := func(timeout time.Duration, messageWatchers ...*watcher.Watcher) flow.Runnable {
		return func(ctx flow.Context) error {
			for _, m := range messageWatchers {
				m.Assert(ctx, 25*timeout)
			}
			return nil
		}
	}

	flow.New(t, "servicebus queues certification - message metadata").
		Step(app.Run(appID1, fmt.Sprintf(":%d", appPort),
			subscriberApplication(appID1, queueActiveName, consumerGroup1))).
		Step(sidecar.Run(sidecarName1,
			append(componentRuntimeOptions(),
				embedded.WithComponentsPath("./components/consumer_one"),
				embedded.WithAppProtocol(protocol.HTTPProtocol, strconv.Itoa(appPort)),
				embedded.WithDaprGRPCPort(strconv.Itoa(runtime.DefaultDaprAPIGRPCPort)),
				embedded.WithDaprHTTPPort(strconv.Itoa(runtime.DefaultDaprHTTPPort)),
			)...,
		)).
		Step("publish messages with metadata", publishMessages(metadata, sidecarName1, queueActiveName, consumerGroup1)).
		Step("verify messages received", assertMessages(10*time.Second, consumerGroup1)).
		Step("reset", flow.Reset(consumerGroup1)).
		Run()
}

// TestServicebusQueuesMultipleQueues tests publishing to multiple queues
func TestServicebusQueuesMultipleQueues(t *testing.T) {
	activeQueueWatcher := watcher.NewUnordered()
	passiveQueueWatcher := watcher.NewUnordered()

	metadata := map[string]string{
		messageKey: partition0,
	}

	multiQueueSubscriber := func(appID string, activeWatcher, passiveWatcher *watcher.Watcher) app.SetupFn {
		return func(ctx flow.Context, s common.Service) error {
			return multierr.Combine(
				s.AddTopicEventHandler(&common.Subscription{
					PubsubName: pubsubName,
					Topic:      queueActiveName,
					Route:      "/active",
				}, func(_ context.Context, e *common.TopicEvent) (retry bool, err error) {
					activeWatcher.Observe(e.Data)
					ctx.Logf("Active Queue - Message Received: %s", e.Data)
					return false, nil
				}),
				s.AddTopicEventHandler(&common.Subscription{
					PubsubName: pubsubName,
					Topic:      queuePassiveName,
					Route:      "/passive",
				}, func(_ context.Context, e *common.TopicEvent) (retry bool, err error) {
					passiveWatcher.Observe(e.Data)
					ctx.Logf("Passive Queue - Message Received: %s", e.Data)
					return false, nil
				}),
			)
		}
	}

	publishMessages := func(metadata map[string]string, sidecarName string, queueName string, messageWatchers ...*watcher.Watcher) flow.Runnable {
		return func(ctx flow.Context) error {
			messages := make([]string, numMessages)
			for i := range messages {
				messages[i] = fmt.Sprintf("partitionKey: %s, message for queue: %s, index: %03d, uniqueId: %s", metadata[messageKey], queueName, i, uuid.New().String())
			}

			for _, messageWatcher := range messageWatchers {
				messageWatcher.ExpectStrings(messages...)
			}

			client := sidecar.GetClient(ctx, sidecarName)
			ctx.Logf("Publishing messages. sidecarName: %s, queueName: %s", sidecarName, queueName)

			publishOptions := dapr.PublishEventWithMetadata(metadata)

			for _, message := range messages {
				ctx.Logf("Publishing: %q", message)
				err := client.PublishEvent(ctx, pubsubName, queueName, message, publishOptions)
				require.NoError(ctx, err, "error publishing message")
			}
			return nil
		}
	}

	assertMessages := func(timeout time.Duration, messageWatchers ...*watcher.Watcher) flow.Runnable {
		return func(ctx flow.Context) error {
			for _, m := range messageWatchers {
				m.Assert(ctx, 25*timeout)
			}
			return nil
		}
	}

	flow.New(t, "servicebus queues certification - multiple queues").
		Step(app.Run(appID1, fmt.Sprintf(":%d", appPort),
			multiQueueSubscriber(appID1, activeQueueWatcher, passiveQueueWatcher))).
		Step(sidecar.Run(sidecarName1,
			append(componentRuntimeOptions(),
				embedded.WithComponentsPath("./components/consumer_one"),
				embedded.WithAppProtocol(protocol.HTTPProtocol, strconv.Itoa(appPort)),
				embedded.WithDaprGRPCPort(strconv.Itoa(runtime.DefaultDaprAPIGRPCPort)),
				embedded.WithDaprHTTPPort(strconv.Itoa(runtime.DefaultDaprHTTPPort)),
			)...,
		)).
		Step("publish to active queue", publishMessages(metadata, sidecarName1, queueActiveName, activeQueueWatcher)).
		Step("publish to passive queue", publishMessages(metadata, sidecarName1, queuePassiveName, passiveQueueWatcher)).
		Step("verify active queue messages", assertMessages(10*time.Second, activeQueueWatcher)).
		Step("verify passive queue messages", assertMessages(10*time.Second, passiveQueueWatcher)).
		Step("reset", flow.Reset(activeQueueWatcher, passiveQueueWatcher)).
		Run()
}

// TestServicebusQueuesLargeMessages tests handling of larger message payloads
func TestServicebusQueuesLargeMessages(t *testing.T) {
	consumerGroup1 := watcher.NewUnordered()

	subscriberApplication := func(appID string, queueName string, messagesWatcher *watcher.Watcher) app.SetupFn {
		return func(ctx flow.Context, s common.Service) error {
			return multierr.Combine(
				s.AddTopicEventHandler(&common.Subscription{
					PubsubName: pubsubName,
					Topic:      queueName,
					Route:      "/orders",
				}, func(_ context.Context, e *common.TopicEvent) (retry bool, err error) {
					messagesWatcher.Observe(e.Data)
					ctx.Logf("Large Message Received, length: %d", len(fmt.Sprintf("%v", e.Data)))
					return false, nil
				}),
			)
		}
	}

	publishLargeMessages := func(sidecarName string, queueName string, messageWatchers ...*watcher.Watcher) flow.Runnable {
		return func(ctx flow.Context) error {
			// Create larger messages (1KB each)
			messages := make([]string, 5)
			for i := range messages {
				// Create a 1KB payload
				payload := make([]byte, 1024)
				for j := range payload {
					payload[j] = byte('A' + (j % 26))
				}
				messages[i] = fmt.Sprintf("large-message-%03d-%s-%s", i, uuid.New().String(), string(payload))
			}

			for _, messageWatcher := range messageWatchers {
				messageWatcher.ExpectStrings(messages...)
			}

			client := sidecar.GetClient(ctx, sidecarName)
			ctx.Logf("Publishing large messages. sidecarName: %s, queueName: %s", sidecarName, queueName)

			for _, message := range messages {
				ctx.Logf("Publishing large message of length: %d", len(message))
				err := client.PublishEvent(ctx, pubsubName, queueName, message)
				require.NoError(ctx, err, "error publishing large message")
			}
			return nil
		}
	}

	assertMessages := func(timeout time.Duration, messageWatchers ...*watcher.Watcher) flow.Runnable {
		return func(ctx flow.Context) error {
			for _, m := range messageWatchers {
				m.Assert(ctx, 25*timeout)
			}
			return nil
		}
	}

	flow.New(t, "servicebus queues certification - large messages").
		Step(app.Run(appID1, fmt.Sprintf(":%d", appPort),
			subscriberApplication(appID1, queueActiveName, consumerGroup1))).
		Step(sidecar.Run(sidecarName1,
			append(componentRuntimeOptions(),
				embedded.WithComponentsPath("./components/consumer_one"),
				embedded.WithAppProtocol(protocol.HTTPProtocol, strconv.Itoa(appPort)),
				embedded.WithDaprGRPCPort(strconv.Itoa(runtime.DefaultDaprAPIGRPCPort)),
				embedded.WithDaprHTTPPort(strconv.Itoa(runtime.DefaultDaprHTTPPort)),
			)...,
		)).
		Step("publish large messages", publishLargeMessages(sidecarName1, queueActiveName, consumerGroup1)).
		Step("verify large messages received", assertMessages(15*time.Second, consumerGroup1)).
		Step("reset", flow.Reset(consumerGroup1)).
		Run()
}

// TestServicebusQueuesSequentialPublish tests sequential message publishing and ordering
func TestServicebusQueuesSequentialPublish(t *testing.T) {
	consumerGroup1 := watcher.NewUnordered()

	subscriberApplication := func(appID string, queueName string, messagesWatcher *watcher.Watcher) app.SetupFn {
		return func(ctx flow.Context, s common.Service) error {
			return multierr.Combine(
				s.AddTopicEventHandler(&common.Subscription{
					PubsubName: pubsubName,
					Topic:      queueName,
					Route:      "/orders",
				}, func(_ context.Context, e *common.TopicEvent) (retry bool, err error) {
					messagesWatcher.Observe(e.Data)
					ctx.Logf("Sequential Message Received: %s", e.Data)
					return false, nil
				}),
			)
		}
	}

	publishSequentialMessages := func(sidecarName string, queueName string, batchNum int, messageWatchers ...*watcher.Watcher) flow.Runnable {
		return func(ctx flow.Context) error {
			messages := make([]string, numMessages)
			for i := range messages {
				messages[i] = fmt.Sprintf("batch-%d-message-%03d-%s", batchNum, i, uuid.New().String())
			}

			for _, messageWatcher := range messageWatchers {
				messageWatcher.ExpectStrings(messages...)
			}

			client := sidecar.GetClient(ctx, sidecarName)
			ctx.Logf("Publishing batch %d messages. sidecarName: %s, queueName: %s", batchNum, sidecarName, queueName)

			for _, message := range messages {
				ctx.Logf("Publishing: %q", message)
				err := client.PublishEvent(ctx, pubsubName, queueName, message)
				require.NoError(ctx, err, "error publishing message")
			}
			return nil
		}
	}

	assertMessages := func(timeout time.Duration, messageWatchers ...*watcher.Watcher) flow.Runnable {
		return func(ctx flow.Context) error {
			for _, m := range messageWatchers {
				m.Assert(ctx, 25*timeout)
			}
			return nil
		}
	}

	flow.New(t, "servicebus queues certification - sequential publish").
		Step(app.Run(appID1, fmt.Sprintf(":%d", appPort),
			subscriberApplication(appID1, queueActiveName, consumerGroup1))).
		Step(sidecar.Run(sidecarName1,
			append(componentRuntimeOptions(),
				embedded.WithComponentsPath("./components/consumer_one"),
				embedded.WithAppProtocol(protocol.HTTPProtocol, strconv.Itoa(appPort)),
				embedded.WithDaprGRPCPort(strconv.Itoa(runtime.DefaultDaprAPIGRPCPort)),
				embedded.WithDaprHTTPPort(strconv.Itoa(runtime.DefaultDaprHTTPPort)),
			)...,
		)).
		Step("publish batch 1", publishSequentialMessages(sidecarName1, queueActiveName, 1, consumerGroup1)).
		Step("publish batch 2", publishSequentialMessages(sidecarName1, queueActiveName, 2, consumerGroup1)).
		Step("verify all messages received", assertMessages(15*time.Second, consumerGroup1)).
		Step("reset", flow.Reset(consumerGroup1)).
		Run()
}

// TestServicebusQueuesReconnection tests that the component reconnects after sidecar restart
func TestServicebusQueuesReconnection(t *testing.T) {
	consumerGroup1 := watcher.NewUnordered()
	// Use unique queue name to avoid interference from other tests
	uniqueQueueName := fmt.Sprintf("certification-reconnection-%s", uuid.New().String()[:8])

	subscriberApplication := func(appID string, queueName string, messagesWatcher *watcher.Watcher) app.SetupFn {
		return func(ctx flow.Context, s common.Service) error {
			return multierr.Combine(
				s.AddTopicEventHandler(&common.Subscription{
					PubsubName: pubsubName,
					Topic:      queueName,
					Route:      "/orders",
				}, func(_ context.Context, e *common.TopicEvent) (retry bool, err error) {
					messagesWatcher.Observe(e.Data)
					ctx.Logf("Message Received after reconnection: %s", e.Data)
					return false, nil
				}),
			)
		}
	}

	publishMessages := func(sidecarName string, queueName string, prefix string, messageWatchers ...*watcher.Watcher) flow.Runnable {
		return func(ctx flow.Context) error {
			messages := make([]string, numMessages)
			for i := range messages {
				messages[i] = fmt.Sprintf("%s-message-%03d-%s", prefix, i, uuid.New().String())
			}

			for _, messageWatcher := range messageWatchers {
				messageWatcher.ExpectStrings(messages...)
			}

			client := sidecar.GetClient(ctx, sidecarName)
			ctx.Logf("Publishing %s messages. sidecarName: %s, queueName: %s", prefix, sidecarName, queueName)

			for _, message := range messages {
				ctx.Logf("Publishing: %q", message)
				err := client.PublishEvent(ctx, pubsubName, queueName, message)
				require.NoError(ctx, err, "error publishing message")
			}
			return nil
		}
	}

	assertMessages := func(timeout time.Duration, messageWatchers ...*watcher.Watcher) flow.Runnable {
		return func(ctx flow.Context) error {
			for _, m := range messageWatchers {
				m.Assert(ctx, 25*timeout)
			}
			return nil
		}
	}

	flow.New(t, "servicebus queues certification - reconnection").
		Step(app.Run(appID1, fmt.Sprintf(":%d", appPort),
			subscriberApplication(appID1, uniqueQueueName, consumerGroup1))).
		Step(sidecar.Run(sidecarName1,
			append(componentRuntimeOptions(),
				embedded.WithComponentsPath("./components/consumer_one"),
				embedded.WithAppProtocol(protocol.HTTPProtocol, strconv.Itoa(appPort)),
				embedded.WithDaprGRPCPort(strconv.Itoa(runtime.DefaultDaprAPIGRPCPort)),
				embedded.WithDaprHTTPPort(strconv.Itoa(runtime.DefaultDaprHTTPPort)),
			)...,
		)).
		Step("publish initial messages", publishMessages(sidecarName1, uniqueQueueName, "initial", consumerGroup1)).
		Step("verify initial messages", assertMessages(10*time.Second, consumerGroup1)).
		Step("reset watcher", flow.Reset(consumerGroup1)).
		Step("stop sidecar", sidecar.Stop(sidecarName1)).
		Step("wait for shutdown", flow.Sleep(5*time.Second)).
		Step(sidecar.Run(sidecarName1,
			append(componentRuntimeOptions(),
				embedded.WithComponentsPath("./components/consumer_one"),
				embedded.WithAppProtocol(protocol.HTTPProtocol, strconv.Itoa(appPort)),
				embedded.WithDaprGRPCPort(strconv.Itoa(runtime.DefaultDaprAPIGRPCPort)),
				embedded.WithDaprHTTPPort(strconv.Itoa(runtime.DefaultDaprHTTPPort)),
			)...,
		)).
		Step("publish after reconnect", publishMessages(sidecarName1, uniqueQueueName, "reconnected", consumerGroup1)).
		Step("verify reconnected messages", assertMessages(10*time.Second, consumerGroup1)).
		Step("reset", flow.Reset(consumerGroup1)).
		Run()
}

// TestServicebusQueuesEmptyMessages tests handling of minimal message payloads
func TestServicebusQueuesEmptyMessages(t *testing.T) {
	consumerGroup1 := watcher.NewUnordered()
	// Use unique queue name to avoid interference from other tests
	uniqueQueueName := fmt.Sprintf("certification-minimal-messages-%s", uuid.New().String()[:8])

	subscriberApplication := func(appID string, queueName string, messagesWatcher *watcher.Watcher) app.SetupFn {
		return func(ctx flow.Context, s common.Service) error {
			return multierr.Combine(
				s.AddTopicEventHandler(&common.Subscription{
					PubsubName: pubsubName,
					Topic:      queueName,
					Route:      "/orders",
				}, func(_ context.Context, e *common.TopicEvent) (retry bool, err error) {
					messagesWatcher.Observe(e.Data)
					ctx.Logf("Message Received: %v (length: %d)", e.Data, len(fmt.Sprintf("%v", e.Data)))
					return false, nil
				}),
			)
		}
	}

	publishMinimalMessages := func(sidecarName string, queueName string, messageWatchers ...*watcher.Watcher) flow.Runnable {
		return func(ctx flow.Context) error {
			// Test minimal valid messages with unique identifiers to avoid collisions
			testID := uuid.New().String()[:8]
			messages := []string{
				fmt.Sprintf("minimal-a-%s", testID),
				fmt.Sprintf("minimal-ab-%s", testID),
				fmt.Sprintf("minimal-test-%s", testID),
				fmt.Sprintf("minimal-hello-%s", testID),
				fmt.Sprintf("minimal-12345-%s", testID),
			}

			for _, messageWatcher := range messageWatchers {
				messageWatcher.ExpectStrings(messages...)
			}

			client := sidecar.GetClient(ctx, sidecarName)
			ctx.Logf("Publishing minimal messages. sidecarName: %s, queueName: %s", sidecarName, queueName)

			for _, message := range messages {
				ctx.Logf("Publishing minimal message: %q", message)
				err := client.PublishEvent(ctx, pubsubName, queueName, message)
				require.NoError(ctx, err, "error publishing minimal message")
			}
			return nil
		}
	}

	assertMessages := func(timeout time.Duration, messageWatchers ...*watcher.Watcher) flow.Runnable {
		return func(ctx flow.Context) error {
			for _, m := range messageWatchers {
				m.Assert(ctx, 25*timeout)
			}
			return nil
		}
	}

	flow.New(t, "servicebus queues certification - minimal messages").
		Step(app.Run(appID1, fmt.Sprintf(":%d", appPort),
			subscriberApplication(appID1, uniqueQueueName, consumerGroup1))).
		Step(sidecar.Run(sidecarName1,
			append(componentRuntimeOptions(),
				embedded.WithComponentsPath("./components/consumer_one"),
				embedded.WithAppProtocol(protocol.HTTPProtocol, strconv.Itoa(appPort)),
				embedded.WithDaprGRPCPort(strconv.Itoa(runtime.DefaultDaprAPIGRPCPort)),
				embedded.WithDaprHTTPPort(strconv.Itoa(runtime.DefaultDaprHTTPPort)),
			)...,
		)).
		Step("publish minimal messages", publishMinimalMessages(sidecarName1, uniqueQueueName, consumerGroup1)).
		Step("verify minimal messages received", assertMessages(10*time.Second, consumerGroup1)).
		Step("reset", flow.Reset(consumerGroup1)).
		Run()
}

// TestServicebusQueuesConcurrentPublishers tests multiple publishers sending to the same queue
func TestServicebusQueuesConcurrentPublishers(t *testing.T) {
	sharedWatcher := watcher.NewUnordered()

	subscriberApplication := func(appID string, queueName string, messagesWatcher *watcher.Watcher) app.SetupFn {
		return func(ctx flow.Context, s common.Service) error {
			return multierr.Combine(
				s.AddTopicEventHandler(&common.Subscription{
					PubsubName: pubsubName,
					Topic:      queueName,
					Route:      "/orders",
				}, func(_ context.Context, e *common.TopicEvent) (retry bool, err error) {
					messagesWatcher.Observe(e.Data)
					ctx.Logf("Message from concurrent publisher: %s", e.Data)
					return false, nil
				}),
			)
		}
	}

	publishFromSidecar := func(sidecarName string, queueName string, publisherID string, messageWatchers ...*watcher.Watcher) flow.Runnable {
		return func(ctx flow.Context) error {
			messages := make([]string, 5) // 5 messages per publisher
			for i := range messages {
				messages[i] = fmt.Sprintf("publisher-%s-message-%03d-%s", publisherID, i, uuid.New().String())
			}

			for _, messageWatcher := range messageWatchers {
				messageWatcher.ExpectStrings(messages...)
			}

			client := sidecar.GetClient(ctx, sidecarName)
			ctx.Logf("Publisher %s sending messages via %s", publisherID, sidecarName)

			for _, message := range messages {
				ctx.Logf("Publishing: %q", message)
				err := client.PublishEvent(ctx, pubsubName, queueName, message)
				require.NoError(ctx, err, "error publishing message")
			}
			return nil
		}
	}

	assertMessages := func(timeout time.Duration, messageWatchers ...*watcher.Watcher) flow.Runnable {
		return func(ctx flow.Context) error {
			for _, m := range messageWatchers {
				m.Assert(ctx, 25*timeout)
			}
			return nil
		}
	}

	flow.New(t, "servicebus queues certification - concurrent publishers").
		Step(app.Run(appID1, fmt.Sprintf(":%d", appPort),
			subscriberApplication(appID1, queueActiveName, sharedWatcher))).
		Step(sidecar.Run(sidecarName1,
			append(componentRuntimeOptions(),
				embedded.WithComponentsPath("./components/consumer_one"),
				embedded.WithAppProtocol(protocol.HTTPProtocol, strconv.Itoa(appPort)),
				embedded.WithDaprGRPCPort(strconv.Itoa(runtime.DefaultDaprAPIGRPCPort)),
				embedded.WithDaprHTTPPort(strconv.Itoa(runtime.DefaultDaprHTTPPort)),
			)...,
		)).
		Step(sidecar.Run(sidecarName2,
			append(componentRuntimeOptions(),
				embedded.WithComponentsPath("./components/consumer_two"),
				embedded.WithoutApp(),
				embedded.WithDaprGRPCPort(strconv.Itoa(runtime.DefaultDaprAPIGRPCPort+portOffset)),
				embedded.WithDaprHTTPPort(strconv.Itoa(runtime.DefaultDaprHTTPPort+portOffset)),
				embedded.WithProfilePort(strconv.Itoa(runtime.DefaultProfilePort+portOffset)),
			)...,
		)).
		Step("publisher 1 sends messages", publishFromSidecar(sidecarName1, queueActiveName, "1", sharedWatcher)).
		Step("publisher 2 sends messages", publishFromSidecar(sidecarName2, queueActiveName, "2", sharedWatcher)).
		Step("verify all messages from both publishers", assertMessages(15*time.Second, sharedWatcher)).
		Step("reset", flow.Reset(sharedWatcher)).
		Run()
}

func componentRuntimeOptions() []embedded.Option {
	log := logger.NewLogger("dapr.components")
	log.SetOutputLevel(logger.DebugLevel)

	pubsubRegistry := pubsub_loader.NewRegistry()
	pubsubRegistry.Logger = log
	pubsubRegistry.RegisterComponent(pubsub_servicebus.NewAzureServiceBusQueues, "azure.servicebus.queues")

	secretstoreRegistry := secretstores_loader.NewRegistry()
	secretstoreRegistry.Logger = log
	secretstoreRegistry.RegisterComponent(secretstore_env.NewEnvSecretStore, "local.env")

	return []embedded.Option{
		embedded.WithPubSubs(pubsubRegistry),
		embedded.WithSecretStores(secretstoreRegistry),
	}
}
