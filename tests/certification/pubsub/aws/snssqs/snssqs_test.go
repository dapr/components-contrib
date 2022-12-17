/*
Copyright 2022 The Dapr Authors
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

package snssqs_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"go.uber.org/multierr"

	// Pub-Sub.
	pubsub_snssqs "github.com/dapr/components-contrib/pubsub/aws/snssqs"
	pubsub_loader "github.com/dapr/dapr/pkg/components/pubsub"
	"github.com/dapr/kit/logger"

	"github.com/dapr/dapr/pkg/runtime"
	dapr "github.com/dapr/go-sdk/client"
	"github.com/dapr/go-sdk/service/common"

	// Certification testing runnables
	"github.com/dapr/components-contrib/tests/certification/embedded"
	"github.com/dapr/components-contrib/tests/certification/flow"
	"github.com/dapr/components-contrib/tests/certification/flow/app"
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
	pubsubName       = "snssqs-cert-tests"
	topicActiveName  = "certification-pubsub-topic-active"
	topicPassiveName = "certification-pubsub-topic-passive"
	topicToBeCreated = "certification-topic-per-test-run"
	topicDefaultName = "certification-topic-default"
	partition0       = "partition-0"
	partition1       = "partition-1"
)

// The following Queue names must match
// the values of the "consumerID" metadata properties
// found inside each of the components/*/pubsub.yaml files
var queues = []string{
	"snssqscerttest1",
	"snssqscerttest2",
}

func TestAWSSNSSQSCertificationTests(t *testing.T) {
	defer teardown(t)

	t.Run("SNSSQSBasic", func(t *testing.T) {
		SNSSQSBasic(t)
	})

	t.Run("SNSSQSMultipleSubsSameConsumerIDs", func(t *testing.T) {
		SNSSQSMultipleSubsSameConsumerIDs(t)
	})

	t.Run("SNSSQSMultipleSubsDifferentConsumerIDs", func(t *testing.T) {
		SNSSQSMultipleSubsDifferentConsumerIDs(t)
	})

	t.Run("SNSSQSMultiplePubSubsDifferentConsumerIDs", func(t *testing.T) {
		SNSSQSMultiplePubSubsDifferentConsumerIDs(t)
	})

	t.Run("SNSSQSNonexistingTopic", func(t *testing.T) {
		SNSSQSNonexistingTopic(t)
	})

	t.Run("SNSSQSEntityManagement", func(t *testing.T) {
		SNSSQSEntityManagement(t)
	})

	t.Run("SNSSQSDefaultTtl", func(t *testing.T) {
		SNSSQSDefaultTtl(t)
	})
}

// Verify with single publisher / single subscriber
func SNSSQSBasic(t *testing.T) {

	consumerGroup1 := watcher.NewUnordered()
	consumerGroup2 := watcher.NewUnordered()

	// subscriber of the given topic
	subscriberApplication := func(appID string, topicName string, messagesWatcher *watcher.Watcher) app.SetupFn {
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
					messagesWatcher.Observe(e.Data)
					ctx.Logf("Message Received appID: %s,pubsub: %s, topic: %s, id: %s, data: %s", appID, e.PubsubName, e.Topic, e.ID, e.Data)
					return false, nil
				}),
			)
		}
	}

	publishMessages := func(metadata map[string]string, sidecarName string, topicName string, messageWatchers ...*watcher.Watcher) flow.Runnable {
		return func(ctx flow.Context) error {
			// prepare the messages
			messages := make([]string, numMessages)
			for i := range messages {
				messages[i] = fmt.Sprintf("partitionKey: %s, message for topic: %s, index: %03d, uniqueId: %s", metadata[messageKey], topicName, i, uuid.New().String())
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
				require.NoError(ctx, err, "SNSSQSBasic - error publishing message")
			}
			return nil
		}
	}

	assertMessages := func(timeout time.Duration, messageWatchers ...*watcher.Watcher) flow.Runnable {
		return func(ctx flow.Context) error {
			// assert for messages
			for _, m := range messageWatchers {
				if !m.Assert(ctx, 25*timeout) {
					ctx.Errorf("SNSSQSBasic - message assersion failed for watcher: %#v\n", m)
				}
			}

			return nil
		}
	}

	flow.New(t, "SNSSQS Verify with single publisher / single subscriber").

		// Run subscriberApplication app1
		Step(app.Run(appID1, fmt.Sprintf(":%d", appPort),
			subscriberApplication(appID1, topicActiveName, consumerGroup1))).

		// Run the Dapr sidecar with ConsumerID "snssqscerttest1"
		Step(sidecar.Run(sidecarName1,
			embedded.WithComponentsPath("./components/consumer_one"),
			embedded.WithAppProtocol(runtime.HTTPProtocol, appPort),
			embedded.WithDaprGRPCPort(runtime.DefaultDaprAPIGRPCPort),
			embedded.WithDaprHTTPPort(runtime.DefaultDaprHTTPPort),
			componentRuntimeOptions(),
		)).

		// Run subscriberApplication app2
		Step(app.Run(appID2, fmt.Sprintf(":%d", appPort+portOffset),
			subscriberApplication(appID2, topicActiveName, consumerGroup2))).

		// Run the Dapr sidecar with ConsumerID "snssqscerttest2"
		Step(sidecar.Run(sidecarName2,
			embedded.WithComponentsPath("./components/consumer_two"),
			embedded.WithAppProtocol(runtime.HTTPProtocol, appPort+portOffset),
			embedded.WithDaprGRPCPort(runtime.DefaultDaprAPIGRPCPort+portOffset),
			embedded.WithDaprHTTPPort(runtime.DefaultDaprHTTPPort+portOffset),
			embedded.WithProfilePort(runtime.DefaultProfilePort+portOffset),
			componentRuntimeOptions(),
		)).
		Step("publish messages to active topic ==> "+topicActiveName, publishMessages(nil, sidecarName1, topicActiveName, consumerGroup1, consumerGroup2)).
		Step("publish messages to passive topic ==> "+topicPassiveName, publishMessages(nil, sidecarName1, topicPassiveName)).
		Step("verify if app1 has recevied messages published to active topic", assertMessages(10*time.Second, consumerGroup1)).
		Step("verify if app2 has recevied messages published to passive topic", assertMessages(10*time.Second, consumerGroup2)).
		Step("reset", flow.Reset(consumerGroup1, consumerGroup2)).
		Run()
}

// Verify with single publisher / multiple subscribers with same consumerID
func SNSSQSMultipleSubsSameConsumerIDs(t *testing.T) {
	consumerGroup1 := watcher.NewUnordered()
	consumerGroup2 := watcher.NewUnordered()

	// Set the partition key on all messages so they are written to the same partition. This allows for checking of ordered messages.
	metadata := map[string]string{
		messageKey: partition0,
	}

	metadata1 := map[string]string{
		messageKey: partition1,
	}

	// subscriber of the given topic
	subscriberApplication := func(appID string, topicName string, messagesWatcher *watcher.Watcher) app.SetupFn {
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
					messagesWatcher.Observe(e.Data)
					ctx.Logf("Message Received appID: %s,pubsub: %s, topic: %s, id: %s, data: %s", appID, e.PubsubName, e.Topic, e.ID, e.Data)
					return false, nil
				}),
			)
		}
	}

	publishMessages := func(metadata map[string]string, sidecarName string, topicName string, messageWatchers ...*watcher.Watcher) flow.Runnable {
		return func(ctx flow.Context) error {
			// prepare the messages
			messages := make([]string, numMessages)
			for i := range messages {
				messages[i] = fmt.Sprintf("partitionKey: %s, message for topic: %s, index: %03d, uniqueId: %s", metadata[messageKey], topicName, i, uuid.New().String())
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
				require.NoError(ctx, err, "SNSSQSMultipleSubsSameConsumerIDs - error publishing message")
			}
			return nil
		}
	}

	assertMessages := func(timeout time.Duration, messageWatchers ...*watcher.Watcher) flow.Runnable {
		return func(ctx flow.Context) error {
			// assert for messages
			for _, m := range messageWatchers {
				if !m.Assert(ctx, 25*timeout) {
					ctx.Errorf("SNSSQSMultipleSubsSameConsumerIDs - message assersion failed for watcher: %#v\n", m)
				}
			}

			return nil
		}
	}

	flow.New(t, "SNSSQS certification - single publisher and multiple subscribers with same consumer IDs").

		// Run subscriberApplication app1
		Step(app.Run(appID1, fmt.Sprintf(":%d", appPort),
			subscriberApplication(appID1, topicActiveName, consumerGroup1))).

		// Run the Dapr sidecar with ConsumerID "snssqscerttest1"
		Step(sidecar.Run(sidecarName1,
			embedded.WithComponentsPath("./components/consumer_one"),
			embedded.WithAppProtocol(runtime.HTTPProtocol, appPort),
			embedded.WithDaprGRPCPort(runtime.DefaultDaprAPIGRPCPort),
			embedded.WithDaprHTTPPort(runtime.DefaultDaprHTTPPort),
			componentRuntimeOptions(),
		)).

		// Run subscriberApplication app2
		Step(app.Run(appID2, fmt.Sprintf(":%d", appPort+portOffset),
			subscriberApplication(appID2, topicActiveName, consumerGroup2))).

		// Run the Dapr sidecar with ConsumerID "snssqscerttest2"
		Step(sidecar.Run(sidecarName2,
			embedded.WithComponentsPath("./components/consumer_two"),
			embedded.WithAppProtocol(runtime.HTTPProtocol, appPort+portOffset),
			embedded.WithDaprGRPCPort(runtime.DefaultDaprAPIGRPCPort+portOffset),
			embedded.WithDaprHTTPPort(runtime.DefaultDaprHTTPPort+portOffset),
			embedded.WithProfilePort(runtime.DefaultProfilePort+portOffset),
			componentRuntimeOptions(),
		)).
		Step("publish messages to  ==> "+topicActiveName, publishMessages(metadata, sidecarName1, topicActiveName, consumerGroup2)).
		Step("publish messages to  ==> "+topicActiveName, publishMessages(metadata1, sidecarName2, topicActiveName, consumerGroup2)).
		Step("verify if app1, app2 together have recevied messages published to topic1", assertMessages(10*time.Second, consumerGroup2)).
		Step("reset", flow.Reset(consumerGroup1, consumerGroup2)).
		Run()
}

// Verify with single publisher / multiple subscribers with different consumerIDs
func SNSSQSMultipleSubsDifferentConsumerIDs(t *testing.T) {
	consumerGroup1 := watcher.NewUnordered()
	consumerGroup2 := watcher.NewUnordered()

	// Set the partition key on all messages so they are written to the same partition. This allows for checking of ordered messages.
	metadata := map[string]string{
		messageKey: partition0,
	}

	// subscriber of the given topic
	subscriberApplication := func(appID string, topicName string, messagesWatcher *watcher.Watcher) app.SetupFn {
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
					messagesWatcher.Observe(e.Data)
					ctx.Logf("Message Received appID: %s,pubsub: %s, topic: %s, id: %s, data: %s", appID, e.PubsubName, e.Topic, e.ID, e.Data)
					return false, nil
				}),
			)
		}
	}

	publishMessages := func(metadata map[string]string, sidecarName string, topicName string, messageWatchers ...*watcher.Watcher) flow.Runnable {
		return func(ctx flow.Context) error {
			// prepare the messages
			messages := make([]string, numMessages)
			for i := range messages {
				messages[i] = fmt.Sprintf("partitionKey: %s, message for topic: %s, index: %03d, uniqueId: %s", metadata[messageKey], topicName, i, uuid.New().String())
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
				require.NoError(ctx, err, "SNSSQSMultipleSubsDifferentConsumerIDs - error publishing message")
			}
			return nil
		}
	}

	assertMessages := func(timeout time.Duration, messageWatchers ...*watcher.Watcher) flow.Runnable {
		return func(ctx flow.Context) error {
			// assert for messages
			for _, m := range messageWatchers {
				if !m.Assert(ctx, 25*timeout) {
					ctx.Errorf("SNSSQSMultipleSubsDifferentConsumerIDs - message assersion failed for watcher: %#v\n", m)
				}
			}

			return nil
		}
	}

	flow.New(t, "SNSSQS certification - single publisher and multiple subscribers with different consumer IDs").

		// Run subscriberApplication app1
		Step(app.Run(appID1, fmt.Sprintf(":%d", appPort),
			subscriberApplication(appID1, topicActiveName, consumerGroup1))).

		// Run the Dapr sidecar with ConsumerID "snssqscerttest1"
		Step(sidecar.Run(sidecarName1,
			embedded.WithComponentsPath("./components/consumer_one"),
			embedded.WithAppProtocol(runtime.HTTPProtocol, appPort),
			embedded.WithDaprGRPCPort(runtime.DefaultDaprAPIGRPCPort),
			embedded.WithDaprHTTPPort(runtime.DefaultDaprHTTPPort),
			componentRuntimeOptions(),
		)).

		// Run subscriberApplication app2
		Step(app.Run(appID2, fmt.Sprintf(":%d", appPort+portOffset),
			subscriberApplication(appID2, topicActiveName, consumerGroup2))).

		// RRun the Dapr sidecar with ConsumerID "snssqscerttest2"
		Step(sidecar.Run(sidecarName2,
			embedded.WithComponentsPath("./components/consumer_two"),
			embedded.WithAppProtocol(runtime.HTTPProtocol, appPort+portOffset),
			embedded.WithDaprGRPCPort(runtime.DefaultDaprAPIGRPCPort+portOffset),
			embedded.WithDaprHTTPPort(runtime.DefaultDaprHTTPPort+portOffset),
			embedded.WithProfilePort(runtime.DefaultProfilePort+portOffset),
			componentRuntimeOptions(),
		)).
		Step("publish messages to ==>"+topicActiveName, publishMessages(metadata, sidecarName1, topicActiveName, consumerGroup1)).
		Step("verify if app1, app2 together have recevied messages published to topic1", assertMessages(10*time.Second, consumerGroup1)).
		Step("reset", flow.Reset(consumerGroup1, consumerGroup2)).
		Run()
}

// Verify with multiple publishers / multiple subscribers with different consumerIDs
func SNSSQSMultiplePubSubsDifferentConsumerIDs(t *testing.T) {
	consumerGroup1 := watcher.NewUnordered()
	consumerGroup2 := watcher.NewUnordered()

	// Set the partition key on all messages so they are written to the same partition. This allows for checking of ordered messages.
	metadata := map[string]string{
		messageKey: partition0,
	}

	metadata1 := map[string]string{
		messageKey: partition1,
	}

	// subscriber of the given topic
	subscriberApplication := func(appID string, topicName string, messagesWatcher *watcher.Watcher) app.SetupFn {
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
					messagesWatcher.Observe(e.Data)
					ctx.Logf("Message Received appID: %s,pubsub: %s, topic: %s, id: %s, data: %s", appID, e.PubsubName, e.Topic, e.ID, e.Data)
					return false, nil
				}),
			)
		}
	}

	publishMessages := func(metadata map[string]string, sidecarName string, topicName string, messageWatchers ...*watcher.Watcher) flow.Runnable {
		return func(ctx flow.Context) error {
			// prepare the messages
			messages := make([]string, numMessages)
			for i := range messages {
				messages[i] = fmt.Sprintf("partitionKey: %s, message for topic: %s, index: %03d, uniqueId: %s", metadata[messageKey], topicName, i, uuid.New().String())
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
				require.NoError(ctx, err, "SNSSQSMultiplePubSubsDifferentConsumerIDs - error publishing message")
			}
			return nil
		}
	}

	assertMessages := func(timeout time.Duration, messageWatchers ...*watcher.Watcher) flow.Runnable {
		return func(ctx flow.Context) error {
			// assert for messages
			for _, m := range messageWatchers {
				if !m.Assert(ctx, 25*timeout) {
					ctx.Errorf("SNSSQSMultiplePubSubsDifferentConsumerIDs - message assersion failed for watcher: %#v\n", m)
				}
			}

			return nil
		}
	}

	flow.New(t, "SNSSQS certification - multiple publishers and multiple subscribers with different consumer IDs").

		// Run subscriberApplication app1
		Step(app.Run(appID1, fmt.Sprintf(":%d", appPort),
			subscriberApplication(appID1, topicActiveName, consumerGroup1))).

		// Run the Dapr sidecar with ConsumerID "snssqscerttest1"
		Step(sidecar.Run(sidecarName1,
			embedded.WithComponentsPath("./components/consumer_one"),
			embedded.WithAppProtocol(runtime.HTTPProtocol, appPort),
			embedded.WithDaprGRPCPort(runtime.DefaultDaprAPIGRPCPort),
			embedded.WithDaprHTTPPort(runtime.DefaultDaprHTTPPort),
			componentRuntimeOptions(),
		)).

		// Run subscriberApplication app2
		Step(app.Run(appID2, fmt.Sprintf(":%d", appPort+portOffset),
			subscriberApplication(appID2, topicActiveName, consumerGroup2))).

		// Run the Dapr sidecar with ConsumerID "snssqscerttest2"
		Step(sidecar.Run(sidecarName2,
			embedded.WithComponentsPath("./components/consumer_two"),
			embedded.WithAppProtocol(runtime.HTTPProtocol, appPort+portOffset),
			embedded.WithDaprGRPCPort(runtime.DefaultDaprAPIGRPCPort+portOffset),
			embedded.WithDaprHTTPPort(runtime.DefaultDaprHTTPPort+portOffset),
			embedded.WithProfilePort(runtime.DefaultProfilePort+portOffset),
			componentRuntimeOptions(),
		)).
		Step("publish messages to ==> "+topicActiveName, publishMessages(metadata, sidecarName1, topicActiveName, consumerGroup1)).
		Step("publish messages to ==> "+topicActiveName, publishMessages(metadata1, sidecarName2, topicActiveName, consumerGroup2)).
		Step("verify if app1, app2 together have recevied messages published to topic1", assertMessages(10*time.Second, consumerGroup1)).
		Step("verify if app1, app2 together have recevied messages published to topic1", assertMessages(10*time.Second, consumerGroup2)).
		Step("reset", flow.Reset(consumerGroup1, consumerGroup2)).
		Run()
}

// Verify data with a topic that does not exist
func SNSSQSNonexistingTopic(t *testing.T) {
	consumerGroup1 := watcher.NewUnordered()

	// Set the partition key on all messages so they are written to the same partition. This allows for checking of ordered messages.
	metadata := map[string]string{
		messageKey: partition0,
	}

	// subscriber of the given topic
	subscriberApplication := func(appID string, topicName string, messagesWatcher *watcher.Watcher) app.SetupFn {
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
					messagesWatcher.Observe(e.Data)
					ctx.Logf("Message Received appID: %s,pubsub: %s, topic: %s, id: %s, data: %s", appID, e.PubsubName, e.Topic, e.ID, e.Data)
					return false, nil
				}),
			)
		}
	}

	publishMessages := func(metadata map[string]string, sidecarName string, topicName string, messageWatchers ...*watcher.Watcher) flow.Runnable {
		return func(ctx flow.Context) error {
			// prepare the messages
			messages := make([]string, numMessages)
			for i := range messages {
				messages[i] = fmt.Sprintf("partitionKey: %s, message for topic: %s, index: %03d, uniqueId: %s", metadata[messageKey], topicName, i, uuid.New().String())
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
				require.NoError(ctx, err, "SNSSQSNonexistingTopic - error publishing message")
			}
			return nil
		}
	}

	assertMessages := func(timeout time.Duration, messageWatchers ...*watcher.Watcher) flow.Runnable {
		return func(ctx flow.Context) error {
			// assert for messages
			for _, m := range messageWatchers {
				if !m.Assert(ctx, 25*timeout) {
					ctx.Errorf("SNSSQSNonexistingTopic - message assersion failed for watcher: %#v\n", m)
				}
			}

			return nil
		}
	}

	flow.New(t, "SNSSQS certification - non-existing topic").

		// Run subscriberApplication app1
		Step(app.Run(appID1, fmt.Sprintf(":%d", appPort+portOffset*3),
			subscriberApplication(appID1, topicToBeCreated, consumerGroup1))).

		// Run the Dapr sidecar with ConsumerID "snssqscerttest1"
		Step(sidecar.Run(sidecarName1,
			embedded.WithComponentsPath("./components/consumer_one"),
			embedded.WithAppProtocol(runtime.HTTPProtocol, appPort+portOffset*3),
			embedded.WithDaprGRPCPort(runtime.DefaultDaprAPIGRPCPort+portOffset*3),
			embedded.WithDaprHTTPPort(runtime.DefaultDaprHTTPPort+portOffset*3),
			embedded.WithProfilePort(runtime.DefaultProfilePort+portOffset*3),
			componentRuntimeOptions(),
		)).
		Step(fmt.Sprintf("publish messages to topicToBeCreated: %s", topicToBeCreated), publishMessages(metadata, sidecarName1, topicToBeCreated, consumerGroup1)).
		Step("wait", flow.Sleep(30*time.Second)).
		Step("verify if app1 has recevied messages published to newly created topic", assertMessages(10*time.Second, consumerGroup1)).
		Run()
}

// Verify with an optional parameter `disableEntityManagement` set to true
func SNSSQSEntityManagement(t *testing.T) {
	// TODO: Modify it to looks for component init error in the sidecar itself.
	consumerGroup1 := watcher.NewUnordered()

	// Set the partition key on all messages so they are written to the same partition. This allows for checking of ordered messages.
	metadata := map[string]string{
		messageKey: partition0,
	}

	subscriberApplication := func(appID string, topicName string, messagesWatcher *watcher.Watcher) app.SetupFn {
		return func(ctx flow.Context, s common.Service) error {
			// Setup the /orders event handler.
			return multierr.Combine(
				s.AddTopicEventHandler(&common.Subscription{
					PubsubName: pubsubName,
					Topic:      topicName,
					Route:      "/orders",
				}, func(_ context.Context, e *common.TopicEvent) (retry bool, err error) {
					// Track/Observe the data of the event.
					messagesWatcher.Observe(e.Data)
					ctx.Logf("Message Received appID: %s,pubsub: %s, topic: %s, id: %s, data: %s", appID, e.PubsubName, e.Topic, e.ID, e.Data)
					return false, nil
				}),
			)
		}
	}

	publishMessages := func(metadata map[string]string, sidecarName string, topicName string, messageWatchers ...*watcher.Watcher) flow.Runnable {
		return func(ctx flow.Context) error {
			// prepare the messages
			messages := make([]string, numMessages)
			for i := range messages {
				messages[i] = fmt.Sprintf("partitionKey: %s, message for topic: %s, index: %03d, uniqueId: %s", metadata[messageKey], topicName, i, uuid.New().String())
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
				// Error is expected as the topic does not exist
				require.Error(ctx, err, "SNSSQSEntityManagement - error publishing message")
			}
			return nil
		}
	}

	flow.New(t, "SNSSQS certification - entity management disabled").

		// Run subscriberApplication app1
		Step(app.Run(appID1, fmt.Sprintf(":%d", appPort+portOffset),
			subscriberApplication(appID1, topicActiveName, consumerGroup1))).

		// Run the Dapr sidecar
		Step(sidecar.Run(sidecarName1,
			embedded.WithComponentsPath("./components/entity_mgmt"),
			embedded.WithAppProtocol(runtime.HTTPProtocol, appPort+portOffset),
			embedded.WithDaprGRPCPort(runtime.DefaultDaprAPIGRPCPort+portOffset),
			embedded.WithDaprHTTPPort(runtime.DefaultDaprHTTPPort+portOffset),
			embedded.WithProfilePort(runtime.DefaultProfilePort+portOffset),
			componentRuntimeOptions(),
		)).
		Step(fmt.Sprintf("publish messages to topicDefault: %s", topicDefaultName), publishMessages(metadata, sidecarName1, topicDefaultName, consumerGroup1)).
		Run()
}

// Verify data with an optional parameter `defaultMessageTimeToLiveInSec` set
func SNSSQSDefaultTtl(t *testing.T) {
	consumerGroup1 := watcher.NewUnordered()

	// Set the partition key on all messages so they are written to the same partition. This allows for checking of ordered messages.
	metadata := map[string]string{
		messageKey: partition0,
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
					ctx.Logf("Got message: %s", e.Data)
					messagesWatcher.FailIfNotExpected(t, e.Data)
					return false, nil
				}),
			)
		}
	}

	testTtlPublishMessages := func(metadata map[string]string, sidecarName string, topicName string, messageWatchers ...*watcher.Watcher) flow.Runnable {
		return func(ctx flow.Context) error {
			// prepare the messages
			messages := make([]string, numMessages)
			for i := range messages {
				messages[i] = fmt.Sprintf("partitionKey: %s, message for topic: %s, index: %03d, uniqueId: %s", metadata[messageKey], topicName, i, uuid.New().String())
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
				require.NoError(ctx, err, "SNSSQSDefaultTtl - error publishing message")
			}
			return nil
		}
	}

	assertMessages := func(timeout time.Duration, messageWatchers ...*watcher.Watcher) flow.Runnable {
		return func(ctx flow.Context) error {
			// assert for messages
			for _, m := range messageWatchers {
				if !m.Assert(ctx, 25*timeout) {
					ctx.Errorf("SNSSQSDefaultTtl - message assersion failed for watcher: %#v\n", m)
				}
			}

			return nil
		}
	}

	flow.New(t, "SNSSQS certification - default ttl attribute").

		// Run subscriberApplication app1
		Step(app.Run(appID1, fmt.Sprintf(":%d", appPort+portOffset),
			subscriberApplication(appID1, topicActiveName, consumerGroup1))).

		// Run the Dapr sidecar with the ttl
		Step(sidecar.Run("initalSidecar",
			embedded.WithComponentsPath("./components/default_ttl"),
			embedded.WithoutApp(),
			embedded.WithDaprGRPCPort(runtime.DefaultDaprAPIGRPCPort+portOffset),
			embedded.WithDaprHTTPPort(runtime.DefaultDaprHTTPPort+portOffset),
			embedded.WithProfilePort(runtime.DefaultProfilePort+portOffset),
			componentRuntimeOptions(),
		)).
		Step(fmt.Sprintf("publish messages to topicToBeCreated: %s", topicActiveName), testTtlPublishMessages(metadata, "initalSidecar", topicActiveName, consumerGroup1)).
		Step("stop initial sidecar", sidecar.Stop("initialSidecar")).
		Step("wait", flow.Sleep(20*time.Second)).
		Step(sidecar.Run(sidecarName1,
			embedded.WithComponentsPath("./components/default_ttl"),
			embedded.WithAppProtocol(runtime.HTTPProtocol, appPort+portOffset),
			embedded.WithDaprGRPCPort(runtime.DefaultDaprAPIGRPCPort+portOffset*2),
			embedded.WithDaprHTTPPort(runtime.DefaultDaprHTTPPort+portOffset*2),
			embedded.WithProfilePort(runtime.DefaultProfilePort+portOffset*2),
			componentRuntimeOptions(),
		)).
		Step("verify if app6 has recevied messages published to newly created topic", assertMessages(10*time.Second, consumerGroup1)).
		Run()
}

func componentRuntimeOptions() []runtime.Option {
	log := logger.NewLogger("dapr.components")

	pubsubRegistry := pubsub_loader.NewRegistry()
	pubsubRegistry.Logger = log
	pubsubRegistry.RegisterComponent(pubsub_snssqs.NewSnsSqs, "snssqs")

	return []runtime.Option{
		runtime.WithPubSubs(pubsubRegistry),
	}
}

func teardown(t *testing.T) {
	t.Logf("AWS SNS/SQS CertificationTests teardown...")
	if err := deleteQueues(queues); err != nil {
		t.Error(err)
	}
	t.Logf("AWS SNS/SQS CertificationTests teardown...done!")
}
