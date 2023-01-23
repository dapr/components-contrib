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

	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"go.uber.org/multierr"

	// Pub-Sub.
	pubsub_snssqs "github.com/dapr/components-contrib/pubsub/aws/snssqs"
	secretstore_env "github.com/dapr/components-contrib/secretstores/local/env"
	pubsub_loader "github.com/dapr/dapr/pkg/components/pubsub"
	secretstores_loader "github.com/dapr/dapr/pkg/components/secretstores"
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

var (
	region                        = "us-east-1"                     // replaced with env var AWS_REGION
	existingTopic                 = "existingTopic"                 // replaced with env var PUBSUB_AWS_SNSSQS_TOPIC_3
	messageVisibilityTimeoutTopic = "messageVisibilityTimeoutTopic" // replaced with env var PUBSUB_AWS_SNSSQS_TOPIC_MVT
<<<<<<< HEAD
	deadLetterTopic               = "deadLetterTopic"               // replaced with env var PUBSUB_AWS_SNSSQS_TOPIC_DL
	dealLetterQueueName           = "dealLetterQueueName"           // replaced with env var PUBSUB_AWS_SNSSQS_QUEUE_DLOUT
=======
	fifoTopic                     = "fifoTopic"                     // replaced with env var PUBSUB_AWS_SNSSQS_TOPIC_FIFO
>>>>>>> master
)

// The following Queue names must match
// the values of the "consumerID" metadata properties
// found inside each of the components/*/pubsub.yaml files
// The names will be passed in with Env Vars like:
//
//	PUBSUB_AWS_SNSSQS_QUEUE_*
//	PUBSUB_AWS_SNSSQS_TOPIC_*
var queues = []string{
	"snscerttest1",
	"snssqscerttest2",
<<<<<<< HEAD
	"snssqscerttestdeadletterin",
	"snssqscerttestdeadletterout",
=======
	"snssqscerttestfifo",
>>>>>>> master
}

var topics = []string{
	topicActiveName,
	topicPassiveName,
	topicToBeCreated,
	topicDefaultName,
}

func init() {
	qn := os.Getenv("AWS_REGION")
	if qn != "" {
		region = qn
	}

	qn = os.Getenv("PUBSUB_AWS_SNSSQS_QUEUE_1")
	if qn != "" {
		queues[0] = qn
	}
	qn = os.Getenv("PUBSUB_AWS_SNSSQS_QUEUE_2")
	if qn != "" {
		queues[1] = qn
	}
<<<<<<< HEAD
	qn = os.Getenv("PUBSUB_AWS_SNSSQS_QUEUE_DLIN")
	if qn != "" {
		queues[2] = qn
	}
	qn = os.Getenv("PUBSUB_AWS_SNSSQS_QUEUE_DLOUT")
	if qn != "" {
		dealLetterQueueName = qn
		queues[3] = qn
	}
=======
	qn = os.Getenv("PUBSUB_AWS_SNSSQS_QUEUE_FIFO")
	if qn != "" {
		queues[2] = qn
	}
>>>>>>> master

	qn = os.Getenv("PUBSUB_AWS_SNSSQS_TOPIC_3")
	if qn != "" {
		existingTopic = qn
	}
	qn = os.Getenv("PUBSUB_AWS_SNSSQS_TOPIC_MVT")
	if qn != "" {
		messageVisibilityTimeoutTopic = qn
		topics = append(topics, messageVisibilityTimeoutTopic)
	}
	qn = os.Getenv("PUBSUB_AWS_SNSSQS_TOPIC_FIFO")
	if qn != "" {
		fifoTopic = qn
		topics = append(topics, fifoTopic)
	}
	qn = os.Getenv("PUBSUB_AWS_SNSSQS_TOPIC_DL")
	if qn != "" {
		deadLetterTopic = qn
	}
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

	t.Run("SNSSQSExistingQueueAndTopic", func(t *testing.T) {
		SNSSQSExistingQueueAndTopic(t)
	})

	t.Run("SNSSQSExistingQueueNonexistingTopic", func(t *testing.T) {
		SNSSQSExistingQueueNonexistingTopic(t)
	})

	t.Run("SNSSQSNonexistingTopic", func(t *testing.T) {
		SNSSQSNonexistingTopic(t)
	})

	t.Run("SNSSQSEntityManagement", func(t *testing.T) {
		SNSSQSEntityManagement(t)
	})

	t.Run("SNSSQSMessageVisibilityTimeout", func(t *testing.T) {
		SNSSQSMessageVisibilityTimeout(t)
	})

<<<<<<< HEAD
	t.Run("SNSSQSMessageDeadLetter", func(t *testing.T) {
		SNSSQSMessageDeadLetter(t)
=======
	t.Run("SNSSQSFIFOMessages", func(t *testing.T) {
		SNSSQSFIFOMessages(t)
>>>>>>> master
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

		// Run the Dapr sidecar with ConsumerID "PUBSUB_AWS_SNSSQS_QUEUE_1"
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

		// Run the Dapr sidecar with ConsumerID "PUBSUB_AWS_SNSSQS_QUEUE_2"
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

		// Run the Dapr sidecar with ConsumerID "PUBSUB_AWS_SNSSQS_QUEUE_1"
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

		// Run the Dapr sidecar with ConsumerID "PUBSUB_AWS_SNSSQS_QUEUE_2"
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

		// Run the Dapr sidecar with ConsumerID "PUBSUB_AWS_SNSSQS_QUEUE_1"
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

		// RRun the Dapr sidecar with ConsumerID "PUBSUB_AWS_SNSSQS_QUEUE_2"
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

		// Run the Dapr sidecar with ConsumerID "PUBSUB_AWS_SNSSQS_QUEUE_1"
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

		// Run the Dapr sidecar with ConsumerID "PUBSUB_AWS_SNSSQS_QUEUE_2"
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

		// Run the Dapr sidecar with ConsumerID "PUBSUB_AWS_SNSSQS_QUEUE_1"
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

// Verify data with an existing Queue and existing Topic
func SNSSQSExistingQueueAndTopic(t *testing.T) {
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
				require.NoError(ctx, err, "SNSSQSExistingQueueAndTopic - error publishing message")
			}
			return nil
		}
	}

	assertMessages := func(timeout time.Duration, messageWatchers ...*watcher.Watcher) flow.Runnable {
		return func(ctx flow.Context) error {
			// assert for messages
			for _, m := range messageWatchers {
				if !m.Assert(ctx, 25*timeout) {
					ctx.Errorf("SNSSQSExistingQueueAndTopic - message assersion failed for watcher: %#v\n", m)
				}
			}

			return nil
		}
	}

	flow.New(t, "SNSSQS certification - Existing Queue Existing Topic").

		// Run subscriberApplication app1
		Step(app.Run(appID1, fmt.Sprintf(":%d", appPort+portOffset*3),
			subscriberApplication(appID1, existingTopic, consumerGroup1))).

		// Run the Dapr sidecar with ConsumerID "PUBSUB_AWS_SNSSQS_QUEUE_3"
		Step(sidecar.Run(sidecarName1,
			embedded.WithComponentsPath("./components/existing_queue"),
			embedded.WithAppProtocol(runtime.HTTPProtocol, appPort+portOffset*3),
			embedded.WithDaprGRPCPort(runtime.DefaultDaprAPIGRPCPort+portOffset*3),
			embedded.WithDaprHTTPPort(runtime.DefaultDaprHTTPPort+portOffset*3),
			embedded.WithProfilePort(runtime.DefaultProfilePort+portOffset*3),
			componentRuntimeOptions(),
		)).
		Step(fmt.Sprintf("publish messages to existingTopic: %s", existingTopic), publishMessages(metadata, sidecarName1, existingTopic, consumerGroup1)).
		Step("wait", flow.Sleep(30*time.Second)).
		Step("verify if app1 has recevied messages published to newly created topic", assertMessages(10*time.Second, consumerGroup1)).
		Run()
}

// Verify data with an existing Queue with a topic that does not exist
func SNSSQSExistingQueueNonexistingTopic(t *testing.T) {
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
				require.NoError(ctx, err, "SNSSQSExistingQueueNonexistingTopic - error publishing message")
			}
			return nil
		}
	}

	assertMessages := func(timeout time.Duration, messageWatchers ...*watcher.Watcher) flow.Runnable {
		return func(ctx flow.Context) error {
			// assert for messages
			for _, m := range messageWatchers {
				if !m.Assert(ctx, 25*timeout) {
					ctx.Errorf("SNSSQSExistingQueueNonexistingTopic - message assersion failed for watcher: %#v\n", m)
				}
			}

			return nil
		}
	}

	flow.New(t, "SNSSQS certification - Existing Queue None Existing Topic").

		// Run subscriberApplication app1
		Step(app.Run(appID1, fmt.Sprintf(":%d", appPort+portOffset*3),
			subscriberApplication(appID1, topicToBeCreated, consumerGroup1))).

		// Run the Dapr sidecar with ConsumerID "PUBSUB_AWS_SNSSQS_QUEUE_3"
		Step(sidecar.Run(sidecarName1,
			embedded.WithComponentsPath("./components/existing_queue"),
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

// Verify data with an optional parameter `messageVisibilityTimeout` takes affect
func SNSSQSMessageVisibilityTimeout(t *testing.T) {
	consumerGroup1 := watcher.NewUnordered()
	metadata := map[string]string{}
	latch := make(chan struct{})
	busyTime := 10 * time.Second
	messagesToSend := 1
	waitForLatch := func(appID string, ctx flow.Context, l chan struct{}) error {
		ctx.Logf("waitForLatch %s is waiting...\n", appID)
		<-l
		ctx.Logf("waitForLatch %s ready to continue!\n", appID)
		return nil
	}

	subscriberMVTimeoutApp := func(appID string, topicName string, messagesWatcher *watcher.Watcher, l chan struct{}) app.SetupFn {
		return func(ctx flow.Context, s common.Service) error {
			ctx.Logf("SNSSQSMessageVisibilityTimeout.subscriberApplicationMVTimeout App: %q topicName: %q\n", appID, topicName)
			return multierr.Combine(
				s.AddTopicEventHandler(&common.Subscription{
					PubsubName: pubsubName,
					Topic:      topicName,
					Route:      "/orders",
				}, func(_ context.Context, e *common.TopicEvent) (retry bool, err error) {
					ctx.Logf("SNSSQSMessageVisibilityTimeout.subscriberApplicationMVTimeout App: %q got message: %s busy for %v\n", appID, e.Data, busyTime)
					time.Sleep(busyTime)
					ctx.Logf("SNSSQSMessageVisibilityTimeout.subscriberApplicationMVTimeout App: %q - notifying next Subscriber to continue...\n", appID)
					l <- struct{}{}
					ctx.Logf("SNSSQSMessageVisibilityTimeout.subscriberApplicationMVTimeoutApp: %q - sent  busy for %v\n", appID, busyTime)
					time.Sleep(busyTime)
					ctx.Logf("SNSSQSMessageVisibilityTimeout.subscriberApplicationMVTimeoutApp: %q - done!\n", appID)
					return false, nil
				}),
			)
		}
	}

	notExpectingMessagesSubscriberApp := func(appID string, topicName string, messagesWatcher *watcher.Watcher, l chan struct{}) app.SetupFn {
		return func(ctx flow.Context, s common.Service) error {
			ctx.Logf("SNSSQSMessageVisibilityTimeout.notExpectingMessagesSubscriberApp App: %q topicName: %q waiting for notification to start receiving messages\n", appID, topicName)
			return multierr.Combine(
				waitForLatch(appID, ctx, l),
				s.AddTopicEventHandler(&common.Subscription{
					PubsubName: pubsubName,
					Topic:      topicName,
					Route:      "/orders",
				}, func(_ context.Context, e *common.TopicEvent) (retry bool, err error) {
					ctx.Logf("SNSSQSMessageVisibilityTimeout.notExpectingMessagesSubscriberApp App: %q got unexpected message: %s\n", appID, e.Data)
					messagesWatcher.FailIfNotExpected(t, e.Data)
					return false, nil
				}),
			)
		}
	}

	testTtlPublishMessages := func(metadata map[string]string, sidecarName string, topicName string, messageWatchers ...*watcher.Watcher) flow.Runnable {
		return func(ctx flow.Context) error {
			// prepare the messages
			messages := make([]string, messagesToSend)
			for i := range messages {
				messages[i] = fmt.Sprintf("partitionKey: %s, message for topic: %s, index: %03d, uniqueId: %s", metadata[messageKey], topicName, i, uuid.New().String())
			}

			ctx.Logf("####### get the sidecar (dapr) client sidecarName: %s, topicName: %s", sidecarName, topicName)
			// get the sidecar (dapr) client
			client := sidecar.GetClient(ctx, sidecarName)

			// publish messages
			ctx.Logf("####### Publishing messages. sidecarName: %s, topicName: %s", sidecarName, topicName)

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
				require.NoError(ctx, err, "SNSSQSMessageVisibilityTimeout - error publishing message")
			}
			return nil
		}
	}

	connectToSideCar := func(sidecarName string) flow.Runnable {
		return func(ctx flow.Context) error {
			ctx.Logf("####### connect to sidecar (dapr) client sidecarName: %s and exit", sidecarName)
			// get the sidecar (dapr) client
			sidecar.GetClient(ctx, sidecarName)
			return nil
		}
	}

	flow.New(t, "SNSSQS certification - messageVisibilityTimeout attribute receive").
		// App1 should receive the messages, wait some time (busy), notify App2, wait some time (busy),
		// and finish processing message.
		Step(app.Run(appID1, fmt.Sprintf(":%d", appPort+portOffset),
			subscriberMVTimeoutApp(appID1, messageVisibilityTimeoutTopic, consumerGroup1, latch))).
		Step(sidecar.Run(sidecarName1,
			embedded.WithComponentsPath("./components/message_visibility_timeout"),
			embedded.WithAppProtocol(runtime.HTTPProtocol, appPort+portOffset),
			embedded.WithDaprGRPCPort(runtime.DefaultDaprAPIGRPCPort+portOffset),
			embedded.WithDaprHTTPPort(runtime.DefaultDaprHTTPPort+portOffset),
			embedded.WithProfilePort(runtime.DefaultProfilePort+portOffset),
			componentRuntimeOptions(),
		)).
		Step(fmt.Sprintf("publish messages to messageVisibilityTimeoutTopic: %s", messageVisibilityTimeoutTopic),
			testTtlPublishMessages(metadata, sidecarName1, messageVisibilityTimeoutTopic, consumerGroup1)).

		// App2 waits for App1 notification to subscribe to message
		// After subscribing, if App2 receives any messages, the messageVisibilityTimeoutTopic is either too short,
		// or code is broken somehow
		Step(app.Run(appID2, fmt.Sprintf(":%d", appPort+portOffset+2),
			notExpectingMessagesSubscriberApp(appID2, messageVisibilityTimeoutTopic, consumerGroup1, latch))).
		Step(sidecar.Run(sidecarName2,
			embedded.WithComponentsPath("./components/message_visibility_timeout"),
			embedded.WithAppProtocol(runtime.HTTPProtocol, appPort+portOffset+2),
			embedded.WithDaprGRPCPort(runtime.DefaultDaprAPIGRPCPort+portOffset+2),
			embedded.WithDaprHTTPPort(runtime.DefaultDaprHTTPPort+portOffset+2),
			embedded.WithProfilePort(runtime.DefaultProfilePort+portOffset+2),
			componentRuntimeOptions(),
		)).
		Step("No messages will be sent here",
			connectToSideCar(sidecarName2)).
		Step("wait", flow.Sleep(10*time.Second)).
		Run()

}

<<<<<<< HEAD
// Verify data with an optional parameters `sqsDeadLettersQueueName` and `messageReceiveLimit` takes affect
func SNSSQSMessageDeadLetter(t *testing.T) {
	consumerGroup1 := watcher.NewUnordered()
	deadLetterConsumerGroup := watcher.NewUnordered()
	failedMessagesNum := 1

	// subscriber of the given topic
=======
// Verify data with an optional parameters `fifo` and `fifoMessageGroupID` takes affect (SNSSQSFIFOMessages)
func SNSSQSFIFOMessages(t *testing.T) {
	consumerGroup1 := watcher.NewOrdered()

	// prepare the messages
	maxFifoMessages := 20
	fifoMessages := make([]string, maxFifoMessages)
	for i := 0; i < maxFifoMessages; i++ {
		fifoMessages[i] = fmt.Sprintf("m%d", i+1)
	}
	consumerGroup1.ExpectStrings(fifoMessages...)

	// There are multiple publishers so the following
	// generator will supply messages to each one in order
	msgCh := make(chan string)
	go func(mc chan string) {
		for _, m := range fifoMessages {
			mc <- m
		}
		close(mc)
	}(msgCh)

	doNothingApp := func(appID string, topicName string, messagesWatcher *watcher.Watcher) app.SetupFn {
		return func(ctx flow.Context, s common.Service) error {
			return nil
		}
	}

>>>>>>> master
	subscriberApplication := func(appID string, topicName string, messagesWatcher *watcher.Watcher) app.SetupFn {
		return func(ctx flow.Context, s common.Service) error {
			return multierr.Combine(
				s.AddTopicEventHandler(&common.Subscription{
					PubsubName: pubsubName,
					Topic:      topicName,
					Route:      "/orders",
				}, func(_ context.Context, e *common.TopicEvent) (retry bool, err error) {
<<<<<<< HEAD
					ctx.Logf("subscriberApplication - Message Received appID: %s,pubsub: %s, topic: %s, id: %s, data: %s - causing failure...", appID, e.PubsubName, e.Topic, e.ID, e.Data)
					return true, fmt.Errorf("failure on purpose")
=======
					ctx.Logf("SNSSQSFIFOMessages.subscriberApplication: Message Received appID: %s,pubsub: %s, topic: %s, id: %s, data: %#v", appID, e.PubsubName, e.Topic, e.ID, e.Data)
					messagesWatcher.Observe(e.Data)
					return false, nil
>>>>>>> master
				}),
			)
		}
	}

<<<<<<< HEAD
	// subscriber of the given deadletter queue
	deadLetterReceiverApplication := func(appID string, deadLetterQueueName string, messagesWatcher *watcher.Watcher) app.SetupFn {
		return func(ctx flow.Context, s common.Service) error {
			return multierr.Combine(
				func() error {
					ctx.Logf("%s is waiting for messages...\n", appID)
					// Create a SQS session
					sqsSess := sqsService()

					// Get URL of queue
					queueUrl, err := getQueueURL(sqsSess, &deadLetterQueueName)
					if err != nil {
						return fmt.Errorf("%s error getting the queue URL: %q err:%v", appID, deadLetterQueueName, err)
					}

					// Wait for DeadLetter Queue message to arrive
					for {
						msgResult, err := getMessages(sqsSess, queueUrl)
						if err != nil {
							return fmt.Errorf("%s error getting messages from dead letter queue URL: %q err:%v", appID, queueUrl, err)
						}

						if len(msgResult.Messages) == 0 {
							ctx.Logf("%s no messages sleeping and trying again...\n", appID)
							time.Sleep(5 * time.Second)
							continue
						}

						for _, msg := range msgResult.Messages {
							ctx.Logf("deadLetterReceiverApplication - Message Received appID: %s,deadLetterQueueName: %s, data: %s", appID, deadLetterQueueName, *msg.Body)
							// Track/Observe deadletter message.
							messagesWatcher.Observe(*msg.Body)
							break
						}
					}

					ctx.Logf("%s done!\n", appID)
					return nil
				}(),
			)
		}
	}

	publishMessages := func(metadata map[string]string, sidecarName string, topicName string, messageWatchers ...*watcher.Watcher) flow.Runnable {
		return func(ctx flow.Context) error {
			// prepare the messages
			messages := make([]string, failedMessagesNum)
			for i := range messages {
				messages[i] = fmt.Sprintf("partitionKey: %s, message for topic: %s, index: %03d, uniqueId: %s", metadata[messageKey], topicName, i, uuid.New().String())
			}

			// add the messages as expectations to the watchers
			for _, messageWatcher := range messageWatchers {
				messageWatcher.ExpectStrings(messages...)
			}
=======
	publishMessages := func(metadata map[string]string, sidecarName string, topicName string, mw *watcher.Watcher) flow.Runnable {
		return func(ctx flow.Context) error {
>>>>>>> master

			// get the sidecar (dapr) client
			client := sidecar.GetClient(ctx, sidecarName)

			// publish messages
<<<<<<< HEAD
			ctx.Logf("SNSSQSMessageDeadLetter - Publishing messages. sidecarName: %s, topicName: %s", sidecarName, topicName)
=======
			ctx.Logf("SNSSQSFIFOMessages Publishing messages. sidecarName: %s, topicName: %s", sidecarName, topicName)
>>>>>>> master

			var publishOptions dapr.PublishEventOption

			if metadata != nil {
				publishOptions = dapr.PublishEventWithMetadata(metadata)
			}

<<<<<<< HEAD
			for _, message := range messages {
				ctx.Logf("Publishing: %q", message)
=======
			for message := range msgCh {
				ctx.Logf("SNSSQSFIFOMessages Publishing: sidecarName: %s, topicName: %s - %q", sidecarName, topicName, message)
>>>>>>> master
				var err error

				if publishOptions != nil {
					err = client.PublishEvent(ctx, pubsubName, topicName, message, publishOptions)
				} else {
					err = client.PublishEvent(ctx, pubsubName, topicName, message)
				}
<<<<<<< HEAD
				require.NoError(ctx, err, "SNSSQSMessageDeadLetter - error publishing message")
=======
				require.NoError(ctx, err, "SNSSQSFIFOMessages - error publishing message")
>>>>>>> master
			}
			return nil
		}
	}
<<<<<<< HEAD

=======
>>>>>>> master
	assertMessages := func(timeout time.Duration, messageWatchers ...*watcher.Watcher) flow.Runnable {
		return func(ctx flow.Context) error {
			// assert for messages
			for _, m := range messageWatchers {
<<<<<<< HEAD
				if !m.Assert(ctx, 2*timeout) {
					ctx.Errorf("SNSSQSMessageDeadLetter - message assersion failed for watcher: %#v\n", m)
=======
				if !m.Assert(ctx, 10*timeout) {
					ctx.Errorf("SNSSQSFIFOMessages - message assersion failed for watcher: %#v\n", m)
>>>>>>> master
				}
			}

			return nil
		}
	}

<<<<<<< HEAD
	connectToSideCar := func(sidecarName string) flow.Runnable {
		return func(ctx flow.Context) error {
			ctx.Logf("####### connect to sidecar (dapr) client sidecarName: %s and exit", sidecarName)
			// get the sidecar (dapr) client
			sidecar.GetClient(ctx, sidecarName)
			return nil
		}
	}

	prefix := "SNSSQSMessageDeadLetter-"
	subApp := prefix + "subscriberApp"
	deadletterApp := prefix + "deadLetterReceiverApp"
	deadLetterSideCar := prefix + sidecarName1
	subAppSideCar := prefix + sidecarName2

	flow.New(t, "SNSSQSMessageDeadLetter Verify with single publisher / single subscriber and DeadLetter").

		// Run deadLetterReceiverApplication - should receive messages from dead letter queue
		Step(app.Run(deadletterApp, fmt.Sprintf(":%d", appPort+portOffset+2),
			deadLetterReceiverApplication(deadletterApp, dealLetterQueueName, deadLetterConsumerGroup))).
		Step(sidecar.Run(deadletterApp,
			embedded.WithComponentsPath("./components/message_visibility_timeout"),
=======
	pub1 := "publisher1"
	sc1 := pub1 + "_sidecar"
	pub2 := "publisher2"
	sc2 := pub2 + "_sidecar"
	sub := "subscriber"
	subsc := sub + "_sidecar"

	flow.New(t, "SNSSQSFIFOMessages Verify FIFO with multiple publishers and single subscriber receiving messages in order").

		// Subscriber
		Step(app.Run(sub, fmt.Sprintf(":%d", appPort),
			subscriberApplication(sub, fifoTopic, consumerGroup1))).
		Step(sidecar.Run(subsc,
			embedded.WithComponentsPath("./components/fifo"),
			embedded.WithAppProtocol(runtime.HTTPProtocol, appPort),
			embedded.WithDaprGRPCPort(runtime.DefaultDaprAPIGRPCPort),
			embedded.WithDaprHTTPPort(runtime.DefaultDaprHTTPPort),
			componentRuntimeOptions(),
		)).
		Step("wait", flow.Sleep(5*time.Second)).

		// Publisher 1
		Step(app.Run(pub1, fmt.Sprintf(":%d", appPort+portOffset+2),
			doNothingApp(pub1, fifoTopic, consumerGroup1))).
		Step(sidecar.Run(sc1,
			embedded.WithComponentsPath("./components/fifo"),
>>>>>>> master
			embedded.WithAppProtocol(runtime.HTTPProtocol, appPort+portOffset+2),
			embedded.WithDaprGRPCPort(runtime.DefaultDaprAPIGRPCPort+portOffset+2),
			embedded.WithDaprHTTPPort(runtime.DefaultDaprHTTPPort+portOffset+2),
			embedded.WithProfilePort(runtime.DefaultProfilePort+portOffset+2),
			componentRuntimeOptions(),
		)).
<<<<<<< HEAD
		Step("No messages will be sent here",
			connectToSideCar(deadLetterSideCar)).
		Step("wait", flow.Sleep(10*time.Second)).

		// Run subscriberApplication - will fail to process messages
		Step(app.Run(subApp, fmt.Sprintf(":%d", appPort+portOffset+4),
			subscriberApplication(subApp, deadLetterTopic, consumerGroup1))).
		// Run the Dapr sidecar with ConsumerID "PUBSUB_AWS_SNSSQS_QUEUE_DLIN"
		Step(sidecar.Run(subAppSideCar,
			embedded.WithComponentsPath("./components/deadletter"),
			embedded.WithAppProtocol(runtime.HTTPProtocol, appPort+portOffset+4),
			embedded.WithDaprGRPCPort(runtime.DefaultDaprAPIGRPCPort+portOffset+4),
			embedded.WithDaprHTTPPort(runtime.DefaultDaprHTTPPort+portOffset+4),
			componentRuntimeOptions(),
		)).
		Step("publish messages to deadLetterTopic ==> "+deadLetterTopic, publishMessages(nil, subAppSideCar, deadLetterTopic, deadLetterConsumerGroup)).
		Step("verify if app1 has 0 recevied messages published to active topic", assertMessages(10*time.Second, consumerGroup1)).
		Step("verify if app2 has deadletterMessageNum recevied messages send to dead letter queue", assertMessages(10*time.Second, deadLetterConsumerGroup)).
		Step("reset", flow.Reset(consumerGroup1, deadLetterConsumerGroup)).
		Step("wait", flow.Sleep(10*time.Second)).
		Run()
=======
		Step("publish messages to topic ==> "+fifoTopic, publishMessages(nil, sc1, fifoTopic, consumerGroup1)).

		// Publisher 2
		Step(app.Run(pub2, fmt.Sprintf(":%d", appPort+portOffset+4),
			doNothingApp(pub2, fifoTopic, consumerGroup1))).
		Step(sidecar.Run(sc2,
			embedded.WithComponentsPath("./components/fifo"),
			embedded.WithAppProtocol(runtime.HTTPProtocol, appPort+portOffset+4),
			embedded.WithDaprGRPCPort(runtime.DefaultDaprAPIGRPCPort+portOffset+4),
			embedded.WithDaprHTTPPort(runtime.DefaultDaprHTTPPort+portOffset+4),
			embedded.WithProfilePort(runtime.DefaultProfilePort+portOffset+4),
			componentRuntimeOptions(),
		)).
		Step("publish messages to topic ==> "+fifoTopic, publishMessages(nil, sc2, fifoTopic, consumerGroup1)).
		Step("wait", flow.Sleep(10*time.Second)).
		Step("verify if recevied ordered messages published to active topic", assertMessages(1*time.Second, consumerGroup1)).
		Step("reset", flow.Reset(consumerGroup1)).
		Run()

>>>>>>> master
}

func componentRuntimeOptions() []runtime.Option {
	log := logger.NewLogger("dapr.components")

	pubsubRegistry := pubsub_loader.NewRegistry()
	pubsubRegistry.Logger = log
	pubsubRegistry.RegisterComponent(pubsub_snssqs.NewSnsSqs, "snssqs")

	secretstoreRegistry := secretstores_loader.NewRegistry()
	secretstoreRegistry.Logger = log
	secretstoreRegistry.RegisterComponent(secretstore_env.NewEnvSecretStore, "local.env")

	return []runtime.Option{
		runtime.WithPubSubs(pubsubRegistry),
		runtime.WithSecretStores(secretstoreRegistry),
	}
}

func teardown(t *testing.T) {
	t.Logf("AWS SNS/SQS CertificationTests teardown...")
	//Dapr runtime automatically creates the following queues, topics
	//so here they get deleted.
	if err := deleteQueues(queues); err != nil {
		t.Log(err)
	}

	if err := deleteTopics(topics, region); err != nil {
		t.Log(err)
	}

	t.Logf("AWS SNS/SQS CertificationTests teardown...done!")
}
