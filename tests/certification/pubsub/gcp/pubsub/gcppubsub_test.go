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

package pubsub_test

import (
	"context"
	"fmt"
	"os"
	"strconv"

	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"go.uber.org/multierr"

	// Pub-Sub.
	pubsub_gcppubsub "github.com/dapr/components-contrib/pubsub/gcp/pubsub"
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
	"github.com/dapr/components-contrib/tests/certification/flow/sidecar"
	"github.com/dapr/components-contrib/tests/certification/flow/simulate"
	"github.com/dapr/components-contrib/tests/certification/flow/watcher"
)

const (
	sidecarName1 = "dapr-1"
	sidecarName2 = "dapr-2"

	appID1 = "app-1"
	appID2 = "app-2"

	numMessages      = 5
	appPort          = 8000
	portOffset       = 2
	pubsubName       = "gcp-pubsub-cert-tests"
	topicDefaultName = "cert-test-topic-DOESNOT-EXIST"
)

// The following subscriptions IDs will be populated
// from the values of the "consumerID" metadata properties
// found inside each of the components/*/pubsub.yaml files
// The names will be passed in with Env Vars like:
//
//	PUBSUB_GCP_CONSUMER_ID_*
var subscriptions = []string{}
var topics = []string{}

var (
	topicActiveName       = "cert-test-active"
	topicPassiveName      = "cert-test-passive"
	projectID             = "GCP_PROJECT_ID_NOT_SET"
	fifoTopic             = "fifoTopic"           // replaced with env var PUBSUB_GCP_CONSUMER_ID_FIFO
	deadLetterTopicIn     = "deadLetterTopicIn"   // replaced with env var PUBSUB_GCP_PUBSUB_TOPIC_DLIN
	deadLetterTopicName   = "deadLetterTopicName" // replaced with env var PUBSUB_GCP_PUBSUB_TOPIC_DLOUT
	deadLetterSubcription = "deadLetterSubcription"
	existingTopic         = "existingTopic" // replaced with env var PUBSUB_GCP_TOPIC_EXISTS
)

func init() {
	getEnv("GCP_PROJECT_ID", func(ev string) {
		projectID = ev
	})

	uniqueId := getEnvOrDef("UNIQUE_ID", func() string {
		return uuid.New().String()
	})

	topicActiveName = fmt.Sprintf("%s-%s", topicActiveName, uniqueId)
	topicPassiveName = fmt.Sprintf("%s-%s", topicPassiveName, uniqueId)
	topics = append(topics, topicActiveName, topicPassiveName)

	getEnv("PUBSUB_GCP_CONSUMER_ID_1", func(ev string) {
		subscriptions = append(subscriptions, pubsub_gcppubsub.BuildSubscriptionID(ev, topicActiveName))
		subscriptions = append(subscriptions, pubsub_gcppubsub.BuildSubscriptionID(ev, topicPassiveName))
	})

	getEnv("PUBSUB_GCP_CONSUMER_ID_2", func(ev string) {
		subscriptions = append(subscriptions, pubsub_gcppubsub.BuildSubscriptionID(ev, topicActiveName))
		subscriptions = append(subscriptions, pubsub_gcppubsub.BuildSubscriptionID(ev, topicPassiveName))
	})

	getEnv("PUBSUB_GCP_CONSUMER_ID_FIFO", func(ev string) {
		fifoTopic = ev
		topics = append(topics, fifoTopic)
		subscriptions = append(subscriptions, pubsub_gcppubsub.BuildSubscriptionID(ev, fifoTopic))
	})

	getEnv("PUBSUB_GCP_PUBSUB_TOPIC_DLIN", func(ev string) {
		deadLetterTopicIn = ev
		topics = append(topics, deadLetterTopicIn)
		subscriptions = append(subscriptions, pubsub_gcppubsub.BuildSubscriptionID(ev, deadLetterTopicIn))
	})

	getEnv("PUBSUB_GCP_PUBSUB_TOPIC_DLOUT", func(ev string) {
		deadLetterTopicName = ev
		topics = append(topics, deadLetterTopicName)
		deadLetterSubcription = pubsub_gcppubsub.BuildSubscriptionID(ev, deadLetterTopicName)
		subscriptions = append(subscriptions, deadLetterSubcription)
	})

	getEnv("PUBSUB_GCP_TOPIC_EXISTS", func(ev string) {
		existingTopic = ev
	})
}

func getEnv(env string, fn func(ev string), dfns ...func() string) {
	if value := os.Getenv(env); value != "" {
		fn(value)
	} else if len(dfns) == 1 {
		fn(dfns[0]())
	}
}

func getEnvOrDef(env string, dfn func() string) string {
	if value := os.Getenv(env); value != "" {
		return value
	}
	return dfn()
}

func TestGCPPubSubCertificationTests(t *testing.T) {
	defer teardown(t)

	t.Run("GCPPubSubBasic", func(t *testing.T) {
		GCPPubSubBasic(t)
	})

	t.Run("GCPPubSubFIFOMessages", func(t *testing.T) {
		GCPPubSubFIFOMessages(t)
	})

	t.Run("GCPPubSubMessageDeadLetter", func(t *testing.T) {
		GCPPubSubMessageDeadLetter(t)
	})

	t.Run("GCPPubSubEntityManagement", func(t *testing.T) {
		GCPPubSubEntityManagement(t)
	})

	t.Run("GCPPubSubExistingTopic", func(t *testing.T) {
		GCPPubSubExistingTopic(t)
	})
}

// Verify with single publisher / single subscriber
func GCPPubSubBasic(t *testing.T) {

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
				require.NoError(ctx, err, "GCPPubSubBasic - error publishing message")
			}
			return nil
		}
	}

	assertMessages := func(timeout time.Duration, messageWatchers ...*watcher.Watcher) flow.Runnable {
		return func(ctx flow.Context) error {
			// assert for messages
			for _, m := range messageWatchers {
				if !m.Assert(ctx, 25*timeout) {
					ctx.Errorf("GCPPubSubBasic - message assertion failed for watcher: %#v\n", m)
				}
			}

			return nil
		}
	}

	flow.New(t, "GCPPub Verify with single publisher / single subscriber").

		// Run subscriberApplication app1
		Step(app.Run(appID1, fmt.Sprintf(":%d", appPort),
			subscriberApplication(appID1, topicActiveName, consumerGroup1))).

		// Run the Dapr sidecar with ConsumerID "PUBSUB_GCP_CONSUMER_ID_1"
		Step(sidecar.Run(sidecarName1,
			append(componentRuntimeOptions(),
				embedded.WithComponentsPath("./components/consumer_one"),
				embedded.WithAppProtocol(protocol.HTTPProtocol, strconv.Itoa(appPort)),
				embedded.WithDaprGRPCPort(strconv.Itoa(runtime.DefaultDaprAPIGRPCPort)),
				embedded.WithDaprHTTPPort(strconv.Itoa(runtime.DefaultDaprHTTPPort)),
			)...,
		)).

		// Run subscriberApplication app2
		Step(app.Run(appID2, fmt.Sprintf(":%d", appPort+portOffset),
			subscriberApplication(appID2, topicActiveName, consumerGroup2))).

		// Run the Dapr sidecar with ConsumerID "PUBSUB_GCP_CONSUMER_ID_2"
		Step(sidecar.Run(sidecarName2,
			append(componentRuntimeOptions(),
				embedded.WithComponentsPath("./components/consumer_two"),
				embedded.WithAppProtocol(protocol.HTTPProtocol, strconv.Itoa(appPort+portOffset)),
				embedded.WithDaprGRPCPort(strconv.Itoa(runtime.DefaultDaprAPIGRPCPort+portOffset)),
				embedded.WithDaprHTTPPort(strconv.Itoa(runtime.DefaultDaprHTTPPort+portOffset)),
				embedded.WithProfilePort(strconv.Itoa(runtime.DefaultProfilePort+portOffset)),
			)...,
		)).
		Step("publish messages to active topic ==> "+topicActiveName, publishMessages(nil, sidecarName1, topicActiveName, consumerGroup1, consumerGroup2)).
		Step("publish messages to passive topic ==> "+topicPassiveName, publishMessages(nil, sidecarName1, topicPassiveName)).
		Step("verify if app1 has recevied messages published to active topic", assertMessages(10*time.Second, consumerGroup1)).
		Step("verify if app2 has recevied messages published to passive topic", assertMessages(10*time.Second, consumerGroup2)).
		Step("reset", flow.Reset(consumerGroup1, consumerGroup2)).
		Run()
}

// Verify data with an optional parameters `fifo` and `fifoMessageGroupID` takes affect (GCPPubSubFIFOMessages)
func GCPPubSubFIFOMessages(t *testing.T) {
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

	subscriberApplication := func(appID string, topicName string, messagesWatcher *watcher.Watcher) app.SetupFn {
		return func(ctx flow.Context, s common.Service) error {
			return multierr.Combine(
				s.AddTopicEventHandler(&common.Subscription{
					PubsubName: pubsubName,
					Topic:      topicName,
					Route:      "/orders",
				}, func(_ context.Context, e *common.TopicEvent) (retry bool, err error) {
					ctx.Logf("GCPPubSubFIFOMessages.subscriberApplication: Message Received appID: %s,pubsub: %s, topic: %s, id: %s, data: %#v", appID, e.PubsubName, e.Topic, e.ID, e.Data)
					messagesWatcher.Observe(e.Data)
					return false, nil
				}),
			)
		}
	}

	publishMessages := func(metadata map[string]string, sidecarName string, topicName string, mw *watcher.Watcher) flow.Runnable {
		return func(ctx flow.Context) error {

			// get the sidecar (dapr) client
			client := sidecar.GetClient(ctx, sidecarName)

			// publish messages
			ctx.Logf("GCPPubSubFIFOMessages Publishing messages. sidecarName: %s, topicName: %s", sidecarName, topicName)

			var publishOptions dapr.PublishEventOption

			if metadata != nil {
				publishOptions = dapr.PublishEventWithMetadata(metadata)
			}

			for message := range msgCh {
				ctx.Logf("GCPPubSubFIFOMessages Publishing: sidecarName: %s, topicName: %s - %q", sidecarName, topicName, message)
				var err error

				if publishOptions != nil {
					err = client.PublishEvent(ctx, pubsubName, topicName, message, publishOptions)
				} else {
					err = client.PublishEvent(ctx, pubsubName, topicName, message)
				}
				require.NoError(ctx, err, "GCPPubSubFIFOMessages - error publishing message")
			}
			return nil
		}
	}
	assertMessages := func(timeout time.Duration, messageWatchers ...*watcher.Watcher) flow.Runnable {
		return func(ctx flow.Context) error {
			// assert for messages
			for _, m := range messageWatchers {
				if !m.Assert(ctx, 10*timeout) {
					ctx.Errorf("GCPPubSubFIFOMessages - message assertion failed for watcher: %#v\n", m)
				}
			}

			return nil
		}
	}

	pub1 := "publisher1"
	sc1 := pub1 + "_sidecar"
	pub2 := "publisher2"
	sc2 := pub2 + "_sidecar"
	sub := "subscriber"
	subsc := sub + "_sidecar"

	flow.New(t, "GCPPubSubFIFOMessages Verify FIFO with multiple publishers and single subscriber receiving messages in order").

		// Subscriber
		Step(app.Run(sub, fmt.Sprintf(":%d", appPort),
			subscriberApplication(sub, fifoTopic, consumerGroup1))).
		Step(sidecar.Run(subsc,
			append(componentRuntimeOptions(),
				embedded.WithComponentsPath("./components/fifo"),
				embedded.WithAppProtocol(protocol.HTTPProtocol, strconv.Itoa(appPort)),
				embedded.WithDaprGRPCPort(strconv.Itoa(runtime.DefaultDaprAPIGRPCPort)),
				embedded.WithDaprHTTPPort(strconv.Itoa(runtime.DefaultDaprHTTPPort)),
			)...,
		)).
		Step("wait", flow.Sleep(5*time.Second)).

		// Publisher 1
		Step(app.Run(pub1, fmt.Sprintf(":%d", appPort+portOffset+2),
			doNothingApp(pub1, fifoTopic, consumerGroup1))).
		Step(sidecar.Run(sc1,
			append(componentRuntimeOptions(),
				embedded.WithComponentsPath("./components/fifo"),
				embedded.WithAppProtocol(protocol.HTTPProtocol, strconv.Itoa(appPort+portOffset+2)),
				embedded.WithDaprGRPCPort(strconv.Itoa(runtime.DefaultDaprAPIGRPCPort+portOffset+2)),
				embedded.WithDaprHTTPPort(strconv.Itoa(runtime.DefaultDaprHTTPPort+portOffset+2)),
				embedded.WithProfilePort(strconv.Itoa(runtime.DefaultProfilePort+portOffset+2)),
			)...,
		)).
		Step("publish messages to topic ==> "+fifoTopic, publishMessages(nil, sc1, fifoTopic, consumerGroup1)).

		// Publisher 2
		Step(app.Run(pub2, fmt.Sprintf(":%d", appPort+portOffset+4),
			doNothingApp(pub2, fifoTopic, consumerGroup1))).
		Step(sidecar.Run(sc2,
			append(componentRuntimeOptions(),
				embedded.WithComponentsPath("./components/fifo"),
				embedded.WithAppProtocol(protocol.HTTPProtocol, strconv.Itoa(appPort+portOffset+4)),
				embedded.WithDaprGRPCPort(strconv.Itoa(runtime.DefaultDaprAPIGRPCPort+portOffset+4)),
				embedded.WithDaprHTTPPort(strconv.Itoa(runtime.DefaultDaprHTTPPort+portOffset+4)),
				embedded.WithProfilePort(strconv.Itoa(runtime.DefaultProfilePort+portOffset+4)),
			)...,
		)).
		Step("publish messages to topic ==> "+fifoTopic, publishMessages(nil, sc2, fifoTopic, consumerGroup1)).
		Step("wait", flow.Sleep(10*time.Second)).
		Step("verify if recevied ordered messages published to active topic", assertMessages(1*time.Second, consumerGroup1)).
		Step("reset", flow.Reset(consumerGroup1)).
		Run()
}

// Verify data with an optional parameters `DeadLetterTopic` takes affect
func GCPPubSubMessageDeadLetter(t *testing.T) {
	consumerGroup1 := watcher.NewUnordered()
	deadLetterConsumerGroup := watcher.NewUnordered()
	failedMessagesNum := 1

	subscriberApplication := func(appID string, topicName string, messagesWatcher *watcher.Watcher) app.SetupFn {
		return func(ctx flow.Context, s common.Service) error {
			return multierr.Combine(
				s.AddTopicEventHandler(&common.Subscription{
					PubsubName: pubsubName,
					Topic:      topicName,
					Route:      "/orders",
				}, func(_ context.Context, e *common.TopicEvent) (retry bool, err error) {
					ctx.Logf("subscriberApplication - Message Received appID: %s,pubsub: %s, topic: %s, id: %s, data: %s - causing failure on purpose...", appID, e.PubsubName, e.Topic, e.ID, e.Data)
					return true, fmt.Errorf("failure on purpose")
				}),
			)
		}
	}

	var task flow.AsyncTask
	deadLetterReceiverApplication := func(deadLetterTopicName string, msgTimeout time.Duration, messagesWatcher *watcher.Watcher) flow.Runnable {
		return func(ctx flow.Context) error {
			t := time.NewTicker(500 * time.Millisecond)
			defer t.Stop()
			counter := 1
			tm, err := NewTopicManager(projectID)
			if err != nil {
				return fmt.Errorf("deadLetterReceiverApplication - NewTopicManager: %v", err)
			}
			defer func() {
				ctx.Logf("deadLetterReceiverApplication - defer disconnect: %v ", tm.disconnect())
			}()

			for {
				select {
				case <-task.Done():
					ctx.Log("deadLetterReceiverApplication - task done called!")
					return nil
				case <-time.After(msgTimeout * time.Second):
					ctx.Logf("deadLetterReceiverApplication - timeout waiting for messages from (%q)", deadLetterTopicName)
					return fmt.Errorf("deadLetterReceiverApplication - timeout waiting for messages from (%q)", deadLetterTopicName)
				case <-t.C:
					numMsgs, err := tm.GetMessages(deadLetterTopicName, msgTimeout, deadLetterSubcription, func(m *DataMessage) error {
						ctx.Logf("deadLetterReceiverApplication - received message counter(%d) (%v)\n", counter, m.Data)
						messagesWatcher.Observe(m.Data)
						return nil
					})
					if err != nil {
						ctx.Logf("deadLetterReceiverApplication - failed to get messages from (%q) counter(%d) %v  - trying again\n", deadLetterTopicName, counter, err)
						continue
					}
					if numMsgs == 0 {
						// No messages yet, try again
						ctx.Logf("deadLetterReceiverApplication -  no messages yet from (%q) counter(%d)  - trying again\n", deadLetterTopicName, counter)
						continue
					}

					if counter >= failedMessagesNum {
						ctx.Logf("deadLetterReceiverApplication - received all expected (%d) failed message!\n", failedMessagesNum)
						return nil
					}
					counter += numMsgs
				}

			}
		}
	}

	publishMessages := func(metadata map[string]string, sidecarName string, topicName string, messageWatchers ...*watcher.Watcher) flow.Runnable {
		return func(ctx flow.Context) error {
			// prepare the messages
			messages := make([]string, failedMessagesNum)
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
			ctx.Logf("GCPPubSubMessageDeadLetter - Publishing messages. sidecarName: %s, topicName: %s", sidecarName, topicName)

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
				require.NoError(ctx, err, "GCPPubSubMessageDeadLetter - error publishing message")
			}
			return nil
		}
	}

	assertMessages := func(timeout time.Duration, messageWatchers ...*watcher.Watcher) flow.Runnable {
		return func(ctx flow.Context) error {
			// assert for messages
			for _, m := range messageWatchers {
				if !m.Assert(ctx, 3*timeout) {
					ctx.Errorf("GCPPubSubMessageDeadLetter - message assertion failed for watcher: %#v\n", m)
				}
			}

			return nil
		}
	}

	prefix := "GCPPubSubMessageDeadLetter-"
	deadletterApp := prefix + "deadLetterReceiverApp"
	subApp := prefix + "subscriberApp"
	subAppSideCar := prefix + sidecarName2
	msgTimeout := time.Duration(30) //seconds

	flow.New(t, "GCPPubSubMessageDeadLetter Verify with single publisher / single subscriber and DeadLetter").

		// Run deadLetterReceiverApplication - should receive messages from dead letter Topic
		// "PUBSUB_GCP_PUBSUB_TOPIC_DLOUT"
		StepAsync(deadletterApp, &task,
			deadLetterReceiverApplication(deadLetterTopicName, msgTimeout, deadLetterConsumerGroup)).

		// Run subscriberApplication - will fail to process messages
		Step(app.Run(subApp, fmt.Sprintf(":%d", appPort+portOffset+4),
			subscriberApplication(subApp, deadLetterTopicIn, consumerGroup1))).

		// Run the Dapr sidecar with ConsumerID "PUBSUB_GCP_PUBSUB_TOPIC_DLOUT"
		Step(sidecar.Run(subAppSideCar,
			append(componentRuntimeOptions(),
				embedded.WithComponentsPath("./components/deadletter"),
				embedded.WithAppProtocol(protocol.HTTPProtocol, strconv.Itoa(appPort+portOffset+4)),
				embedded.WithDaprGRPCPort(strconv.Itoa(runtime.DefaultDaprAPIGRPCPort+portOffset+4)),
				embedded.WithDaprHTTPPort(strconv.Itoa(runtime.DefaultDaprHTTPPort+portOffset+4)),
				embedded.WithProfilePort(strconv.Itoa(runtime.DefaultProfilePort+portOffset+4)),
			)...,
		)).
		Step("publish messages to deadLetterTopicIn ==> "+deadLetterTopicIn, publishMessages(nil, subAppSideCar, deadLetterTopicIn, deadLetterConsumerGroup)).
		Step("wait", flow.Sleep(10*time.Second)).
		Step("verify if app1 has 0 recevied messages published to active topic", assertMessages(10*time.Second, consumerGroup1)).
		Step("verify if app2 has deadletterMessageNum recevied messages send to dead letter Topic", assertMessages(10*time.Second, deadLetterConsumerGroup)).
		Step("reset", flow.Reset(consumerGroup1, deadLetterConsumerGroup)).
		Run()
}

// Verify with an optional parameter `disableEntityManagement` set to true
func GCPPubSubEntityManagement(t *testing.T) {
	// TODO: Modify it to looks for component init error in the sidecar itself.
	consumerGroup1 := watcher.NewUnordered()

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
				// Error is expected as the topic does not exist
				require.Error(ctx, err, "GCPPubSubEntityManagement - error publishing message")
			}
			return nil
		}
	}

	flow.New(t, "GCPPubSub certification - entity management disabled").

		// Run subscriberApplication app1
		Step(app.Run(appID1, fmt.Sprintf(":%d", appPort+portOffset),
			subscriberApplication(appID1, topicActiveName, consumerGroup1))).

		// Run the Dapr sidecar
		Step(sidecar.Run(sidecarName1,
			append(componentRuntimeOptions(),
				embedded.WithComponentsPath("./components/entity_mgmt"),
				embedded.WithAppProtocol(protocol.HTTPProtocol, strconv.Itoa(appPort+portOffset)),
				embedded.WithDaprGRPCPort(strconv.Itoa(runtime.DefaultDaprAPIGRPCPort+portOffset)),
				embedded.WithDaprHTTPPort(strconv.Itoa(runtime.DefaultDaprHTTPPort+portOffset)),
				embedded.WithProfilePort(strconv.Itoa(runtime.DefaultProfilePort+portOffset)),
			)...,
		)).
		Step(fmt.Sprintf("publish messages to topicDefault: %s", topicDefaultName), publishMessages(nil, sidecarName1, topicDefaultName, consumerGroup1)).
		Run()
}

// Verify data with an existing Topic
func GCPPubSubExistingTopic(t *testing.T) {
	consumerGroup1 := watcher.NewUnordered()

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
				require.NoError(ctx, err, "GCPPubSubExistingTopic - error publishing message")
			}
			return nil
		}
	}

	assertMessages := func(timeout time.Duration, messageWatchers ...*watcher.Watcher) flow.Runnable {
		return func(ctx flow.Context) error {
			// assert for messages
			for _, m := range messageWatchers {
				if !m.Assert(ctx, 5*timeout) {
					ctx.Errorf("GCPPubSubExistingTopic - message assertion failed for watcher: %#v\n", m)
				}
			}

			return nil
		}
	}

	flow.New(t, "GCPPubSub certification - Existing Topic").

		// Run subscriberApplication app1
		Step(app.Run(appID1, fmt.Sprintf(":%d", appPort+portOffset*3),
			subscriberApplication(appID1, existingTopic, consumerGroup1))).

		// Run the Dapr sidecar with ConsumerID "PUBSUB_GCP_CONSUMER_ID_EXISTS"
		Step(sidecar.Run(sidecarName1,
			append(componentRuntimeOptions(),
				embedded.WithComponentsPath("./components/existing_topic"),
				embedded.WithAppProtocol(protocol.HTTPProtocol, strconv.Itoa(appPort+portOffset*3)),
				embedded.WithDaprGRPCPort(strconv.Itoa(runtime.DefaultDaprAPIGRPCPort+portOffset*3)),
				embedded.WithDaprHTTPPort(strconv.Itoa(runtime.DefaultDaprHTTPPort+portOffset*3)),
				embedded.WithProfilePort(strconv.Itoa(runtime.DefaultProfilePort+portOffset*3)),
			)...,
		)).
		Step(fmt.Sprintf("publish messages to existingTopic: %s", existingTopic), publishMessages(nil, sidecarName1, existingTopic, consumerGroup1)).
		Step("wait", flow.Sleep(20*time.Second)).
		Step("verify if app1 has recevied messages published to newly created topic", assertMessages(1*time.Second, consumerGroup1)).
		Run()
}

func componentRuntimeOptions() []embedded.Option {
	log := logger.NewLogger("dapr.components")

	pubsubRegistry := pubsub_loader.NewRegistry()
	pubsubRegistry.Logger = log
	pubsubRegistry.RegisterComponent(pubsub_gcppubsub.NewGCPPubSub, "gcp.pubsub")

	secretstoreRegistry := secretstores_loader.NewRegistry()
	secretstoreRegistry.Logger = log
	secretstoreRegistry.RegisterComponent(secretstore_env.NewEnvSecretStore, "local.env")

	return []embedded.Option{
		embedded.WithPubSubs(pubsubRegistry),
		embedded.WithSecretStores(secretstoreRegistry),
	}
}

func teardown(t *testing.T) {
	t.Logf("GCP PubSub CertificationTests teardown...")
	//Dapr runtime automatically creates the following subscriptions, topics
	//so here they get deleted.
	if err := deleteSubscriptions(projectID, subscriptions); err != nil {
		t.Log(err)
	}

	if err := deleteTopics(projectID, topics); err != nil {
		t.Log(err)
	}

	t.Logf("GCP PubSub CertificationTests teardown...done!")
}
