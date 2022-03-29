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

package eventhubs_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"go.uber.org/multierr"

	// Pub-Sub.
	"github.com/dapr/components-contrib/pubsub"
	pubsub_evethubs "github.com/dapr/components-contrib/pubsub/azure/eventhubs"
	pubsub_loader "github.com/dapr/dapr/pkg/components/pubsub"

	// Dapr runtime and Go-SDK
	"github.com/dapr/dapr/pkg/runtime"
	dapr "github.com/dapr/go-sdk/client"
	"github.com/dapr/go-sdk/service/common"
	"github.com/dapr/kit/logger"

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
	sidecarName3 = "dapr-3"
	sidecarName4 = "dapr-4"
	sidecarName5 = "dapr-5"

	appID1 = "app-1"
	appID2 = "app-2"
	appID3 = "app-3"
	appID4 = "app-4"
	appID5 = "app-5"

	numMessages      = 100
	appPort          = 8000
	portOffset       = 2
	messageKey       = "partitionKey"
	pubsubName       = "messagebus"
	topicName1       = "neworder"
	unUsedTopic      = "newinvoice"
	iotTopicName     = "testioteventing"
	topicToBeCreated = "brandneworder"
	partition0       = "partition-0"
)

func TestEventhubs(t *testing.T) {
	log := logger.NewLogger("dapr.components")
	component := pubsub_loader.New("azure.eventhubs", func() pubsub.PubSub {
		return pubsub_evethubs.NewAzureEventHubs(log)
	})

	consumerGroup1 := watcher.NewOrdered()
	consumerGroup2 := watcher.NewOrdered()
	consumerGroup4 := watcher.NewOrdered()
	consumerGroup5 := watcher.NewOrdered()

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

					fmt.Println(e.Data)
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
			publishOptions := dapr.PublishEventWithMetadata(metadata)

			for _, message := range messages {
				ctx.Logf("Publishing: %q", message)
				err := client.PublishEvent(ctx, pubsubName, topicName, message, publishOptions)
				require.NoError(ctx, err, "error publishing message")
			}
			return nil
		}
	}

	assertMessages := func(timeout time.Duration, messageWatchers ...*watcher.Watcher) flow.Runnable {
		return func(ctx flow.Context) error {
			// assert for messages
			for _, m := range messageWatchers {
				m.Assert(ctx, 2*timeout)
			}

			return nil
		}
	}

	// simulate the publish of messages to iot endpoint (./send-iot-device-events.sh)
	addExpectedMessagesforIot := func(messageWatchers *watcher.Watcher) flow.Runnable {
		return func(ctx flow.Context) error {
			messages := make([]string, numMessages)
			for i := range messages {
				messages[i] = fmt.Sprintf("testmessageForEventHubCertificationTest #%v", i+1)
			}
			messageWatchers.ExpectStrings(messages...)
			return nil
		}
	}

	flow.New(t, "eventhubs certification").

		// Run the subscriberApplication 1
		Step(app.Run(appID1, fmt.Sprintf(":%d", appPort),
			subscriberApplication(appID1, topicName1, consumerGroup1))).

		// Run the Dapr sidecar with the eventhubs component 1.
		Step(sidecar.Run(sidecarName1,
			embedded.WithComponentsPath("./components/consumer1"),

			embedded.WithAppProtocol(runtime.HTTPProtocol, appPort),
			embedded.WithDaprGRPCPort(runtime.DefaultDaprAPIGRPCPort),
			embedded.WithDaprHTTPPort(runtime.DefaultDaprHTTPPort),
			runtime.WithPubSubs(component))).

		// Run the second application.
		Step(app.Run(appID2, fmt.Sprintf(":%d", appPort+portOffset),
			subscriberApplication(appID2, topicName1, consumerGroup2))).
		//
		// Run the Dapr sidecar with the component 2.
		Step(sidecar.Run(sidecarName2,
			embedded.WithComponentsPath("./components/consumer2"),
			embedded.WithAppProtocol(runtime.HTTPProtocol, appPort+portOffset),
			embedded.WithDaprGRPCPort(runtime.DefaultDaprAPIGRPCPort+portOffset),
			embedded.WithDaprHTTPPort(runtime.DefaultDaprHTTPPort+portOffset),
			embedded.WithProfilePort(runtime.DefaultProfilePort+portOffset),
			runtime.WithPubSubs(component))).
		Step("publish messages to topic1", publishMessages(metadata, sidecarName1, topicName1, consumerGroup1, consumerGroup2)).
		Step("publish messages to unUsedTopic", publishMessages(metadata, sidecarName1, unUsedTopic)).
		Step("Verify if app1 has recevied messages published to topic1", assertMessages(10*time.Second, consumerGroup1)).
		Step("Verify if app2 has recevied messages published to topic1", assertMessages(10*time.Second, consumerGroup2)).
		Step("reset", flow.Reset(consumerGroup2)).

		// Run the third application.
		Step(app.Run(appID3, fmt.Sprintf(":%d", appPort+portOffset*2),
			subscriberApplication(appID3, topicName1, consumerGroup2))).

		// Run the Dapr sidecar with the component 3.
		Step(sidecar.Run(sidecarName3,
			embedded.WithComponentsPath("./components/consumer3"),
			embedded.WithAppProtocol(runtime.HTTPProtocol, appPort+portOffset*2),
			embedded.WithDaprGRPCPort(runtime.DefaultDaprAPIGRPCPort+portOffset*2),
			embedded.WithDaprHTTPPort(runtime.DefaultDaprHTTPPort+portOffset*2),
			embedded.WithProfilePort(runtime.DefaultProfilePort+portOffset*2),
			runtime.WithPubSubs(component))).

		// publish message again to topic1, however this time there are more than one consumer group on same topic
		Step("publish messages to topic1", publishMessages(metadata, sidecarName1, topicName1, consumerGroup2)).
		Step("publish messages to topic1", publishMessages(metadata, sidecarName2, topicName1, consumerGroup2)).
		Step("Verify if app1, app3 together has recevied messages published to topic1", assertMessages(10*time.Second, consumerGroup2)).

		// Run the 4th application.
		Step(app.Run(appID4, fmt.Sprintf(":%d", appPort+portOffset*3),
			subscriberApplication(appID4, topicToBeCreated, consumerGroup4))).
		//
		// Run the Dapr sidecar with the component entitymanagement
		Step(sidecar.Run(sidecarName4,
			embedded.WithComponentsPath("./components/entitymanagementconsumer"),
			embedded.WithAppProtocol(runtime.HTTPProtocol, appPort+portOffset*3),
			embedded.WithDaprGRPCPort(runtime.DefaultDaprAPIGRPCPort+portOffset*3),
			embedded.WithDaprHTTPPort(runtime.DefaultDaprHTTPPort+portOffset*3),
			embedded.WithProfilePort(runtime.DefaultProfilePort+portOffset*3),
			runtime.WithPubSubs(component))).
		Step("publish messages to topic1", publishMessages(metadata, sidecarName4, topicToBeCreated, consumerGroup4)).
		Step("Verify if app5 has recevied messages published to topic", assertMessages(50*time.Second, consumerGroup4)).

		// IOT Test : app 5
		// PREREQ : Add messages to IOT endpoint via (./send-iot-device-events.sh)
		Step(app.Run(appID5, fmt.Sprintf(":%d", appPort+portOffset*4),
			subscriberApplication(appID5, iotTopicName, consumerGroup5))).
		// Run the Dapr sidecar with the iot component
		Step(sidecar.Run(sidecarName5,
			embedded.WithComponentsPath("./components/iotconsumer"),
			embedded.WithAppProtocol(runtime.HTTPProtocol, appPort+portOffset*4),
			embedded.WithDaprGRPCPort(runtime.DefaultDaprAPIGRPCPort+portOffset*4),
			embedded.WithDaprHTTPPort(runtime.DefaultDaprHTTPPort+portOffset*4),
			embedded.WithProfilePort(runtime.DefaultProfilePort+portOffset*4),
			runtime.WithPubSubs(component))).
		Step("Add expected IOT messages (simulate add message to iot)", addExpectedMessagesforIot(consumerGroup5)).
		Step("Verify if app4 has recevied messages published to iot topic", assertMessages(40*time.Second, consumerGroup5)).
		Run()
}
