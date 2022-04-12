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
	"os/exec"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/multierr"

	// Pub-Sub.
	"github.com/dapr/components-contrib/pubsub"
	pubsub_evethubs "github.com/dapr/components-contrib/pubsub/azure/eventhubs"
	"github.com/dapr/components-contrib/secretstores"
	secretstore_env "github.com/dapr/components-contrib/secretstores/local/env"
	pubsub_loader "github.com/dapr/dapr/pkg/components/pubsub"
	secretstores_loader "github.com/dapr/dapr/pkg/components/secretstores"

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
	topicName1       = "certification-pubsub-topic1"
	unUsedTopic      = "certification-pubsub-topic2"
	iotTopicName     = "testioteventing"
	topicToBeCreated = "certification-topic-per-test-run"
	partition0       = "partition-0"
	partition1       = "partition-1"
)

func TestEventhubs(t *testing.T) {
	log := logger.NewLogger("dapr.components")
	component := pubsub_loader.New("azure.eventhubs", func() pubsub.PubSub {
		return pubsub_evethubs.NewAzureEventHubs(log)
	})

	secretStoreComponent := secretstores_loader.New("local.env", func() secretstores.SecretStore {
		return secretstore_env.NewEnvSecretStore(log)
	})

	consumerGroup1 := watcher.NewUnordered()
	consumerGroup2 := watcher.NewUnordered()
	consumerGroup4 := watcher.NewOrdered()
	// consumerGroup5 := watcher.NewOrdered()

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

	deleteEventhub := func(ctx flow.Context) error {
		output, err := exec.Command("/bin/sh", "delete-eventhub.sh", topicToBeCreated).Output()
		assert.Nil(t, err, "Error in delete-eventhub.sh.:\n%s", string(output))
		return nil
	}

	// TODO : to be uncommented as part of IOT hub
	// publishMessageAsDevice := func(messageWatchers *watcher.Watcher) flow.Runnable {
	// 	return func(ctx flow.Context) error {
	// 		messages := make([]string, 10)
	// 		for i := range messages {
	// 			messages[i] = fmt.Sprintf("testmessageForEventHubCertificationTest #%v", i+1)
	// 		}
	// 		messageWatchers.ExpectStrings(messages...)

	// 		output, err := exec.Command("/bin/sh", "send-iot-device-events.sh", topicToBeCreated).Output()
	// 		assert.Nil(t, err, "Error in send-iot-device-events.sh.:\n%s", string(output))
	// 		return nil
	// 	}
	// }

	flow.New(t, "eventhubs certification").

		// Test : single publisher, multiple subscriber with their own consumerID
		// Run subscriberApplication app1
		Step(app.Run(appID1, fmt.Sprintf(":%d", appPort),
			subscriberApplication(appID1, topicName1, consumerGroup1))).

		// Run the Dapr sidecar with the eventhubs component 1, with permission at namespace level
		Step(sidecar.Run(sidecarName1,
			embedded.WithComponentsPath("./components/consumer1"),
			embedded.WithAppProtocol(runtime.HTTPProtocol, appPort),
			embedded.WithDaprGRPCPort(runtime.DefaultDaprAPIGRPCPort),
			embedded.WithDaprHTTPPort(runtime.DefaultDaprHTTPPort),
			runtime.WithSecretStores(secretStoreComponent),
			runtime.WithPubSubs(component))).

		// Run subscriberApplication app2
		Step(app.Run(appID2, fmt.Sprintf(":%d", appPort+portOffset),
			subscriberApplication(appID2, topicName1, consumerGroup2))).

		// Run the Dapr sidecar with the component 2.
		Step(sidecar.Run(sidecarName2,
			embedded.WithComponentsPath("./components/consumer2"),
			embedded.WithAppProtocol(runtime.HTTPProtocol, appPort+portOffset),
			embedded.WithDaprGRPCPort(runtime.DefaultDaprAPIGRPCPort+portOffset),
			embedded.WithDaprHTTPPort(runtime.DefaultDaprHTTPPort+portOffset),
			embedded.WithProfilePort(runtime.DefaultProfilePort+portOffset),
			runtime.WithSecretStores(secretStoreComponent),
			runtime.WithPubSubs(component))).
		Step("publish messages to topic1", publishMessages(nil, sidecarName1, topicName1, consumerGroup1, consumerGroup2)).
		Step("publish messages to unUsedTopic", publishMessages(nil, sidecarName1, unUsedTopic)).
		Step("verify if app1 has recevied messages published to topic1", assertMessages(10*time.Second, consumerGroup1)).
		Step("verify if app2 has recevied messages published to topic1", assertMessages(10*time.Second, consumerGroup2)).
		Step("reset", flow.Reset(consumerGroup1, consumerGroup2)).

		// Test : multiple publisher with different partitionkey, multiple subscriber with same consumer ID
		// Run subscriberApplication app3
		Step(app.Run(appID3, fmt.Sprintf(":%d", appPort+portOffset*2),
			subscriberApplication(appID3, topicName1, consumerGroup2))).

		// Run the Dapr sidecar with the component 3.
		Step(sidecar.Run(sidecarName3,
			embedded.WithComponentsPath("./components/consumer3"),
			embedded.WithAppProtocol(runtime.HTTPProtocol, appPort+portOffset*2),
			embedded.WithDaprGRPCPort(runtime.DefaultDaprAPIGRPCPort+portOffset*2),
			embedded.WithDaprHTTPPort(runtime.DefaultDaprHTTPPort+portOffset*2),
			embedded.WithProfilePort(runtime.DefaultProfilePort+portOffset*2),
			runtime.WithSecretStores(secretStoreComponent),
			runtime.WithPubSubs(component))).

		// publish message in topic1 from two publisher apps, however there are two subscriber apps (app2,app3) with same consumerID
		Step("publish messages to topic1", publishMessages(metadata, sidecarName1, topicName1, consumerGroup2)).
		Step("publish messages to topic1", publishMessages(metadata1, sidecarName2, topicName1, consumerGroup2)).
		Step("verify if app2, app3 together have recevied messages published to topic1", assertMessages(10*time.Second, consumerGroup2)).
		// Test : Entitymanagement , Test partition key, in order processing with single publisher/subscriber
		// Run subscriberApplication app4
		Step(app.Run(appID4, fmt.Sprintf(":%d", appPort+portOffset*3),
			subscriberApplication(appID4, topicToBeCreated, consumerGroup4))).

		// Run the Dapr sidecar with the component entitymanagement
		Step(sidecar.Run(sidecarName4,
			embedded.WithComponentsPath("./components/entitymanagementconsumer"),
			embedded.WithAppProtocol(runtime.HTTPProtocol, appPort+portOffset*3),
			embedded.WithDaprGRPCPort(runtime.DefaultDaprAPIGRPCPort+portOffset*3),
			embedded.WithDaprHTTPPort(runtime.DefaultDaprHTTPPort+portOffset*3),
			embedded.WithProfilePort(runtime.DefaultProfilePort+portOffset*3),
			runtime.WithSecretStores(secretStoreComponent),
			runtime.WithPubSubs(component))).
		Step(fmt.Sprintf("publish messages to topicToBeCreated: %s", topicToBeCreated), publishMessages(metadata, sidecarName4, topicToBeCreated, consumerGroup4)).
		Step("verify if app4 has recevied messages published to newly created topic", assertMessages(10*time.Second, consumerGroup4)).

		// TODO : Test : IOT hub
		// Run subscriberApplication app5
		// Step(app.Run(appID5, fmt.Sprintf(":%d", appPort+portOffset*4),
		// 	subscriberApplication(appID5, iotTopicName, consumerGroup5))).
		// // Run the Dapr sidecar with the iot component
		// Step(sidecar.Run(sidecarName5,
		// 	embedded.WithComponentsPath("./components/iotconsumer"),
		// 	embedded.WithAppProtocol(runtime.HTTPProtocol, appPort+portOffset*4),
		// 	embedded.WithDaprGRPCPort(runtime.DefaultDaprAPIGRPCPort+portOffset*4),
		// 	embedded.WithDaprHTTPPort(runtime.DefaultDaprHTTPPort+portOffset*4),
		// 	embedded.WithProfilePort(runtime.DefaultProfilePort+portOffset*4),
		// 	runtime.WithSecretStores(secretStoreComponent),
		// 	runtime.WithPubSubs(component))).
		// Step("add expected IOT messages (simulate add message to iot)", publishMessageAsDevice(consumerGroup5)).
		// Step("verify if app5 has recevied messages published to iot topic", assertMessages(40*time.Second, consumerGroup5)).
		Run()

	flow.New(t, "cleanup azure artifacts").
		Step("delete eventhub created as part of the eventhub management test", deleteEventhub).
		Run()
}
