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
	"os"
	"os/exec"
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/multierr"

	// Pub-Sub.
	"github.com/dapr/components-contrib/pubsub"
	pubsub_eventhubs "github.com/dapr/components-contrib/pubsub/azure/eventhubs"
	secretstore_env "github.com/dapr/components-contrib/secretstores/local/env"
	pubsub_loader "github.com/dapr/dapr/pkg/components/pubsub"
	secretstores_loader "github.com/dapr/dapr/pkg/components/secretstores"
	"github.com/dapr/dapr/pkg/config/protocol"
	"github.com/dapr/kit/logger"

	// Dapr runtime and Go-SDK
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
	sidecarName3 = "dapr-3"
	sidecarName4 = "dapr-4"
	sidecarName5 = "dapr-5"

	appID1 = "app-1"
	appID2 = "app-2"
	appID3 = "app-3"
	appID4 = "app-4"
	appID5 = "app-5"

	numMessages      = 10
	appPort          = 8000
	portOffset       = 2
	messageKey       = "partitionKey"
	pubsubName       = "messagebus"
	topicActiveName  = "certification-pubsub-topic-active"
	topicPassiveName = "certification-pubsub-topic-passive"
	topicToBeCreated = "certification-topic-per-test-run"
	iotHubNameEnvKey = "AzureIotHubName"
	partition0       = "partition-0"
	partition1       = "partition-1"
)

func TestEventhubs(t *testing.T) {
	// Tests that simulate failures are un-ordered
	consumerGroup1 := watcher.NewUnordered()
	consumerGroup2 := watcher.NewUnordered()
	consumerGroup4 := watcher.NewOrdered()
	consumerGroup5 := watcher.NewUnordered()

	// Set the partition key on all messages so they are written to the same partition. This allows for checking of ordered messages.
	metadata := map[string]string{
		messageKey: partition0,
	}

	metadata1 := map[string]string{
		messageKey: partition1,
	}

	// subscriber of the given topic
	subscriberApplication := func(appID string, topicName string, messagesWatcher *watcher.Watcher, withFailures bool) app.SetupFn {
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
					if withFailures {
						if err := sim(); err != nil {
							return true, err
						}
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
			logs := fmt.Sprintf("Published messages. sidecarName: %s, topicName: %s", sidecarName, topicName)

			var publishOptions dapr.PublishEventOption

			if metadata != nil {
				publishOptions = dapr.PublishEventWithMetadata(metadata)
			}

			for i, message := range messages {
				logs += fmt.Sprintf("\nMessage %d: %s", i, message)
				var err error

				if publishOptions != nil {
					err = client.PublishEvent(ctx, pubsubName, topicName, message, publishOptions)
				} else {
					err = client.PublishEvent(ctx, pubsubName, topicName, message)
				}

				require.NoErrorf(ctx, err, "error publishing message %s", message)
			}

			ctx.Log(logs)

			return nil
		}
	}

	assertMessages := func(timeout time.Duration, messageWatchers ...*watcher.Watcher) flow.Runnable {
		return func(ctx flow.Context) error {
			// assert for messages
			for _, m := range messageWatchers {
				m.Assert(ctx, 15*timeout)
			}

			return nil
		}
	}

	flowDoneCh := make(chan struct{}, 1)
	flowDone := func(ctx flow.Context) error {
		close(flowDoneCh)
		return nil
	}

	deleteEventhub := func() error {
		output, err := exec.Command("/bin/sh", "delete-eventhub.sh", topicToBeCreated).Output()
		assert.NoErrorf(t, err, "Error in delete-eventhub.sh.:\n%s", string(output))
		return nil
	}

	publishMessageAsDevice := func(messageWatchers *watcher.Watcher) flow.Runnable {
		return func(ctx flow.Context) error {
			messages := make([]string, 10)
			for i := range messages {
				messages[i] = fmt.Sprintf("testmessageForEventHubCertificationTest #%v", i+1)
			}
			messageWatchers.ExpectStrings(messages...)

			output, err := exec.Command("/bin/sh", "send-iot-device-events.sh", topicToBeCreated).Output()
			assert.NoErrorf(t, err, "Error in send-iot-device-events.sh.:\n%s", string(output))
			return nil
		}
	}
	// Topic name for a IOT device is same as IOTHubName
	iotHubName := os.Getenv(iotHubNameEnvKey)

	// Here so we can comment out tests as needed
	_ = consumerGroup1
	_ = consumerGroup2
	_ = consumerGroup4
	_ = consumerGroup5
	_ = publishMessageAsDevice
	_ = iotHubName
	_ = metadata
	_ = metadata1
	_ = publishMessages

	flow.New(t, "eventhubs certification").

		// Test : single publisher, multiple subscriber with their own consumerID
		// Run subscriberApplication app1
		Step(app.Run(appID1, fmt.Sprintf(":%d", appPort),
			subscriberApplication(appID1, topicActiveName, consumerGroup1, true))).

		// Run the Dapr sidecar with the eventhubs component 1, with permission at namespace level
		Step(sidecar.Run(sidecarName1,
			append(componentRuntimeOptions(1),
				embedded.WithComponentsPath("./components/consumer1"),
				embedded.WithAppProtocol(protocol.HTTPProtocol, strconv.Itoa(appPort)),
				embedded.WithDaprGRPCPort(strconv.Itoa(runtime.DefaultDaprAPIGRPCPort)),
				embedded.WithDaprHTTPPort(strconv.Itoa(runtime.DefaultDaprHTTPPort)),
				embedded.WithProfilePort(strconv.Itoa(runtime.DefaultProfilePort)),
			)...,
		)).

		// Run subscriberApplication app2
		Step(app.Run(appID2, fmt.Sprintf(":%d", appPort+portOffset),
			subscriberApplication(appID2, topicActiveName, consumerGroup2, true))).

		// Run the Dapr sidecar with the component 2.
		Step(sidecar.Run(sidecarName2,
			append(componentRuntimeOptions(2),
				embedded.WithComponentsPath("./components/consumer2"),
				embedded.WithAppProtocol(protocol.HTTPProtocol, strconv.Itoa(appPort+portOffset)),
				embedded.WithDaprGRPCPort(strconv.Itoa(runtime.DefaultDaprAPIGRPCPort+portOffset)),
				embedded.WithDaprHTTPPort(strconv.Itoa(runtime.DefaultDaprHTTPPort+portOffset)),
				embedded.WithProfilePort(strconv.Itoa(runtime.DefaultProfilePort+portOffset)),
			)...,
		)).
		Step("wait", flow.Sleep(15*time.Second)).
		Step("publish messages to topic1", publishMessages(nil, sidecarName1, topicActiveName, consumerGroup1, consumerGroup2)).
		Step("publish messages to unUsedTopic", publishMessages(nil, sidecarName1, topicPassiveName)).
		Step("verify if app1 has recevied messages published to topic1", assertMessages(10*time.Second, consumerGroup1)).
		Step("verify if app2 has recevied messages published to topic1", assertMessages(10*time.Second, consumerGroup2)).
		Step("reset", flow.Reset(consumerGroup1, consumerGroup2)).

		// Test : multiple publisher with different partitionkey, multiple subscriber with same consumer ID
		// Run subscriberApplication app3
		Step(app.Run(appID3, fmt.Sprintf(":%d", appPort+portOffset*2),
			subscriberApplication(appID3, topicActiveName, consumerGroup2, true))).

		// Run the Dapr sidecar with the component 3.
		Step(sidecar.Run(sidecarName3,
			append(componentRuntimeOptions(3),
				embedded.WithComponentsPath("./components/consumer3"),
				embedded.WithAppProtocol(protocol.HTTPProtocol, strconv.Itoa(appPort+portOffset*2)),
				embedded.WithDaprGRPCPort(strconv.Itoa(runtime.DefaultDaprAPIGRPCPort+portOffset*2)),
				embedded.WithDaprHTTPPort(strconv.Itoa(runtime.DefaultDaprHTTPPort+portOffset*2)),
				embedded.WithProfilePort(strconv.Itoa(runtime.DefaultProfilePort+portOffset*2)),
			)...,
		)).
		Step("wait", flow.Sleep(15*time.Second)).

		// publish message in topic1 from two publisher apps, however there are two subscriber apps (app2,app3) with same consumerID
		Step("publish messages to topic1 from app1", publishMessages(metadata, sidecarName1, topicActiveName, consumerGroup2)).
		Step("publish messages to topic1 from app2", publishMessages(metadata1, sidecarName2, topicActiveName, consumerGroup2)).
		Step("verify if app2, app3 together have recevied messages published to topic1", assertMessages(10*time.Second, consumerGroup2)).

		// Test : Entitymanagement , Test partition key, in order processing with single publisher/subscriber
		// Run subscriberApplication app4
		Step(app.Run(appID4, fmt.Sprintf(":%d", appPort+portOffset*3),
			subscriberApplication(appID4, topicToBeCreated, consumerGroup4, false))).

		// Run the Dapr sidecar with the component entitymanagement
		Step(sidecar.Run(sidecarName4,
			append(componentRuntimeOptions(4),
				embedded.WithComponentsPath("./components/entitymanagementconsumer"),
				embedded.WithAppProtocol(protocol.HTTPProtocol, strconv.Itoa(appPort+portOffset*3)),
				embedded.WithDaprGRPCPort(strconv.Itoa(runtime.DefaultDaprAPIGRPCPort+portOffset*3)),
				embedded.WithDaprHTTPPort(strconv.Itoa(runtime.DefaultDaprHTTPPort+portOffset*3)),
				embedded.WithProfilePort(strconv.Itoa(runtime.DefaultProfilePort+portOffset*3)),
			)...,
		)).
		Step("wait", flow.Sleep(15*time.Second)).
		Step(fmt.Sprintf("publish messages to topicToBeCreated: %s", topicToBeCreated), publishMessages(metadata, sidecarName4, topicToBeCreated, consumerGroup4)).
		Step("verify if app4 has recevied messages published to newly created topic", assertMessages(10*time.Second, consumerGroup4)).

		// Test : IOT hub
		// Run subscriberApplication app5
		Step(app.Run(appID5, fmt.Sprintf(":%d", appPort+portOffset*4),
			subscriberApplication(appID5, iotHubName, consumerGroup5, true))).
		// Run the Dapr sidecar with the iot component
		Step(sidecar.Run(sidecarName5,
			append(componentRuntimeOptions(5),
				embedded.WithComponentsPath("./components/iotconsumer"),
				embedded.WithAppProtocol(protocol.HTTPProtocol, strconv.Itoa(appPort+portOffset*4)),
				embedded.WithDaprGRPCPort(strconv.Itoa(runtime.DefaultDaprAPIGRPCPort+portOffset*4)),
				embedded.WithDaprHTTPPort(strconv.Itoa(runtime.DefaultDaprHTTPPort+portOffset*4)),
				embedded.WithProfilePort(strconv.Itoa(runtime.DefaultProfilePort+portOffset*4)),
			)...,
		)).
		Step("wait", flow.Sleep(20*time.Second)).
		Step("add expected IoT messages (simulate add message to IoT Hub)", publishMessageAsDevice(consumerGroup5)).
		Step("verify if app5 has recevied messages published to IoT topic", assertMessages(10*time.Second, consumerGroup5)).

		// Run the flow
		Step("mark as complete", flowDone).
		Run()

	// Cleanup Azure assets created as part of tests
	<-flowDoneCh
	time.Sleep(5 * time.Second)
	fmt.Println("Deleting EventHub resources created as part of the management test…")
	deleteEventhub()
}

func componentRuntimeOptions(instance int) []embedded.Option {
	log := logger.NewLogger("dapr.components")
	log.SetOutputLevel(logger.DebugLevel)

	pubsubRegistry := pubsub_loader.NewRegistry()
	pubsubRegistry.Logger = log
	pubsubRegistry.RegisterComponent(func(l logger.Logger) pubsub.PubSub {
		l = l.WithFields(map[string]any{
			"component": "pubsub.azure.eventhubs",
			"instance":  instance,
		})
		l.Infof("Instantiated log for instance %d", instance)
		return pubsub_eventhubs.NewAzureEventHubs(l)
	}, "azure.eventhubs")

	secretstoreRegistry := secretstores_loader.NewRegistry()
	secretstoreRegistry.Logger = log
	secretstoreRegistry.RegisterComponent(secretstore_env.NewEnvSecretStore, "local.env")

	return []embedded.Option{
		embedded.WithPubSubs(pubsubRegistry),
		embedded.WithSecretStores(secretstoreRegistry),
	}
}
