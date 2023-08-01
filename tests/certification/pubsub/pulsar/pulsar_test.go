/*
Copyright 2023 The Dapr Authors
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

package pulsar_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"go.uber.org/multierr"

	pubsub_pulsar "github.com/dapr/components-contrib/pubsub/pulsar"
	pubsub_loader "github.com/dapr/dapr/pkg/components/pubsub"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/dapr/dapr/pkg/runtime"
	dapr "github.com/dapr/go-sdk/client"
	"github.com/dapr/go-sdk/service/common"
	"github.com/dapr/kit/logger"

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
	sidecarName1 = "dapr-1"
	sidecarName2 = "dapr-2"

	appID1 = "app-1"
	appID2 = "app-2"

	numMessages             = 10
	appPort                 = 8000
	portOffset              = 2
	messageKey              = "partitionKey"
	pubsubName              = "messagebus"
	topicActiveName         = "certification-pubsub-topic-active"
	topicPassiveName        = "certification-pubsub-topic-passive"
	topicToBeCreated        = "certification-topic-per-test-run"
	topicDefaultName        = "certification-topic-default"
	topicMultiPartitionName = "certification-topic-multi-partition8"
	partition0              = "partition-0"
	partition1              = "partition-1"
	clusterName             = "pulsarcertification"
	dockerComposeYAML       = "docker-compose.yml"
	pulsarURL               = "localhost:6650"

	subscribeTypeKey = "subscribeType"

	subscribeTypeExclusive = "exclusive"
	subscribeTypeShared    = "shared"
	subscribeTypeFailover  = "failover"
	subscribeTypeKeyShared = "key_shared"

	processModeKey   = "processMode"
	processModeAsync = "async"
	processModeSync  = "sync"
)

func subscriberApplication(appID string, topicName string, messagesWatcher *watcher.Watcher) app.SetupFn {
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

func subscriberApplicationWithoutError(appID string, topicName string, messagesWatcher *watcher.Watcher) app.SetupFn {
	return func(ctx flow.Context, s common.Service) error {
		// Setup the /orders event handler.
		return multierr.Combine(
			s.AddTopicEventHandler(&common.Subscription{
				PubsubName: pubsubName,
				Topic:      topicName,
				Route:      "/orders",
				Metadata: map[string]string{
					subscribeTypeKey: subscribeTypeKeyShared,
					processModeKey:   processModeSync,
				},
			}, func(_ context.Context, e *common.TopicEvent) (retry bool, err error) {
				// Track/Observe the data of the event.
				messagesWatcher.Observe(e.Data)
				ctx.Logf("Message Received appID: %s,pubsub: %s, topic: %s, id: %s, data: %s", appID, e.PubsubName, e.Topic, e.ID, e.Data)
				return false, nil
			}),
		)
	}
}

func subscriberSchemaApplication(appID string, topicName string, messagesWatcher *watcher.Watcher) app.SetupFn {
	return func(ctx flow.Context, s common.Service) error {
		// Setup the /orders event handler.
		return multierr.Combine(
			s.AddTopicEventHandler(&common.Subscription{
				PubsubName: pubsubName,
				Topic:      topicName,
				Route:      "/orders",
			}, func(_ context.Context, e *common.TopicEvent) (retry bool, err error) {
				// Track/Observe the data of the event.
				messagesWatcher.ObserveJSON(e.Data)
				ctx.Logf("Message Received appID: %s,pubsub: %s, topic: %s, id: %s, data: %s", appID, e.PubsubName, e.Topic, e.ID, e.Data)
				return false, nil
			}),
		)
	}
}

func publishMessages(metadata map[string]string, sidecarName string, topicName string, messageWatchers ...*watcher.Watcher) flow.Runnable {
	return func(ctx flow.Context) error {
		// prepare the messages
		messages := make([]string, numMessages)
		for i := range messages {
			messages[i] = fmt.Sprintf("partitionKey: %s, message for topic: %s, index: %03d, uniqueId: %s", metadata[messageKey], topicName, i, uuid.New().String())
		}

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

func assertMessages(timeout time.Duration, messageWatchers ...*watcher.Watcher) flow.Runnable {
	return func(ctx flow.Context) error {
		// assert for messages
		for _, m := range messageWatchers {
			m.Assert(ctx, 25*timeout)
		}

		return nil
	}
}

func TestPulsar(t *testing.T) {
	consumerGroup1 := watcher.NewUnordered()
	consumerGroup2 := watcher.NewUnordered()

	publishMessages := func(metadata map[string]string, sidecarName string, topicName string, messageWatchers ...*watcher.Watcher) flow.Runnable {
		return func(ctx flow.Context) error {
			// prepare the messages
			messages := make([]string, numMessages)
			for i := range messages {
				messages[i] = fmt.Sprintf("partitionKey: %s, message for topic: %s, index: %03d, uniqueId: %s", metadata[messageKey], topicName, i, uuid.New().String())
			}

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

	flow.New(t, "pulsar certification basic test").

		// Run subscriberApplication app1
		Step(app.Run(appID1, fmt.Sprintf(":%d", appPort),
			subscriberApplication(appID1, topicActiveName, consumerGroup1))).
		Step(dockercompose.Run(clusterName, dockerComposeYAML)).
		Step("wait", flow.Sleep(10*time.Second)).
		Step("wait for pulsar readiness", retry.Do(10*time.Second, 30, func(ctx flow.Context) error {
			client, err := pulsar.NewClient(pulsar.ClientOptions{URL: "pulsar://localhost:6650"})
			if err != nil {
				return fmt.Errorf("could not create pulsar client: %v", err)
			}

			defer client.Close()

			consumer, err := client.Subscribe(pulsar.ConsumerOptions{
				Topic:            "topic-1",
				SubscriptionName: "my-sub",
				Type:             pulsar.Shared,
			})
			if err != nil {
				return fmt.Errorf("could not create pulsar Topic: %v", err)
			}
			defer consumer.Close()

			return err
		})).
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

		// Run the Dapr sidecar with the component 2.
		Step(sidecar.Run(sidecarName2,
			embedded.WithComponentsPath("./components/consumer_two"),
			embedded.WithAppProtocol(runtime.HTTPProtocol, appPort+portOffset),
			embedded.WithDaprGRPCPort(runtime.DefaultDaprAPIGRPCPort+portOffset),
			embedded.WithDaprHTTPPort(runtime.DefaultDaprHTTPPort+portOffset),
			embedded.WithProfilePort(runtime.DefaultProfilePort+portOffset),
			componentRuntimeOptions(),
		)).
		Step("publish messages to topic1", publishMessages(nil, sidecarName1, topicActiveName, consumerGroup1, consumerGroup2)).
		Step("publish messages to unUsedTopic", publishMessages(nil, sidecarName1, topicPassiveName)).
		Step("verify if app1 has received messages published to active topic", assertMessages(10*time.Second, consumerGroup1)).
		Step("verify if app2 has received messages published to passive topic", assertMessages(10*time.Second, consumerGroup2)).
		Step("reset", flow.Reset(consumerGroup1, consumerGroup2)).
		Run()
}

func TestPulsarMultipleSubsSameConsumerIDs(t *testing.T) {
	consumerGroup1 := watcher.NewUnordered()
	consumerGroup2 := watcher.NewUnordered()

	metadata := map[string]string{
		messageKey: partition0,
	}

	metadata1 := map[string]string{
		messageKey: partition1,
	}

	flow.New(t, "pulsar certification - single publisher and multiple subscribers with same consumer IDs").

		// Run subscriberApplication app1
		Step(app.Run(appID1, fmt.Sprintf(":%d", appPort),
			subscriberApplication(appID1, topicActiveName, consumerGroup1))).
		Step(dockercompose.Run(clusterName, dockerComposeYAML)).
		Step("wait", flow.Sleep(10*time.Second)).
		Step("wait for pulsar readiness", retry.Do(10*time.Second, 30, func(ctx flow.Context) error {
			client, err := pulsar.NewClient(pulsar.ClientOptions{URL: "pulsar://localhost:6650"})
			if err != nil {
				return fmt.Errorf("could not create pulsar client: %v", err)
			}

			defer client.Close()

			consumer, err := client.Subscribe(pulsar.ConsumerOptions{
				Topic:            "topic-1",
				SubscriptionName: "my-sub",
				Type:             pulsar.Shared,
			})
			if err != nil {
				return fmt.Errorf("could not create pulsar Topic: %v", err)
			}
			defer consumer.Close()

			return err
		})).
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

		// Run the Dapr sidecar with the component 2.
		Step(sidecar.Run(sidecarName2,
			embedded.WithComponentsPath("./components/consumer_two"),
			embedded.WithAppProtocol(runtime.HTTPProtocol, appPort+portOffset),
			embedded.WithDaprGRPCPort(runtime.DefaultDaprAPIGRPCPort+portOffset),
			embedded.WithDaprHTTPPort(runtime.DefaultDaprHTTPPort+portOffset),
			embedded.WithProfilePort(runtime.DefaultProfilePort+portOffset),
			componentRuntimeOptions(),
		)).
		Step("publish messages to topic1", publishMessages(metadata, sidecarName1, topicActiveName, consumerGroup2)).
		Step("publish messages to topic1", publishMessages(metadata1, sidecarName2, topicActiveName, consumerGroup2)).
		Step("verify if app1, app2 together have received messages published to topic1", assertMessages(10*time.Second, consumerGroup2)).
		Step("reset", flow.Reset(consumerGroup1, consumerGroup2)).
		Run()
}

func TestPulsarMultipleSubsDifferentConsumerIDs(t *testing.T) {
	consumerGroup1 := watcher.NewUnordered()
	consumerGroup2 := watcher.NewUnordered()

	// Set the partition key on all messages so they are written to the same partition. This allows for checking of ordered messages.
	metadata := map[string]string{
		messageKey: partition0,
	}

	flow.New(t, "pulsar certification - single publisher and multiple subscribers with different consumer IDs").

		// Run subscriberApplication app1
		Step(app.Run(appID1, fmt.Sprintf(":%d", appPort),
			subscriberApplication(appID1, topicActiveName, consumerGroup1))).
		Step(dockercompose.Run(clusterName, dockerComposeYAML)).
		Step("wait", flow.Sleep(10*time.Second)).
		Step("wait for pulsar readiness", retry.Do(10*time.Second, 30, func(ctx flow.Context) error {
			client, err := pulsar.NewClient(pulsar.ClientOptions{URL: "pulsar://localhost:6650"})
			if err != nil {
				return fmt.Errorf("could not create pulsar client: %v", err)
			}

			defer client.Close()

			consumer, err := client.Subscribe(pulsar.ConsumerOptions{
				Topic:            "topic-1",
				SubscriptionName: "my-sub",
				Type:             pulsar.Shared,
			})
			if err != nil {
				return fmt.Errorf("could not create pulsar Topic: %v", err)
			}
			defer consumer.Close()

			// Ensure the brokers are ready by attempting to consume
			// a topic partition.
			return err
		})).
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

		// Run the Dapr sidecar with the component 2.
		Step(sidecar.Run(sidecarName2,
			embedded.WithComponentsPath("./components/consumer_two"),
			embedded.WithAppProtocol(runtime.HTTPProtocol, appPort+portOffset),
			embedded.WithDaprGRPCPort(runtime.DefaultDaprAPIGRPCPort+portOffset),
			embedded.WithDaprHTTPPort(runtime.DefaultDaprHTTPPort+portOffset),
			embedded.WithProfilePort(runtime.DefaultProfilePort+portOffset),
			componentRuntimeOptions(),
		)).
		Step("publish messages to topic1", publishMessages(metadata, sidecarName1, topicActiveName, consumerGroup1)).
		Step("verify if app1, app2 together have received messages published to topic1", assertMessages(10*time.Second, consumerGroup1)).
		Step("reset", flow.Reset(consumerGroup1, consumerGroup2)).
		Run()
}

func TestPulsarMultiplePubSubsDifferentConsumerIDs(t *testing.T) {
	consumerGroup1 := watcher.NewUnordered()
	consumerGroup2 := watcher.NewUnordered()

	// Set the partition key on all messages so they are written to the same partition. This allows for checking of ordered messages.
	metadata := map[string]string{
		messageKey: partition0,
	}

	metadata1 := map[string]string{
		messageKey: partition1,
	}

	flow.New(t, "pulsar certification - multiple publishers and multiple subscribers with different consumer IDs").

		// Run subscriberApplication app1
		Step(app.Run(appID1, fmt.Sprintf(":%d", appPort),
			subscriberApplication(appID1, topicActiveName, consumerGroup1))).
		Step(dockercompose.Run(clusterName, dockerComposeYAML)).
		Step("wait", flow.Sleep(10*time.Second)).
		Step("wait for pulsar readiness", retry.Do(10*time.Second, 30, func(ctx flow.Context) error {
			client, err := pulsar.NewClient(pulsar.ClientOptions{URL: "pulsar://localhost:6650"})
			if err != nil {
				return fmt.Errorf("could not create pulsar client: %v", err)
			}

			defer client.Close()

			consumer, err := client.Subscribe(pulsar.ConsumerOptions{
				Topic:            "topic-1",
				SubscriptionName: "my-sub",
				Type:             pulsar.Shared,
			})
			if err != nil {
				return fmt.Errorf("could not create pulsar Topic: %v", err)
			}
			defer consumer.Close()

			// Ensure the brokers are ready by attempting to consume
			// a topic partition.
			return err
		})).
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

		// Run the Dapr sidecar with the component 2.
		Step(sidecar.Run(sidecarName2,
			embedded.WithComponentsPath("./components/consumer_two"),
			embedded.WithAppProtocol(runtime.HTTPProtocol, appPort+portOffset),
			embedded.WithDaprGRPCPort(runtime.DefaultDaprAPIGRPCPort+portOffset),
			embedded.WithDaprHTTPPort(runtime.DefaultDaprHTTPPort+portOffset),
			embedded.WithProfilePort(runtime.DefaultProfilePort+portOffset),
			componentRuntimeOptions(),
		)).
		Step("publish messages to topic1", publishMessages(metadata, sidecarName1, topicActiveName, consumerGroup1)).
		Step("publish messages to topic1", publishMessages(metadata1, sidecarName2, topicActiveName, consumerGroup2)).
		Step("verify if app1, app2 together have received messages published to topic1", assertMessages(10*time.Second, consumerGroup1)).
		Step("verify if app1, app2 together have received messages published to topic1", assertMessages(10*time.Second, consumerGroup2)).
		Step("reset", flow.Reset(consumerGroup1, consumerGroup2)).
		Run()
}

func TestPulsarNonexistingTopic(t *testing.T) {
	consumerGroup1 := watcher.NewUnordered()

	// Set the partition key on all messages so they are written to the same partition. This allows for checking of ordered messages.
	metadata := map[string]string{
		messageKey: partition0,
	}

	flow.New(t, "pulsar certification - non-existing topic").

		// Run subscriberApplication app1
		Step(app.Run(appID1, fmt.Sprintf(":%d", appPort+portOffset*3),
			subscriberApplication(appID1, topicToBeCreated, consumerGroup1))).
		Step(dockercompose.Run(clusterName, dockerComposeYAML)).
		Step("wait", flow.Sleep(10*time.Second)).
		Step("wait for pulsar readiness", retry.Do(10*time.Second, 30, func(ctx flow.Context) error {
			client, err := pulsar.NewClient(pulsar.ClientOptions{URL: "pulsar://localhost:6650"})
			if err != nil {
				return fmt.Errorf("could not create pulsar client: %v", err)
			}

			defer client.Close()

			consumer, err := client.Subscribe(pulsar.ConsumerOptions{
				Topic:            "topic-1",
				SubscriptionName: "my-sub",
				Type:             pulsar.Shared,
			})
			if err != nil {
				return fmt.Errorf("could not create pulsar Topic: %v", err)
			}
			defer consumer.Close()

			// Ensure the brokers are ready by attempting to consume
			// a topic partition.
			return err
		})).
		// Run the Dapr sidecar with the component entitymanagement
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
		Step("verify if app1 has received messages published to newly created topic", assertMessages(10*time.Second, consumerGroup1)).
		Run()
}

func TestPulsarNetworkInterruption(t *testing.T) {
	consumerGroup1 := watcher.NewUnordered()

	// Set the partition key on all messages so they are written to the same partition. This allows for checking of ordered messages.
	metadata := map[string]string{
		messageKey: partition0,
	}

	flow.New(t, "pulsar certification - network interruption").

		// Run subscriberApplication app1
		Step(app.Run(appID1, fmt.Sprintf(":%d", appPort+portOffset),
			subscriberApplication(appID1, topicActiveName, consumerGroup1))).
		Step(dockercompose.Run(clusterName, dockerComposeYAML)).
		Step("wait", flow.Sleep(10*time.Second)).
		Step("wait for pulsar readiness", retry.Do(10*time.Second, 30, func(ctx flow.Context) error {
			client, err := pulsar.NewClient(pulsar.ClientOptions{URL: "pulsar://localhost:6650"})
			if err != nil {
				return fmt.Errorf("could not create pulsar client: %v", err)
			}

			defer client.Close()

			consumer, err := client.Subscribe(pulsar.ConsumerOptions{
				Topic:            "topic-1",
				SubscriptionName: "my-sub",
				Type:             pulsar.Shared,
			})
			if err != nil {
				return fmt.Errorf("could not create pulsar Topic: %v", err)
			}
			defer consumer.Close()

			// Ensure the brokers are ready by attempting to consume
			// a topic partition.
			return err
		})).
		// Run the Dapr sidecar with the component entitymanagement
		Step(sidecar.Run(sidecarName1,
			embedded.WithComponentsPath("./components/consumer_one"),
			embedded.WithAppProtocol(runtime.HTTPProtocol, appPort+portOffset),
			embedded.WithDaprGRPCPort(runtime.DefaultDaprAPIGRPCPort+portOffset),
			embedded.WithDaprHTTPPort(runtime.DefaultDaprHTTPPort+portOffset),
			embedded.WithProfilePort(runtime.DefaultProfilePort+portOffset),
			componentRuntimeOptions(),
		)).
		Step(fmt.Sprintf("publish messages to topicToBeCreated: %s", topicActiveName), publishMessages(metadata, sidecarName1, topicActiveName, consumerGroup1)).
		Step("interrupt network", network.InterruptNetwork(30*time.Second, nil, nil, "6650")).
		Step("wait", flow.Sleep(30*time.Second)).
		Step("verify if app1 has received messages published to newly created topic", assertMessages(10*time.Second, consumerGroup1)).
		Run()
}

func TestPulsarPersitant(t *testing.T) {
	consumerGroup1 := watcher.NewUnordered()

	flow.New(t, "pulsar certification persistant test").

		// Run subscriberApplication app1
		Step(app.Run(appID1, fmt.Sprintf(":%d", appPort),
			subscriberApplication(appID1, topicActiveName, consumerGroup1))).
		Step(dockercompose.Run(clusterName, dockerComposeYAML)).
		Step("wait", flow.Sleep(10*time.Second)).
		Step("wait for pulsar readiness", retry.Do(10*time.Second, 30, func(ctx flow.Context) error {
			client, err := pulsar.NewClient(pulsar.ClientOptions{URL: "pulsar://localhost:6650"})
			if err != nil {
				return fmt.Errorf("could not create pulsar client: %v", err)
			}

			defer client.Close()

			consumer, err := client.Subscribe(pulsar.ConsumerOptions{
				Topic:            "topic-1",
				SubscriptionName: "my-sub",
				Type:             pulsar.Shared,
			})
			if err != nil {
				return fmt.Errorf("could not create pulsar Topic: %v", err)
			}
			defer consumer.Close()

			// Ensure the brokers are ready by attempting to consume
			// a topic partition.
			return err
		})).
		Step(sidecar.Run(sidecarName1,
			embedded.WithComponentsPath("./components/consumer_one"),
			embedded.WithAppProtocol(runtime.HTTPProtocol, appPort),
			embedded.WithDaprGRPCPort(runtime.DefaultDaprAPIGRPCPort),
			embedded.WithDaprHTTPPort(runtime.DefaultDaprHTTPPort),
			componentRuntimeOptions(),
		)).
		Step("publish messages to topic1", publishMessages(nil, sidecarName1, topicActiveName, consumerGroup1)).
		Step("stop pulsar server", dockercompose.Stop(clusterName, dockerComposeYAML, "standalone")).
		Step("wait", flow.Sleep(5*time.Second)).
		Step("start pulsar server", dockercompose.Start(clusterName, dockerComposeYAML, "standalone")).
		Step("wait", flow.Sleep(10*time.Second)).
		Step("verify if app1 has received messages published to active topic", assertMessages(10*time.Second, consumerGroup1)).
		Step("reset", flow.Reset(consumerGroup1)).
		Run()
}

func TestPulsarDelay(t *testing.T) {
	consumerGroup1 := watcher.NewUnordered()

	date := time.Now()
	deliverTime := date.Add(time.Second * 60)

	metadataAfter := map[string]string{
		"deliverAfter": "30s",
	}

	metadataAt := map[string]string{
		"deliverAt": deliverTime.Format(time.RFC3339Nano),
	}

	assertMessagesNot := func(timeout time.Duration, messageWatchers ...*watcher.Watcher) flow.Runnable {
		return func(ctx flow.Context) error {
			// assert for messages
			for _, m := range messageWatchers {
				m.AssertNotDelivered(ctx, 5*timeout)
			}

			return nil
		}
	}

	flow.New(t, "pulsar certification delay test").

		// Run subscriberApplication app1
		Step(app.Run(appID1, fmt.Sprintf(":%d", appPort),
			subscriberApplication(appID1, topicActiveName, consumerGroup1))).
		Step(dockercompose.Run(clusterName, dockerComposeYAML)).
		Step("wait", flow.Sleep(10*time.Second)).
		Step("wait for pulsar readiness", retry.Do(10*time.Second, 30, func(ctx flow.Context) error {
			client, err := pulsar.NewClient(pulsar.ClientOptions{URL: "pulsar://localhost:6650"})
			if err != nil {
				return fmt.Errorf("could not create pulsar client: %v", err)
			}

			defer client.Close()

			consumer, err := client.Subscribe(pulsar.ConsumerOptions{
				Topic:            "topic-1",
				SubscriptionName: "my-sub",
				Type:             pulsar.Shared,
			})
			if err != nil {
				return fmt.Errorf("could not create pulsar Topic: %v", err)
			}
			defer consumer.Close()

			// Ensure the brokers are ready by attempting to consume
			// a topic partition.
			return err
		})).
		Step(sidecar.Run(sidecarName1,
			embedded.WithComponentsPath("./components/consumer_three"),
			embedded.WithAppProtocol(runtime.HTTPProtocol, appPort),
			embedded.WithDaprGRPCPort(runtime.DefaultDaprAPIGRPCPort),
			embedded.WithDaprHTTPPort(runtime.DefaultDaprHTTPPort),
			componentRuntimeOptions(),
		)).
		Step("publish messages to topic1", publishMessages(metadataAfter, sidecarName1, topicActiveName, consumerGroup1)).
		// receive no messages due to deliverAfter delay
		Step("verify if app1 has received no messages published to topic", assertMessagesNot(1*time.Second, consumerGroup1)).
		// delay has passed, messages should be received
		Step("verify if app1 has received messages published to topic", assertMessages(10*time.Second, consumerGroup1)).
		Step("reset", flow.Reset(consumerGroup1)).
		// publish messages using deliverAt property
		Step("publish messages to topic1", publishMessages(metadataAt, sidecarName1, topicActiveName, consumerGroup1)).
		Step("verify if app1 has received messages published to topic", assertMessages(10*time.Second, consumerGroup1)).
		Run()
}

type schemaTest struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
}

func TestPulsarSchema(t *testing.T) {
	consumerGroup1 := watcher.NewUnordered()

	publishMessages := func(sidecarName string, topicName string, messageWatchers ...*watcher.Watcher) flow.Runnable {
		return func(ctx flow.Context) error {
			// prepare the messages
			messages := make([]string, numMessages)
			for i := range messages {
				test := &schemaTest{
					ID:   i,
					Name: uuid.New().String(),
				}

				b, _ := json.Marshal(test)
				messages[i] = string(b)
			}

			for _, messageWatcher := range messageWatchers {
				messageWatcher.ExpectStrings(messages...)
			}

			// get the sidecar (dapr) client
			client := sidecar.GetClient(ctx, sidecarName)

			// publish messages
			ctx.Logf("Publishing messages. sidecarName: %s, topicName: %s", sidecarName, topicName)

			for _, message := range messages {
				ctx.Logf("Publishing: %q", message)

				err := client.PublishEvent(ctx, pubsubName, topicName, message)
				require.NoError(ctx, err, "error publishing message")
			}
			return nil
		}
	}

	flow.New(t, "pulsar certification schema test").

		// Run subscriberApplication app1
		Step(app.Run(appID1, fmt.Sprintf(":%d", appPort),
			subscriberSchemaApplication(appID1, topicActiveName, consumerGroup1))).
		Step(dockercompose.Run(clusterName, dockerComposeYAML)).
		Step("wait", flow.Sleep(10*time.Second)).
		Step("wait for pulsar readiness", retry.Do(10*time.Second, 30, func(ctx flow.Context) error {
			client, err := pulsar.NewClient(pulsar.ClientOptions{URL: "pulsar://localhost:6650"})
			if err != nil {
				return fmt.Errorf("could not create pulsar client: %v", err)
			}

			defer client.Close()

			consumer, err := client.Subscribe(pulsar.ConsumerOptions{
				Topic:            "topic-1",
				SubscriptionName: "my-sub",
				Type:             pulsar.Shared,
			})
			if err != nil {
				return fmt.Errorf("could not create pulsar Topic: %v", err)
			}
			defer consumer.Close()

			return err
		})).
		Step(sidecar.Run(sidecarName1,
			embedded.WithComponentsPath("./components/consumer_four"),
			embedded.WithAppProtocol(runtime.HTTPProtocol, appPort),
			embedded.WithDaprGRPCPort(runtime.DefaultDaprAPIGRPCPort),
			embedded.WithDaprHTTPPort(runtime.DefaultDaprHTTPPort),
			componentRuntimeOptions(),
		)).
		Step("publish messages to topic1", publishMessages(sidecarName1, topicActiveName, consumerGroup1)).
		Step("verify if app1 has received messages published to topic", assertMessages(10*time.Second, consumerGroup1)).
		Run()
}

func componentRuntimeOptions() []runtime.Option {
	log := logger.NewLogger("dapr.components")

	pubsubRegistry := pubsub_loader.NewRegistry()
	pubsubRegistry.Logger = log
	pubsubRegistry.RegisterComponent(pubsub_pulsar.NewPulsar, "pulsar")

	return []runtime.Option{
		runtime.WithPubSubs(pubsubRegistry),
	}
}

func createMultiPartitionTopic(tenant, namespace, topic string, partition int) flow.Runnable {
	return func(ctx flow.Context) error {
		reqURL := fmt.Sprintf("http://localhost:8080/admin/v2/persistent/%s/%s/%s/partitions",
			tenant, namespace, topic)

		reqBody, err := json.Marshal(partition)

		if err != nil {
			return fmt.Errorf("createMultiPartitionTopic json.Marshal(%d) err: %s", partition, err.Error())
		}

		req, err := http.NewRequest(http.MethodPut, reqURL, bytes.NewBuffer(reqBody))

		if err != nil {
			return fmt.Errorf("createMultiPartitionTopic NewRequest(url: %s, body: %s) err:%s",
				reqURL, reqBody, err.Error())
		}

		req.Header.Set("Content-Type", "application/json")

		rsp, err := http.DefaultClient.Do(req)

		if err != nil {
			return fmt.Errorf("createMultiPartitionTopic(url: %s, body: %s) err:%s",
				reqURL, reqBody, err.Error())
		}

		defer rsp.Body.Close()

		if rsp.StatusCode >= http.StatusOK && rsp.StatusCode <= http.StatusMultipleChoices {
			return nil
		}

		rspBody, _ := ioutil.ReadAll(rsp.Body)

		return fmt.Errorf("createMultiPartitionTopic(url: %s, body: %s) statusCode: %d, resBody: %s",
			reqURL, reqBody, rsp.StatusCode, string(rspBody))
	}
}

func TestPulsarPartitionedOrderingProcess(t *testing.T) {
	consumerGroup1 := watcher.NewOrdered()

	// Set the partition key on all messages so they are written to the same partition. This allows for checking of ordered messages.
	metadata := map[string]string{
		messageKey: partition0,
	}

	flow.New(t, "pulsar certification -  process message in order with partitioned-topic").
		Step(dockercompose.Run(clusterName, dockerComposeYAML)).

		// Run subscriberApplication app1
		Step(app.Run(appID1, fmt.Sprintf(":%d", appPort+portOffset),
			subscriberApplicationWithoutError(appID1, topicMultiPartitionName, consumerGroup1))).
		Step("wait", flow.Sleep(10*time.Second)).
		Step("wait for pulsar readiness", retry.Do(10*time.Second, 30, func(ctx flow.Context) error {
			client, err := pulsar.NewClient(pulsar.ClientOptions{URL: "pulsar://localhost:6650"})
			if err != nil {
				return fmt.Errorf("could not create pulsar client: %v", err)
			}

			defer client.Close()

			consumer, err := client.Subscribe(pulsar.ConsumerOptions{
				Topic:            "topic-1",
				SubscriptionName: "my-sub",
				Type:             pulsar.Shared,
			})
			if err != nil {
				return fmt.Errorf("could not create pulsar Topic: %v", err)
			}

			defer consumer.Close()

			// Ensure the brokers are ready by attempting to consume
			// a topic partition.
			return err
		})).
		Step("create multi-partition topic explicitly", retry.Do(10*time.Second, 30,
			createMultiPartitionTopic("public", "default", topicMultiPartitionName, 4))).
		// Run the Dapr sidecar with the component entitymanagement
		Step(sidecar.Run(sidecarName1,
			embedded.WithComponentsPath("./components/consumer_one"),
			embedded.WithAppProtocol(runtime.HTTPProtocol, appPort+portOffset),
			embedded.WithDaprGRPCPort(runtime.DefaultDaprAPIGRPCPort+portOffset),
			embedded.WithDaprHTTPPort(runtime.DefaultDaprHTTPPort+portOffset),
			embedded.WithProfilePort(runtime.DefaultProfilePort+portOffset),
			componentRuntimeOptions(),
		)).
		// Run subscriberApplication app2
		Step(app.Run(appID2, fmt.Sprintf(":%d", appPort+portOffset*3),
			subscriberApplicationWithoutError(appID2, topicActiveName, consumerGroup1))).

		// Run the Dapr sidecar with the component 2.
		Step(sidecar.Run(sidecarName2,
			embedded.WithComponentsPath("./components/consumer_two"),
			embedded.WithAppProtocol(runtime.HTTPProtocol, appPort+portOffset*3),
			embedded.WithDaprGRPCPort(runtime.DefaultDaprAPIGRPCPort+portOffset*3),
			embedded.WithDaprHTTPPort(runtime.DefaultDaprHTTPPort+portOffset*3),
			embedded.WithProfilePort(runtime.DefaultProfilePort+portOffset*3),
			componentRuntimeOptions(),
		)).
		Step(fmt.Sprintf("publish messages to topicToBeCreated: %s", topicMultiPartitionName), publishMessages(metadata, sidecarName1, topicMultiPartitionName, consumerGroup1)).
		Step("wait", flow.Sleep(30*time.Second)).
		Step("verify if app1 has received messages published to newly created topic", assertMessages(10*time.Second, consumerGroup1)).
		Step("reset", flow.Reset(consumerGroup1)).
		Run()
}

func TestPulsarEncryptionFromFile(t *testing.T) {
	consumerGroup1 := watcher.NewUnordered()

	publishMessages := func(sidecarName string, topicName string, messageWatchers ...*watcher.Watcher) flow.Runnable {
		return func(ctx flow.Context) error {
			// prepare the messages
			messages := make([]string, numMessages)
			for i := range messages {
				test := &schemaTest{
					ID:   i,
					Name: uuid.New().String(),
				}

				b, _ := json.Marshal(test)
				messages[i] = string(b)
			}

			for _, messageWatcher := range messageWatchers {
				messageWatcher.ExpectStrings(messages...)
			}

			// get the sidecar (dapr) client
			client := sidecar.GetClient(ctx, sidecarName)

			// publish messages
			ctx.Logf("Publishing messages. sidecarName: %s, topicName: %s", sidecarName, topicName)

			for _, message := range messages {
				ctx.Logf("Publishing: %q", message)

				err := client.PublishEvent(ctx, pubsubName, topicName, message)
				require.NoError(ctx, err, "error publishing message")
			}
			return nil
		}
	}

	flow.New(t, "pulsar encryption test with file path").

		// Run subscriberApplication app1
		Step(app.Run(appID1, fmt.Sprintf(":%d", appPort),
			subscriberSchemaApplication(appID1, topicActiveName, consumerGroup1))).
		Step(dockercompose.Run(clusterName, dockerComposeYAML)).
		Step("wait", flow.Sleep(10*time.Second)).
		Step("wait for pulsar readiness", retry.Do(10*time.Second, 30, func(ctx flow.Context) error {
			client, err := pulsar.NewClient(pulsar.ClientOptions{URL: "pulsar://localhost:6650"})
			if err != nil {
				return fmt.Errorf("could not create pulsar client: %v", err)
			}

			defer client.Close()

			consumer, err := client.Subscribe(pulsar.ConsumerOptions{
				Topic:            "topic-1",
				SubscriptionName: "my-sub",
				Type:             pulsar.Shared,
			})
			if err != nil {
				return fmt.Errorf("could not create pulsar Topic: %v", err)
			}
			defer consumer.Close()

			return err
		})).
		Step(sidecar.Run(sidecarName1,
			embedded.WithComponentsPath("./components/consumer_five"),
			embedded.WithAppProtocol(runtime.HTTPProtocol, appPort),
			embedded.WithDaprGRPCPort(runtime.DefaultDaprAPIGRPCPort),
			embedded.WithDaprHTTPPort(runtime.DefaultDaprHTTPPort),
			componentRuntimeOptions(),
		)).
		Step("publish messages to topic1", publishMessages(sidecarName1, topicActiveName, consumerGroup1)).
		Step("verify if app1 has received messages published to topic", assertMessages(10*time.Second, consumerGroup1)).
		Step("reset", flow.Reset(consumerGroup1)).
		Run()
}

func TestPulsarEncryptionFromData(t *testing.T) {
	consumerGroup1 := watcher.NewUnordered()

	publishMessages := func(sidecarName string, topicName string, messageWatchers ...*watcher.Watcher) flow.Runnable {
		return func(ctx flow.Context) error {
			// prepare the messages
			messages := make([]string, numMessages)
			for i := range messages {
				test := &schemaTest{
					ID:   i,
					Name: uuid.New().String(),
				}

				b, _ := json.Marshal(test)
				messages[i] = string(b)
			}

			for _, messageWatcher := range messageWatchers {
				messageWatcher.ExpectStrings(messages...)
			}

			// get the sidecar (dapr) client
			client := sidecar.GetClient(ctx, sidecarName)

			// publish messages
			ctx.Logf("Publishing messages. sidecarName: %s, topicName: %s", sidecarName, topicName)

			for _, message := range messages {
				ctx.Logf("Publishing: %q", message)

				err := client.PublishEvent(ctx, pubsubName, topicName, message)
				require.NoError(ctx, err, "error publishing message")
			}
			return nil
		}
	}

	flow.New(t, "pulsar encryption test with data").

		// Run subscriberApplication app2
		Step(app.Run(appID1, fmt.Sprintf(":%d", appPort),
			subscriberSchemaApplication(appID1, topicActiveName, consumerGroup1))).
		Step(dockercompose.Run(clusterName, dockerComposeYAML)).
		Step("wait", flow.Sleep(10*time.Second)).
		Step("wait for pulsar readiness", retry.Do(10*time.Second, 30, func(ctx flow.Context) error {
			client, err := pulsar.NewClient(pulsar.ClientOptions{URL: "pulsar://localhost:6650"})
			if err != nil {
				return fmt.Errorf("could not create pulsar client: %v", err)
			}

			defer client.Close()

			consumer, err := client.Subscribe(pulsar.ConsumerOptions{
				Topic:            "topic-1",
				SubscriptionName: "my-sub",
				Type:             pulsar.Shared,
			})
			if err != nil {
				return fmt.Errorf("could not create pulsar Topic: %v", err)
			}
			defer consumer.Close()

			return err
		})).
		Step(sidecar.Run(sidecarName1,
			embedded.WithComponentsPath("./components/consumer_six"),
			embedded.WithAppProtocol(runtime.HTTPProtocol, appPort),
			embedded.WithDaprGRPCPort(runtime.DefaultDaprAPIGRPCPort),
			embedded.WithDaprHTTPPort(runtime.DefaultDaprHTTPPort),
			componentRuntimeOptions(),
		)).
		Step("publish messages to topic1", publishMessages(sidecarName1, topicActiveName, consumerGroup1)).
		Step("verify if app1 has received messages published to topic", assertMessages(10*time.Second, consumerGroup1)).
		Step("reset", flow.Reset(consumerGroup1)).
		Run()
}
