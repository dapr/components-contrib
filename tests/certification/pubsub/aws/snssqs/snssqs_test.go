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
)

func TestSNSSQS(t *testing.T) {
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
				require.NoError(ctx, err, "error publishing message")
			}
			return nil
		}
	}

	assertMessages := func(timeout time.Duration, messageWatchers ...*watcher.Watcher) flow.Runnable {
		return func(ctx flow.Context) error {
			// assert for messages
			for _, m := range messageWatchers {
				if !m.Assert(ctx, 25*timeout) {
					ctx.Errorf("message assersion failed: %#v\n", m)
				}
			}

			return nil
		}
	}

	flow.New(t, "SNSSQS certification basic test").

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

func componentRuntimeOptions() []runtime.Option {
	log := logger.NewLogger("dapr.components")

	pubsubRegistry := pubsub_loader.NewRegistry()
	pubsubRegistry.Logger = log
	pubsubRegistry.RegisterComponent(pubsub_snssqs.NewSnsSqs, "snssqs")

	return []runtime.Option{
		runtime.WithPubSubs(pubsubRegistry),
	}
}
