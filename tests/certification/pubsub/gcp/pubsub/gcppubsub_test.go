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

	numMessages = 10
	appPort     = 8000
	portOffset  = 2
	pubsubName  = "gcp-pubsub-cert-tests"
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
	topicActiveName  = "cert-test-active"
	topicPassiveName = "cert-test-passive"
	projectID        = "GCP_PROJECT_ID_NOT_SET"
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
}

func getEnv(env string, fn func(ev string), dfns ...func() string) {
	if value := os.Getenv(env); value != "" {
		fn(value)
	} else if len(dfns) == 0 {
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
			embedded.WithComponentsPath("./components/consumer_one"),
			embedded.WithAppProtocol(runtime.HTTPProtocol, appPort),
			embedded.WithDaprGRPCPort(runtime.DefaultDaprAPIGRPCPort),
			embedded.WithDaprHTTPPort(runtime.DefaultDaprHTTPPort),
			componentRuntimeOptions(),
		)).

		// Run subscriberApplication app2
		Step(app.Run(appID2, fmt.Sprintf(":%d", appPort+portOffset),
			subscriberApplication(appID2, topicActiveName, consumerGroup2))).

		// Run the Dapr sidecar with ConsumerID "PUBSUB_GCP_CONSUMER_ID_2"
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
	pubsubRegistry.RegisterComponent(pubsub_gcppubsub.NewGCPPubSub, "gcp.pubsub")

	secretstoreRegistry := secretstores_loader.NewRegistry()
	secretstoreRegistry.Logger = log
	secretstoreRegistry.RegisterComponent(secretstore_env.NewEnvSecretStore, "local.env")

	return []runtime.Option{
		runtime.WithPubSubs(pubsubRegistry),
		runtime.WithSecretStores(secretstoreRegistry),
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
