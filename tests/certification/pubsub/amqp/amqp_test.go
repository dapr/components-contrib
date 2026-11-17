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

package amqp_test

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/multierr"

	// Pub/Sub.
	pubsub_amqp "github.com/dapr/components-contrib/pubsub/amqp"
	pubsub_loader "github.com/dapr/dapr/pkg/components/pubsub"
	"github.com/dapr/dapr/pkg/config/protocol"

	// Dapr runtime and Go-SDK.
	"github.com/dapr/dapr/pkg/runtime"
	"github.com/dapr/go-sdk/service/common"
	"github.com/dapr/kit/logger"

	// Certification testing runnables.
	"github.com/dapr/components-contrib/tests/certification/embedded"
	"github.com/dapr/components-contrib/tests/certification/flow"
	"github.com/dapr/components-contrib/tests/certification/flow/app"
	"github.com/dapr/components-contrib/tests/certification/flow/dockercompose"
	"github.com/dapr/components-contrib/tests/certification/flow/network"
	"github.com/dapr/components-contrib/tests/certification/flow/sidecar"
	"github.com/dapr/components-contrib/tests/certification/flow/simulate"
	"github.com/dapr/components-contrib/tests/certification/flow/watcher"
)

const (
	sidecarName1 = "dapr-1"
	sidecarName2 = "dapr-2"
	sidecarName3 = "dapr-3"
	appID1       = "app-1"
	appID2       = "app-2"
	appID3       = "app-3"

	clusterName       = "amqpcertification"
	brokerService     = "artemis"
	dockerComposeYAML = "docker-compose.yml"

	numMessages = 100
	appPort     = 8000
	portOffset  = 2

	pubsubName = "messagebus"
	topicName  = "neworder"
	// The prefixed component namespaces its addresses, so it needs its own
	// topic to make the isolation obvious in the logs.
	prefixedTopicName = "prefixedorder"
)

var brokers = []string{"localhost:5673"}

func TestAMQP(t *testing.T) {
	logger.ApplyOptionsToLoggers(&logger.Options{OutputLevel: "debug"})

	// AMQP gives no cross-link ordering guarantee.
	consumerGroup1 := watcher.NewUnordered()
	consumerGroup2 := watcher.NewUnordered()
	consumerGroup3 := watcher.NewUnordered()

	// application subscribes to a topic and records what it receives.
	application := func(messages *watcher.Watcher, appID, topic string, withErrors bool) app.SetupFn {
		return func(ctx flow.Context, s common.Service) error {
			// Reject one message in every 100, to exercise the NAK path and
			// prove that a redelivered message still arrives.
			sim := func() error { return nil }
			if withErrors {
				sim = simulate.PeriodicError(ctx, 100)
			}

			return multierr.Combine(
				s.AddTopicEventHandler(&common.Subscription{
					PubsubName: pubsubName,
					Topic:      topic,
					Route:      "/orders",
				}, func(_ context.Context, e *common.TopicEvent) (retry bool, err error) {
					if err := sim(); err != nil {
						return true, err
					}

					messages.Observe(e.Data)
					ctx.Logf("%s Event - pubsub: %s, topic: %s, id: %s, data: %s",
						appID, e.PubsubName, e.Topic, e.ID, e.Data)

					return false, nil
				}),
			)
		}
	}

	// publishAndVerify sends a batch of messages through sidecarName and waits
	// for every watcher to observe all of them.
	publishAndVerify := func(sidecarName, topic, prefix string, messages ...*watcher.Watcher) flow.Runnable {
		return func(ctx flow.Context) error {
			client := sidecar.GetClient(ctx, sidecarName)

			msgs := make([]string, numMessages)
			for i := range msgs {
				msgs[i] = fmt.Sprintf("%s#%03d", prefix, i)
			}
			for _, m := range messages {
				m.ExpectStrings(msgs...)
			}

			ctx.Logf("Publishing %d messages to %q", len(msgs), topic)
			for _, msg := range msgs {
				require.NoError(ctx,
					client.PublishEvent(ctx, pubsubName, topic, msg),
					"error publishing message")
			}

			for _, m := range messages {
				m.Assert(ctx, 2*time.Minute)
			}

			return nil
		}
	}

	flow.New(t, "amqp certification").
		Step(dockercompose.Run(clusterName, dockerComposeYAML)).
		Step("wait for the broker socket",
			network.WaitForAddresses(5*time.Minute, brokers...)).
		// Artemis accepts TCP before it accepts AMQP, so give it a moment.
		Step("wait for broker readiness", flow.Sleep(20*time.Second)).
		//
		// Two apps on the default addressing, which is what an ActiveMQ
		// Artemis user gets out of the box.
		Step(app.Run(appID1, fmt.Sprintf(":%d", appPort),
			application(consumerGroup1, appID1, topicName, true))).
		Step(sidecar.Run(sidecarName1,
			append(componentRuntimeOptions(),
				embedded.WithResourcesPath("./components/consumer1"),
				embedded.WithAppProtocol(protocol.HTTPProtocol, strconv.Itoa(appPort)),
				embedded.WithDaprGRPCPort(strconv.Itoa(runtime.DefaultDaprAPIGRPCPort)),
				embedded.WithDaprHTTPPort(strconv.Itoa(runtime.DefaultDaprHTTPPort)),
				embedded.WithProfilePort(strconv.Itoa(runtime.DefaultProfilePort)),
				embedded.WithGracefulShutdownDuration(0),
			)...,
		)).
		Step(app.Run(appID2, fmt.Sprintf(":%d", appPort+portOffset),
			application(consumerGroup2, appID2, topicName, false))).
		Step(sidecar.Run(sidecarName2,
			append(componentRuntimeOptions(),
				embedded.WithResourcesPath("./components/consumer2"),
				embedded.WithAppProtocol(protocol.HTTPProtocol, strconv.Itoa(appPort+portOffset)),
				embedded.WithDaprGRPCPort(strconv.Itoa(runtime.DefaultDaprAPIGRPCPort+portOffset)),
				embedded.WithDaprHTTPPort(strconv.Itoa(runtime.DefaultDaprHTTPPort+portOffset)),
				embedded.WithProfilePort(strconv.Itoa(runtime.DefaultProfilePort+portOffset)),
				embedded.WithGracefulShutdownDuration(0),
			)...,
		)).
		Step("wait for the subscriptions to attach", flow.Sleep(10*time.Second)).
		//
		// Both subscribers must receive every message.
		Step("publish and verify",
			publishAndVerify(sidecarName1, topicName, "hello", consumerGroup1, consumerGroup2)).
		Step("reset", flow.Reset(consumerGroup1, consumerGroup2)).
		//
		// Restart the broker underneath a live subscription.
		//
		// This is the regression guard for reconnect. Before reconnect existed,
		// a detached link left the receive loop returning the same error
		// forever, so the subscription never recovered and the sidecar had to
		// be restarted to resume delivery.
		Step("stop the broker",
			dockercompose.Stop(clusterName, dockerComposeYAML, brokerService)).
		Step("wait while the broker is down", flow.Sleep(15*time.Second)).
		Step("start the broker",
			dockercompose.Start(clusterName, dockerComposeYAML, brokerService)).
		Step("wait for the broker socket",
			network.WaitForAddresses(5*time.Minute, brokers...)).
		Step("wait for the component to reconnect", flow.Sleep(30*time.Second)).
		Step("publish and verify after the broker restart",
			publishAndVerify(sidecarName1, topicName, "afterrestart", consumerGroup1, consumerGroup2)).
		Step("reset", flow.Reset(consumerGroup1, consumerGroup2)).
		//
		// Drop the broker's socket without a clean shutdown.
		Step("interrupt the network",
			network.InterruptNetwork(15*time.Second, nil, nil, "5673")).
		Step("wait for the component to recover", flow.Sleep(30*time.Second)).
		Step("publish and verify after the network interruption",
			publishAndVerify(sidecarName1, topicName, "afterinterrupt", consumerGroup1, consumerGroup2)).
		//
		// A component configured with address prefixes, which is what a broker
		// with namespaced destinations needs. Publisher and subscriber both
		// apply the prefix, so they meet on the namespaced address.
		Step(app.Run(appID3, fmt.Sprintf(":%d", appPort+(portOffset*2)),
			application(consumerGroup3, appID3, prefixedTopicName, false))).
		Step(sidecar.Run(sidecarName3,
			append(componentRuntimeOptions(),
				embedded.WithResourcesPath("./components/prefixed"),
				embedded.WithAppProtocol(protocol.HTTPProtocol, strconv.Itoa(appPort+(portOffset*2))),
				embedded.WithDaprGRPCPort(strconv.Itoa(runtime.DefaultDaprAPIGRPCPort+(portOffset*2))),
				embedded.WithDaprHTTPPort(strconv.Itoa(runtime.DefaultDaprHTTPPort+(portOffset*2))),
				embedded.WithProfilePort(strconv.Itoa(runtime.DefaultProfilePort+(portOffset*2))),
				embedded.WithGracefulShutdownDuration(0),
			)...,
		)).
		Step("wait for the prefixed subscription to attach", flow.Sleep(10*time.Second)).
		Step("publish and verify with address prefixes",
			publishAndVerify(sidecarName3, prefixedTopicName, "prefixed", consumerGroup3)).
		Run()
}

func componentRuntimeOptions() []embedded.Option {
	log := logger.NewLogger("dapr.components")

	pubsubRegistry := pubsub_loader.NewRegistry()
	pubsubRegistry.Logger = log
	pubsubRegistry.RegisterComponent(pubsub_amqp.NewAMQPPubsub, "amqp")
	// The deprecated type must keep resolving for as long as it is registered
	// in dapr/dapr.
	pubsubRegistry.RegisterComponent(pubsub_amqp.NewSolaceAMQPPubsub, "solace.amqp")

	return []embedded.Option{
		embedded.WithPubSubs(pubsubRegistry),
	}
}
