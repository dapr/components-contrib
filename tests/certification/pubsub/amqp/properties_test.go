/*
Copyright 2026 The Dapr Authors
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
	"strconv"
	"testing"
	"time"

	amqp "github.com/Azure/go-amqp"
	"github.com/stretchr/testify/require"

	"github.com/dapr/dapr/pkg/runtime"
	dapr "github.com/dapr/go-sdk/client"
	"github.com/dapr/kit/logger"

	"github.com/dapr/components-contrib/tests/certification/embedded"
	"github.com/dapr/components-contrib/tests/certification/flow"
	"github.com/dapr/components-contrib/tests/certification/flow/dockercompose"
	"github.com/dapr/components-contrib/tests/certification/flow/network"
	"github.com/dapr/components-contrib/tests/certification/flow/sidecar"
)

const (
	propertiesSidecar = "dapr-properties"
	propertiesTopic   = "propertiesorder"

	// Offset from the ports the other tests in this package use.
	propertiesPortOffset = 20
)

// TestAMQPApplicationProperties publishes through a sidecar whose component
// sets applicationPropertiesFromMetadata, and reads the message back with a
// plain AMQP client to check what reached the broker.
func TestAMQPApplicationProperties(t *testing.T) {
	logger.ApplyOptionsToLoggers(&logger.Options{OutputLevel: "debug"})

	var receiver *amqp.Receiver

	flow.New(t, "amqp application properties").
		Step(dockercompose.Run(clusterName, dockerComposeYAML)).
		Step("wait for the broker socket",
			network.WaitForAddresses(5*time.Minute, brokers...)).
		Step("wait for broker readiness", flow.Sleep(20*time.Second)).
		Step(sidecar.Run(propertiesSidecar,
			append(componentRuntimeOptions(),
				embedded.WithoutApp(),
				embedded.WithResourcesPath("./components/properties"),
				embedded.WithDaprGRPCPort(strconv.Itoa(runtime.DefaultDaprAPIGRPCPort+propertiesPortOffset)),
				embedded.WithDaprHTTPPort(strconv.Itoa(runtime.DefaultDaprHTTPPort+propertiesPortOffset)),
				embedded.WithProfilePort(strconv.Itoa(runtime.DefaultProfilePort+propertiesPortOffset)),
				embedded.WithGracefulShutdownDuration(0),
			)...,
		)).
		// Attach before publishing, so that the subscription exists when the
		// message is routed.
		Step("attach a plain AMQP receiver", func(ctx flow.Context) error {
			conn, err := amqp.Dial(ctx, "amqp://localhost:5673", &amqp.ConnOptions{SASLType: amqp.SASLTypePlain("artemis", "artemis")})
			require.NoError(ctx, err)
			ctx.Cleanup(func() { _ = conn.Close() })

			session, err := conn.NewSession(ctx, nil)
			require.NoError(ctx, err)

			receiver, err = session.NewReceiver(ctx, propertiesTopic, nil)
			require.NoError(ctx, err)

			return nil
		}).
		Step("publish with metadata and read the message back", func(ctx flow.Context) error {
			client := sidecar.GetClient(ctx, propertiesSidecar)
			require.NoError(ctx, client.PublishEvent(ctx, pubsubName, propertiesTopic, "hello",
				dapr.PublishEventWithMetadata(map[string]string{
					"eventType":       "invoice",
					"subject":         "invoice",
					"ttlInSeconds":    "600",
					"cloudevent.type": "com.example.invoice",
				})))

			receiveCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
			defer cancel()

			msg, err := receiver.Receive(receiveCtx, nil)
			require.NoError(ctx, err)
			require.NoError(ctx, receiver.AcceptMessage(ctx, msg))

			require.Equal(ctx, "invoice", msg.ApplicationProperties["eventType"])
			require.NotNil(ctx, msg.Properties)
			require.NotNil(ctx, msg.Properties.Subject)
			require.Equal(ctx, "invoice", *msg.Properties.Subject)

			// Keys that control the publish, or that override the CloudEvent
			// envelope in the body, stay off the message.
			require.NotContains(ctx, msg.ApplicationProperties, "subject")
			require.NotContains(ctx, msg.ApplicationProperties, "ttlInSeconds")
			require.NotContains(ctx, msg.ApplicationProperties, "cloudevent.type")

			return nil
		}).
		Run()
}
