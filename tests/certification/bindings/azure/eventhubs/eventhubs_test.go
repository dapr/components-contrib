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
	//"os"
	//"os/exec"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"go.uber.org/multierr"

	"github.com/dapr/components-contrib/bindings"

	bindings_loader "github.com/dapr/dapr/pkg/components/bindings"
	"github.com/dapr/dapr/pkg/runtime"
	dapr_testing "github.com/dapr/dapr/pkg/testing"
	"github.com/dapr/kit/logger"

	"github.com/dapr/components-contrib/secretstores"
	secretstore_env "github.com/dapr/components-contrib/secretstores/local/env"
	secretstores_loader "github.com/dapr/dapr/pkg/components/secretstores"

	"github.com/dapr/components-contrib/bindings/azure/eventhubs"
	"github.com/dapr/components-contrib/tests/certification/embedded"
	"github.com/dapr/components-contrib/tests/certification/flow"
	"github.com/dapr/components-contrib/tests/certification/flow/app"
	"github.com/dapr/components-contrib/tests/certification/flow/network"
	"github.com/dapr/components-contrib/tests/certification/flow/sidecar"
	"github.com/dapr/components-contrib/tests/certification/flow/simulate"
	"github.com/dapr/components-contrib/tests/certification/flow/watcher"
	dapr "github.com/dapr/go-sdk/client"
	"github.com/dapr/go-sdk/service/common"
)

const (
	numMessages      = 100
	messageKey       = "partitionKey"
	iotHubNameEnvKey = "AzureIotHubName"
)

func TestSinglePartition(t *testing.T) {
	logger := logger.NewLogger("dapr.components")
	out_component := bindings_loader.NewOutput("azure.eventhubs", func() bindings.OutputBinding {
		return eventhubs.NewAzureEventHubs(logger)
	})
	in_component := bindings_loader.NewInput("azure.eventhubs", func() bindings.InputBinding {
		return eventhubs.NewAzureEventHubs(logger)
	})
	secrets_components := secretstores_loader.New("local.env", func() secretstores.SecretStore {
		return secretstore_env.NewEnvSecretStore(logger)
	})

	ports, _ := dapr_testing.GetFreePorts(3)
	grpcPort := ports[0]
	httpPort := ports[1]
	appPort := ports[2]

	consumerGroup1 := watcher.NewUnordered()

	metadata := map[string]string{
		messageKey: "test",
	}

	sendAndReceive := func(metadata map[string]string, messages ...*watcher.Watcher) flow.Runnable {
		_, hasKey := metadata[messageKey]
		return func(ctx flow.Context) error {
			client, err := dapr.NewClientWithPort(fmt.Sprintf("%d", grpcPort))
			require.NoError(t, err, "dapr init failed")

			// Define what is expected
			outputmsg := make([]string, numMessages)
			for i := 0; i < numMessages; i++ {
				outputmsg[i] = fmt.Sprintf("output binding: Message %03d", i)
			}
			consumerGroup1.ExpectStrings(outputmsg...)
			time.Sleep(120 * time.Second)
			if !hasKey {
				metadata[messageKey] = uuid.NewString()
			}
			// Send events from output binding
			for _, msg := range outputmsg {
				ctx.Logf("Sending eventhub message: %q", msg)

				err := client.InvokeOutputBinding(
					ctx, &dapr.InvokeBindingRequest{
						Name:      "azure-single-partition-binding",
						Operation: "create",
						Data:      []byte(msg),
						Metadata:  metadata,
					})
				require.NoError(ctx, err, "error publishing message")
			}

			// Assert the observed messages
			consumerGroup1.Assert(ctx, time.Minute)
			return nil
		}
	}

	// Application logic that tracks messages from eventhub.
	application := func(ctx flow.Context, s common.Service) (err error) {
		// Simulate periodic errors.
		sim := simulate.PeriodicError(ctx, 100)
		// Setup the binding endpoints
		err = multierr.Combine(err,
			s.AddBindingInvocationHandler("azure-single-partition-binding", func(_ context.Context, in *common.BindingEvent) ([]byte, error) {
				consumerGroup1.Observe(string(in.Data))
				if err := sim(); err != nil {
					return nil, err
				}
				ctx.Logf("Receiving eventhubs message: %s", string(in.Data))
				return []byte("{}"), nil
			}))
		return err
	}

	/*iotHubName := os.Getenv(iotHubNameEnvKey)
	consumerGroup3 := watcher.NewUnordered()
	sendIOTDevice := func(messages *watcher.Watcher) flow.Runnable {
		return func(ctx flow.Context) error {
			// Define what is expected
			outputmsg := make([]string, numMessages)
			for i := 0; i < numMessages; i++ {
				outputmsg[i] = fmt.Sprintf("messages to test iothub: Message %03d", i)
			}
			messages.ExpectStrings(outputmsg...)

			cmd := exec.Command("/bin/bash", "send-iot-device-events.sh")
			cmd.Env = append(os.Environ(), fmt.Sprintf("IOT_HUB_NAME=%s", iotHubName))
			cmd.CombinedOutput()
			return nil
		}
	}
	flow.New(t, "eventhubs binding IoTHub testing").
		Step(app.Run("app", fmt.Sprintf(":%d", appPort), application)).
		Step(sidecar.Run("sidecar",
			embedded.WithAppProtocol(runtime.HTTPProtocol, appPort),
			embedded.WithDaprGRPCPort(grpcPort),
			embedded.WithDaprHTTPPort(httpPort),
			embedded.WithComponentsPath("./components/binding/iothub"),
			runtime.WithSecretStores(secrets_components),
			runtime.WithOutputBindings(out_component),
			runtime.WithInputBindings(in_component),
		)).
		Step("Send messages to IoT", sendIOTDevice(consumerGroup3)).
		Run()

	// Flow of events: Start app, sidecar, interrupt network to check reconnection, send and receive
	flow.New(t, "eventhubs binding authentication using service principal").
		Step(app.Run("app", fmt.Sprintf(":%d", appPort), application)).
		Step(sidecar.Run("sidecar",
			embedded.WithAppProtocol(runtime.HTTPProtocol, appPort),
			embedded.WithDaprGRPCPort(grpcPort),
			embedded.WithDaprHTTPPort(httpPort),
			embedded.WithComponentsPath("./components/binding/serviceprincipal"),
			runtime.WithSecretStores(secrets_components),
			runtime.WithOutputBindings(out_component),
			runtime.WithInputBindings(in_component),
		)).
		Step("interrupt network", network.InterruptNetwork(30*time.Second, nil, nil, "443", "5671", "5672")).
		Step("send and wait", sendAndReceive(metadata)).
		Run()*/

	// Flow of events: Start app, sidecar, interrupt network to check reconnection, send and receive
	flow.New(t, "eventhubs binding authentication using connection string single partition").
		Step(app.Run("app", fmt.Sprintf(":%d", appPort), application)).
		Step(sidecar.Run("sidecar",
			embedded.WithAppProtocol(runtime.HTTPProtocol, appPort),
			embedded.WithDaprGRPCPort(grpcPort),
			embedded.WithDaprHTTPPort(httpPort),
			embedded.WithComponentsPath("./components/binding/consumer1"),
			runtime.WithSecretStores(secrets_components),
			runtime.WithOutputBindings(out_component),
			runtime.WithInputBindings(in_component),
		)).
		Step("interrupt network", network.InterruptNetwork(30*time.Second, nil, nil, "443", "5671", "5672")).
		Step("send and wait", sendAndReceive(metadata)).
		Run()
}

/*func TestEventhubBindingMultipleSenders(t *testing.T) {

	logger := logger.NewLogger("dapr.components")
	out_component := bindings_loader.NewOutput("azure.eventhubs", func() bindings.OutputBinding {
		return eventhubs.NewAzureEventHubs(logger)
	})
	in_component := bindings_loader.NewInput("azure.eventhubs", func() bindings.InputBinding {
		return eventhubs.NewAzureEventHubs(logger)
	})
	secrets_components := secretstores_loader.New("local.env", func() secretstores.SecretStore {
		return secretstore_env.NewEnvSecretStore(logger)
	})

	ports, _ := dapr_testing.GetFreePorts(3)
	grpcPort := ports[0]
	httpPort := ports[1]
	appPort := ports[2]

	consumerGroup1 := watcher.NewUnordered()
	consumerGroup2 := watcher.NewUnordered()

	sendAndReceive := func(ctx flow.Context) error {

		client, err := dapr.NewClientWithPort(fmt.Sprintf("%d", grpcPort))
		require.NoError(t, err, "dapr init failed")

		// Define what is expected
		outputmsg := make([]string, numMessages)
		for i := 0; i < numMessages; i++ {
			outputmsg[i] = fmt.Sprintf("input binding: Message %03d", i)
		}
		consumerGroup1.ExpectStrings(outputmsg...)
		time.Sleep(40 * time.Second)

		// Send events from input binding
		for _, msg := range outputmsg {
			ctx.Logf("Sending eventhub message: %q", msg)

			err := client.InvokeOutputBinding(
				ctx, &dapr.InvokeBindingRequest{
					Name:      "azure-input-binding",
					Operation: "create",
					Data:      []byte(msg),
				})
			require.NoError(ctx, err, "error publishing message")
		}

		// Assert the observed messages
		consumerGroup1.Assert(ctx, time.Minute)

		outputmsg2 := make([]string, numMessages)
		for i := 0; i < numMessages; i++ {
			outputmsg2[i] = fmt.Sprintf("output binding: Message %03d", i)
		}
		consumerGroup2.ExpectStrings(outputmsg2...)
		time.Sleep(40 * time.Second)

		// Send events from output binding
		for _, msg2 := range outputmsg2 {
			ctx.Logf("Sending eventhub message: %q", msg2)

			err := client.InvokeOutputBinding(
				ctx, &dapr.InvokeBindingRequest{
					Name:      "azure-output-binding",
					Operation: "create",
					Data:      []byte(msg2),
				})
			require.NoError(ctx, err, "error publishing message")
		}

		// Assert the observed messages
		consumerGroup2.Assert(ctx, time.Minute)
		return nil
	}

	// Application logic that tracks messages from eventhub.
	application := func(ctx flow.Context, s common.Service) (err error) {
		// Simulate periodic errors.
		sim := simulate.PeriodicError(ctx, 100)
		// Setup the binding endpoints
		err = multierr.Combine(err,
			s.AddBindingInvocationHandler("azure-output-binding", func(_ context.Context, in *common.BindingEvent) ([]byte, error) {
				consumerGroup2.Observe(string(in.Data))
				if err := sim(); err != nil {
					return nil, err
				}
				ctx.Logf("Output binding - Receiving eventhubs message: %s", string(in.Data))
				return []byte("{}"), nil
			}),

			s.AddBindingInvocationHandler("azure-input-binding", func(_ context.Context, in *common.BindingEvent) ([]byte, error) {
				consumerGroup1.Observe(string(in.Data))
				if err := sim(); err != nil {
					return nil, err
				}
				ctx.Logf("Input binding: Receiving eventhubs message: %s", string(in.Data))
				return []byte("{}"), nil
			}))

		return err
	}
	flow.New(t, "eventhubs binding authentication using multiple senders and receivers").
		Step(app.Run("app", fmt.Sprintf(":%d", appPort), application)).
		Step(sidecar.Run("sidecar",
			embedded.WithAppProtocol(runtime.HTTPProtocol, appPort),
			embedded.WithDaprGRPCPort(grpcPort),
			embedded.WithDaprHTTPPort(httpPort),
			embedded.WithComponentsPath("./components/binding/consumer2"),
			runtime.WithSecretStores(secrets_components),
			runtime.WithOutputBindings(out_component),
			runtime.WithInputBindings(in_component),
		)).
		Step("interrupt network", network.InterruptNetwork(30*time.Second, nil, nil, "443", "5671", "5672")).
		Step("send and wait", sendAndReceive).
		Run()
}

func TestEventhubBindingMultiplePartition(t *testing.T) {
	logger := logger.NewLogger("dapr.components")
	out_component := bindings_loader.NewOutput("azure.eventhubs", func() bindings.OutputBinding {
		return eventhubs.NewAzureEventHubs(logger)
	})
	in_component := bindings_loader.NewInput("azure.eventhubs", func() bindings.InputBinding {
		return eventhubs.NewAzureEventHubs(logger)
	})
	secrets_components := secretstores_loader.New("local.env", func() secretstores.SecretStore {
		return secretstore_env.NewEnvSecretStore(logger)
	})

	ports, _ := dapr_testing.GetFreePorts(3)
	grpcPort := ports[0]
	httpPort := ports[1]
	appPort := ports[2]

	consumerGroup1 := watcher.NewUnordered()
	consumerGroup2 := watcher.NewUnordered()

	sendAndReceive := func(ctx flow.Context) error {

		client, err := dapr.NewClientWithPort(fmt.Sprintf("%d", grpcPort))
		require.NoError(t, err, "dapr init failed")

		// Define what is expected
		outputmsg := make([]string, numMessages)
		for i := 0; i < numMessages; i++ {
			outputmsg[i] = fmt.Sprintf("output binding: Message %03d", i)
		}
		consumerGroup1.ExpectStrings(outputmsg...)
		time.Sleep(40 * time.Second)

		// Send events from output binding
		for _, msg := range outputmsg {
			ctx.Logf("Sending eventhub message: %q", msg)

			err := client.InvokeOutputBinding(
				ctx, &dapr.InvokeBindingRequest{
					Name:      "azure-partition0-binding",
					Operation: "create",
					Data:      []byte(msg),
				})
			require.NoError(ctx, err, "error publishing message")
		}

		// Assert the observed messages
		consumerGroup1.Assert(ctx, time.Minute)

		// Define what is expected
		outputmsg2 := make([]string, numMessages)
		for i := 0; i < numMessages; i++ {
			outputmsg2[i] = fmt.Sprintf("output binding: Message %03d", i)
		}
		consumerGroup2.ExpectStrings(outputmsg2...)
		time.Sleep(40 * time.Second)

		// Send events from output binding
		for _, msg2 := range outputmsg2 {
			ctx.Logf("Sending eventhub message: %q", msg2)

			err := client.InvokeOutputBinding(
				ctx, &dapr.InvokeBindingRequest{
					Name:      "azure-partition1-binding",
					Operation: "create",
					Data:      []byte(msg2),
				})
			require.NoError(ctx, err, "error publishing message")
		}

		// Assert the observed messages
		consumerGroup2.Assert(ctx, time.Minute)
		return nil
	}

	// Application logic that tracks messages from eventhub.
	application := func(ctx flow.Context, s common.Service) (err error) {
		// Simulate periodic errors.
		sim := simulate.PeriodicError(ctx, 100)
		// Setup the binding endpoints
		err = multierr.Combine(err,
			s.AddBindingInvocationHandler("azure-partition0-binding", func(_ context.Context, in *common.BindingEvent) ([]byte, error) {
				consumerGroup1.Observe(string(in.Data))
				if err := sim(); err != nil {
					return nil, err
				}
				ctx.Logf("Receiving eventhubs message: %s", string(in.Data))
				return []byte("{}"), nil
			}),

			s.AddBindingInvocationHandler("azure-partition1-binding", func(_ context.Context, in *common.BindingEvent) ([]byte, error) {
				consumerGroup2.Observe(string(in.Data))
				if err := sim(); err != nil {
					return nil, err
				}
				ctx.Logf("Receiving eventhubs message: %s", string(in.Data))
				return []byte("{}"), nil
			}))
		return err
	}
	flow.New(t, "eventhubs binding authentication using connection string all partitions").
		Step(app.Run("app", fmt.Sprintf(":%d", appPort), application)).
		Step(sidecar.Run("sidecar",
			embedded.WithAppProtocol(runtime.HTTPProtocol, appPort),
			embedded.WithDaprGRPCPort(grpcPort),
			embedded.WithDaprHTTPPort(httpPort),
			embedded.WithComponentsPath("./components/binding/consumer3"),
			runtime.WithSecretStores(secrets_components),
			runtime.WithOutputBindings(out_component),
			runtime.WithInputBindings(in_component),
		)).
		Step("interrupt network", network.InterruptNetwork(30*time.Second, nil, nil, "443", "5671", "5672")).
		Step("send and wait", sendAndReceive).
		Run()
}*/
