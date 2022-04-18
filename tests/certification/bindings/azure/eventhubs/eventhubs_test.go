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

	"github.com/stretchr/testify/assert"
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
	numMessages = 100
)

func TestEventhubBinding(t *testing.T) {
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
		time.Sleep(20 * time.Second)

		// Send events from output binding
		for _, msg := range outputmsg {
			ctx.Logf("Sending eventhub message: %q", msg)

			err := client.InvokeOutputBinding(
				ctx, &dapr.InvokeBindingRequest{
					Name:      "azure-eventhubs-binding",
					Operation: "create",
					Data:      []byte(msg),
				})
			require.NoError(ctx, err, "error publishing message")
		}

		// Assert the observed messages
		consumerGroup1.Assert(ctx, time.Minute)
		return nil
	}

	// Application logic that tracks messages from eventhub.
	application := func(ctx flow.Context, s common.Service) (err error) {
		// Simulate periodic errors.
		sim := simulate.PeriodicError(ctx, 100)
		// Setup the binding endpoints
		err = multierr.Combine(err,
			s.AddBindingInvocationHandler("azure-eventhubs-binding", func(_ context.Context, in *common.BindingEvent) ([]byte, error) {
				consumerGroup1.Observe(string(in.Data))
				if err := sim(); err != nil {
					return nil, err
				}
				ctx.Logf("Receiving eventhubs message: %s", string(in.Data))
				return []byte("{}"), nil
			}))
		return err
	}

	publishMessages := func(ctx flow.Context) error {
		// Define what is expected
		outputmsg := make([]string, numMessages)
		for i := 0; i < numMessages; i++ {
			outputmsg[i] = fmt.Sprintf("publish messages to device: Message %03d", i)
		}
		consumerGroup2.ExpectStrings(outputmsg...)
		cmd := exec.Command("/bin/bash", "../../../tests/scripts/send-iot-device-events.sh")
		out, err := cmd.CombinedOutput()
		assert.Nil(t, err, "Error in send-iot-device-events.sh:\n%s", out)
		consumerGroup2.Assert(ctx, time.Minute)
		return nil
	}

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
		Step("interrupt network", network.InterruptNetwork(30*time.Second, nil, nil, "21092", "31092", "41092")).
		Step("send and wait", sendAndReceive).
		Run()

	// Flow of events: Start app, sidecar, interrupt network to check reconnection, send and receive
	flow.New(t, "eventhubs binding authentication using connection string").
		Step(app.Run("app", fmt.Sprintf(":%d", appPort), application)).
		Step(sidecar.Run("sidecar",
			embedded.WithAppProtocol(runtime.HTTPProtocol, appPort),
			embedded.WithDaprGRPCPort(grpcPort),
			embedded.WithDaprHTTPPort(httpPort),
			embedded.WithComponentsPath("./components/binding/connectionstring"),
			runtime.WithSecretStores(secrets_components),
			runtime.WithOutputBindings(out_component),
			runtime.WithInputBindings(in_component),
		)).
		Step("interrupt network", network.InterruptNetwork(30*time.Second, nil, nil, "21092", "31092", "41092")).
		Step("send and wait", sendAndReceive).
		Run()

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
		Step("Send messages to IoT", publishMessages).
		Run()
}
