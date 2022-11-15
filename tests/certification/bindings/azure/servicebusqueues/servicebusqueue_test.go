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

package servicebusqueue_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/multierr"

	"github.com/dapr/components-contrib/bindings"
	binding_asb "github.com/dapr/components-contrib/bindings/azure/servicebusqueues"
	secretstore_env "github.com/dapr/components-contrib/secretstores/local/env"

	bindings_loader "github.com/dapr/dapr/pkg/components/bindings"
	secretstores_loader "github.com/dapr/dapr/pkg/components/secretstores"
	"github.com/dapr/dapr/pkg/runtime"
	dapr_testing "github.com/dapr/dapr/pkg/testing"

	daprClient "github.com/dapr/go-sdk/client"
	"github.com/dapr/go-sdk/service/common"

	"github.com/dapr/kit/logger"

	"github.com/dapr/components-contrib/tests/certification/embedded"
	"github.com/dapr/components-contrib/tests/certification/flow"
	"github.com/dapr/components-contrib/tests/certification/flow/app"
	"github.com/dapr/components-contrib/tests/certification/flow/network"
	"github.com/dapr/components-contrib/tests/certification/flow/sidecar"
	"github.com/dapr/components-contrib/tests/certification/flow/simulate"
	"github.com/dapr/components-contrib/tests/certification/flow/watcher"
)

const (
	numMessages = 100
)

func TestServiceBusQueue(t *testing.T) {
	messagesFor1 := watcher.NewOrdered()
	messagesFor2 := watcher.NewOrdered()

	ports, _ := dapr_testing.GetFreePorts(3)
	grpcPort := ports[0]
	httpPort := ports[1]
	appPort := ports[2]

	test := func(ctx flow.Context) error {
		client, err := daprClient.NewClientWithPort(fmt.Sprintf("%d", grpcPort))
		require.NoError(t, err, "Could not initialize dapr client.")

		// Declare what is expected BEFORE performing any steps
		// that will satisfy the test.
		msgsFor1 := make([]string, numMessages/2)
		msgsFor2 := make([]string, numMessages/2)
		for i := 0; i < numMessages/2; i++ {
			msgsFor1[i] = fmt.Sprintf("sb-binding-1: Message %03d", i)
		}

		for i := numMessages / 2; i < numMessages; i++ {
			msgsFor2[i-(numMessages/2)] = fmt.Sprintf("sb-binding-2: Message %03d", i)
		}

		messagesFor1.ExpectStrings(msgsFor1...)
		messagesFor2.ExpectStrings(msgsFor2...)

		// Send events that the application above will observe.
		ctx.Log("Invoking binding 1!")
		for _, msg := range msgsFor1 {
			ctx.Logf("Sending: %q", msg)

			req := &daprClient.InvokeBindingRequest{Name: "sb-binding-1", Operation: "create", Data: []byte(msg)}
			err := client.InvokeOutputBinding(ctx, req)
			require.NoError(ctx, err, "error publishing message")
		}

		ctx.Log("Invoking binding 2!")
		for _, msg := range msgsFor2 {
			ctx.Logf("Sending: %q", msg)

			req := &daprClient.InvokeBindingRequest{Name: "sb-binding-2", Operation: "create", Data: []byte(msg)}
			err := client.InvokeOutputBinding(ctx, req)
			require.NoError(ctx, err, "error publishing message")
		}

		// Do the messages we observed match what we expect?
		messagesFor1.Assert(ctx, time.Minute)
		messagesFor2.Assert(ctx, time.Minute)

		return nil
	}

	// Application logic that tracks messages from a topic.
	application := func(ctx flow.Context, s common.Service) (err error) {
		// Setup the input binding endpoints
		err = multierr.Combine(err,
			s.AddBindingInvocationHandler("sb-binding-1", func(_ context.Context, in *common.BindingEvent) ([]byte, error) {
				messagesFor1.Observe(string(in.Data))
				ctx.Logf("Got message: %s", string(in.Data))
				return []byte("{}"), nil
			}),
			s.AddBindingInvocationHandler("sb-binding-2", func(_ context.Context, in *common.BindingEvent) ([]byte, error) {
				messagesFor2.Observe(string(in.Data))
				ctx.Logf("Got message: %s", string(in.Data))
				return []byte("{}"), nil
			}))

		return err
	}

	flow.New(t, "servicebusqueue certification").
		// Run the application logic above.
		Step(app.Run("basicApp", fmt.Sprintf(":%d", appPort), application)).
		Step(sidecar.Run("basicSidecar",
			embedded.WithAppProtocol(runtime.HTTPProtocol, appPort),
			embedded.WithDaprGRPCPort(grpcPort),
			embedded.WithDaprHTTPPort(httpPort),
			embedded.WithComponentsPath("./components/standard"),
			componentRuntimeOptions(),
		)).
		// Block the standard AMPQ ports.
		Step("interrupt network", network.InterruptNetwork(time.Minute, []string{}, []string{}, "5671", "5672")).
		Step("send and wait", test).
		Run()
}

func TestAzureServiceBusQueuesTTLs(t *testing.T) {
	ttlMessages := watcher.NewUnordered()

	ports, _ := dapr_testing.GetFreePorts(3)
	grpcPort := ports[0]
	httpPort := ports[1]
	appPort := ports[2]

	sendTTLMessages := func(ctx flow.Context) error {
		client, err := daprClient.NewClientWithPort(fmt.Sprintf("%d", grpcPort))
		require.NoError(t, err, "Could not initialize dapr client.")

		ctx.Logf("Sending messages for expiration.")
		for i := 0; i < numMessages; i++ {
			msg := fmt.Sprintf("Expiring message %d", i)

			metadata := make(map[string]string)

			// Send to the queue with TTL.
			queueTTLReq := &daprClient.InvokeBindingRequest{Name: "queuettl", Operation: "create", Data: []byte(msg), Metadata: metadata}
			err := client.InvokeOutputBinding(ctx, queueTTLReq)
			require.NoError(ctx, err, "error publishing message")

			// Send message with TTL.
			messageTTLReq := &daprClient.InvokeBindingRequest{Name: "messagettl", Operation: "create", Data: []byte(msg), Metadata: metadata}
			messageTTLReq.Metadata["ttlInSeconds"] = "10"
			err = client.InvokeOutputBinding(ctx, messageTTLReq)
			require.NoError(ctx, err, "error publishing message")

			// Send message with TTL to ensure it overwrites Queue TTL.
			mixedTTLReq := &daprClient.InvokeBindingRequest{Name: "mixedttl", Operation: "create", Data: []byte(msg), Metadata: metadata}
			mixedTTLReq.Metadata["ttlInSeconds"] = "10"
			err = client.InvokeOutputBinding(ctx, mixedTTLReq)
			require.NoError(ctx, err, "error publishing message")
		}

		// Wait for double the TTL after sending the last message.
		time.Sleep(time.Second * 20)
		return nil
	}

	ttlApplication := func(ctx flow.Context, s common.Service) (err error) {
		// Setup the input binding endpoints
		err = multierr.Combine(err,
			s.AddBindingInvocationHandler("queuettl", func(_ context.Context, in *common.BindingEvent) ([]byte, error) {
				ctx.Logf("Oh no! Got message: %s", string(in.Data))
				ttlMessages.FailIfNotExpected(t, string(in.Data))
				return []byte("{}"), nil
			}),
			s.AddBindingInvocationHandler("messagettl", func(_ context.Context, in *common.BindingEvent) ([]byte, error) {
				ctx.Logf("Oh no! Got message: %s", string(in.Data))
				ttlMessages.FailIfNotExpected(t, string(in.Data))
				return []byte("{}"), nil
			}),
			s.AddBindingInvocationHandler("mixedttl", func(_ context.Context, in *common.BindingEvent) ([]byte, error) {
				ctx.Logf("Oh no! Got message: %s", string(in.Data))
				ttlMessages.FailIfNotExpected(t, string(in.Data))
				return []byte("{}"), nil
			}))

		return err
	}

	freshPorts, _ := dapr_testing.GetFreePorts(2)

	flow.New(t, "servicebusqueue ttl certification").
		Step(sidecar.Run("ttlSidecar",
			embedded.WithoutApp(),
			embedded.WithDaprGRPCPort(grpcPort),
			embedded.WithDaprHTTPPort(httpPort),
			embedded.WithComponentsPath("./components/ttl"),
			componentRuntimeOptions(),
		)).
		Step("send ttl messages", sendTTLMessages).
		Step("stop initial sidecar", sidecar.Stop("ttlSidecar")).
		Step(app.Run("ttlApp", fmt.Sprintf(":%d", appPort), ttlApplication)).
		Step(sidecar.Run("appSidecar",
			embedded.WithAppProtocol(runtime.HTTPProtocol, appPort),
			embedded.WithDaprGRPCPort(freshPorts[0]),
			embedded.WithDaprHTTPPort(freshPorts[1]),
			componentRuntimeOptions(),
		)).
		Step("verify no messages", func(ctx flow.Context) error {
			ttlMessages.Assert(t, time.Minute)
			return nil
		}).
		Run()
}

func TestAzureServiceBusQueueRetriesOnError(t *testing.T) {
	messages := watcher.NewUnordered()

	ports, _ := dapr_testing.GetFreePorts(3)
	grpcPort := ports[0]
	httpPort := ports[1]
	appPort := ports[2]

	test := func(ctx flow.Context) error {
		client, err := daprClient.NewClientWithPort(fmt.Sprintf("%d", grpcPort))
		require.NoError(t, err, "Could not initialize dapr client.")

		// Declare what is expected BEFORE performing any steps
		// that will satisfy the test.
		msgs := make([]string, numMessages/2)
		for i := 0; i < numMessages/2; i++ {
			msgs[i] = fmt.Sprintf("Message %03d", i)
		}

		messages.ExpectStrings(msgs...)

		// Send events that the application above will observe.
		ctx.Log("Invoking binding 1!")
		for _, msg := range msgs {
			ctx.Logf("Sending: %q", msg)

			req := &daprClient.InvokeBindingRequest{Name: "retry-binding", Operation: "create", Data: []byte(msg)}
			err := client.InvokeOutputBinding(ctx, req)
			require.NoError(ctx, err, "error publishing message")
		}

		// Do the messages we observed match what we expect?
		messages.Assert(ctx, time.Minute)

		return nil
	}

	// Application logic that tracks messages from a topic.
	application := func(ctx flow.Context, s common.Service) (err error) {
		// Simulate periodic errors.
		sim := simulate.PeriodicError(ctx, 10)

		// Setup the input binding endpoint
		err = multierr.Combine(err,
			s.AddBindingInvocationHandler("retry-binding", func(_ context.Context, in *common.BindingEvent) ([]byte, error) {
				if err := sim(); err != nil {
					ctx.Logf("Failing message: %s", string(in.Data))
					return nil, err
				}

				messages.Observe(string(in.Data))
				ctx.Logf("Got message: %s", string(in.Data))
				return []byte("{}"), nil
			}))

		return err
	}

	flow.New(t, "servicebusqueue retry certification").
		// Run the application logic above.
		Step(app.Run("retryApp", fmt.Sprintf(":%d", appPort), application)).
		Step(sidecar.Run("retrySidecar",
			embedded.WithAppProtocol(runtime.HTTPProtocol, appPort),
			embedded.WithDaprGRPCPort(grpcPort),
			embedded.WithDaprHTTPPort(httpPort),
			embedded.WithComponentsPath("./components/retry"),
			componentRuntimeOptions(),
		)).
		Step("send and wait", test).
		Run()
}

func TestServiceBusQueueMetadata(t *testing.T) {
	messages := watcher.NewUnordered()

	ports, _ := dapr_testing.GetFreePorts(3)
	grpcPort := ports[0]
	httpPort := ports[1]
	appPort := ports[2]

	test := func(ctx flow.Context) error {
		client, err := daprClient.NewClientWithPort(fmt.Sprintf("%d", grpcPort))
		require.NoError(t, err, "Could not initialize dapr client.")

		// Send events that the application above will observe.
		ctx.Log("Invoking binding!")
		req := &daprClient.InvokeBindingRequest{Name: "sb-binding-1", Operation: "create", Data: []byte("test msg"), Metadata: map[string]string{"Testmetadata": "Some Metadata"}}
		err = client.InvokeOutputBinding(ctx, req)
		require.NoError(ctx, err, "error publishing message")

		// Do the messages we observed match what we expect?
		messages.Assert(ctx, time.Minute)

		return nil
	}

	// Application logic that tracks messages from a topic.
	application := func(ctx flow.Context, s common.Service) (err error) {
		// Setup the input binding endpoints
		err = multierr.Combine(err,
			s.AddBindingInvocationHandler("sb-binding-1", func(_ context.Context, in *common.BindingEvent) ([]byte, error) {
				messages.Observe(string(in.Data))
				ctx.Logf("Got message: %s - %+v", string(in.Data), in.Metadata)
				require.NotEmpty(t, in.Metadata)
				require.Contains(t, in.Metadata, "Testmetadata")
				require.Equal(t, "Some Metadata", in.Metadata["Testmetadata"])

				return []byte("{}"), nil
			}))

		return err
	}

	flow.New(t, "servicebusqueue certification").
		// Run the application logic above.
		Step(app.Run("metadataApp", fmt.Sprintf(":%d", appPort), application)).
		Step(sidecar.Run("metadataSidecar",
			embedded.WithAppProtocol(runtime.HTTPProtocol, appPort),
			embedded.WithDaprGRPCPort(grpcPort),
			embedded.WithDaprHTTPPort(httpPort),
			embedded.WithComponentsPath("./components/standard"),
			componentRuntimeOptions(),
		)).
		Step("send and wait", test).
		Run()
}

func TestServiceBusQueueDisableEntityManagement(t *testing.T) {
	ports, _ := dapr_testing.GetFreePorts(2)
	grpcPort := ports[0]
	httpPort := ports[1]

	testWithExpectedFailure := func(ctx flow.Context) error {
		client, err := daprClient.NewClientWithPort(fmt.Sprintf("%d", grpcPort))
		require.NoError(t, err, "Could not initialize dapr client.")

		// Send events that the application above will observe.
		ctx.Log("Invoking binding!")
		req := &daprClient.InvokeBindingRequest{Name: "mgmt-binding", Operation: "create", Data: []byte("test msg"), Metadata: map[string]string{"TestMetadata": "Some Metadata"}}
		err = client.InvokeOutputBinding(ctx, req)
		require.Error(ctx, err, "error publishing message")
		return nil
	}

	flow.New(t, "servicebus queues certification - entity management disabled").
		// Run the application logic above.
		Step(sidecar.Run("metadataSidecar",
			embedded.WithoutApp(),
			embedded.WithDaprGRPCPort(grpcPort),
			embedded.WithDaprHTTPPort(httpPort),
			embedded.WithComponentsPath("./components/disable_entity_mgmt"),
			componentRuntimeOptions(),
		)).
		Step("send and wait", testWithExpectedFailure).
		Run()
}

func componentRuntimeOptions() []runtime.Option {
	log := logger.NewLogger("dapr.components")

	bindingsRegistry := bindings_loader.NewRegistry()
	bindingsRegistry.Logger = log
	bindingsRegistry.RegisterInputBinding(func(l logger.Logger) bindings.InputBinding {
		return binding_asb.NewAzureServiceBusQueues(l)
	}, "azure.servicebusqueues")
	bindingsRegistry.RegisterOutputBinding(func(l logger.Logger) bindings.OutputBinding {
		return binding_asb.NewAzureServiceBusQueues(l)
	}, "azure.servicebusqueues")

	secretstoreRegistry := secretstores_loader.NewRegistry()
	secretstoreRegistry.Logger = log
	secretstoreRegistry.RegisterComponent(secretstore_env.NewEnvSecretStore, "local.env")

	return []runtime.Option{
		runtime.WithBindings(bindingsRegistry),
		runtime.WithSecretStores(secretstoreRegistry),
	}
}
