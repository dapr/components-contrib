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

package cron_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/components-contrib/bindings/cron"
	bindings_loader "github.com/dapr/dapr/pkg/components/bindings"

	"github.com/dapr/dapr/pkg/runtime"
	"github.com/dapr/kit/logger"

	dapr_testing "github.com/dapr/dapr/pkg/testing"
	"github.com/stretchr/testify/require"

	daprClient "github.com/dapr/go-sdk/client"
	"github.com/dapr/go-sdk/service/common"

	// Certification testing runnables
	"github.com/dapr/components-contrib/tests/certification/embedded"
	"github.com/dapr/components-contrib/tests/certification/flow"
	"github.com/dapr/components-contrib/tests/certification/flow/app"
	"github.com/dapr/components-contrib/tests/certification/flow/sidecar"
	"github.com/dapr/components-contrib/tests/certification/flow/watcher"
)

// Test cron trigger with most frequent schedule possible : @every 1s with a context deadline
func TestCronBindingFrequentTrigger(t *testing.T) {
	ports, _ := dapr_testing.GetFreePorts(3)
	grpcPort := ports[0]
	httpPort := ports[1]
	appPort := ports[2]

	triggerWatcher := watcher.NewOrdered()
	for i := 1; i <= 10; i++ {
		triggerWatcher.ExpectInts(i)
	}

	// test if cron triggers 10 times within 10 seconds
	testAssert := func(ctx flow.Context) error {
		triggerWatcher.Assert(ctx, time.Second*10)
		return nil
	}

	application := func(ctx flow.Context, s common.Service) error {
		triggeredCount := 0
		// Setup the input binding endpoint
		err := s.AddBindingInvocationHandler("cron", func(_ context.Context, in *common.BindingEvent) ([]byte, error) {
			ctx.Logf("Cron triggered at %s", time.Now().String())
			triggeredCount++
			triggerWatcher.Observe(triggeredCount)
			return []byte("{}"), nil
		})
		require.NoError(t, err)
		return nil
	}

	testDelete := func(ctx flow.Context) error {
		client := sidecar.GetClient(ctx, "cron-sidecar")

		req := &daprClient.InvokeBindingRequest{Name: "cron", Operation: "delete"}
		res, err := client.InvokeBinding(ctx, req)

		require.Equal(t, res.Metadata["schedule"], "@every 1s")
		require.NotEmpty(t, res.Metadata["stopTimeUTC"])
		require.NoError(t, err, "Error cancelling cron schedule")
		return nil
	}

	flow.New(t, "test cron trigger schedule @every 1s").
		Step(app.Run("cronapp", fmt.Sprintf(":%d", appPort), application)).
		Step(sidecar.Run("cron-sidecar",
			embedded.WithComponentsPath("./components"),
			embedded.WithDaprGRPCPort(grpcPort),
			embedded.WithAppProtocol(runtime.HTTPProtocol, appPort),
			embedded.WithDaprHTTPPort(httpPort),
			componentRuntimeOptions(),
		)).
		Step("assert cron triggered within deadline", testAssert).
		Step("test trigger delete operation", testDelete).
		Step("stop sidecar", sidecar.Stop("cron-sidecar")).
		Step("stop app", app.Stop("cronapp")).
		Run()
}

// For cron component with trigger @every 3s, check if the app is invoked correctly on app restart
func TestCronBindingWithAppRestart(t *testing.T) {
	ports, _ := dapr_testing.GetFreePorts(3)
	grpcPort := ports[0]
	httpPort := ports[1]
	appPort := ports[2]

	triggerWatcher := watcher.NewOrdered()
	for i := 1; i <= 5; i++ {
		triggerWatcher.ExpectInts(i)
	}

	// Test if cron triggers 5 times within 15 seconds
	testAssert := func(ctx flow.Context) error {
		triggerWatcher.Assert(ctx, time.Second*15)
		return nil
	}

	application := func(ctx flow.Context, s common.Service) error {
		triggeredCount := 0
		// Setup the input binding endpoint
		err := s.AddBindingInvocationHandler("cron3s", func(_ context.Context, in *common.BindingEvent) ([]byte, error) {
			ctx.Logf("Cron triggered at %s", time.Now().String())
			triggeredCount++
			triggerWatcher.Observe(triggeredCount)
			return []byte("{}"), nil
		})
		require.NoError(t, err)
		return nil
	}

	flow.New(t, "test cron trigger schedule @every3s with app restart").
		Step(app.Run("cronapp", fmt.Sprintf(":%d", appPort), application)).
		Step(sidecar.Run("cron-sidecar",
			embedded.WithComponentsPath("./components"),
			embedded.WithDaprGRPCPort(grpcPort),
			embedded.WithAppProtocol(runtime.HTTPProtocol, appPort),
			embedded.WithDaprHTTPPort(httpPort),
			componentRuntimeOptions(),
		)).
		Step("allow cron to trigger once", flow.Sleep(time.Second*4)).
		Step("stop app", app.Stop("cronapp")).
		Step("wait before app restart", flow.Sleep(time.Second*5)).
		Step(app.Run("cronapp", fmt.Sprintf(":%d", appPort), application)).
		Step("assert cron triggered within deadline", testAssert).
		Step("stop sidecar", sidecar.Stop("cron-sidecar3s")).
		Step("stop app", app.Stop("cronapp3s")).
		Run()
}

// For cron component with trigger @every 3s, check if the app is invoked correctly on sidecar restart
func TestCronBindingWithSidecarRestart(t *testing.T) {
	ports, _ := dapr_testing.GetFreePorts(3)
	grpcPort := ports[0]
	httpPort := ports[1]
	appPort := ports[2]

	triggerWatcher := watcher.NewOrdered()
	for i := 1; i <= 5; i++ {
		triggerWatcher.ExpectInts(i)
	}

	// Test if cron triggers 5 times within 15 seconds
	testAssert := func(ctx flow.Context) error {
		triggerWatcher.Assert(ctx, time.Second*15)
		return nil
	}

	application := func(ctx flow.Context, s common.Service) error {
		triggeredCount := 0
		// Setup the input binding endpoint
		err := s.AddBindingInvocationHandler("cron3s", func(_ context.Context, in *common.BindingEvent) ([]byte, error) {
			ctx.Logf("Cron triggered at %s", time.Now().String())
			triggeredCount++
			triggerWatcher.Observe(triggeredCount)
			return []byte("{}"), nil
		})
		require.NoError(t, err)
		return nil
	}

	flow.New(t, "test cron trigger schedule @every 3s with sidecar restart").
		Step(app.Run("cronapp", fmt.Sprintf(":%d", appPort), application)).
		Step(sidecar.Run("cron-sidecar",
			embedded.WithComponentsPath("./components"),
			embedded.WithDaprGRPCPort(grpcPort),
			embedded.WithAppProtocol(runtime.HTTPProtocol, appPort),
			embedded.WithDaprHTTPPort(httpPort),
			componentRuntimeOptions(),
		)).
		Step("allow cron to trigger once", flow.Sleep(time.Second*4)).
		Step("stop sidecar", sidecar.Stop("cron-sidecar")).
		Step("wait before sidecar restart", flow.Sleep(time.Second*5)).
		Step(sidecar.Run("cron-sidecar",
			embedded.WithComponentsPath("./components"),
			embedded.WithDaprGRPCPort(grpcPort),
			embedded.WithAppProtocol(runtime.HTTPProtocol, appPort),
			embedded.WithDaprHTTPPort(httpPort),
			componentRuntimeOptions(),
		)).
		Step("assert cron triggered within deadline", testAssert).
		Step("stop sidecar", sidecar.Stop("cron-sidecar")).
		Step("stop app", app.Stop("cronapp")).
		Run()
}

func componentRuntimeOptions() []runtime.Option {
	log := logger.NewLogger("dapr.components")

	bindingsRegistry := bindings_loader.NewRegistry()
	bindingsRegistry.Logger = log
	bindingsRegistry.RegisterInputBinding(func(l logger.Logger) bindings.InputBinding {
		return cron.NewCron(l)
	}, "cron")
	bindingsRegistry.RegisterOutputBinding(func(l logger.Logger) bindings.OutputBinding {
		return cron.NewCron(l)
	}, "cron")

	return []runtime.Option{
		runtime.WithBindings(bindingsRegistry),
	}
}
