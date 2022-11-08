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

	cronName := "cron"
	appName := "cronapp"
	sidecarName := "cron-sidecar"

	// check if cron triggers 10 times within 10 seconds
	expectedTriggerCount := 10
	timeoutToObeserveTrigger := time.Second * 10

	// wait after delete operation to check if cron still triggers
	waitAfterDelete := time.Second * 5

	triggerWatcher := watcher.NewOrdered()
	for i := 1; i <= expectedTriggerCount; i++ {
		triggerWatcher.ExpectInts(i)
	}

	// total times cron is triggered
	triggeredCount := 0
	// store triggered count at the time of cron delete operation
	var triggeredCountAtDelete int

	flow.New(t, "test cron trigger schedule @every 1s").
		Step(app.Run(appName, fmt.Sprintf(":%d", appPort), appWithTriggerCounter(t, cronName, triggerWatcher, &triggeredCount))).
		Step(sidecar.Run(sidecarName,
			embedded.WithComponentsPath("./components"),
			embedded.WithDaprGRPCPort(grpcPort),
			embedded.WithAppProtocol(runtime.HTTPProtocol, appPort),
			embedded.WithDaprHTTPPort(httpPort),
			componentRuntimeOptions(),
		)).
		Step("assert cron triggered within deadline", assertTriggerCount(triggerWatcher, timeoutToObeserveTrigger)).
		Step("delete cron trigger", deleteCronTrigger(t, cronName, sidecarName, &triggeredCount, &triggeredCountAtDelete)).
		Step("allow cron to trigger if delete operation failed", flow.Sleep(waitAfterDelete)).
		Step("assert cron did not trigger after delete", assertTriggerDelete(t, &triggeredCountAtDelete, &triggeredCount)).
		Step("stop sidecar", sidecar.Stop(sidecarName)).
		Step("stop app", app.Stop(appName)).
		Run()
}

// For cron component with trigger @every 3s, check if the app is invoked correctly on app restart
func TestCronBindingWithAppRestart(t *testing.T) {
	ports, _ := dapr_testing.GetFreePorts(3)
	grpcPort := ports[0]
	httpPort := ports[1]
	appPort := ports[2]

	cronName := "cron3s"
	appName := "cronapp3s"
	sidecarName := "cron-sidecar"

	// Test if cron triggers 5 times within 15 seconds
	expectedTriggerCount := 5
	timeoutToObeserveTrigger := time.Second * 15

	// allow cron to trigger for some time before stopping the app
	waitBeforeAppStop := time.Second * 5

	// wait for some time after the app has stopped, before restarting the app
	waitBeforeAppRestart := time.Second * 5

	// wait after delete operation to check if cron still triggers
	waitAfterDelete := time.Second * 10

	triggerWatcher := watcher.NewOrdered()
	for i := 1; i <= expectedTriggerCount; i++ {
		triggerWatcher.ExpectInts(i)
	}

	// total times cron is triggered
	triggeredCount := 0
	// store triggered count at the time of cron delete operation
	var triggeredCountAtDelete int

	flow.New(t, "test cron trigger schedule @every3s with app restart").
		Step(app.Run(appName, fmt.Sprintf(":%d", appPort), appWithTriggerCounter(t, cronName, triggerWatcher, &triggeredCount))).
		Step(sidecar.Run(sidecarName,
			embedded.WithComponentsPath("./components"),
			embedded.WithDaprGRPCPort(grpcPort),
			embedded.WithAppProtocol(runtime.HTTPProtocol, appPort),
			embedded.WithDaprHTTPPort(httpPort),
			componentRuntimeOptions(),
		)).
		Step("allow cron to trigger once", flow.Sleep(waitBeforeAppStop)).
		Step("stop app", app.Stop(appName)).
		Step("wait before app restart", flow.Sleep(waitBeforeAppRestart)).
		Step(app.Run(appName, fmt.Sprintf(":%d", appPort), appWithTriggerCounter(t, cronName, triggerWatcher, &triggeredCount))).
		Step("assert cron triggered within deadline", assertTriggerCount(triggerWatcher, timeoutToObeserveTrigger)).
		Step("delete cron trigger", deleteCronTrigger(t, cronName, sidecarName, &triggeredCount, &triggeredCountAtDelete)).
		Step("allow cron to trigger if delete operation failed", flow.Sleep(waitAfterDelete)).
		Step("assert cron did not trigger after delete", assertTriggerDelete(t, &triggeredCountAtDelete, &triggeredCount)).
		Step("stop sidecar", sidecar.Stop(sidecarName)).
		Step("stop app", app.Stop(appName)).
		Run()
}

// For cron component with trigger @every 3s, check if the app is invoked correctly on sidecar restart
func TestCronBindingWithSidecarRestart(t *testing.T) {
	ports, _ := dapr_testing.GetFreePorts(3)
	grpcPort := ports[0]
	httpPort := ports[1]
	appPort := ports[2]

	cronName := "cron3s"
	appName := "cronapp3s"
	sidecarName := "cron-sidecar"

	// Test if cron triggers 5 times within 15 seconds
	expectedTriggerCount := 5
	timeoutToObeserveTrigger := time.Second * 15

	// allow cron to trigger for some time before stopping the sidecar
	waitBeforeSidecarStop := time.Second * 5

	// wait for some time after the app has stopped, before restarting the sidecar
	waitBeforeSidecarRestart := time.Second * 5

	// wait after delete operation to check if cron still triggers
	waitAfterDelete := time.Second * 10

	triggerWatcher := watcher.NewOrdered()
	for i := 1; i <= expectedTriggerCount; i++ {
		triggerWatcher.ExpectInts(i)
	}

	// total times cron is triggered
	triggeredCount := 0
	// store triggered count at the time of cron delete operation
	var triggeredCountAtDelete int

	flow.New(t, "test cron trigger schedule @every 3s with sidecar restart").
		Step(app.Run(appName, fmt.Sprintf(":%d", appPort), appWithTriggerCounter(t, cronName, triggerWatcher, &triggeredCount))).
		Step(sidecar.Run(sidecarName,
			embedded.WithComponentsPath("./components"),
			embedded.WithDaprGRPCPort(grpcPort),
			embedded.WithAppProtocol(runtime.HTTPProtocol, appPort),
			embedded.WithDaprHTTPPort(httpPort),
			componentRuntimeOptions(),
		)).
		Step("allow cron to trigger once", flow.Sleep(waitBeforeSidecarStop)).
		Step("stop sidecar", sidecar.Stop(sidecarName)).
		Step("wait before sidecar restart", flow.Sleep(waitBeforeSidecarRestart)).
		Step(sidecar.Run(sidecarName,
			embedded.WithComponentsPath("./components"),
			embedded.WithDaprGRPCPort(grpcPort),
			embedded.WithAppProtocol(runtime.HTTPProtocol, appPort),
			embedded.WithDaprHTTPPort(httpPort),
			componentRuntimeOptions(),
		)).
		Step("assert cron triggered within deadline", assertTriggerCount(triggerWatcher, timeoutToObeserveTrigger)).
		Step("delete cron trigger", deleteCronTrigger(t, cronName, sidecarName, &triggeredCount, &triggeredCountAtDelete)).
		Step("allow cron to trigger if delete operation failed", flow.Sleep(waitAfterDelete)).
		Step("assert cron did not trigger after delete", assertTriggerDelete(t, &triggeredCountAtDelete, &triggeredCount)).
		Step("stop sidecar", sidecar.Stop(sidecarName)).
		Step("stop app", app.Stop(appName)).
		Run()
}

func appWithTriggerCounter(t *testing.T, cronName string, triggerWatcher *watcher.Watcher, triggeredCount *int) func(ctx flow.Context, s common.Service) error {
	return func(ctx flow.Context, s common.Service) error {
		// Setup the input binding endpoint
		err := s.AddBindingInvocationHandler(cronName, func(_ context.Context, in *common.BindingEvent) ([]byte, error) {
			ctx.Logf("Cron triggered at %s", time.Now().String())
			(*triggeredCount)++
			triggerWatcher.Observe(*triggeredCount)
			return []byte("{}"), nil
		})
		require.NoError(t, err)
		return err
	}
}

func assertTriggerCount(triggerWatcher *watcher.Watcher, time time.Duration) func(ctx flow.Context) error {
	return func(ctx flow.Context) error {
		triggerWatcher.Assert(ctx, time)
		return nil
	}
}

func deleteCronTrigger(t *testing.T, triggerName string, sidecarName string, triggeredCount *int, triggeredCountAtDelete *int) func(ctx flow.Context) error {
	return func(ctx flow.Context) error {
		client := sidecar.GetClient(ctx, sidecarName)

		req := &daprClient.InvokeBindingRequest{Name: triggerName, Operation: "delete"}
		res, err := client.InvokeBinding(ctx, req)

		require.NotEmpty(t, res.Metadata["schedule"])
		require.NotEmpty(t, res.Metadata["stopTimeUTC"])
		require.NoError(t, err, "Error cancelling cron schedule")
		// Store the count at the time of delete
		*triggeredCountAtDelete = *triggeredCount
		return nil
	}
}

func assertTriggerDelete(t *testing.T, triggerCountAtDelete *int, triggerCountAfterDelete *int) func(ctx flow.Context) error {
	return func(ctx flow.Context) error {
		require.Equal(t, *triggerCountAtDelete, *triggerCountAfterDelete, "Cron triggered even after delete operation")
		return nil
	}
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
