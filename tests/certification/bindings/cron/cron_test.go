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

	"github.com/benbjohnson/clock"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/components-contrib/bindings/cron"
	"github.com/dapr/dapr/pkg/runtime"
	"github.com/dapr/go-sdk/service/common"
	"github.com/dapr/kit/logger"

	// Certification testing runnables
	"github.com/dapr/components-contrib/tests/certification/embedded"
	"github.com/dapr/components-contrib/tests/certification/flow"
	"github.com/dapr/components-contrib/tests/certification/flow/app"
	"github.com/dapr/components-contrib/tests/certification/flow/sidecar"

	bindings_loader "github.com/dapr/dapr/pkg/components/bindings"
	dapr_testing "github.com/dapr/dapr/pkg/testing"
)

// Test cron trigger with most frequent schedule possible : @every 1s with a deadline
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
	// total times cron is triggered
	observedTriggerCount := 0
	// total time for all triggers to be observed
	timeoutToObserveTriggers := time.Second * 10

	clk := clock.NewMock()

	flow.New(t, "test cron trigger schedule @every 1s").
		Step(app.Run(appName, fmt.Sprintf(":%d", appPort), appWithTriggerCounter(t, cronName, &observedTriggerCount))).
		Step(sidecar.Run(sidecarName,
			embedded.WithComponentsPath("./components"),
			embedded.WithDaprGRPCPort(grpcPort),
			embedded.WithAppProtocol(runtime.HTTPProtocol, appPort),
			embedded.WithDaprHTTPPort(httpPort),
			componentRuntimeOptions(clk),
		)).
		Step("advance the clock time", addTimeToMockClock(clk, timeoutToObserveTriggers)).
		Step("assert cron triggered within deadline", assertTriggerCount(t, expectedTriggerCount, &observedTriggerCount)).
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

	// check if cron triggers 5 times within 15 seconds
	expectedTriggerCount := 5
	// total times cron is triggered
	observedTriggerCount := 0
	// total time for all triggers to be observed
	timeoutToObserveTriggers := time.Second * 15
	// allow cron to trigger once before stopping the app
	waitBeforeAppStop := time.Second * 5
	// wait for some time after the app has stopped, before restarting the app
	waitBeforeAppRestart := time.Second * 5

	clk := clock.NewMock()

	flow.New(t, "test cron trigger schedule @every3s with app restart").
		Step(app.Run(appName, fmt.Sprintf(":%d", appPort), appWithTriggerCounter(t, cronName, &observedTriggerCount))).
		Step(sidecar.Run(sidecarName,
			embedded.WithComponentsPath("./components"),
			embedded.WithDaprGRPCPort(grpcPort),
			embedded.WithAppProtocol(runtime.HTTPProtocol, appPort),
			embedded.WithDaprHTTPPort(httpPort),
			componentRuntimeOptions(clk),
		)).
		Step("allow cron to trigger once", addTimeToMockClock(clk, waitBeforeAppStop), flow.Sleep(waitBeforeAppStop)).
		Step("stop app", app.Stop(appName)).
		Step("wait before app restart", addTimeToMockClock(clk, waitBeforeAppRestart), flow.Sleep(waitBeforeAppRestart)).
		Step(app.Run(appName, fmt.Sprintf(":%d", appPort), appWithTriggerCounter(t, cronName, &observedTriggerCount))).
		Step("advance the clock time", addTimeToMockClock(clk, timeoutToObserveTriggers)).
		Step("assert cron triggered within deadline", assertTriggerCount(t, expectedTriggerCount, &observedTriggerCount)).
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

	// check if cron triggers 5 times within 15 seconds
	expectedTriggerCount := 5
	// total times cron is triggered
	observedTriggerCount := 0
	// total time for all triggers to be observed
	timeoutToObserveTriggers := time.Second * 15
	// allow cron to trigger once before stopping the sidecar
	waitBeforeSidecarStop := time.Second * 5
	// wait for some time after the app has stopped, before restarting the sidecar
	waitBeforeSidecarRestart := time.Second * 5

	clk := clock.NewMock()

	flow.New(t, "test cron trigger schedule @every 3s with sidecar restart").
		Step(app.Run(appName, fmt.Sprintf(":%d", appPort), appWithTriggerCounter(t, cronName, &observedTriggerCount))).
		Step(sidecar.Run(sidecarName,
			embedded.WithComponentsPath("./components"),
			embedded.WithDaprGRPCPort(grpcPort),
			embedded.WithAppProtocol(runtime.HTTPProtocol, appPort),
			embedded.WithDaprHTTPPort(httpPort),
			componentRuntimeOptions(clk),
		)).
		Step("allow cron to trigger once", addTimeToMockClock(clk, waitBeforeSidecarStop), flow.Sleep(waitBeforeSidecarStop)).
		Step("stop sidecar", sidecar.Stop(sidecarName)).
		Step("wait before sidecar restart", addTimeToMockClock(clk, waitBeforeSidecarRestart), flow.Sleep(waitBeforeSidecarRestart)).
		Step(sidecar.Run(sidecarName,
			embedded.WithComponentsPath("./components"),
			embedded.WithDaprGRPCPort(grpcPort),
			embedded.WithAppProtocol(runtime.HTTPProtocol, appPort),
			embedded.WithDaprHTTPPort(httpPort),
			componentRuntimeOptions(clk),
		)).
		Step("advance the clock time", addTimeToMockClock(clk, timeoutToObserveTriggers)).
		Step("assert cron triggered within deadline", assertTriggerCount(t, expectedTriggerCount, &observedTriggerCount)).
		Step("stop sidecar", sidecar.Stop(sidecarName)).
		Step("stop app", app.Stop(appName)).
		Run()
}

func appWithTriggerCounter(t *testing.T, cronName string, triggeredCount *int) func(ctx flow.Context, s common.Service) error {
	return func(ctx flow.Context, s common.Service) error {
		// Setup the input binding endpoint
		err := s.AddBindingInvocationHandler(cronName, func(_ context.Context, in *common.BindingEvent) ([]byte, error) {
			ctx.Logf("Cron triggered at %s", time.Now().String())
			(*triggeredCount)++
			return []byte("{}"), nil
		})
		require.NoError(t, err)
		return err
	}
}

func addTimeToMockClock(clk *clock.Mock, timeToAdd time.Duration) func(ctx flow.Context) error {
	return func(ctx flow.Context) error {
		clk.Add(timeToAdd)
		return nil
	}
}

func assertTriggerCount(t *testing.T, expectedTriggerCount int, observedTriggerCount *int) func(ctx flow.Context) error {
	return func(ctx flow.Context) error {
		// allow up to 1 extra trigger to account for additional timeout(@schedule interval of cron trigger) provided in the tests
		if *observedTriggerCount != expectedTriggerCount && *observedTriggerCount != expectedTriggerCount+1 {
			t.Errorf("expected %d triggers, got %d", expectedTriggerCount, *observedTriggerCount)
		}
		return nil
	}
}

func componentRuntimeOptions(clk clock.Clock) []runtime.Option {
	log := logger.NewLogger("dapr.components")

	bindingsRegistry := bindings_loader.NewRegistry()
	bindingsRegistry.Logger = log

	bindingsRegistry.RegisterInputBinding(func(l logger.Logger) bindings.InputBinding {
		return cron.NewCronWithClock(l, clk)
	}, "cron")
	return []runtime.Option{
		runtime.WithBindings(bindingsRegistry),
	}
}
