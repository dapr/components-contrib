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

type cronTest struct {
	cronName                 string        // name of the cron binding
	schedule                 string        // cron schedule
	expectedTriggerCount     int           // expected number of triggers within the deadline
	timeoutToObserveTriggers time.Duration // time to add to the mock clock to observe triggers
	clk                      *clock.Mock   // mock clock
}

// Test cron triggers with different schedules
//
//nolint:dupword
func TestCronBindingTrigger(t *testing.T) {
	appName := "cronapp"
	sidecarName := "cron-sidecar"

	// starting time for the mock clock
	startTime := time.Date(2022, time.January, 1, 0, 0, 0, 0, time.UTC)

	testMatrix := []cronTest{
		{
			cronName:                 "cron1s",
			schedule:                 "@every 1s", // Test macro cron format
			expectedTriggerCount:     10,
			timeoutToObserveTriggers: time.Second * 10,
		},
		{
			cronName:                 "cron3s",
			schedule:                 "*/3 * * * * *", // Test non-standard crontab format
			expectedTriggerCount:     5,
			timeoutToObserveTriggers: time.Second * 15,
		},
		{
			cronName:                 "cron15m",
			schedule:                 "*/15 * * * *", // Test standard crontab format
			expectedTriggerCount:     4,
			timeoutToObserveTriggers: time.Hour * 1,
		},
		{
			cronName:                 "cron6h",
			schedule:                 "0 0 */6 ? * *", // Test quartz cron format
			expectedTriggerCount:     4,
			timeoutToObserveTriggers: time.Hour * 24,
		},
		{
			cronName:                 "cronMonthly",
			schedule:                 "0 0 1 * *", // Test standard cron format
			expectedTriggerCount:     1,
			timeoutToObserveTriggers: time.Hour * 24 * 31, // Add 31 days to the mock clock
		},
	}

	for _, cronTest := range testMatrix {
		cronTest.clk = clock.NewMock()
		cronTest.clk.Set(startTime)

		ports, _ := dapr_testing.GetFreePorts(3)
		grpcPort := ports[0]
		httpPort := ports[1]
		appPort := ports[2]

		// total times cron is triggered
		observedTriggerCount := 0

		flow.New(t, "test cron trigger with different schedules").
			Step(app.Run(appName, fmt.Sprintf(":%d", appPort), appWithTriggerCounter(t, cronTest.cronName, &observedTriggerCount))).
			Step(sidecar.Run(sidecarName,
				embedded.WithComponentsPath("./components"),
				embedded.WithDaprGRPCPort(grpcPort),
				embedded.WithAppProtocol(runtime.HTTPProtocol, appPort),
				embedded.WithDaprHTTPPort(httpPort),
				componentRuntimeOptions(cronTest.clk),
			)).
			Step("advance the clock time", addTimeToMockClock(cronTest.clk, cronTest.timeoutToObserveTriggers)).
			Step("assert cron triggered within deadline", assertTriggerCount(t, cronTest.expectedTriggerCount, &observedTriggerCount)).
			Step("stop sidecar", sidecar.Stop(sidecarName)).
			Step("stop app", app.Stop(appName)).
			Run()
	}
}

// Test two cron bindings having different schedules @every 1s and @every 3s triggering the same app route
func TestCronBindingsWithSameRoute(t *testing.T) {
	ports, _ := dapr_testing.GetFreePorts(3)
	grpcPort := ports[0]
	httpPort := ports[1]
	appPort := ports[2]

	cronName := "cron"
	appName := "cronapp"
	sidecarName := "cron-sidecar"

	// check if cron triggers 20 times within 15 seconds (15 times from @every 1s binding and 5 times from @every 3s binding)
	expectedTriggerCount := 20
	// total times cron is triggered
	observedTriggerCount := 0
	// total time for all triggers to be observed
	timeoutToObserveTriggers := time.Second * 15

	clk := clock.NewMock()

	flow.New(t, "test cron bindings with different schedules and same route").
		Step(app.Run(appName, fmt.Sprintf(":%d", appPort), appWithTriggerCounter(t, cronName, &observedTriggerCount))).
		Step(sidecar.Run(sidecarName,
			embedded.WithComponentsPath("./components_sameroute"),
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
