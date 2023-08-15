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

//nolint:dupword
package cron_test

import (
	"context"
	"errors"
	"fmt"
	goruntime "runtime"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/utils/clock"
	clocktesting "k8s.io/utils/clock/testing"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/components-contrib/bindings/cron"
	"github.com/dapr/dapr/pkg/config/protocol"
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
	cronName             string                  // name of the cron binding
	schedule             string                  // cron schedule
	expectedTriggerCount int64                   // expected number of triggers within the deadline
	step                 time.Duration           // duration to advance the mock clock every time
	testDuration         time.Duration           // test duration (in the mock clock)
	clk                  *clocktesting.FakeClock // mock clock
}

// starting time for the mock clock
var startTime = time.Date(2022, time.January, 1, 0, 0, 0, 0, time.UTC)

// Test cron triggers with different schedules
func TestCronBindingTrigger(t *testing.T) {
	const appName = "cronapp"
	const sidecarName = "cron-sidecar"

	testMatrix := []cronTest{
		{
			cronName:             "cron1s",
			schedule:             "@every 1s", // Test macro cron format
			expectedTriggerCount: 10,
			step:                 time.Second / 5,
			testDuration:         time.Second * 10,
		},
		{
			cronName:             "cron3s",
			schedule:             "*/3 * * * * *", // Test non-standard crontab format
			expectedTriggerCount: 10,
			step:                 time.Second,
			testDuration:         time.Second * 30,
		},
		{
			cronName:             "cron15m",
			schedule:             "*/15 * * * *", // Test standard crontab format
			expectedTriggerCount: 12,
			step:                 30 * time.Second,
			testDuration:         time.Hour * 3,
		},
		{
			cronName:             "cron6h",
			schedule:             "0 0 */6 ? * *", // Test quartz cron format
			expectedTriggerCount: 12,
			step:                 time.Minute,
			testDuration:         time.Hour * 24 * 3,
		},
	}

	for _, cronTest := range testMatrix {
		t.Run(cronTest.cronName, func(t *testing.T) {
			cronTest.clk = clocktesting.NewFakeClock(startTime)

			ports, _ := dapr_testing.GetFreePorts(3)
			grpcPort := ports[0]
			httpPort := ports[1]
			appPort := ports[2]

			testFn, triggeredCb := testerFn(cronTest.clk, cronTest.testDuration, cronTest.expectedTriggerCount, cronTest.step)

			flow.New(t, "test cron trigger with different schedules").
				Step(app.Run(appName,
					fmt.Sprintf(":%d", appPort),
					appWithTriggerCounter(t, cronTest.clk, cronTest.cronName, triggeredCb),
				)).
				Step(sidecar.Run(sidecarName,
					append(componentRuntimeOptions(cronTest.clk),
						embedded.WithResourcesPath("./components"),
						embedded.WithDaprGRPCPort(strconv.Itoa(grpcPort)),
						embedded.WithAppProtocol(protocol.HTTPProtocol, strconv.Itoa(appPort)),
						embedded.WithDaprHTTPPort(strconv.Itoa(httpPort)),
					)...,
				)).
				Step("run test", testFn).
				Step("stop sidecar", sidecar.Stop(sidecarName)).
				Step("stop app", app.Stop(appName)).
				Run()
		})
	}
}

// Test two cron bindings having different schedules @every 1s and @every 3s triggering the same app route
func TestCronBindingsWithSameRoute(t *testing.T) {
	ports, _ := dapr_testing.GetFreePorts(3)
	grpcPort := ports[0]
	httpPort := ports[1]
	appPort := ports[2]

	const cronName = "cron"
	const appName = "cronapp"
	const sidecarName = "cron-sidecar"

	// check if cron triggers 20 times within 15 seconds (15 times from @every 1s binding and 5 times from @every 3s binding)
	const expectedTriggerCount = 20
	// total time for all triggers to be observed
	const testDuration = time.Second * 15

	clk := clocktesting.NewFakeClock(startTime)

	testFn, triggeredCb := testerFn(clk, testDuration, expectedTriggerCount, time.Second/2)

	flow.New(t, "test cron bindings with different schedules and same route").
		Step(app.Run(appName,
			fmt.Sprintf(":%d", appPort),
			appWithTriggerCounter(t, clk, cronName, triggeredCb),
		)).
		Step(sidecar.Run(sidecarName,
			append(componentRuntimeOptions(clk),
				embedded.WithResourcesPath("./components_sameroute"),
				embedded.WithDaprGRPCPort(strconv.Itoa(grpcPort)),
				embedded.WithAppProtocol(protocol.HTTPProtocol, strconv.Itoa(appPort)),
				embedded.WithDaprHTTPPort(strconv.Itoa(httpPort)),
			)...,
		)).
		Step("run test", testFn).
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

	const cronName = "cron3s"
	const appName = "cronapp3s"
	const sidecarName = "cron-sidecar"

	// check if cron triggers 5 times within 15 seconds
	const expectedTriggerCount = 5
	// total time for all triggers to be observed
	const testDuration = time.Second * 15
	// allow cron to trigger once before stopping the app
	const waitBeforeAppStop = time.Second * 5
	// wait for some time after the app has stopped, before restarting the app
	const waitBeforeAppRestart = time.Second * 5

	clk := clocktesting.NewFakeClock(startTime)

	observedTriggerCountCh := make(chan int64)
	counterFn, triggeredCb := counterFn(clk, testDuration, time.Second/2)

	flow.New(t, "test cron trigger schedule @every3s with app restart").
		Step(app.Run(appName,
			fmt.Sprintf(":%d", appPort),
			appWithTriggerCounter(t, clk, cronName, triggeredCb),
		)).
		Step(sidecar.Run(sidecarName,
			append(componentRuntimeOptions(clk),
				embedded.WithResourcesPath("./components"),
				embedded.WithDaprGRPCPort(strconv.Itoa(grpcPort)),
				embedded.WithAppProtocol(protocol.HTTPProtocol, strconv.Itoa(appPort)),
				embedded.WithDaprHTTPPort(strconv.Itoa(httpPort)),
			)...,
		)).
		Step("start counting executions", func(ctx flow.Context) error {
			go func() {
				observedTriggerCountCh <- counterFn()
			}()
			return nil
		}).
		Step("allow cron to trigger once", flow.Sleep(waitBeforeAppStop)).
		Step("stop app", app.Stop(appName)).
		Step("wait before app restart", flow.Sleep(waitBeforeAppRestart)).
		Step(app.Run(appName,
			fmt.Sprintf(":%d", appPort),
			appWithTriggerCounter(t, clk, cronName, triggeredCb),
		)).
		Step("assert cron triggered within deadline", func(ctx flow.Context) error {
			// Assert number of executions
			// Allow up to 2 less or extra trigger to account for additional timeout(@schedule interval of cron trigger) provided in the tests or if unable to observe up to 2 triggers during app or sidecar restart
			assert.InDelta(ctx.T, float64(expectedTriggerCount), float64(<-observedTriggerCountCh), 2)

			switch {
			case ctx.T.Failed():
				return errors.New("test failed")
			default:
				return nil
			}
		}).
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

	const cronName = "cron3s"
	const appName = "cronapp3s"
	const sidecarName = "cron-sidecar"

	// check if cron triggers 5 times within 15 seconds
	const expectedTriggerCount = 5
	// total time for all triggers to be observed
	const testDuration = time.Second * 15
	// allow cron to trigger once before stopping the sidecar
	const waitBeforeSidecarStop = time.Second * 5
	// wait for some time after the app has stopped, before restarting the sidecar
	const waitBeforeSidecarRestart = time.Second * 5

	clk := clocktesting.NewFakeClock(startTime)

	observedTriggerCountCh := make(chan int64)
	counterFn, triggeredCb := counterFn(clk, testDuration, time.Second/2)

	flow.New(t, "test cron trigger schedule @every 3s with sidecar restart").
		Step(app.Run(appName,
			fmt.Sprintf(":%d", appPort),
			appWithTriggerCounter(t, clk, cronName, triggeredCb),
		)).
		Step(sidecar.Run(sidecarName,
			append(componentRuntimeOptions(clk),
				embedded.WithResourcesPath("./components"),
				embedded.WithDaprGRPCPort(strconv.Itoa(grpcPort)),
				embedded.WithAppProtocol(protocol.HTTPProtocol, strconv.Itoa(appPort)),
				embedded.WithDaprHTTPPort(strconv.Itoa(httpPort)),
			)...,
		)).
		Step("start counting executions", func(ctx flow.Context) error {
			go func() {
				observedTriggerCountCh <- counterFn()
			}()
			return nil
		}).
		Step("allow cron to trigger once", flow.Sleep(waitBeforeSidecarStop)).
		Step("stop sidecar", sidecar.Stop(sidecarName)).
		Step("wait before sidecar restart", flow.Sleep(waitBeforeSidecarRestart)).
		Step(sidecar.Run(sidecarName,
			append(componentRuntimeOptions(clk),
				embedded.WithResourcesPath("./components"),
				embedded.WithDaprGRPCPort(strconv.Itoa(grpcPort)),
				embedded.WithAppProtocol(protocol.HTTPProtocol, strconv.Itoa(appPort)),
				embedded.WithDaprHTTPPort(strconv.Itoa(httpPort)),
			)...,
		)).
		Step("assert cron triggered within deadline", func(ctx flow.Context) error {
			// Assert number of executions
			// Allow up to 2 less or extra trigger to account for additional timeout(@schedule interval of cron trigger) provided in the tests or if unable to observe up to 2 triggers during app or sidecar restart
			assert.InDelta(ctx.T, float64(expectedTriggerCount), float64(<-observedTriggerCountCh), 2)

			switch {
			case ctx.T.Failed():
				return errors.New("test failed")
			default:
				return nil
			}
		}).
		Step("stop sidecar", sidecar.Stop(sidecarName)).
		Step("stop app", app.Stop(appName)).
		Run()
}

func counterFn(clk *clocktesting.FakeClock, testDuration time.Duration, step time.Duration) (func() int64, func()) {
	observedTriggerCount := atomic.Int64{}
	triggeredCb := func() {
		observedTriggerCount.Add(1)
	}

	return func() int64 {
		// In background advance the clock and count the executions
		doneCh := make(chan struct{})
		go func() {
			end := clk.Now().Add(testDuration)
			// We can't use Before because we want to count the equal time too
			for !clk.Now().After(end) {
				clk.Step(step)

				// Sleep on the wall clock to allow goroutines to advance
				goruntime.Gosched()
				time.Sleep(time.Millisecond / 2)
			}

			// Close doneCh
			close(doneCh)
		}()

		// Sleep for 2s to allow goroutines to catch up if needed
		goruntime.Gosched()
		time.Sleep(2 * time.Second)

		// Wait for test to end
		<-doneCh

		return observedTriggerCount.Load()
	}, triggeredCb
}

func testerFn(clk *clocktesting.FakeClock, testDuration time.Duration, expectedTriggerCount int64, step time.Duration) (func(ctx flow.Context) error, func()) {
	counter, triggeredCb := counterFn(clk, testDuration, step)

	return func(ctx flow.Context) error {
		t := ctx.T

		// Count executions
		// This call blocks until the test duration
		observedTriggerCount := counter()

		// Assert number of executions
		assert.Equal(t, expectedTriggerCount, observedTriggerCount)

		switch {
		case t.Failed():
			return errors.New("test failed")
		default:
			return nil
		}
	}, triggeredCb
}

func appWithTriggerCounter(t *testing.T, clk clock.Clock, cronName string, triggeredCb func()) func(ctx flow.Context, s common.Service) error {
	return func(ctx flow.Context, s common.Service) error {
		// Setup the input binding endpoint
		err := s.AddBindingInvocationHandler(cronName, func(_ context.Context, in *common.BindingEvent) ([]byte, error) {
			ctx.Logf("Cron triggered at %s", clk.Now().String())
			triggeredCb()
			return []byte("{}"), nil
		})
		require.NoError(t, err)
		return err
	}
}

func componentRuntimeOptions(clk clock.Clock) []embedded.Option {
	log := logger.NewLogger("dapr.components")

	bindingsRegistry := bindings_loader.NewRegistry()
	bindingsRegistry.Logger = log

	bindingsRegistry.RegisterInputBinding(func(l logger.Logger) bindings.InputBinding {
		return cron.NewCronWithClock(l, clk)
	}, "cron")
	return []embedded.Option{
		embedded.WithBindings(bindingsRegistry),
	}
}
