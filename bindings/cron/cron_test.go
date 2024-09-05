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

package cron

import (
	"context"
	"os"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/utils/clock"
	clocktesting "k8s.io/utils/clock/testing"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/kit/logger"
)

func getTestMetadata(schedule string) bindings.Metadata {
	m := bindings.Metadata{}
	m.Properties = map[string]string{
		"schedule": schedule,
	}

	return m
}

func getNewCron() *Binding {
	clk := clocktesting.NewFakeClock(time.Now())
	return getNewCronWithClock(clk)
}

func getNewCronWithClock(clk clock.Clock) *Binding {
	l := logger.NewLogger("cron")
	if os.Getenv("DEBUG") != "" {
		l.SetOutputLevel(logger.DebugLevel)
	}
	return NewCronWithClock(l, clk).(*Binding)
}

func TestCronInitSuccess(t *testing.T) {
	initTests := []struct {
		schedule      string
		errorExpected bool
	}{
		{
			schedule:      "@every 1s", // macro cron format
			errorExpected: false,
		},
		{
			schedule:      "*/3 * * * * *", // non standard cron format
			errorExpected: false,
		},
		{
			schedule:      "*/15 * * * *", // standard cron format
			errorExpected: false,
		},
		{
			schedule:      "0 0 1 * *", // standard cron format
			errorExpected: false,
		},
		{
			schedule:      "0 0 */6 ? * *", // quartz cron format
			errorExpected: false,
		},
		{
			schedule:      "INVALID_SCHEDULE", // invalid cron format
			errorExpected: true,
		},
	}

	for _, test := range initTests {
		c := getNewCron()
		err := c.Init(context.Background(), getTestMetadata(test.schedule))
		if test.errorExpected {
			require.Errorf(t, err, "Got no error while initializing an invalid schedule: %s", test.schedule)
		} else {
			require.NoErrorf(t, err, "error initializing valid schedule: %s", test.schedule)
		}
	}
}

// TestLongRead
// go test -v -count=1 -timeout 15s -run TestLongRead ./bindings/cron/.
func TestCronRead(t *testing.T) {
	clk := clocktesting.NewFakeClock(time.Now())
	c := getNewCronWithClock(clk)
	schedule := "@every 1s"
	require.NoErrorf(t, c.Init(context.Background(), getTestMetadata(schedule)), "error initializing valid schedule")
	expectedCount := int32(5)
	var observedCount atomic.Int32
	err := c.Read(context.Background(), func(ctx context.Context, res *bindings.ReadResponse) ([]byte, error) {
		assert.NotNil(t, res)
		observedCount.Add(1)
		return nil, nil
	})
	// Check if cron triggers 5 times in 5 seconds
	for range expectedCount {
		// Add time to mock clock in 1 second intervals using loop to allow cron go routine to run
		clk.Step(time.Second)
		runtime.Gosched()
		time.Sleep(100 * time.Millisecond)
	}
	// Wait for 1 second after adding the last second to mock clock to allow cron to finish triggering
	assert.Eventually(t, func() bool {
		return observedCount.Load() == expectedCount
	}, time.Second, time.Millisecond*10,
		"Cron did not trigger expected number of times, expected %d, got %d", expectedCount, observedCount.Load())
	require.NoErrorf(t, err, "error on read")
	require.NoError(t, c.Close())
}

func TestCronReadWithContextCancellation(t *testing.T) {
	clk := clocktesting.NewFakeClock(time.Now())
	c := getNewCronWithClock(clk)
	schedule := "@every 1s"
	require.NoErrorf(t, c.Init(context.Background(), getTestMetadata(schedule)), "error initializing valid schedule")
	expectedCount := int32(5)
	var observedCount atomic.Int32
	ctx, cancel := context.WithCancel(context.Background())
	err := c.Read(ctx, func(ctx context.Context, res *bindings.ReadResponse) ([]byte, error) {
		assert.NotNil(t, res)
		assert.LessOrEqualf(t, observedCount.Load(), expectedCount, "Invoke didn't stop the schedule")
		observedCount.Add(1)
		if observedCount.Load() == expectedCount {
			// Cancel context after 5 triggers
			cancel()
		}
		return nil, nil
	})
	// Check if cron triggers only 5 times in 10 seconds since context should be cancelled after 5 triggers
	for range 10 {
		// Add time to mock clock in 1 second intervals using loop to allow cron go routine to run
		clk.Step(time.Second)
		runtime.Gosched()
		time.Sleep(100 * time.Millisecond)
	}
	assert.Eventually(t, func() bool {
		return observedCount.Load() == expectedCount
	}, time.Second, time.Millisecond*10,
		"Cron did not trigger expected number of times, expected %d, got %d", expectedCount, observedCount.Load())
	require.NoErrorf(t, err, "error on read")
	require.NoError(t, c.Close())
}
