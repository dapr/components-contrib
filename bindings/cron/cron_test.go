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
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/stretchr/testify/assert"

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
	clk := clock.New()
	return getNewCronWithClock(clk)
}

func getNewCronWithClock(clk clock.Clock) *Binding {
	l := logger.NewLogger("cron")
	if os.Getenv("DEBUG") != "" {
		l.SetOutputLevel(logger.DebugLevel)
	}
	return NewCronWithClock(l, clk).(*Binding)
}

// go test -v -timeout 15s -count=1 ./bindings/cron/.
func TestCronInitSuccess(t *testing.T) {
	c := getNewCron()
	err := c.Init(getTestMetadata("@every 1h"))
	assert.NoErrorf(t, err, "error initializing valid schedule")
}

func TestCronInitWithSeconds(t *testing.T) {
	c := getNewCron()
	err := c.Init(getTestMetadata("15 * * * * *"))
	assert.NoErrorf(t, err, "error initializing schedule with seconds")
}

func TestCronInitFailure(t *testing.T) {
	c := getNewCron()
	err := c.Init(getTestMetadata("invalid schedule"))
	assert.Errorf(t, err, "no error while initializing invalid schedule")
}

// TestLongRead
// go test -v -count=1 -timeout 15s -run TestLongRead ./bindings/cron/.
func TestCronRead(t *testing.T) {
	clk := clock.NewMock()
	c := getNewCronWithClock(clk)
	schedule := "@every 1s"
	assert.NoErrorf(t, c.Init(getTestMetadata(schedule)), "error initializing valid schedule")
	expectedCount := 5
	observedCount := 0
	err := c.Read(context.Background(), func(ctx context.Context, res *bindings.ReadResponse) ([]byte, error) {
		assert.NotNil(t, res)
		observedCount++
		return nil, nil
	})
	// Check if cron triggers 5 times in 5 seconds
	clk.Add(5 * time.Second)
	assert.Equal(t, expectedCount, observedCount, "Cron did not trigger expected number of times, expected %d, got %d", expectedCount, observedCount)
	assert.NoErrorf(t, err, "error on read")
}

func TestCronReadWithContextCancellation(t *testing.T) {
	clk := clock.NewMock()
	c := getNewCronWithClock(clk)
	schedule := "@every 1s"
	assert.NoErrorf(t, c.Init(getTestMetadata(schedule)), "error initializing valid schedule")
	expectedCount := 5
	observedCount := 0
	ctx, cancel := context.WithCancel(context.Background())
	err := c.Read(ctx, func(ctx context.Context, res *bindings.ReadResponse) ([]byte, error) {
		assert.NotNil(t, res)
		assert.LessOrEqualf(t, observedCount, expectedCount, "Invoke didn't stop the schedule")
		observedCount++
		if observedCount == expectedCount {
			// Cancel context after 5 triggers
			cancel()
		}
		return nil, nil
	})
	// Check if cron triggers only 5 times in 10 seconds since context should be cancelled after 5 triggers
	clk.Add(10 * time.Second)
	assert.Equal(t, expectedCount, observedCount, "Cron did not trigger expected number of times, expected %d, got %d", expectedCount, observedCount)
	assert.NoErrorf(t, err, "error on read")
}
