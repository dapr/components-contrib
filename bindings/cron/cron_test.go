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
	l := logger.NewLogger("cron")
	if os.Getenv("DEBUG") != "" {
		l.SetOutputLevel(logger.DebugLevel)
	}

	return NewCron(l).(*Binding)
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
	c := getNewCron()
	schedule := "@every 1s"
	assert.NoErrorf(t, c.Init(getTestMetadata(schedule)), "error initializing valid schedule")
	i := 0
	err := c.Read(context.Background(), func(ctx context.Context, res *bindings.ReadResponse) ([]byte, error) {
		assert.NotNil(t, res)
		i++
		return nil, nil
	})
	time.Sleep(time.Second * 5)
	// Check if cron triggers at least twice within 5 seconds
	assert.GreaterOrEqual(t, i, 2, "Cron did not trigger enough times")
	assert.NoErrorf(t, err, "error on read")
}