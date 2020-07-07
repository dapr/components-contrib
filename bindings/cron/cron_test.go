// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package twitter

import (
	"os"
	"testing"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/dapr/pkg/logger"
	"github.com/stretchr/testify/assert"
)

func getTestMetadata(schedule string) bindings.Metadata {
	m := bindings.Metadata{}
	m.Properties = map[string]string{
		"schedule": schedule,
	}
	return m
}

// go test -v -timeout 15s -count=1 ./bindings/cron/
func TestInitSuccess(t *testing.T) {
	c := NewCron(logger.NewLogger("test"))
	err := c.Init(getTestMetadata("@every 1h"))
	assert.Nilf(t, err, "error initializing valid schedule")
}

func TestInitFailure(t *testing.T) {
	c := NewCron(logger.NewLogger("test"))
	err := c.Init(getTestMetadata("invalid schedule"))
	assert.NotNilf(t, err, "no error while initializing invalid schedule")
}

// TestRead excutes the Read method
// go test -v -count=1 -timeout 15s -run TestRead ./bindings/cron/
func TestRead(t *testing.T) {
	l := logger.NewLogger("test")
	l.SetOutputLevel(logger.DebugLevel)
	c := NewCron(l)
	err := c.Init(getTestMetadata("@every 1s"))
	assert.Nilf(t, err, "error initializing valid schedule")

	h := func(res *bindings.ReadResponse) error {
		assert.NotNil(t, res)
		os.Exit(0)
		return nil
	}

	err = c.Read(h)
	assert.Nilf(t, err, "error on read")
}
