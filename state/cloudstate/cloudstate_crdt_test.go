// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package cloudstate

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/dapr/components-contrib/state"
	"github.com/dapr/kit/logger"
)

func TestMetadata(t *testing.T) {
	testLogger := logger.NewLogger("test")
	t.Run("With all required fields", func(t *testing.T) {
		properties := map[string]string{
			"host":       "localhost:8080",
			"serverPort": "9002",
		}
		c := NewCRDT(testLogger)
		err := c.Init(state.Metadata{
			Properties: properties,
		})
		assert.Nil(t, err)
	})

	t.Run("Missing host", func(t *testing.T) {
		properties := map[string]string{
			"serverPort": "9002",
		}
		c := NewCRDT(testLogger)
		err := c.Init(state.Metadata{
			Properties: properties,
		})
		assert.NotNil(t, err)
	})

	t.Run("Missing serverPort", func(t *testing.T) {
		properties := map[string]string{
			"host": "localhost:8080",
		}
		c := NewCRDT(testLogger)
		err := c.Init(state.Metadata{
			Properties: properties,
		})
		assert.NotNil(t, err)
	})
}
