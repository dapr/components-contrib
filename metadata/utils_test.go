// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package metadata

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIsRawPayload(t *testing.T) {
	t.Run("Metadata not found", func(t *testing.T) {
		val, err := IsRawPayload(map[string]string{
			"notfound": "1",
		})

		assert.Equal(t, false, val)
		assert.Nil(t, err)
	})

	t.Run("Metadata map is nil", func(t *testing.T) {
		val, err := IsRawPayload(nil)

		assert.Equal(t, false, val)
		assert.Nil(t, err)
	})

	t.Run("Metadata with bad value", func(t *testing.T) {
		val, err := IsRawPayload(map[string]string{
			"rawPayload": "Not a boolean",
		})

		assert.Equal(t, false, val)
		assert.NotNil(t, err)
	})

	t.Run("Metadata with correct value as false", func(t *testing.T) {
		val, err := IsRawPayload(map[string]string{
			"rawPayload": "false",
		})

		assert.Equal(t, false, val)
		assert.Nil(t, err)
	})

	t.Run("Metadata with correct value as true", func(t *testing.T) {
		val, err := IsRawPayload(map[string]string{
			"rawPayload": "true",
		})

		assert.Equal(t, true, val)
		assert.Nil(t, err)
	})
}
