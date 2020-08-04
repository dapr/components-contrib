// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package state

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestSetRequestWithOptions is used to test request options
func TestSetRequestWithOptions(t *testing.T) {
	t.Run("set with default options", func(t *testing.T) {
		counter := 0
		SetWithOptions(func(req *SetRequest) error {
			counter++
			return nil
		}, &SetRequest{})
		assert.Equal(t, 1, counter, "should execute only once")
	})

	t.Run("set with no explicit options", func(t *testing.T) {
		counter := 0
		SetWithOptions(func(req *SetRequest) error {
			counter++
			return nil
		}, &SetRequest{
			Options: SetStateOption{},
		})
		assert.Equal(t, 1, counter, "should execute only once")
	})
}
