// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package state

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestSetRequestWithOptions is used to test request options.
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

// TestCheckRequestOptions is used to validate request options.
func TestCheckRequestOptions(t *testing.T) {
	t.Run("set state options", func(t *testing.T) {
		ro := SetStateOption{Concurrency: FirstWrite, Consistency: Eventual}
		err := CheckRequestOptions(ro)
		assert.NoError(t, err)
	})
	t.Run("delete state options", func(t *testing.T) {
		ro := DeleteStateOption{Concurrency: FirstWrite, Consistency: Eventual}
		err := CheckRequestOptions(ro)
		assert.NoError(t, err)
	})
	t.Run("get state options", func(t *testing.T) {
		ro := GetStateOption{Consistency: Eventual}
		err := CheckRequestOptions(ro)
		assert.NoError(t, err)
	})
	t.Run("invalid state options", func(t *testing.T) {
		ro := SetStateOption{Concurrency: "invalid", Consistency: Eventual}
		err := CheckRequestOptions(ro)
		assert.Error(t, err)
	})
}
