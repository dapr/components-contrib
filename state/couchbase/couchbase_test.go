// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package couchbase

import (
	"testing"

	"github.com/dapr/components-contrib/state"

	"github.com/stretchr/testify/assert"
)

func TestValidateMetadata(t *testing.T) {
	t.Run("with all fields", func(t *testing.T) {
		props := map[string]string{
			couchbaseURL: "foo://bar",
			username:     "kehsihba",
			password:     "secret",
			bucket:       "testbucket",
		}
		metadata := state.Metadata{Properties: props}

		err := validateMetadata(metadata)
		assert.Equal(t, nil, err)
	})
	t.Run("With missing couchbase URL", func(t *testing.T) {
		props := map[string]string{
			username: "kehsihba",
			password: "secret",
			bucket:   "testbucket",
		}
		metadata := state.Metadata{Properties: props}
		err := validateMetadata(metadata)
		assert.NotNil(t, err)
	})
	t.Run("With missing username", func(t *testing.T) {
		props := map[string]string{
			couchbaseURL: "foo://bar",
			password:     "secret",
			bucket:       "testbucket",
		}
		metadata := state.Metadata{Properties: props}
		err := validateMetadata(metadata)
		assert.NotNil(t, err)
	})
	t.Run("With missing password", func(t *testing.T) {
		props := map[string]string{
			couchbaseURL: "foo://bar",
			username:     "kehsihba",
			bucket:       "testbucket",
		}
		metadata := state.Metadata{Properties: props}
		err := validateMetadata(metadata)
		assert.NotNil(t, err)
	})
	t.Run("With missing bucket", func(t *testing.T) {
		props := map[string]string{
			couchbaseURL: "foo://bar",
			username:     "kehsihba",
			password:     "secret",
		}
		metadata := state.Metadata{Properties: props}
		err := validateMetadata(metadata)
		assert.NotNil(t, err)
	})
}
