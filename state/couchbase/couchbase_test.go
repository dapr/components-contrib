// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package couchbase

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"gopkg.in/couchbase/gocb.v1"

	"github.com/dapr/components-contrib/state"
)

func TestValidateMetadata(t *testing.T) {
	t.Run("with mandatory fields", func(t *testing.T) {
		props := map[string]string{
			couchbaseURL: "foo://bar",
			username:     "kehsihba",
			password:     "secret",
			bucketName:   "testbucket",
		}
		metadata := state.Metadata{Properties: props}

		err := validateMetadata(metadata)
		assert.Equal(t, nil, err)
	})
	t.Run("with optional fields", func(t *testing.T) {
		props := map[string]string{
			couchbaseURL:                  "foo://bar",
			username:                      "kehsihba",
			password:                      "secret",
			bucketName:                    "testbucket",
			numReplicasDurablePersistence: "1",
			numReplicasDurableReplication: "2",
		}
		metadata := state.Metadata{Properties: props}

		err := validateMetadata(metadata)
		assert.Equal(t, nil, err)
	})
	t.Run("With missing couchbase URL", func(t *testing.T) {
		props := map[string]string{
			username:   "kehsihba",
			password:   "secret",
			bucketName: "testbucket",
		}
		metadata := state.Metadata{Properties: props}
		err := validateMetadata(metadata)
		assert.NotNil(t, err)
	})
	t.Run("With missing username", func(t *testing.T) {
		props := map[string]string{
			couchbaseURL: "foo://bar",
			password:     "secret",
			bucketName:   "testbucket",
		}
		metadata := state.Metadata{Properties: props}
		err := validateMetadata(metadata)
		assert.NotNil(t, err)
	})
	t.Run("With missing password", func(t *testing.T) {
		props := map[string]string{
			couchbaseURL: "foo://bar",
			username:     "kehsihba",
			bucketName:   "testbucket",
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
	t.Run("With invalid durable replication", func(t *testing.T) {
		props := map[string]string{
			couchbaseURL:                  "foo://bar",
			username:                      "kehsihba",
			password:                      "secret",
			numReplicasDurableReplication: "junk",
		}
		metadata := state.Metadata{Properties: props}
		err := validateMetadata(metadata)
		assert.NotNil(t, err)
	})
	t.Run("With invalid durable persistence", func(t *testing.T) {
		props := map[string]string{
			couchbaseURL:                  "foo://bar",
			username:                      "kehsihba",
			password:                      "secret",
			numReplicasDurablePersistence: "junk",
		}
		metadata := state.Metadata{Properties: props}
		err := validateMetadata(metadata)
		assert.NotNil(t, err)
	})
}

func TestETagToCas(t *testing.T) {
	t.Run("with valid string", func(t *testing.T) {
		casStr := "1572938024378368000"
		ver := uint64(1572938024378368000)
		var expectedCas gocb.Cas = gocb.Cas(ver)
		cas, err := eTagToCas(casStr)
		assert.Equal(t, nil, err)
		assert.Equal(t, expectedCas, cas)
	})
	t.Run("with empty string", func(t *testing.T) {
		_, err := eTagToCas("")
		assert.NotNil(t, err)
	})
}
