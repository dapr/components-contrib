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

package couchbase

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"gopkg.in/couchbase/gocb.v1"

	"github.com/dapr/components-contrib/metadata"
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
		metadata := state.Metadata{Base: metadata.Base{Properties: props}}

		meta, err := parseAndValidateMetadata(metadata)
		assert.Equal(t, nil, err)
		assert.Equal(t, props[couchbaseURL], meta.CouchbaseURL)
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
		metadata := state.Metadata{Base: metadata.Base{Properties: props}}

		meta, err := parseAndValidateMetadata(metadata)
		assert.Equal(t, nil, err)
		assert.Equal(t, props[couchbaseURL], meta.CouchbaseURL)
		assert.Equal(t, props[numReplicasDurablePersistence], fmt.Sprintf("%d", meta.NumReplicasDurablePersistence))
	})
	t.Run("With missing couchbase URL", func(t *testing.T) {
		props := map[string]string{
			username:   "kehsihba",
			password:   "secret",
			bucketName: "testbucket",
		}
		metadata := state.Metadata{Base: metadata.Base{Properties: props}}
		_, err := parseAndValidateMetadata(metadata)
		assert.NotNil(t, err)
	})
	t.Run("With missing username", func(t *testing.T) {
		props := map[string]string{
			couchbaseURL: "foo://bar",
			password:     "secret",
			bucketName:   "testbucket",
		}
		metadata := state.Metadata{Base: metadata.Base{Properties: props}}
		_, err := parseAndValidateMetadata(metadata)
		assert.NotNil(t, err)
	})
	t.Run("With missing password", func(t *testing.T) {
		props := map[string]string{
			couchbaseURL: "foo://bar",
			username:     "kehsihba",
			bucketName:   "testbucket",
		}
		metadata := state.Metadata{Base: metadata.Base{Properties: props}}
		_, err := parseAndValidateMetadata(metadata)
		assert.NotNil(t, err)
	})
	t.Run("With missing bucket", func(t *testing.T) {
		props := map[string]string{
			couchbaseURL: "foo://bar",
			username:     "kehsihba",
			password:     "secret",
		}
		metadata := state.Metadata{Base: metadata.Base{Properties: props}}
		_, err := parseAndValidateMetadata(metadata)
		assert.NotNil(t, err)
	})
	t.Run("With invalid durable replication", func(t *testing.T) {
		props := map[string]string{
			couchbaseURL:                  "foo://bar",
			username:                      "kehsihba",
			password:                      "secret",
			numReplicasDurableReplication: "junk",
		}
		metadata := state.Metadata{Base: metadata.Base{Properties: props}}
		_, err := parseAndValidateMetadata(metadata)
		assert.NotNil(t, err)
	})
	t.Run("With invalid durable persistence", func(t *testing.T) {
		props := map[string]string{
			couchbaseURL:                  "foo://bar",
			username:                      "kehsihba",
			password:                      "secret",
			numReplicasDurablePersistence: "junk",
		}
		metadata := state.Metadata{Base: metadata.Base{Properties: props}}
		_, err := parseAndValidateMetadata(metadata)
		assert.NotNil(t, err)
	})
}

func TestETagToCas(t *testing.T) {
	t.Run("with valid string", func(t *testing.T) {
		casStr := "1572938024378368000"
		ver := uint64(1572938024378368000)
		expectedCas := gocb.Cas(ver)
		cas, err := eTagToCas(casStr)
		assert.Equal(t, nil, err)
		assert.Equal(t, expectedCas, cas)
	})
	t.Run("with empty string", func(t *testing.T) {
		_, err := eTagToCas("")
		assert.NotNil(t, err)
	})
}
