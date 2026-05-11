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

package etcd

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/state"
)

func TestGetEtcdMetadata(t *testing.T) {
	t.Run("With required configuration", func(t *testing.T) {
		properties := map[string]string{
			"endpoints": "127.0.0.1:2379",
		}
		m := state.Metadata{
			Base: metadata.Base{Properties: properties},
		}

		metadata, err := metadataToConfig(m.Properties)
		require.NoError(t, err)
		assert.Equal(t, properties["endpoints"], metadata.Endpoints)
	})

	t.Run("with optional configuration", func(t *testing.T) {
		properties := map[string]string{
			"endpoints":     "127.0.0.1:2379",
			"keyPrefixPath": "dapr",
			"tlsEnable":     "false",
		}
		m := state.Metadata{
			Base: metadata.Base{Properties: properties},
		}

		metadata, err := metadataToConfig(m.Properties)
		require.NoError(t, err)
		assert.Equal(t, properties["endpoints"], metadata.Endpoints)
		assert.Equal(t, properties["keyPrefixPath"], metadata.KeyPrefixPath)
		assert.Equal(t, properties["tlsEnable"], metadata.TLSEnable)
	})
}

func TestBulkGetEmpty(t *testing.T) {
	// Empty input must short-circuit without touching the etcd client.
	e := &Etcd{}
	res, err := e.BulkGet(t.Context(), nil, state.BulkGetOpts{})
	require.NoError(t, err)
	assert.Empty(t, res)
}

func TestFeatures(t *testing.T) {
	s := newETCD(nil, schemaV2{}).(*Etcd)
	assert.True(t, state.FeatureDeleteWithPrefix.IsPresent(s.Features()),
		"etcd must advertise FeatureDeleteWithPrefix for workflow purge fast path")
	assert.True(t, state.FeatureTransactional.IsPresent(s.Features()))
}
