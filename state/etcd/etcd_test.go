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
		assert.NoError(t, err)
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
		assert.NoError(t, err)
		assert.Equal(t, properties["endpoints"], metadata.Endpoints)
		assert.Equal(t, properties["keyPrefixPath"], metadata.KeyPrefixPath)
		assert.Equal(t, properties["tlsEnable"], metadata.TLSEnable)
	})
}
