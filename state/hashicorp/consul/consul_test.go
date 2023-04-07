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

package consul

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/state"
)

func TestGetConsulMetadata(t *testing.T) {
	t.Run("With required configuration", func(t *testing.T) {
		properties := map[string]string{
			"datacenter": "dc1",
			"httpAddr":   "127.0.0.1:8500",
		}
		m := state.Metadata{
			Base: metadata.Base{Properties: properties},
		}

		metadata, err := metadataToConfig(m.Properties)
		assert.NoError(t, err)
		assert.Equal(t, properties["datacenter"], metadata.Datacenter)
		assert.Equal(t, properties["httpAddr"], metadata.HTTPAddr)
	})

	t.Run("with optional configuration", func(t *testing.T) {
		properties := map[string]string{
			"datacenter":    "dc1",
			"httpAddr":      "127.0.0.1:8500",
			"aclToken":      "abcde",
			"scheme":        "http",
			"keyPrefixPath": "example/path/key",
		}
		m := state.Metadata{
			Base: metadata.Base{Properties: properties},
		}

		metadata, err := metadataToConfig(m.Properties)
		assert.NoError(t, err)
		assert.Equal(t, properties["datacenter"], metadata.Datacenter)
		assert.Equal(t, properties["httpAddr"], metadata.HTTPAddr)
		assert.Equal(t, properties["aclToken"], metadata.ACLToken)
		assert.Equal(t, properties["scheme"], metadata.Scheme)
		assert.Equal(t, properties["keyPrefixPath"], metadata.KeyPrefixPath)
	})
}
