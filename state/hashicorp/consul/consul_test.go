// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package consul

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/dapr/components-contrib/state"
)

func TestGetConsulMetadata(t *testing.T) {
	t.Run("With required configuration", func(t *testing.T) {
		properties := map[string]string{
			"datacenter": "dc1",
			"httpAddr":   "127.0.0.1:8500",
		}
		m := state.Metadata{
			Properties: properties,
		}

		metadata, err := metadataToConfig(m.Properties)
		assert.Nil(t, err)
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
			Properties: properties,
		}

		metadata, err := metadataToConfig(m.Properties)
		assert.Nil(t, err)
		assert.Equal(t, properties["datacenter"], metadata.Datacenter)
		assert.Equal(t, properties["httpAddr"], metadata.HTTPAddr)
		assert.Equal(t, properties["aclToken"], metadata.ACLToken)
		assert.Equal(t, properties["scheme"], metadata.Scheme)
		assert.Equal(t, properties["keyPrefixPath"], metadata.KeyPrefixPath)
	})
}
