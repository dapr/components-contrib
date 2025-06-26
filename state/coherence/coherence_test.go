/*
Copyright 2025 The Dapr Authors
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

package coherence

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/state"
)

func TestValidateBaseMetadata(t *testing.T) {
	t.Run("no configuration all defaults", func(t *testing.T) {
		properties := map[string]string{}
		m := state.Metadata{
			Base: metadata.Base{Properties: properties},
		}
		meta, err := retrieveCoherenceMetadata(m)
		require.NoError(t, err)
		assert.False(t, meta.TLSEnabled)
		assert.Equal(t, "localhost:1408", meta.ServerAddress)
		assert.Equal(t, time.Duration(30)*time.Second, meta.RequestTimeout)
		assert.Equal(t, defaultScopeNameConfig, meta.ScopeName)
		assert.Equal(t, time.Duration(0), meta.NearCacheTTL)
		assert.Equal(t, int64(0), meta.NearCacheUnits)
		assert.Equal(t, int64(0), meta.NearCacheMemory)
	})

	t.Run("without valid request timeout", func(t *testing.T) {
		properties := map[string]string{
			"requestTimeout": "more rubbish",
		}
		m := state.Metadata{
			Base: metadata.Base{Properties: properties},
		}
		_, err := retrieveCoherenceMetadata(m)
		require.Error(t, err)
	})

	t.Run("with valid request timeout", func(t *testing.T) {
		properties := map[string]string{
			requestTimeoutConfig: "35s",
		}
		m := state.Metadata{
			Base: metadata.Base{Properties: properties},
		}
		meta, err := retrieveCoherenceMetadata(m)
		require.NoError(t, err)
		assert.Equal(t, time.Duration(35000)*time.Millisecond, meta.RequestTimeout)
	})

	t.Run("without default request timeout", func(t *testing.T) {
		properties := map[string]string{}
		m := state.Metadata{
			Base: metadata.Base{Properties: properties},
		}
		meta, err := retrieveCoherenceMetadata(m)
		require.NoError(t, err)
		assert.Equal(t, time.Duration(30)*time.Second, meta.RequestTimeout)
	})
}

func TestNTLSMetadata(t *testing.T) {
	t.Run("without valid tlsEnabled", func(t *testing.T) {
		properties := map[string]string{
			tlsEnabledConfig: "rubbish",
		}
		m := state.Metadata{
			Base: metadata.Base{Properties: properties},
		}
		_, err := retrieveCoherenceMetadata(m)
		require.Error(t, err)
	})

	t.Run("without valid ignoreInvalidCerts", func(t *testing.T) {
		properties := map[string]string{
			ignoreInvalidCerts: "rubbish",
		}
		m := state.Metadata{
			Base: metadata.Base{Properties: properties},
		}
		_, err := retrieveCoherenceMetadata(m)
		require.Error(t, err)
	})

	t.Run("with tlsEnabled but no certs", func(t *testing.T) {
		properties := map[string]string{
			tlsEnabledConfig: "true",
		}
		m := state.Metadata{
			Base: metadata.Base{Properties: properties},
		}
		_, err := retrieveCoherenceMetadata(m)
		require.Error(t, err)
	})

	t.Run("with valid tlsEnabled", func(t *testing.T) {
		properties := map[string]string{
			tlsEnabledConfig:        "true",
			tlsClientKeyConfig:      "keyConfig",
			tlsClientCertPathConfig: "certConfig",
			tlsCertsPathConfig:      "certsPath",
		}
		m := state.Metadata{
			Base: metadata.Base{Properties: properties},
		}
		meta, err := retrieveCoherenceMetadata(m)
		require.NoError(t, err)
		assert.True(t, meta.TLSEnabled)
		assert.Equal(t, "keyConfig", meta.TLSClientKey)
		assert.Equal(t, "certConfig", meta.TLSClientCertPath)
		assert.Equal(t, "certsPath", meta.TLSCertsPath)
	})

	t.Run("with valid false tls", func(t *testing.T) {
		properties := map[string]string{
			"tlsEnabled": "false",
		}
		m := state.Metadata{
			Base: metadata.Base{Properties: properties},
		}
		meta, err := retrieveCoherenceMetadata(m)
		require.NoError(t, err)
		assert.False(t, meta.TLSEnabled)
	})
}

func TestNearCacheMetadata(t *testing.T) {
	t.Run("with valid near cache ttl", func(t *testing.T) {
		properties := map[string]string{
			nearCacheTTLConfig: "30s",
		}
		m := state.Metadata{
			Base: metadata.Base{Properties: properties},
		}
		meta, err := retrieveCoherenceMetadata(m)
		require.NoError(t, err)
		assert.Equal(t, time.Duration(30)*time.Second, meta.NearCacheTTL)
		assert.Equal(t, int64(0), meta.NearCacheMemory)
		assert.Equal(t, int64(0), meta.NearCacheUnits)
	})

	t.Run("with invalid near cache ttl", func(t *testing.T) {
		properties := map[string]string{
			nearCacheTTLConfig: "more rubbish",
		}
		m := state.Metadata{
			Base: metadata.Base{Properties: properties},
		}
		_, err := retrieveCoherenceMetadata(m)
		require.Error(t, err)
	})

	t.Run("with invalid units and memory", func(t *testing.T) {
		properties := map[string]string{
			nearCacheMemoryConfig: "300000",
			nearCacheUnitsConfig:  "300000",
		}
		m := state.Metadata{
			Base: metadata.Base{Properties: properties},
		}
		_, err := retrieveCoherenceMetadata(m)
		require.Error(t, err)
	})

	t.Run("with valid near cache ttl and units", func(t *testing.T) {
		properties := map[string]string{
			nearCacheTTLConfig:   "30s",
			nearCacheUnitsConfig: "300000",
		}
		m := state.Metadata{
			Base: metadata.Base{Properties: properties},
		}
		meta, err := retrieveCoherenceMetadata(m)
		require.NoError(t, err)
		assert.Equal(t, time.Duration(30)*time.Second, meta.NearCacheTTL)
		assert.Equal(t, int64(300000), meta.NearCacheUnits)
		assert.Equal(t, int64(0), meta.NearCacheMemory)
	})

	t.Run("with valid near cache ttl and memory", func(t *testing.T) {
		properties := map[string]string{
			nearCacheTTLConfig:    "30s",
			nearCacheMemoryConfig: "310000",
		}
		m := state.Metadata{
			Base: metadata.Base{Properties: properties},
		}
		meta, err := retrieveCoherenceMetadata(m)
		require.NoError(t, err)
		assert.Equal(t, time.Duration(30)*time.Second, meta.NearCacheTTL)
		assert.Equal(t, int64(310000), meta.NearCacheMemory)
		assert.Equal(t, int64(0), meta.NearCacheUnits)
	})

	t.Run("with invalid units", func(t *testing.T) {
		properties := map[string]string{
			nearCacheUnitsConfig: "xyz",
		}
		m := state.Metadata{
			Base: metadata.Base{Properties: properties},
		}
		_, err := retrieveCoherenceMetadata(m)
		require.Error(t, err)
	})

	t.Run("with invalid memory", func(t *testing.T) {
		properties := map[string]string{
			nearCacheMemoryConfig: "abc",
		}
		m := state.Metadata{
			Base: metadata.Base{Properties: properties},
		}
		_, err := retrieveCoherenceMetadata(m)
		require.Error(t, err)
	})
}
