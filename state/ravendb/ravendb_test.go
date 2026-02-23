package ravendb

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/state"
)

func TestGetRavenDBMetadata(t *testing.T) {
	t.Run("With default database name", func(t *testing.T) {
		properties := map[string]string{
			serverURL: "127.0.0.1",
		}
		m := state.Metadata{
			Base: metadata.Base{Properties: properties},
		}

		metadata, err := getRavenDBMetaData(m)
		require.NoError(t, err)
		assert.Equal(t, properties[serverURL], metadata.ServerURL)
		assert.Equal(t, defaultDatabaseName, metadata.DatabaseName)
		assert.Equal(t, defaultEnableTTL, metadata.EnableTTL)
		assert.Equal(t, defaultTTLFrequency, metadata.TTLFrequency)
	})

	t.Run("With custom database name", func(t *testing.T) {
		properties := map[string]string{
			serverURL:    "127.0.0.1",
			databaseName: "TestDB",
		}
		m := state.Metadata{
			Base: metadata.Base{Properties: properties},
		}

		metadata, err := getRavenDBMetaData(m)
		require.NoError(t, err)
		assert.Equal(t, properties[serverURL], metadata.ServerURL)
		assert.Equal(t, properties[databaseName], metadata.DatabaseName)
		assert.Equal(t, defaultEnableTTL, metadata.EnableTTL)
		assert.Equal(t, defaultTTLFrequency, metadata.TTLFrequency)
	})

	t.Run("With custom enable ttl values", func(t *testing.T) {
		properties := map[string]string{
			serverURL:    "127.0.0.1",
			databaseName: "TestDB",
			enableTTL:    "false",
			ttlFrequency: "15",
		}
		m := state.Metadata{
			Base: metadata.Base{Properties: properties},
		}

		metadata, err := getRavenDBMetaData(m)
		require.NoError(t, err)
		assert.Equal(t, properties[serverURL], metadata.ServerURL)
		assert.Equal(t, properties[databaseName], metadata.DatabaseName)
		assert.False(t, metadata.EnableTTL)
		assert.Equal(t, int64(15), metadata.TTLFrequency)
	})

	t.Run("with https without cert and key", func(t *testing.T) {
		properties := map[string]string{
			serverURL:    "https://test.live.ravendb.com",
			databaseName: "TestDB",
		}

		m := state.Metadata{
			Base: metadata.Base{Properties: properties},
		}

		_, err := getRavenDBMetaData(m)
		require.Errorf(t, err, "certificate and key are required for secure connection")
	})

	t.Run("with https without key", func(t *testing.T) {
		properties := map[string]string{
			serverURL:    "https://test.live.ravendb.com",
			databaseName: "TestDB",
			certPath:     "/path/to/cert",
		}

		m := state.Metadata{
			Base: metadata.Base{Properties: properties},
		}

		_, err := getRavenDBMetaData(m)
		require.Errorf(t, err, "certificate and key are required for secure connection")
	})

	t.Run("with https without cert", func(t *testing.T) {
		properties := map[string]string{
			serverURL:    "https://test.live.ravendb.com",
			databaseName: "TestDB",
			keyPath:      "/path/to/key",
		}

		m := state.Metadata{
			Base: metadata.Base{Properties: properties},
		}

		_, err := getRavenDBMetaData(m)
		require.Errorf(t, err, "certificate and key are required for secure connection")
	})

	t.Run("with https", func(t *testing.T) {
		properties := map[string]string{
			serverURL:    "https://test.live.ravendb.com",
			databaseName: "TestDB",
			certPath:     "/path/to/cert",
			keyPath:      "/path/to/key",
		}

		m := state.Metadata{
			Base: metadata.Base{Properties: properties},
		}

		metadata, err := getRavenDBMetaData(m)
		require.NoError(t, err)
		assert.Equal(t, properties[serverURL], metadata.ServerURL)
		assert.Equal(t, properties[databaseName], metadata.DatabaseName)
		assert.Equal(t, properties[certPath], metadata.CertPath)
		assert.Equal(t, properties[keyPath], metadata.KeyPath)
	})
}
