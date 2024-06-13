package ravendb

import (
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/state"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
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
	})
}
