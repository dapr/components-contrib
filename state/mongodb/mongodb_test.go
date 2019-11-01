// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package mongodb

import (
	"testing"

	"github.com/dapr/components-contrib/state"
	"github.com/stretchr/testify/assert"
)

func TestGetMongoDBMetadata(t *testing.T) {
	t.Run("With defaults", func(t *testing.T) {
		properties := map[string]string{
			host: "127.0.0.1",
		}
		m := state.Metadata{
			Properties: properties,
		}

		metadata, err := getMongoDBMetaData(m)
		assert.Nil(t, err)
		assert.Equal(t, properties[host], metadata.host)
		assert.Equal(t, defaultDatabaseName, metadata.databaseName)
		assert.Equal(t, defaultCollectionName, metadata.collectionName)
	})

	t.Run("With custom values", func(t *testing.T) {
		properties := map[string]string{
			host:           "127.0.0.2",
			databaseName:   "TestDB",
			collectionName: "TestCollection",
			username:       "username",
			password:       "password",
		}
		m := state.Metadata{
			Properties: properties,
		}

		metadata, err := getMongoDBMetaData(m)
		assert.Nil(t, err)
		assert.Equal(t, properties[host], metadata.host)
		assert.Equal(t, properties[databaseName], metadata.databaseName)
		assert.Equal(t, properties[collectionName], metadata.collectionName)
		assert.Equal(t, properties[username], metadata.username)
		assert.Equal(t, properties[password], metadata.password)
	})

	t.Run("Missing hosts", func(t *testing.T) {
		properties := map[string]string{
			username: "username",
			password: "password",
		}
		m := state.Metadata{
			Properties: properties,
		}

		_, err := getMongoDBMetaData(m)
		assert.NotNil(t, err)
	})
}
