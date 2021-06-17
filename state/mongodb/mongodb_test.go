// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
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

	t.Run("Valid connectionstring without params", func(t *testing.T) {
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

		uri := getMongoURI(metadata)
		expected := "mongodb://username:password@127.0.0.2/TestDB"

		assert.Equal(t, expected, uri)
	})

	t.Run("Valid connectionstring without username", func(t *testing.T) {
		properties := map[string]string{
			host:           "localhost:27017",
			databaseName:   "TestDB",
			collectionName: "TestCollection",
		}
		m := state.Metadata{
			Properties: properties,
		}

		metadata, err := getMongoDBMetaData(m)
		assert.Nil(t, err)

		uri := getMongoURI(metadata)
		expected := "mongodb://localhost:27017/TestDB"

		assert.Equal(t, expected, uri)
	})

	t.Run("Valid connectionstring with params", func(t *testing.T) {
		properties := map[string]string{
			host:           "127.0.0.2",
			databaseName:   "TestDB",
			collectionName: "TestCollection",
			username:       "username",
			password:       "password",
			params:         "?ssl=true",
		}
		m := state.Metadata{
			Properties: properties,
		}

		metadata, err := getMongoDBMetaData(m)
		assert.Nil(t, err)

		uri := getMongoURI(metadata)
		expected := "mongodb://username:password@127.0.0.2/TestDB?ssl=true"

		assert.Equal(t, expected, uri)
	})

	t.Run("Valid connectionstring with DNS SRV", func(t *testing.T) {
		properties := map[string]string{
			server:         "server.example.com",
			databaseName:   "TestDB",
			collectionName: "TestCollection",
			params:         "?ssl=true",
		}
		m := state.Metadata{
			Properties: properties,
		}

		metadata, err := getMongoDBMetaData(m)
		assert.Nil(t, err)

		uri := getMongoURI(metadata)
		expected := "mongodb+srv://server.example.com/?ssl=true"

		assert.Equal(t, expected, uri)
	})

	t.Run("Invalid without host/server", func(t *testing.T) {
		properties := map[string]string{
			databaseName:   "TestDB",
			collectionName: "TestCollection",
		}
		m := state.Metadata{
			Properties: properties,
		}

		_, err := getMongoDBMetaData(m)
		assert.NotNil(t, err)

		expected := "must set 'host' or 'server' fields in metadata"
		assert.Equal(t, expected, err.Error())
	})

	t.Run("Invalid with both host/server", func(t *testing.T) {
		properties := map[string]string{
			server:         "server.example.com",
			host:           "127.0.0.2",
			databaseName:   "TestDB",
			collectionName: "TestCollection",
		}
		m := state.Metadata{
			Properties: properties,
		}

		_, err := getMongoDBMetaData(m)
		assert.NotNil(t, err)

		expected := "'host' or 'server' fields are mutually exclusive"
		assert.Equal(t, expected, err.Error())
	})
}
