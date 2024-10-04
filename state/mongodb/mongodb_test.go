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

package mongodb

import (
	"context"
	"encoding/json"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.uber.org/goleak"

	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/state"
	"github.com/dapr/kit/logger"
)

func TestGetMongoDBMetadata(t *testing.T) {
	t.Run("With defaults", func(t *testing.T) {
		properties := map[string]string{
			host: "127.0.0.1",
		}
		m := state.Metadata{
			Base: metadata.Base{Properties: properties},
		}

		metadata, err := getMongoDBMetaData(m)
		require.NoError(t, err)
		assert.Equal(t, properties[host], metadata.Host)
		assert.Equal(t, defaultDatabaseName, metadata.DatabaseName)
		assert.Equal(t, defaultCollectionName, metadata.CollectionName)
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
			Base: metadata.Base{Properties: properties},
		}

		metadata, err := getMongoDBMetaData(m)
		require.NoError(t, err)
		assert.Equal(t, properties[host], metadata.Host)
		assert.Equal(t, properties[databaseName], metadata.DatabaseName)
		assert.Equal(t, properties[collectionName], metadata.CollectionName)
		assert.Equal(t, properties[username], metadata.Username)
		assert.Equal(t, properties[password], metadata.Password)
	})

	t.Run("Missing hosts", func(t *testing.T) {
		properties := map[string]string{
			username: "username",
			password: "password",
		}
		m := state.Metadata{
			Base: metadata.Base{Properties: properties},
		}

		_, err := getMongoDBMetaData(m)
		require.Error(t, err)
	})

	t.Run("Valid connection details without params", func(t *testing.T) {
		properties := map[string]string{
			host:           "127.0.0.2",
			databaseName:   "TestDB",
			collectionName: "TestCollection",
			username:       "username",
			password:       "password",
		}
		m := state.Metadata{
			Base: metadata.Base{Properties: properties},
		}

		metadata, err := getMongoDBMetaData(m)
		require.NoError(t, err)

		uri := metadata.getMongoConnectionString()
		expected := "mongodb://username:password@127.0.0.2/TestDB"

		assert.Equal(t, expected, uri)
	})

	t.Run("Valid connection details without username", func(t *testing.T) {
		properties := map[string]string{
			host:           "localhost:27017",
			databaseName:   "TestDB",
			collectionName: "TestCollection",
		}
		m := state.Metadata{
			Base: metadata.Base{Properties: properties},
		}

		metadata, err := getMongoDBMetaData(m)
		require.NoError(t, err)

		uri := metadata.getMongoConnectionString()
		expected := "mongodb://localhost:27017/TestDB"

		assert.Equal(t, expected, uri)
	})

	t.Run("Valid connection details with params", func(t *testing.T) {
		properties := map[string]string{
			host:           "127.0.0.2",
			databaseName:   "TestDB",
			collectionName: "TestCollection",
			username:       "username",
			password:       "password",
			params:         "?ssl=true",
		}
		m := state.Metadata{
			Base: metadata.Base{Properties: properties},
		}

		metadata, err := getMongoDBMetaData(m)
		require.NoError(t, err)

		uri := metadata.getMongoConnectionString()
		expected := "mongodb://username:password@127.0.0.2/TestDB?ssl=true"

		assert.Equal(t, expected, uri)
	})

	t.Run("Valid connection details with DNS SRV", func(t *testing.T) {
		properties := map[string]string{
			server:         "server.example.com",
			databaseName:   "TestDB",
			collectionName: "TestCollection",
			params:         "?ssl=true",
		}
		m := state.Metadata{
			Base: metadata.Base{Properties: properties},
		}

		metadata, err := getMongoDBMetaData(m)
		require.NoError(t, err)

		uri := metadata.getMongoConnectionString()
		expected := "mongodb+srv://server.example.com/?ssl=true"

		assert.Equal(t, expected, uri)
	})

	t.Run("Invalid without host/server", func(t *testing.T) {
		properties := map[string]string{
			databaseName:   "TestDB",
			collectionName: "TestCollection",
		}
		m := state.Metadata{
			Base: metadata.Base{Properties: properties},
		}

		_, err := getMongoDBMetaData(m)
		require.Error(t, err)

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
			Base: metadata.Base{Properties: properties},
		}

		_, err := getMongoDBMetaData(m)
		require.Error(t, err)

		expected := "'host' or 'server' fields are mutually exclusive"
		assert.Equal(t, expected, err.Error())
	})

	t.Run("Connection string ignores all other connection details", func(t *testing.T) {
		properties := map[string]string{
			host:               "localhost:27017",
			databaseName:       "TestDB",
			collectionName:     "TestCollection",
			"connectionString": "mongodb://localhost:99999/UnchanedDB",
		}
		m := state.Metadata{
			Base: metadata.Base{Properties: properties},
		}

		metadata, err := getMongoDBMetaData(m)
		require.NoError(t, err)

		uri := metadata.getMongoConnectionString()
		expected := "mongodb://localhost:99999/UnchanedDB"

		assert.Equal(t, expected, uri)
	})

	t.Run("test decode", func(t *testing.T) {
		mongo := MongoDB{}
		time.Local = time.UTC
		thetime, err := time.Parse(time.RFC3339, "2023-02-01T12:13:09Z")
		require.NoError(t, err)
		timestring := thetime.UTC().Format("2006-01-02T15:04:05Z")

		msg := bson.M{
			"message": "test message",
			"time":    thetime,
			"decimal": 123.456789,
			"integer": 123456789,
			"boolean": true,
		}

		res, err := mongo.decodeData(msg)
		require.NoError(t, err)

		var data map[string]interface{}
		err = json.Unmarshal(res, &data)
		require.NoError(t, err)

		assert.Equal(t, "test message", data["message"])
		assert.Equal(t, timestring, data["time"])
		assert.Equal(t, 123.456789, data["decimal"])
		assert.Equal(t, float64(123456789), data["integer"])
		assert.True(t, data["boolean"].(bool))

		msg2 := bson.D{
			{Key: "message", Value: "test message"},
			{Key: "time", Value: thetime},
			{Key: "decimal", Value: 123.456789},
			{Key: "integer", Value: 123456789},
			{Key: "boolean", Value: true},
			{Key: "nestedarray", Value: bson.A{
				"test", 123, 123.456, true, timestring,
			}},
		}

		res2, err := mongo.decodeData(msg2)
		require.NoError(t, err)

		var data2 map[string]interface{}
		err = json.Unmarshal(res2, &data2)
		require.NoError(t, err)

		assert.Equal(t, "test message", data2["message"])
		assert.Equal(t, timestring, data2["time"])
		assert.Equal(t, 123.456789, data2["decimal"])
		assert.Equal(t, float64(123456789), data2["integer"])
		assert.True(t, data2["boolean"].(bool))
		assert.Equal(t, []interface{}{"test", float64(123), 123.456, true, timestring}, data2["nestedarray"])

		// this is an array type
		msg3 := bson.A{
			thetime, "test message", 123.456789, 123456789, true,
			bson.D{{Key: "somedecimal", Value: 9.87654321}, {Key: "sometime", Value: thetime}},
		}
		res3, err := mongo.decodeData(msg3)
		require.NoError(t, err)
		var data3 []interface{}
		err = json.Unmarshal(res3, &data3)
		require.NoError(t, err)

		assert.Contains(t, data3, "test message")
		assert.Contains(t, data3, timestring)
		assert.Contains(t, data3, 123.456789)
		assert.Contains(t, data3, float64(123456789))
		assert.Contains(t, data3, true)

		targetMap := map[string]interface{}{
			"somedecimal": 9.87654321,
			"sometime":    timestring,
		}
		assert.Contains(t, data3, targetMap)
	})
}

func TestGoroutineLeak(t *testing.T) {
	defer goleak.VerifyNone(t)

	t.Run("Valid connection", func(t *testing.T) {
		properties := map[string]string{
			host:           "127.0.0.1",
			databaseName:   "TestDB",
			collectionName: "TestCollection",
			username:       "username",
			password:       "password",
		}
		m := state.Metadata{
			Base: metadata.Base{
				Name:       "mongo",
				Properties: properties,
			},
		}

		s := NewMongoDB(logger.NewLogger("test"))
		// ignore errors on init
		_ = s.Init(context.Background(), m)

		// close the connection
		closer, ok := s.(io.Closer)
		require.True(t, ok)
		err := closer.Close()
		require.NoError(t, err)
	})
}
