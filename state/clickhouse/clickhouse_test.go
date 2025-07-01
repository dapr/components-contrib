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

package clickhouse

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/state"
	"github.com/dapr/kit/logger"
)

const (
	testURL      = "http://localhost:8123"
	testDatabase = "dapr_test"
	testTable    = "state_test"
	testUsername = "default"
	testPassword = "clickhouse_password"
)

func TestClickHouseIntegration(t *testing.T) {
	// Skip if not running integration tests
	if testing.Short() {
		t.Skip("Skipping integration test")
	}

	store := NewClickHouseStateStore(logger.NewLogger("test"))
	ctx := t.Context()

	// Initialize store with credentials
	err := store.Init(ctx, state.Metadata{
		Base: metadata.Base{
			Properties: map[string]string{
				"clickhouseURL": testURL,
				"databaseName":  testDatabase,
				"tableName":     testTable,
				"username":      testUsername,
				"password":      testPassword,
			},
		},
	})
	require.NoError(t, err)

	// Cleanup after tests
	t.Cleanup(func() {
		if s, ok := store.(*StateStore); ok {
			// Drop test table
			if s.db != nil {
				_, _ = s.db.ExecContext(ctx, "DROP TABLE IF EXISTS "+testDatabase+"."+testTable)
				// Drop test database
				_, _ = s.db.ExecContext(ctx, "DROP DATABASE IF EXISTS "+testDatabase)
			}
			// Close the connection
			_ = store.Close()
		}
	})

	t.Run("Test CRUD operations", func(t *testing.T) {
		testKey := "test-key"
		testValue := []byte("test-value")

		// Test Set
		err := store.Set(ctx, &state.SetRequest{
			Key:   testKey,
			Value: testValue,
		})
		require.NoError(t, err)

		// Test Get
		response, err := store.Get(ctx, &state.GetRequest{
			Key: testKey,
		})
		require.NoError(t, err)
		assert.Equal(t, testValue, response.Data)

		// Test Delete
		err = store.Delete(ctx, &state.DeleteRequest{
			Key: testKey,
		})
		require.NoError(t, err)

		// Verify deletion
		response, err = store.Get(ctx, &state.GetRequest{
			Key: testKey,
		})
		require.NoError(t, err)
		assert.Nil(t, response.Data)
	})

	t.Run("Test ETag support", func(t *testing.T) {
		testKey := "etag-key"
		testValue := []byte("etag-value")

		// Set initial value
		err := store.Set(ctx, &state.SetRequest{
			Key:   testKey,
			Value: testValue,
		})
		require.NoError(t, err)

		// Get value and ETag
		response, err := store.Get(ctx, &state.GetRequest{
			Key: testKey,
		})
		require.NoError(t, err)
		assert.NotEmpty(t, response.ETag)

		// Update with correct ETag
		err = store.Set(ctx, &state.SetRequest{
			Key:   testKey,
			Value: []byte("new-value"),
			ETag:  response.ETag,
		})
		require.NoError(t, err)

		// Cleanup
		err = store.Delete(ctx, &state.DeleteRequest{
			Key: testKey,
		})
		require.NoError(t, err)
	})

	t.Run("Test empty key", func(t *testing.T) {
		// Test Set with empty key
		err := store.Set(ctx, &state.SetRequest{
			Key:   "",
			Value: []byte("test"),
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "key is empty")

		// Test Get with empty key
		_, err = store.Get(ctx, &state.GetRequest{
			Key: "",
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "key is empty")

		// Test Delete with empty key
		err = store.Delete(ctx, &state.DeleteRequest{
			Key: "",
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "key is empty")
	})

	t.Run("Test non-existent key", func(t *testing.T) {
		response, err := store.Get(ctx, &state.GetRequest{
			Key: "non-existent-key",
		})
		require.NoError(t, err)
		assert.Nil(t, response.Data)
	})

	t.Run("Test Features", func(t *testing.T) {
		features := store.Features()
		assert.Contains(t, features, state.FeatureETag)
	})
}

func TestParseAndValidateMetadata(t *testing.T) {
	t.Run("With valid metadata", func(t *testing.T) {
		properties := map[string]string{
			"clickhouseURL": "tcp://127.0.0.1:9000",
			"databaseName":  "default",
			"tableName":     "statestore",
		}
		m := state.Metadata{
			Base: metadata.Base{Properties: properties},
		}

		metadata, err := parseAndValidateMetadata(m)
		require.NoError(t, err)
		assert.Equal(t, properties["clickhouseURL"], metadata.ClickHouseURL)
		assert.Equal(t, properties["databaseName"], metadata.Database)
		assert.Equal(t, properties["tableName"], metadata.Table)
	})

	t.Run("Missing clickhouseURL", func(t *testing.T) {
		properties := map[string]string{
			"databaseName": "default",
			"tableName":    "statestore",
		}
		m := state.Metadata{
			Base: metadata.Base{Properties: properties},
		}

		_, err := parseAndValidateMetadata(m)
		require.Error(t, err)
		assert.Equal(t, "ClickHouse URL is missing", err.Error())
	})

	t.Run("Missing databaseName", func(t *testing.T) {
		properties := map[string]string{
			"clickhouseURL": "tcp://127.0.0.1:9000",
			"tableName":     "statestore",
		}
		m := state.Metadata{
			Base: metadata.Base{Properties: properties},
		}

		_, err := parseAndValidateMetadata(m)
		require.Error(t, err)
		assert.Equal(t, "ClickHouse database name is missing", err.Error())
	})

	t.Run("Missing tableName", func(t *testing.T) {
		properties := map[string]string{
			"clickhouseURL": "tcp://127.0.0.1:9000",
			"databaseName":  "default",
		}
		m := state.Metadata{
			Base: metadata.Base{Properties: properties},
		}

		_, err := parseAndValidateMetadata(m)
		require.Error(t, err)
		assert.Equal(t, "ClickHouse table name is missing", err.Error())
	})
}
