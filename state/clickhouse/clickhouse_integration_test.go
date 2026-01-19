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

package clickhouse

import (
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/state"
	"github.com/dapr/kit/logger"
)

const (
	connectionStringEnvKey = "DAPR_TEST_CLICKHOUSE_CONNSTRING" // Environment variable containing the connection string
)

type fakeItem struct {
	Color string
}

func TestClickHouseIntegrationFull(t *testing.T) {
	connectionString := getConnectionString()
	if connectionString == "" {
		t.Skipf("ClickHouse state integration tests skipped. To enable define the connection string using environment variable '%s' (example 'export %s=\"tcp://localhost:9000\")", connectionStringEnvKey, connectionStringEnvKey)
	}

	t.Run("Test init configurations", func(t *testing.T) {
		testInitConfiguration(t)
	})

	metadata := state.Metadata{
		Base: metadata.Base{Properties: map[string]string{
			"clickhouseURL": connectionString,
			"databaseName":  "dapr_test",
			"tableName":     "state_test",
		}},
	}

	chs := NewClickHouseStateStore(logger.NewLogger("test")).(*StateStore)
	t.Cleanup(func() {
		defer chs.Close()
	})

	err := chs.Init(t.Context(), metadata)
	if err != nil {
		t.Fatal(err)
	}

	t.Run("Get Set Delete one item", func(t *testing.T) {
		t.Parallel()
		setGetUpdateDeleteOneItem(t, chs)
	})

	t.Run("Get item that does not exist", func(t *testing.T) {
		t.Parallel()
		getItemThatDoesNotExist(t, chs)
	})

	t.Run("Get item with no key fails", func(t *testing.T) {
		t.Parallel()
		getItemWithNoKey(t, chs)
	})

	t.Run("Set item with no key fails", func(t *testing.T) {
		t.Parallel()
		setItemWithNoKey(t, chs)
	})

	t.Run("Update and delete with etag succeeds", func(t *testing.T) {
		t.Parallel()
		updateAndDeleteWithEtagSucceeds(t, chs)
	})

	t.Run("Update with old etag fails", func(t *testing.T) {
		t.Parallel()
		updateWithOldEtagFails(t, chs)
	})

	t.Run("Insert with etag fails", func(t *testing.T) {
		t.Parallel()
		newItemWithEtagFails(t, chs)
	})

	t.Run("Delete with invalid etag fails", func(t *testing.T) {
		t.Parallel()
		deleteWithInvalidEtagFails(t, chs)
	})

	t.Run("Delete item with no key fails", func(t *testing.T) {
		t.Parallel()
		deleteItemWithNoKey(t, chs)
	})

	t.Run("Delete item that does not exist", func(t *testing.T) {
		t.Parallel()
		deleteItemThatDoesNotExist(t, chs)
	})

	t.Run("Bulk set and bulk delete", func(t *testing.T) {
		t.Parallel()
		testBulkSetAndBulkDelete(t, chs)
	})

	t.Run("Set item with invalid TTL", func(t *testing.T) {
		t.Parallel()
		testSetItemWithInvalidTTL(t, chs)
	})

	t.Run("Set item with negative TTL", func(t *testing.T) {
		t.Parallel()
		testSetItemWithNegativeTTL(t, chs)
	})

	t.Run("Set with TTL updates the expiration field", func(t *testing.T) {
		t.Parallel()
		setTTLUpdatesExpiry(t, chs)
	})

	t.Run("Expired item cannot be read", func(t *testing.T) {
		t.Parallel()
		expiredStateCannotBeRead(t, chs)
	})

	t.Run("Unexpired item can be read", func(t *testing.T) {
		t.Parallel()
		unexpiredStateCanBeRead(t, chs)
	})

	t.Run("Test Features", func(t *testing.T) {
		t.Parallel()
		testFeatures(t, chs)
	})
}

func getConnectionString() string {
	return os.Getenv(connectionStringEnvKey)
}

func testInitConfiguration(t *testing.T) {
	logger := logger.NewLogger("test")
	tests := []struct {
		name        string
		properties  map[string]string
		expectedErr string
	}{
		{
			name:        "Empty",
			properties:  map[string]string{},
			expectedErr: "ClickHouse URL is missing",
		},
		{
			name: "Missing database name",
			properties: map[string]string{
				"clickhouseUrl": "tcp://localhost:9000",
			},
			expectedErr: "ClickHouse database name is missing",
		},
		{
			name: "Missing table name",
			properties: map[string]string{
				"clickhouseUrl": "tcp://localhost:9000",
				"databaseName":  "test",
			},
			expectedErr: "ClickHouse table name is missing",
		},
		{
			name: "Valid configuration",
			properties: map[string]string{
				"clickhouseUrl": "tcp://localhost:9000",
				"databaseName":  "test",
				"tableName":     "state",
			},
			expectedErr: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := NewClickHouseStateStore(logger)
			metadata := state.Metadata{
				Base: metadata.Base{Properties: tt.properties},
			}

			err := store.Init(t.Context(), metadata)
			if tt.expectedErr == "" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectedErr)
			}
		})
	}
}

// setGetUpdateDeleteOneItem validates setting one item, getting it, and deleting it.
func setGetUpdateDeleteOneItem(t *testing.T, s state.Store) {
	key := randomKey()
	value := &fakeItem{Color: "yellow"}

	setItem(t, s, key, value, nil)

	getResponse, outputObject := getItem(t, s, key)
	assert.Equal(t, value, outputObject)

	newValue := &fakeItem{Color: "green"}
	setItem(t, s, key, newValue, getResponse.ETag)
	getResponse, outputObject = getItem(t, s, key)

	assert.Equal(t, newValue, outputObject)

	deleteItem(t, s, key, getResponse.ETag)
}

// getItemThatDoesNotExist validates the behavior of retrieving an item that does not exist.
func getItemThatDoesNotExist(t *testing.T, s state.Store) {
	key := randomKey()
	response, outputObject := getItem(t, s, key)
	assert.Nil(t, response.Data)
	assert.Equal(t, "", outputObject.Color)
}

// getItemWithNoKey validates that attempting a Get operation without providing a key will return an error.
func getItemWithNoKey(t *testing.T, s state.Store) {
	getReq := &state.GetRequest{
		Key: "",
	}

	response, err := s.Get(t.Context(), getReq)
	require.Error(t, err)
	assert.Nil(t, response)
}

// setItemWithNoKey validates that attempting a Set operation without providing a key will return an error.
func setItemWithNoKey(t *testing.T, s state.Store) {
	setReq := &state.SetRequest{
		Key: "",
	}

	err := s.Set(t.Context(), setReq)
	require.Error(t, err)
}

// newItemWithEtagFails creates a new item and also supplies a non existent ETag and requests FirstWrite, which is invalid - expect failure.
func newItemWithEtagFails(t *testing.T, s state.Store) {
	value := &fakeItem{Color: "teal"}
	invalidEtag := "12345"

	setReq := &state.SetRequest{
		Key:   randomKey(),
		ETag:  &invalidEtag,
		Value: value,
		Options: state.SetStateOption{
			Concurrency: state.FirstWrite,
		},
	}

	err := s.Set(t.Context(), setReq)
	require.Error(t, err)
}

func updateWithOldEtagFails(t *testing.T, s state.Store) {
	// Create and retrieve new item.
	key := randomKey()
	value := &fakeItem{Color: "gray"}
	setItem(t, s, key, value, nil)
	getResponse, _ := getItem(t, s, key)
	assert.NotNil(t, getResponse.ETag)
	originalEtag := getResponse.ETag

	// Change the value and get the updated etag.
	newValue := &fakeItem{Color: "silver"}
	setItem(t, s, key, newValue, originalEtag)
	getResponse, _ = getItem(t, s, key)
	assert.NotNil(t, getResponse.ETag)
	currentEtag := getResponse.ETag

	// Update again with the old etag - this should fail.
	newValue = &fakeItem{Color: "maroon"}
	setReq := &state.SetRequest{
		Key:   key,
		ETag:  originalEtag,
		Value: newValue,
	}
	err := s.Set(t.Context(), setReq)
	require.Error(t, err)

	// Cleanup
	deleteItem(t, s, key, currentEtag)
}

func updateAndDeleteWithEtagSucceeds(t *testing.T, s state.Store) {
	// Create and retrieve new item.
	key := randomKey()
	value := &fakeItem{Color: "hazel"}
	setItem(t, s, key, value, nil)
	getResponse, outputObject := getItem(t, s, key)
	assert.Equal(t, value, outputObject)
	assert.NotNil(t, getResponse.ETag)

	// Change the value and compare.
	value.Color = "purple"
	setItem(t, s, key, value, getResponse.ETag)
	updateResponse, outputObject := getItem(t, s, key)
	assert.Equal(t, value, outputObject)
	assert.NotNil(t, updateResponse.ETag)
	assert.NotEqual(t, getResponse.ETag, updateResponse.ETag)

	// ETag should change when item is updated.
	deleteItem(t, s, key, updateResponse.ETag)
}

func deleteWithInvalidEtagFails(t *testing.T, s state.Store) {
	// Create new item.
	key := randomKey()
	value := &fakeItem{Color: "mauve"}
	setItem(t, s, key, value, nil)
	getResponse, _ := getItem(t, s, key)
	assert.NotNil(t, getResponse.ETag)

	// Delete with invalid etag.
	invalidEtag := "12345"
	deleteReq := &state.DeleteRequest{
		Key:  key,
		ETag: &invalidEtag,
	}
	err := s.Delete(t.Context(), deleteReq)
	require.Error(t, err)

	// Cleanup
	deleteItem(t, s, key, getResponse.ETag)
}

func deleteItemWithNoKey(t *testing.T, s state.Store) {
	deleteReq := &state.DeleteRequest{
		Key: "",
	}
	err := s.Delete(t.Context(), deleteReq)
	require.Error(t, err)
}

func deleteItemThatDoesNotExist(t *testing.T, s state.Store) {
	// Delete the item with a key not in the store.
	deleteReq := &state.DeleteRequest{
		Key: randomKey(),
	}
	err := s.Delete(t.Context(), deleteReq)
	require.NoError(t, err)
}

// Tests valid bulk sets and deletes.
func testBulkSetAndBulkDelete(t *testing.T, s state.Store) {
	setReq := []state.SetRequest{
		{
			Key:   randomKey(),
			Value: &fakeItem{Color: "oceanblue"},
		},
		{
			Key:   randomKey(),
			Value: &fakeItem{Color: "livingwhite"},
		},
	}

	err := s.BulkSet(t.Context(), setReq, state.BulkStoreOpts{})
	require.NoError(t, err)
	assert.True(t, storeItemExists(t, s, setReq[0].Key))
	assert.True(t, storeItemExists(t, s, setReq[1].Key))

	deleteReq := []state.DeleteRequest{
		{
			Key: setReq[0].Key,
		},
		{
			Key: setReq[1].Key,
		},
	}

	err = s.BulkDelete(t.Context(), deleteReq, state.BulkStoreOpts{})
	require.NoError(t, err)
	assert.False(t, storeItemExists(t, s, setReq[0].Key))
	assert.False(t, storeItemExists(t, s, setReq[1].Key))
}

func testSetItemWithInvalidTTL(t *testing.T, s state.Store) {
	setReq := &state.SetRequest{
		Key:   randomKey(),
		Value: &fakeItem{Color: "red"},
		Metadata: map[string]string{
			"ttlInSeconds": "XX",
		},
	}

	err := s.Set(t.Context(), setReq)
	require.Error(t, err)
}

func testSetItemWithNegativeTTL(t *testing.T, s state.Store) {
	setReq := &state.SetRequest{
		Key:   randomKey(),
		Value: &fakeItem{Color: "red"},
		Metadata: map[string]string{
			"ttlInSeconds": "-1",
		},
	}

	err := s.Set(t.Context(), setReq)
	require.Error(t, err)
}

func setTTLUpdatesExpiry(t *testing.T, s state.Store) {
	key := randomKey()
	setReq := &state.SetRequest{
		Key:   key,
		Value: &fakeItem{Color: "red"},
		Metadata: map[string]string{
			"ttlInSeconds": "1000",
		},
	}

	err := s.Set(t.Context(), setReq)
	require.NoError(t, err)

	// Check that item exists and has TTL metadata
	getReq := &state.GetRequest{Key: key}
	resp, err := s.Get(t.Context(), getReq)
	require.NoError(t, err)
	assert.NotNil(t, resp.Data)
	assert.NotEmpty(t, resp.Metadata[state.GetRespMetaKeyTTLExpireTime])

	// Cleanup
	deleteReq := &state.DeleteRequest{Key: key}
	err = s.Delete(t.Context(), deleteReq)
	require.NoError(t, err)
}

func expiredStateCannotBeRead(t *testing.T, s state.Store) {
	key := randomKey()
	setReq := &state.SetRequest{
		Key:   key,
		Value: &fakeItem{Color: "red"},
		Metadata: map[string]string{
			"ttlInSeconds": "1", // 1 second TTL
		},
	}

	err := s.Set(t.Context(), setReq)
	require.NoError(t, err)

	// Wait for expiration
	time.Sleep(2 * time.Second)

	// Try to get expired item
	getReq := &state.GetRequest{Key: key}
	resp, err := s.Get(t.Context(), getReq)
	require.NoError(t, err)
	assert.Nil(t, resp.Data)
}

func unexpiredStateCanBeRead(t *testing.T, s state.Store) {
	key := randomKey()
	value := &fakeItem{Color: "blue"}
	setReq := &state.SetRequest{
		Key:   key,
		Value: value,
		Metadata: map[string]string{
			"ttlInSeconds": "1000", // 1000 second TTL
		},
	}

	err := s.Set(t.Context(), setReq)
	require.NoError(t, err)

	// Get item before expiration
	getReq := &state.GetRequest{Key: key}
	resp, err := s.Get(t.Context(), getReq)
	require.NoError(t, err)
	assert.NotNil(t, resp.Data)

	var outputObject fakeItem
	err = json.Unmarshal(resp.Data, &outputObject)
	require.NoError(t, err)
	assert.Equal(t, value, &outputObject)

	// Cleanup
	deleteReq := &state.DeleteRequest{Key: key}
	err = s.Delete(t.Context(), deleteReq)
	require.NoError(t, err)
}

func testFeatures(t *testing.T, s state.Store) {
	features := s.Features()
	assert.Contains(t, features, state.FeatureETag)
	assert.Contains(t, features, state.FeatureTTL)
}

// Helper functions

func setItem(t *testing.T, s state.Store, key string, value interface{}, etag *string) {
	setReq := &state.SetRequest{
		Key:   key,
		ETag:  etag,
		Value: value,
	}

	err := s.Set(t.Context(), setReq)
	require.NoError(t, err)
}

func getItem(t *testing.T, s state.Store, key string) (*state.GetResponse, *fakeItem) {
	getReq := &state.GetRequest{
		Key: key,
	}

	response, err := s.Get(t.Context(), getReq)
	require.NoError(t, err)
	assert.NotNil(t, response)

	outputObject := &fakeItem{}
	if response.Data != nil {
		err = json.Unmarshal(response.Data, outputObject)
		require.NoError(t, err)
	}

	return response, outputObject
}

func deleteItem(t *testing.T, s state.Store, key string, etag *string) {
	deleteReq := &state.DeleteRequest{
		Key:  key,
		ETag: etag,
	}

	err := s.Delete(t.Context(), deleteReq)
	require.NoError(t, err)

	// Item is not in the data store.
	assert.False(t, storeItemExists(t, s, key))
}

func storeItemExists(t *testing.T, s state.Store, key string) bool {
	getReq := &state.GetRequest{
		Key: key,
	}
	response, err := s.Get(t.Context(), getReq)
	require.NoError(t, err)

	return response.Data != nil
}

func randomKey() string {
	return uuid.New().String()
}
