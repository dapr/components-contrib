/*
Copyright 2023 The Dapr Authors
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

package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/state"
	"github.com/dapr/kit/logger"
)

const (
	// Environment variable containing the connection string.
	// By default, uses an in-memory database for testing.
	connectionStringEnvKey = "DAPR_TEST_SQLITE_DATABASE_CONNECTSTRING"
)

func TestSqliteIntegration(t *testing.T) {
	connectionString := getConnectionString()

	t.Run("Test init configurations", func(t *testing.T) {
		testInitConfiguration(t)
	})

	metadata := state.Metadata{
		Base: metadata.Base{
			Properties: map[string]string{
				"connectionString": connectionString,
				"tableName":        "test_state",
			},
		},
	}

	s := NewSQLiteStateStore(logger.NewLogger("test"))
	t.Cleanup(func() {
		defer s.Close()
	})

	if initerror := s.Init(context.Background(), metadata); initerror != nil {
		t.Fatal(initerror)
	}

	t.Run("Get Set Delete one item", func(t *testing.T) {
		setGetUpdateDeleteOneItem(t, s)
	})

	t.Run("Get item that does not exist", func(t *testing.T) {
		getItemThatDoesNotExist(t, s)
	})

	t.Run("Get item with no key fails", func(t *testing.T) {
		getItemWithNoKey(t, s)
	})

	t.Run("Set item with invalid (non numeric) TTL", func(t *testing.T) {
		testSetItemWithInvalidTTL(t, s)
	})
	t.Run("Set item with negative TTL", func(t *testing.T) {
		testSetItemWithNegativeTTL(t, s)
	})
	t.Run("Set with TTL updates the expiration field", func(t *testing.T) {
		setTTLUpdatesExpiry(t, s)
	})
	t.Run("Set with TTL followed by set without TTL resets the expiration field", func(t *testing.T) {
		setNoTTLUpdatesExpiry(t, s)
	})
	t.Run("Expired item cannot be read", func(t *testing.T) {
		expiredStateCannotBeRead(t, s)
	})
	t.Run("Unexpired item be read", func(t *testing.T) {
		unexpiredStateCanBeRead(t, s)
	})
	t.Run("Set updates the updatedate field", func(t *testing.T) {
		setUpdatesTheUpdatedateField(t, s)
	})

	t.Run("Set item with no key fails", func(t *testing.T) {
		setItemWithNoKey(t, s)
	})

	t.Run("Bulk set and bulk delete", func(t *testing.T) {
		testBulkSetAndBulkDelete(t, s)
	})

	t.Run("Update and delete with etag succeeds", func(t *testing.T) {
		updateAndDeleteWithEtagSucceeds(t, s)
	})

	t.Run("Update with old etag fails", func(t *testing.T) {
		updateWithOldEtagFails(t, s)
	})

	t.Run("Insert with etag fails", func(t *testing.T) {
		newItemWithEtagFails(t, s)
	})

	t.Run("Delete with invalid etag fails when first write is enforced", func(t *testing.T) {
		deleteWithInvalidEtagFails(t, s)
	})

	t.Run("Delete item with no key fails", func(t *testing.T) {
		deleteWithNoKeyFails(t, s)
	})

	t.Run("Delete an item that does not exist", func(t *testing.T) {
		deleteItemThatDoesNotExist(t, s)
	})
	t.Run("Multi with delete and set", func(t *testing.T) {
		multiWithDeleteAndSet(t, s)
	})

	t.Run("Multi with delete only", func(t *testing.T) {
		multiWithDeleteOnly(t, s)
	})

	t.Run("Multi with set only", func(t *testing.T) {
		multiWithSetOnly(t, s)
	})

	t.Run("ttlExpireTime", func(t *testing.T) {
		getExpireTime(t, s)
		getBulkExpireTime(t, s)
	})

	t.Run("Binary data", func(t *testing.T) {
		key := randomKey()

		err := s.Set(context.Background(), &state.SetRequest{
			Key:   key,
			Value: []byte("🤖"),
		})
		require.NoError(t, err)

		res, err := s.Get(context.Background(), &state.GetRequest{
			Key: key,
		})
		require.NoError(t, err)
		require.NotNil(t, res.ETag)
		assert.NotEmpty(t, res.ETag)
		assert.Equal(t, "🤖", string(res.Data))
	})
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

func deleteItemThatDoesNotExist(t *testing.T, s state.Store) {
	// Delete the item with a key not in the store.
	deleteReq := &state.DeleteRequest{
		Key: randomKey(),
	}
	err := s.Delete(context.Background(), deleteReq)
	require.NoError(t, err)
}

func multiWithSetOnly(t *testing.T, s state.Store) {
	var operations []state.TransactionalStateOperation //nolint:prealloc
	var setRequests []state.SetRequest                 //nolint:prealloc
	for range 3 {
		req := state.SetRequest{
			Key:   randomKey(),
			Value: randomJSON(),
		}
		setRequests = append(setRequests, req)
		operations = append(operations, req)
	}

	err := s.(state.TransactionalStore).Multi(context.Background(), &state.TransactionalStateRequest{
		Operations: operations,
	})
	require.NoError(t, err)

	for _, set := range setRequests {
		assert.True(t, storeItemExists(t, s, set.Key))
		deleteItem(t, s, set.Key, nil)
	}
}

func multiWithDeleteOnly(t *testing.T, s state.Store) {
	var operations []state.TransactionalStateOperation //nolint:prealloc
	var deleteRequests []state.DeleteRequest           //nolint:prealloc
	for range 3 {
		req := state.DeleteRequest{Key: randomKey()}

		// Add the item to the database.
		setItem(t, s, req.Key, randomJSON(), nil) // Add the item to the database.

		// Add the item to a slice of delete requests.
		deleteRequests = append(deleteRequests, req)

		// Add the item to the multi transaction request.
		operations = append(operations, req)
	}

	err := s.(state.TransactionalStore).Multi(context.Background(), &state.TransactionalStateRequest{
		Operations: operations,
	})
	require.NoError(t, err)

	for _, delete := range deleteRequests {
		assert.False(t, storeItemExists(t, s, delete.Key))
	}
}

func multiWithDeleteAndSet(t *testing.T, s state.Store) {
	var operations []state.TransactionalStateOperation //nolint:prealloc
	var deleteRequests []state.DeleteRequest           //nolint:prealloc
	for range 3 {
		req := state.DeleteRequest{Key: randomKey()}

		// Add the item to the database.
		setItem(t, s, req.Key, randomJSON(), nil) // Add the item to the database.

		// Add the item to a slice of delete requests.
		deleteRequests = append(deleteRequests, req)

		// Add the item to the multi transaction request.
		operations = append(operations, req)
	}

	// Create the set requests.
	var setRequests []state.SetRequest //nolint:prealloc
	for range 3 {
		req := state.SetRequest{
			Key:   randomKey(),
			Value: randomJSON(),
		}
		setRequests = append(setRequests, req)
		operations = append(operations, req)
	}

	err := s.(state.TransactionalStore).Multi(context.Background(), &state.TransactionalStateRequest{
		Operations: operations,
	})
	require.NoError(t, err)

	for _, delete := range deleteRequests {
		assert.False(t, storeItemExists(t, s, delete.Key))
	}

	for _, set := range setRequests {
		assert.True(t, storeItemExists(t, s, set.Key))
		deleteItem(t, s, set.Key, nil)
	}
}

func deleteWithInvalidEtagFails(t *testing.T, s state.Store) {
	// Create new item.
	key := randomKey()
	value := &fakeItem{Color: "mauvebrown"}
	setItem(t, s, key, value, nil)

	etag := "1234"
	// Delete the item with a fake etag.
	deleteReq := &state.DeleteRequest{
		Key:  key,
		ETag: &etag,
		Options: state.DeleteStateOption{
			Concurrency: state.FirstWrite,
		},
	}
	err := s.Delete(context.Background(), deleteReq)
	require.Error(t, err, "Deleting an item with the wrong etag while enforcing FirstWrite policy should fail")
}

func deleteWithNoKeyFails(t *testing.T, s state.Store) {
	deleteReq := &state.DeleteRequest{
		Key: "",
	}
	err := s.Delete(context.Background(), deleteReq)
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

	err := s.Set(context.Background(), setReq)
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
	_, updatedItem := getItem(t, s, key)
	assert.Equal(t, newValue, updatedItem)
	getResponse, _ = getItem(t, s, key)
	assert.NotNil(t, getResponse.ETag)
	// Update again with the original etag - expect update failure.
	newValue = &fakeItem{Color: "maroon"}
	setReq := &state.SetRequest{
		Key:   key,
		ETag:  originalEtag,
		Value: newValue,
		Options: state.SetStateOption{
			Concurrency: state.FirstWrite,
		},
	}
	err := s.Set(context.Background(), setReq)
	require.Error(t, err)
}

func updateAndDeleteWithEtagSucceeds(t *testing.T, s state.Store) {
	// Create and retrieve new item.
	key := randomKey()
	value := &fakeItem{Color: "hazel"}
	setItem(t, s, key, value, nil)
	getResponse, _ := getItem(t, s, key)
	assert.NotNil(t, getResponse.ETag)

	// Change the value and compare.
	value.Color = "purple"
	setReq := &state.SetRequest{
		Key:   key,
		ETag:  getResponse.ETag,
		Value: value,
		Options: state.SetStateOption{
			Concurrency: state.FirstWrite,
		},
	}
	err := s.Set(context.Background(), setReq)
	require.NoError(t, err, "Setting the item should be successful")
	updateResponse, updatedItem := getItem(t, s, key)
	assert.Equal(t, value, updatedItem)

	// ETag should change when item is updated..
	assert.NotEqual(t, getResponse.ETag, updateResponse.ETag)

	// Delete.
	deleteReq := &state.DeleteRequest{
		Key:  key,
		ETag: updateResponse.ETag,
		Options: state.DeleteStateOption{
			Concurrency: state.FirstWrite,
		},
	}
	err = s.Delete(context.Background(), deleteReq)
	require.NoError(t, err, "Deleting an item with the right etag while enforcing FirstWrite policy should succeed")

	// Item is not in the data store.
	assert.False(t, storeItemExists(t, s, key))
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

	response, getErr := s.Get(context.Background(), getReq)
	require.Error(t, getErr)
	assert.Nil(t, response)
}

// setUpdatesTheUpdatedateField proves that the updateddate is set for an update.
func setUpdatesTheUpdatedateField(t *testing.T, s state.Store) {
	key := randomKey()
	value := &fakeItem{Color: "orange"}
	setItem(t, s, key, value, nil)

	// insertdate should have a value and updatedate should be nil.
	_, updatedate := getRowData(t, s, key)
	assert.NotNil(t, updatedate)

	// make sure update time changes from creation.
	time.Sleep(1 * time.Second)

	// insertdate should not change, updatedate should have a value.
	value = &fakeItem{Color: "aqua"}
	setItem(t, s, key, value, nil)
	_, newupdatedate := getRowData(t, s, key)
	assert.NotEqual(t, updatedate.String, newupdatedate.String)

	deleteItem(t, s, key, nil)
}

// setTTLUpdatesExpiry proves that the expirydate is set when a TTL is passed for a key.
func setTTLUpdatesExpiry(t *testing.T, s state.Store) {
	key := randomKey()
	value := &fakeItem{Color: "darkgray"}
	setOptions := state.SetStateOption{}
	setReq := &state.SetRequest{
		Key:     key,
		ETag:    nil,
		Value:   value,
		Options: setOptions,
		Metadata: map[string]string{
			"ttlInSeconds": "1000",
		},
	}

	err := s.Set(context.Background(), setReq)
	require.NoError(t, err)

	// expirationTime should be set (to a date in the future).
	_, expirationTime := getTimesForRow(t, s, key)

	assert.NotNil(t, expirationTime)
	assert.True(t, expirationTime.Valid, "Expiration Time should have a value after set with TTL value")
	deleteItem(t, s, key, nil)
}

// setNoTTLUpdatesExpiry proves that the expirydate is reset when a state element with expiration time (TTL) loses TTL upon second set without TTL.
func setNoTTLUpdatesExpiry(t *testing.T, s state.Store) {
	key := randomKey()
	value := &fakeItem{Color: "darkorange"}
	setOptions := state.SetStateOption{}
	setReq := &state.SetRequest{
		Key:     key,
		ETag:    nil,
		Value:   value,
		Options: setOptions,
		Metadata: map[string]string{
			"ttlInSeconds": "1000",
		},
	}
	err := s.Set(context.Background(), setReq)
	require.NoError(t, err)
	delete(setReq.Metadata, "ttlInSeconds")
	err = s.Set(context.Background(), setReq)
	require.NoError(t, err)

	// expirationTime should not be set.
	_, expirationTime := getTimesForRow(t, s, key)

	assert.False(t, expirationTime.Valid, "Expiration Time should not have a value after first being set with TTL value and then being set without TTL value")
	deleteItem(t, s, key, nil)
}

func getExpireTime(t *testing.T, s state.Store) {
	key1 := randomKey()
	require.NoError(t, s.Set(context.Background(), &state.SetRequest{
		Key:   key1,
		Value: "123",
		Metadata: map[string]string{
			"ttlInSeconds": "1000",
		},
	}))

	resp, err := s.Get(context.Background(), &state.GetRequest{Key: key1})
	require.NoError(t, err)
	assert.Equal(t, `"123"`, string(resp.Data))
	require.Len(t, resp.Metadata, 1)
	expireTime, err := time.Parse(time.RFC3339, resp.Metadata["ttlExpireTime"])
	require.NoError(t, err)
	assert.InDelta(t, time.Now().Add(time.Second*1000).Unix(), expireTime.Unix(), 10)
}

func getBulkExpireTime(t *testing.T, s state.Store) {
	key1 := randomKey()
	key2 := randomKey()

	require.NoError(t, s.Set(context.Background(), &state.SetRequest{
		Key:   key1,
		Value: "123",
		Metadata: map[string]string{
			"ttlInSeconds": "1000",
		},
	}))
	require.NoError(t, s.Set(context.Background(), &state.SetRequest{
		Key:   key2,
		Value: "456",
		Metadata: map[string]string{
			"ttlInSeconds": "2001",
		},
	}))

	resp, err := s.BulkGet(context.Background(), []state.GetRequest{
		{Key: key1}, {Key: key2},
	}, state.BulkGetOpts{})
	require.NoError(t, err)
	assert.Len(t, resp, 2)
	sort.Slice(resp, func(i, j int) bool {
		return string(resp[i].Data) < string(resp[j].Data)
	})

	assert.Equal(t, `"123"`, string(resp[0].Data))
	assert.Equal(t, `"456"`, string(resp[1].Data))
	require.Len(t, resp[0].Metadata, 1)
	require.Len(t, resp[1].Metadata, 1)
	expireTime, err := time.Parse(time.RFC3339, resp[0].Metadata["ttlExpireTime"])
	require.NoError(t, err)
	assert.InDelta(t, time.Now().Add(time.Second*1000).Unix(), expireTime.Unix(), 10)
	expireTime, err = time.Parse(time.RFC3339, resp[1].Metadata["ttlExpireTime"])
	require.NoError(t, err)
	assert.InDelta(t, time.Now().Add(time.Second*2001).Unix(), expireTime.Unix(), 10)
}

// expiredStateCannotBeRead proves that an expired state element can not be read.
func expiredStateCannotBeRead(t *testing.T, s state.Store) {
	key := randomKey()
	value := &fakeItem{Color: "darkgray"}
	setOptions := state.SetStateOption{}
	setReq := &state.SetRequest{
		Key:     key,
		ETag:    nil,
		Value:   value,
		Options: setOptions,
		Metadata: map[string]string{
			"ttlInSeconds": "1",
		},
	}
	err := s.Set(context.Background(), setReq)
	require.NoError(t, err)

	time.Sleep(time.Second * time.Duration(2))
	getResponse, err := s.Get(context.Background(), &state.GetRequest{Key: key})
	assert.Equal(t, &state.GetResponse{}, getResponse, "Response must be empty")
	require.NoError(t, err, "Expired element must not be treated as error")

	deleteItem(t, s, key, nil)
}

// unexpiredStateCanBeRead proves that a state element with TTL - but no yet expired - can be read.
func unexpiredStateCanBeRead(t *testing.T, s state.Store) {
	key := randomKey()
	value := &fakeItem{Color: "dark white"}
	setOptions := state.SetStateOption{}
	setReq := &state.SetRequest{
		Key:     key,
		ETag:    nil,
		Value:   value,
		Options: setOptions,
		Metadata: map[string]string{
			"ttlInSeconds": "10000",
		},
	}
	err := s.Set(context.Background(), setReq)
	require.NoError(t, err)
	_, getValue := getItem(t, s, key)
	assert.Equal(t, value.Color, getValue.Color, "Response must be as set")
	require.NoError(t, err, "Unexpired element with future expiration time must not be treated as error")

	deleteItem(t, s, key, nil)
}

func setItemWithNoKey(t *testing.T, s state.Store) {
	setReq := &state.SetRequest{
		Key: "",
	}

	err := s.Set(context.Background(), setReq)
	require.Error(t, err)
}

func testSetItemWithInvalidTTL(t *testing.T, s state.Store) {
	setReq := &state.SetRequest{
		Key:   randomKey(),
		Value: &fakeItem{Color: "oceanblue"},
		Metadata: (map[string]string{
			"ttlInSeconds": "XX",
		}),
	}
	err := s.Set(context.Background(), setReq)
	require.Error(t, err, "Setting a value with a proper key and a incorrect TTL value should be produce an error")
}

func testSetItemWithNegativeTTL(t *testing.T, s state.Store) {
	setReq := &state.SetRequest{
		Key:   randomKey(),
		Value: &fakeItem{Color: "oceanblue"},
		Metadata: (map[string]string{
			"ttlInSeconds": "-10",
		}),
	}
	err := s.Set(context.Background(), setReq)
	require.Error(t, err, "Setting a value with a proper key and a negative (other than -1) TTL value should be produce an error")
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

	err := s.BulkSet(context.Background(), setReq, state.BulkStoreOpts{})
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

	err = s.BulkDelete(context.Background(), deleteReq, state.BulkStoreOpts{})
	require.NoError(t, err)
	assert.False(t, storeItemExists(t, s, setReq[0].Key))
	assert.False(t, storeItemExists(t, s, setReq[1].Key))
}

// testInitConfiguration tests valid and invalid config settings.
func testInitConfiguration(t *testing.T) {
	logger := logger.NewLogger("test")
	tests := []struct {
		name        string
		props       map[string]string
		expectedErr string
	}{
		{
			name:        "Empty",
			props:       map[string]string{},
			expectedErr: "missing connection string",
		},
		{
			name: "Valid connection string",
			props: map[string]string{
				"connectionString": getConnectionString(),
				"tableName":        "test_state",
			},
			expectedErr: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := NewSQLiteStateStore(logger)
			defer p.Close()

			metadata := state.Metadata{
				Base: metadata.Base{
					Properties: tt.props,
				},
			}

			err := p.Init(context.Background(), metadata)
			if tt.expectedErr == "" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				require.ErrorContains(t, err, tt.expectedErr)
			}
		})
	}
}

func getConnectionString() string {
	s := os.Getenv(connectionStringEnvKey)
	if s == "" {
		// default integration test with in-memory db.
		s = ":memory:"
	}
	return s
}

func setItem(t *testing.T, s state.Store, key string, value interface{}, etag *string) {
	setOptions := state.SetStateOption{}
	if etag != nil {
		setOptions.Concurrency = state.FirstWrite
	}

	setReq := &state.SetRequest{
		Key:     key,
		ETag:    etag,
		Value:   value,
		Options: setOptions,
	}

	err := s.Set(context.Background(), setReq)
	require.NoError(t, err)
	itemExists := storeItemExists(t, s, key)
	assert.True(t, itemExists, "Item should exist after set has been executed")
}

func getItem(t *testing.T, s state.Store, key string) (*state.GetResponse, *fakeItem) {
	getReq := &state.GetRequest{
		Key:     key,
		Options: state.GetStateOption{},
	}

	response, getErr := s.Get(context.Background(), getReq)
	require.NoError(t, getErr)
	assert.NotNil(t, response)
	outputObject := &fakeItem{}
	_ = json.Unmarshal(response.Data, outputObject)

	return response, outputObject
}

func deleteItem(t *testing.T, s state.Store, key string, etag *string) {
	deleteReq := &state.DeleteRequest{
		Key:     key,
		ETag:    etag,
		Options: state.DeleteStateOption{},
	}

	deleteErr := s.Delete(context.Background(), deleteReq)
	require.NoError(t, deleteErr)
	assert.False(t, storeItemExists(t, s, key), "item should no longer exist after delete has been performed")
}

func storeItemExists(t *testing.T, s state.Store, key string) bool {
	dba := s.(interface{ GetDBAccess() *sqliteDBAccess }).GetDBAccess()
	tableName := dba.metadata.TableName
	db := dba.db
	var rowCount int32
	stmttpl := "SELECT count(key) FROM %s WHERE key = ?"
	statement := fmt.Sprintf(stmttpl, tableName)
	err := db.QueryRow(statement, key).Scan(&rowCount)
	require.NoError(t, err)
	exists := rowCount > 0
	return exists
}

func getRowData(t *testing.T, s state.Store, key string) (returnValue string, updatedate sql.NullString) {
	dba := s.(interface{ GetDBAccess() *sqliteDBAccess }).GetDBAccess()
	tableName := dba.metadata.TableName
	db := dba.db
	err := db.QueryRow(fmt.Sprintf("SELECT value, update_time FROM %s WHERE key = ?", tableName), key).
		Scan(&returnValue, &updatedate)
	require.NoError(t, err)

	return returnValue, updatedate
}

func getTimesForRow(t *testing.T, s state.Store, key string) (updatedate sql.NullString, expirationtime sql.NullString) {
	dba := s.(interface{ GetDBAccess() *sqliteDBAccess }).GetDBAccess()
	tableName := dba.metadata.TableName
	db := dba.db
	err := db.QueryRow(fmt.Sprintf("SELECT update_time, expiration_time FROM %s WHERE key = ?", tableName), key).
		Scan(&updatedate, &expirationtime)
	require.NoError(t, err)

	return updatedate, expirationtime
}
