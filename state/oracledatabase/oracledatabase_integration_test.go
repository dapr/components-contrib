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

package oracledatabase

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
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
	connectionStringEnvKey     = "DAPR_TEST_ORACLE_DATABASE_CONNECTSTRING" // Environment variable containing the connection string.
	oracleWalletLocationEnvKey = "DAPR_TEST_ORACLE_WALLET_LOCATION"        // Environment variable containing the directory that contains the Oracle Wallet contents.
)

type fakeItem struct {
	Color string `json:"color"`
}

func TestOracleDatabaseIntegration(t *testing.T) {
	connectionString := getConnectionString()
	if connectionString == "" {
		// first run export DAPR_TEST_ORACLE_DATABASE_CONNECTSTRING="oracle://demo:demo@localhost:1521/xe".

		// for autonomous first run: export DAPR_TEST_ORACLE_DATABASE_CONNECTSTRING="oracle://demo:Modem123mode@adb.us-ashburn-1.oraclecloud.com:1522/k8j2fvxbaujdcfy_daprdb_low.adb.oraclecloud.com".
		// then also run: export DAPR_TEST_ORACLE_WALLET_LOCATION="/home/lucas/dapr-work/components-contrib/state/oracledatabase/Wallet_daprDB/".

		t.Skipf("Oracle Database state integration tests skipped. To enable define the connection string using environment variable '%s' (example 'export %s=\"oracle://username:password@host:port/servicename\")", connectionStringEnvKey, connectionStringEnvKey)
	}

	t.Run("Test init configurations", func(t *testing.T) {
		testInitConfiguration(t)
	})
	oracleWalletLocation := getWalletLocation()

	metadata := state.Metadata{
		Base: metadata.Base{Properties: map[string]string{
			connectionStringKey:     connectionString,
			oracleWalletLocationKey: oracleWalletLocation,
		}},
	}

	ods := NewOracleDatabaseStateStore(logger.NewLogger("test"))
	t.Cleanup(func() {
		defer ods.Close()
	})

	if initerror := ods.Init(context.Background(), metadata); initerror != nil {
		t.Fatal(initerror)
	}

	t.Run("Create table succeeds", func(t *testing.T) {
		testCreateTable(t, ods.(*OracleDatabase).GetDBAccess())
	})

	t.Run("Get Set Delete one item", func(t *testing.T) {
		setGetUpdateDeleteOneItem(t, ods)
	})

	t.Run("Get item that does not exist", func(t *testing.T) {
		getItemThatDoesNotExist(t, ods)
	})

	t.Run("Get item with no key fails", func(t *testing.T) {
		getItemWithNoKey(t, ods)
	})

	t.Run("Set item with invalid (non numeric) TTL", func(t *testing.T) {
		testSetItemWithInvalidTTL(t, ods)
	})
	t.Run("Set item with negative TTL", func(t *testing.T) {
		testSetItemWithNegativeTTL(t, ods)
	})
	t.Run("Set with TTL updates the expiration field", func(t *testing.T) {
		setTTLUpdatesExpiry(t, ods)
	})
	t.Run("Set with TTL followed by set without TTL resets the expiration field", func(t *testing.T) {
		setNoTTLUpdatesExpiry(t, ods)
	})
	t.Run("Expired item cannot be read", func(t *testing.T) {
		expiredStateCannotBeRead(t, ods)
	})
	t.Run("Unexpired item be read", func(t *testing.T) {
		unexpiredStateCanBeRead(t, ods)
	})
	t.Run("Set updates the updatedate field", func(t *testing.T) {
		setUpdatesTheUpdatedateField(t, ods)
	})

	t.Run("Set item with no key fails", func(t *testing.T) {
		setItemWithNoKey(t, ods)
	})

	t.Run("Bulk set and bulk delete", func(t *testing.T) {
		testBulkSetAndBulkDelete(t, ods)
	})

	t.Run("Update and delete with etag succeeds", func(t *testing.T) {
		updateAndDeleteWithEtagSucceeds(t, ods)
	})

	t.Run("Update with old etag fails", func(t *testing.T) {
		updateWithOldEtagFails(t, ods)
	})

	t.Run("Insert with etag fails", func(t *testing.T) {
		newItemWithEtagFails(t, ods)
	})

	t.Run("Delete with invalid etag fails when first write is enforced", func(t *testing.T) {
		deleteWithInvalidEtagFails(t, ods)
	})

	t.Run("Delete item with no key fails", func(t *testing.T) {
		deleteWithNoKeyFails(t, ods)
	})

	t.Run("Delete an item that does not exist", func(t *testing.T) {
		deleteItemThatDoesNotExist(t, ods)
	})
	t.Run("Multi with delete and set", func(t *testing.T) {
		multiWithDeleteAndSet(t, ods)
	})

	t.Run("Multi with delete only", func(t *testing.T) {
		multiWithDeleteOnly(t, ods)
	})

	t.Run("Multi with set only", func(t *testing.T) {
		multiWithSetOnly(t, ods)
	})
}

// setGetUpdateDeleteOneItem validates setting one item, getting it, and deleting it.
func setGetUpdateDeleteOneItem(t *testing.T, ods state.Store) {
	key := randomKey()
	value := &fakeItem{Color: "yellow"}

	setItem(t, ods, key, value, nil)

	getResponse, outputObject := getItem(t, ods, key)
	assert.Equal(t, value, outputObject)

	newValue := &fakeItem{Color: "green"}
	setItem(t, ods, key, newValue, getResponse.ETag)
	getResponse, outputObject = getItem(t, ods, key)

	assert.Equal(t, newValue, outputObject)

	deleteItem(t, ods, key, getResponse.ETag)
}

// testCreateTable tests the ability to create the state table.
func testCreateTable(t *testing.T, dba *oracleDatabaseAccess) {
	tableName := "test_state"

	// Drop the table if it already exists.
	exists, err := tableExists(dba.db, tableName)
	require.NoError(t, err)
	if exists {
		dropTable(t, dba.db, tableName)
	}

	// Create the state table and test for its existence.
	err = dba.ensureStateTable(tableName)
	require.NoError(t, err)
	exists, err = tableExists(dba.db, tableName)
	require.NoError(t, err)
	assert.True(t, exists)

	// Drop the state table.
	dropTable(t, dba.db, tableName)
}

func dropTable(t *testing.T, db *sql.DB, tableName string) {
	_, err := db.Exec("DROP TABLE " + tableName)
	require.NoError(t, err)
}

func deleteItemThatDoesNotExist(t *testing.T, ods state.Store) {
	// Delete the item with a key not in the store.
	deleteReq := &state.DeleteRequest{
		Key: randomKey(),
	}
	err := ods.Delete(context.Background(), deleteReq)
	require.NoError(t, err)
}

func multiWithSetOnly(t *testing.T, ods state.Store) {
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

	err := ods.(state.TransactionalStore).Multi(context.Background(), &state.TransactionalStateRequest{
		Operations: operations,
	})
	require.NoError(t, err)

	db := getDB(ods)

	for _, set := range setRequests {
		assert.True(t, storeItemExists(t, db, set.Key))
		deleteItem(t, ods, set.Key, nil)
	}
}

func multiWithDeleteOnly(t *testing.T, ods state.Store) {
	var operations []state.TransactionalStateOperation //nolint:prealloc
	var deleteRequests []state.DeleteRequest           //nolint:prealloc
	for range 3 {
		req := state.DeleteRequest{Key: randomKey()}

		// Add the item to the database.
		setItem(t, ods, req.Key, randomJSON(), nil) // Add the item to the database.

		// Add the item to a slice of delete requests.
		deleteRequests = append(deleteRequests, req)

		// Add the item to the multi transaction request.
		operations = append(operations, req)
	}

	err := ods.(state.TransactionalStore).Multi(context.Background(), &state.TransactionalStateRequest{
		Operations: operations,
	})
	require.NoError(t, err)

	db := getDB(ods)
	for _, delete := range deleteRequests {
		assert.False(t, storeItemExists(t, db, delete.Key))
	}
}

func multiWithDeleteAndSet(t *testing.T, ods state.Store) {
	var operations []state.TransactionalStateOperation //nolint:prealloc
	var deleteRequests []state.DeleteRequest           //nolint:prealloc
	for range 3 {
		req := state.DeleteRequest{Key: randomKey()}

		// Add the item to the database.
		setItem(t, ods, req.Key, randomJSON(), nil) // Add the item to the database.

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

	err := ods.(state.TransactionalStore).Multi(context.Background(), &state.TransactionalStateRequest{
		Operations: operations,
	})
	require.NoError(t, err)

	db := getDB(ods)
	for _, delete := range deleteRequests {
		assert.False(t, storeItemExists(t, db, delete.Key))
	}
	for _, set := range setRequests {
		assert.True(t, storeItemExists(t, db, set.Key))
		deleteItem(t, ods, set.Key, nil)
	}
}

func deleteWithInvalidEtagFails(t *testing.T, ods state.Store) {
	// Create new item.
	key := randomKey()
	value := &fakeItem{Color: "mauvebrown"}
	setItem(t, ods, key, value, nil)

	etag := "1234"
	// Delete the item with a fake etag.
	deleteReq := &state.DeleteRequest{
		Key:  key,
		ETag: &etag,
		Options: state.DeleteStateOption{
			Concurrency: state.FirstWrite,
		},
	}
	err := ods.Delete(context.Background(), deleteReq)
	require.Error(t, err, "Deleting an item with the wrong etag while enforcing FirstWrite policy should fail")
}

func deleteWithNoKeyFails(t *testing.T, ods state.Store) {
	deleteReq := &state.DeleteRequest{
		Key: "",
	}
	err := ods.Delete(context.Background(), deleteReq)
	require.Error(t, err)
}

// newItemWithEtagFails creates a new item and also supplies a non existent ETag and requests FirstWrite, which is invalid - expect failure.
func newItemWithEtagFails(t *testing.T, ods state.Store) {
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

	err := ods.Set(context.Background(), setReq)
	require.Error(t, err)
}

func updateWithOldEtagFails(t *testing.T, ods state.Store) {
	// Create and retrieve new item.
	key := randomKey()
	value := &fakeItem{Color: "gray"}
	setItem(t, ods, key, value, nil)
	getResponse, _ := getItem(t, ods, key)
	assert.NotNil(t, getResponse.ETag)
	originalEtag := getResponse.ETag

	// Change the value and get the updated etag.
	newValue := &fakeItem{Color: "silver"}
	setItem(t, ods, key, newValue, originalEtag)
	_, updatedItem := getItem(t, ods, key)
	assert.Equal(t, newValue, updatedItem)
	getResponse, _ = getItem(t, ods, key)
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
	err := ods.Set(context.Background(), setReq)
	require.Error(t, err)
}

func updateAndDeleteWithEtagSucceeds(t *testing.T, ods state.Store) {
	// Create and retrieve new item.
	key := randomKey()
	value := &fakeItem{Color: "hazel"}
	setItem(t, ods, key, value, nil)
	getResponse, _ := getItem(t, ods, key)
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
	err := ods.Set(context.Background(), setReq)
	require.NoError(t, err, "Setting the item should be successful")
	updateResponse, updatedItem := getItem(t, ods, key)
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
	err = ods.Delete(context.Background(), deleteReq)
	require.NoError(t, err, "Deleting an item with the right etag while enforcing FirstWrite policy should succeed")

	// Item is not in the data store.
	db := getDB(ods)
	assert.False(t, storeItemExists(t, db, key))
}

// getItemThatDoesNotExist validates the behavior of retrieving an item that does not exist.
func getItemThatDoesNotExist(t *testing.T, ods state.Store) {
	key := randomKey()
	response, outputObject := getItem(t, ods, key)
	assert.Nil(t, response.Data)

	assert.Equal(t, "", outputObject.Color)
}

// getItemWithNoKey validates that attempting a Get operation without providing a key will return an error.
func getItemWithNoKey(t *testing.T, ods state.Store) {
	getReq := &state.GetRequest{
		Key: "",
	}

	response, getErr := ods.Get(context.Background(), getReq)
	require.Error(t, getErr)
	assert.Nil(t, response)
}

// setUpdatesTheUpdatedateField proves that the updateddate is set for an update, and not set upon insert.
func setUpdatesTheUpdatedateField(t *testing.T, ods state.Store) {
	key := randomKey()
	value := &fakeItem{Color: "orange"}
	setItem(t, ods, key, value, nil)

	db := getDB(ods)

	// insertdate should have a value and updatedate should be nil.
	_, insertdate, updatedate := getRowData(t, db, key)
	assert.NotNil(t, insertdate)
	assert.Equal(t, "", updatedate.String)

	// insertdate should not change, updatedate should have a value.
	value = &fakeItem{Color: "aqua"}
	setItem(t, ods, key, value, nil)
	_, newinsertdate, updatedate := getRowData(t, db, key)
	assert.Equal(t, insertdate, newinsertdate) // The insertdate should not change.
	assert.NotEqual(t, "", updatedate.String)

	deleteItem(t, ods, key, nil)
}

// setTTLUpdatesExpiry proves that the expirydate is set when a TTL is passed for a key.
func setTTLUpdatesExpiry(t *testing.T, ods state.Store) {
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

	err := ods.Set(context.Background(), setReq)
	require.NoError(t, err)

	// expirationTime should be set (to a date in the future).
	db := getDB(ods)
	_, _, expirationTime := getTimesForRow(t, db, key)

	assert.NotNil(t, expirationTime)
	assert.True(t, expirationTime.Valid, "Expiration Time should have a value after set with TTL value")

	resp, err := ods.Get(context.Background(), &state.GetRequest{Key: key})
	require.NoError(t, err)
	assert.Contains(t, resp.Metadata, "ttlExpireTime")
	expireTime, err := time.Parse(time.RFC3339, resp.Metadata["ttlExpireTime"])
	require.NoError(t, err)
	assert.InDelta(t, time.Now().Add(time.Second*1000).Unix(), expireTime.Unix(), 10)

	deleteItem(t, ods, key, nil)
}

// setNoTTLUpdatesExpiry proves that the expirydate is reset when a state element with expiration time (TTL) loses TTL upon second set without TTL.
func setNoTTLUpdatesExpiry(t *testing.T, ods state.Store) {
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
	err := ods.Set(context.Background(), setReq)
	require.NoError(t, err)
	delete(setReq.Metadata, "ttlInSeconds")
	err = ods.Set(context.Background(), setReq)
	require.NoError(t, err)

	// expirationTime should not be set.
	db := getDB(ods)
	_, _, expirationTime := getTimesForRow(t, db, key)
	resp, err := ods.Get(context.Background(), &state.GetRequest{Key: key})
	require.NoError(t, err)
	assert.NotContains(t, resp.Metadata, "ttlExpireTime")

	assert.False(t, expirationTime.Valid, "Expiration Time should not have a value after first being set with TTL value and then being set without TTL value")
	deleteItem(t, ods, key, nil)
}

// expiredStateCannotBeRead proves that an expired state element can not be read.
func expiredStateCannotBeRead(t *testing.T, ods state.Store) {
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
	err := ods.Set(context.Background(), setReq)
	require.NoError(t, err)

	time.Sleep(time.Second * time.Duration(2))
	getResponse, err := ods.Get(context.Background(), &state.GetRequest{Key: key})
	assert.Equal(t, &state.GetResponse{}, getResponse, "Response must be empty")
	require.NoError(t, err, "Expired element must not be treated as error")

	deleteItem(t, ods, key, nil)
}

// unexpiredStateCanBeRead proves that a state element with TTL - but no yet expired - can be read.
func unexpiredStateCanBeRead(t *testing.T, ods state.Store) {
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
	err := ods.Set(context.Background(), setReq)
	require.NoError(t, err)
	_, getValue := getItem(t, ods, key)
	assert.Equal(t, value.Color, getValue.Color, "Response must be as set")
	require.NoError(t, err, "Unexpired element with future expiration time must not be treated as error")

	deleteItem(t, ods, key, nil)
}

func setItemWithNoKey(t *testing.T, ods state.Store) {
	setReq := &state.SetRequest{
		Key: "",
	}

	err := ods.Set(context.Background(), setReq)
	require.Error(t, err)
}

func testSetItemWithInvalidTTL(t *testing.T, ods state.Store) {
	setReq := &state.SetRequest{
		Key:   randomKey(),
		Value: &fakeItem{Color: "oceanblue"},
		Metadata: (map[string]string{
			"ttlInSeconds": "XX",
		}),
	}
	err := ods.Set(context.Background(), setReq)
	require.Error(t, err, "Setting a value with a proper key and a incorrect TTL value should be produce an error")
}

func testSetItemWithNegativeTTL(t *testing.T, ods state.Store) {
	setReq := &state.SetRequest{
		Key:   randomKey(),
		Value: &fakeItem{Color: "oceanblue"},
		Metadata: (map[string]string{
			"ttlInSeconds": "-10",
		}),
	}
	err := ods.Set(context.Background(), setReq)
	require.Error(t, err, "Setting a value with a proper key and a negative (other than -1) TTL value should be produce an error")
}

// Tests valid bulk sets and deletes.
func testBulkSetAndBulkDelete(t *testing.T, ods state.Store) {
	db := getDB(ods)

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

	err := ods.BulkSet(context.Background(), setReq, state.BulkStoreOpts{})
	require.NoError(t, err)
	assert.True(t, storeItemExists(t, db, setReq[0].Key))
	assert.True(t, storeItemExists(t, db, setReq[1].Key))

	deleteReq := []state.DeleteRequest{
		{
			Key: setReq[0].Key,
		},
		{
			Key: setReq[1].Key,
		},
	}

	err = ods.BulkDelete(context.Background(), deleteReq, state.BulkStoreOpts{})
	require.NoError(t, err)
	assert.False(t, storeItemExists(t, db, setReq[0].Key))
	assert.False(t, storeItemExists(t, db, setReq[1].Key))
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
			expectedErr: errMissingConnectionString,
		},
		{
			name:        "Valid connection string",
			props:       map[string]string{connectionStringKey: getConnectionString(), oracleWalletLocationKey: getWalletLocation()},
			expectedErr: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := NewOracleDatabaseStateStore(logger)
			defer p.Close()

			metadata := state.Metadata{
				Base: metadata.Base{Properties: tt.props},
			}

			err := p.Init(context.Background(), metadata)
			if tt.expectedErr == "" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				assert.Equal(t, tt.expectedErr, err.Error())
			}
		})
	}
}

func getConnectionString() string {
	return os.Getenv(connectionStringEnvKey)
}

func getDB(ods state.Store) *sql.DB {
	return ods.(*OracleDatabase).getDB()
}

func getWalletLocation() string {
	return os.Getenv(oracleWalletLocationEnvKey)
}

func setItem(t *testing.T, ods state.Store, key string, value any, etag *string) {
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

	db := getDB(ods)

	err := ods.Set(context.Background(), setReq)
	require.NoError(t, err)
	itemExists := storeItemExists(t, db, key)
	assert.True(t, itemExists, "Item should exist after set has been executed")
}

func getItem(t *testing.T, ods state.Store, key string) (*state.GetResponse, *fakeItem) {
	getReq := &state.GetRequest{
		Key:     key,
		Options: state.GetStateOption{},
	}

	response, getErr := ods.Get(context.Background(), getReq)
	require.NoError(t, getErr)
	assert.NotNil(t, response)
	outputObject := &fakeItem{}
	_ = json.Unmarshal(response.Data, outputObject)

	return response, outputObject
}

func deleteItem(t *testing.T, ods state.Store, key string, etag *string) {
	deleteReq := &state.DeleteRequest{
		Key:     key,
		ETag:    etag,
		Options: state.DeleteStateOption{},
	}

	db := getDB(ods)

	deleteErr := ods.Delete(context.Background(), deleteReq)
	require.NoError(t, deleteErr)
	assert.False(t, storeItemExists(t, db, key), "item should no longer exist after delete has been performed")
}

func storeItemExists(t *testing.T, db *sql.DB, key string) bool {
	var got string
	statement := fmt.Sprintf(`SELECT key FROM %s WHERE key = :key`, defaultTableName)
	err := db.QueryRow(statement, key).Scan(&got)
	if errors.Is(err, sql.ErrNoRows) {
		return false
	}
	require.NoError(t, err)
	return true
}

func getRowData(t *testing.T, db *sql.DB, key string) (returnValue string, insertdate sql.NullString, updatedate sql.NullString) {
	err := db.QueryRow(fmt.Sprintf("SELECT value, creation_time, update_time FROM %s WHERE key = :key", defaultTableName), key).Scan(&returnValue, &insertdate, &updatedate)
	require.NoError(t, err)
	return returnValue, insertdate, updatedate
}

func getTimesForRow(t *testing.T, db *sql.DB, key string) (insertdate sql.NullString, updatedate sql.NullString, expirationtime sql.NullString) {
	err := db.QueryRow(fmt.Sprintf("SELECT creation_time, update_time, expiration_time FROM %s WHERE key = :key", defaultTableName), key).
		Scan(&insertdate, &updatedate, &expirationtime)
	require.NoError(t, err)
	return insertdate, updatedate, expirationtime
}

func randomKey() string {
	return uuid.New().String()
}

func randomJSON() *fakeItem {
	return &fakeItem{Color: randomKey()}
}
