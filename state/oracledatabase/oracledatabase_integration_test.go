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
	"database/sql"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/state"
	"github.com/dapr/kit/logger"
)

const (
	connectionStringEnvKey     = "DAPR_TEST_ORACLE_DATABASE_CONNECTSTRING" // Environment variable containing the connection string.
	oracleWalletLocationEnvKey = "DAPR_TEST_ORACLE_WALLET_LOCATION"        // Environment variable containing the directory that contains the Oracle Wallet contents.
)

type fakeItem struct {
	Color string
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
		Base: metadata.Base{Properties: map[string]string{connectionStringKey: connectionString, oracleWalletLocationKey: oracleWalletLocation}},
	}

	ods := NewOracleDatabaseStateStore(logger.NewLogger("test")).(*OracleDatabase)
	t.Cleanup(func() {
		defer ods.Close()
	})

	if initerror := ods.Init(metadata); initerror != nil {
		t.Fatal(initerror)
	}

	t.Run("Create table succeeds", func(t *testing.T) {
		testCreateTable(t, ods.dbaccess.(*oracleDatabaseAccess))
	})

	t.Run("Get Set Delete one item", func(t *testing.T) {
		t.Parallel()
		setGetUpdateDeleteOneItem(t, ods)
	})

	t.Run("Get item that does not exist", func(t *testing.T) {
		t.Parallel()
		getItemThatDoesNotExist(t, ods)
	})

	t.Run("Get item with no key fails", func(t *testing.T) {
		t.Parallel()
		getItemWithNoKey(t, ods)
	})

	t.Run("Set item with invalid (non numeric) TTL", func(t *testing.T) {
		t.Parallel()
		testSetItemWithInvalidTTL(t, ods)
	})
	t.Run("Set item with negative TTL", func(t *testing.T) {
		t.Parallel()
		testSetItemWithNegativeTTL(t, ods)
	})
	t.Run("Set with TTL updates the expiration field", func(t *testing.T) {
		setTTLUpdatesExpiry(t, ods)
	})
	t.Run("Set with TTL followed by set without TTL resets the expiration field", func(t *testing.T) {
		setNoTTLUpdatesExpiry(t, ods)
	})
	t.Run("Expired item cannot be read", func(t *testing.T) {
		t.Parallel()
		expiredStateCannotBeRead(t, ods)
	})
	t.Run("Unexpired item be read", func(t *testing.T) {
		t.Parallel()
		unexpiredStateCanBeRead(t, ods)
	})
	t.Run("Set updates the updatedate field", func(t *testing.T) {
		setUpdatesTheUpdatedateField(t, ods)
	})

	t.Run("Set item with no key fails", func(t *testing.T) {
		t.Parallel()
		setItemWithNoKey(t, ods)
	})

	t.Run("Bulk set and bulk delete", func(t *testing.T) {
		//	t.Parallel()
		testBulkSetAndBulkDelete(t, ods)
	})

	t.Run("Update and delete with etag succeeds", func(t *testing.T) {
		//	t.Parallel()
		updateAndDeleteWithEtagSucceeds(t, ods)
	})

	t.Run("Update with old etag fails", func(t *testing.T) {
		//	t.Parallel()
		updateWithOldEtagFails(t, ods)
	})

	t.Run("Insert with etag fails", func(t *testing.T) {
		t.Parallel()
		newItemWithEtagFails(t, ods)
	})

	t.Run("Delete with invalid etag fails when first write is enforced", func(t *testing.T) {
		t.Parallel()
		deleteWithInvalidEtagFails(t, ods)
	})
	t.Run("Update and Delete with invalid etag and no first write policy enforced succeeds", func(t *testing.T) {
		t.Parallel()
		updateAndDeleteWithWrongEtagAndNoFirstWriteSucceeds(t, ods)
	})

	t.Run("Delete item with no key fails", func(t *testing.T) {
		t.Parallel()
		deleteWithNoKeyFails(t, ods)
	})

	t.Run("Delete an item that does not exist", func(t *testing.T) {
		t.Parallel()
		deleteItemThatDoesNotExist(t, ods)
	})
	t.Run("Multi with delete and set", func(t *testing.T) {
		//	t.Parallel()
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
func setGetUpdateDeleteOneItem(t *testing.T, ods *OracleDatabase) {
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
	assert.Nil(t, err)
	if exists {
		dropTable(t, dba.db, tableName)
	}

	// Create the state table and test for its existence.
	err = dba.ensureStateTable(tableName)
	assert.Nil(t, err)
	exists, err = tableExists(dba.db, tableName)
	assert.Nil(t, err)
	assert.True(t, exists)

	// Drop the state table.
	dropTable(t, dba.db, tableName)
}

func dropTable(t *testing.T, db *sql.DB, tableName string) {
	_, err := db.Exec(fmt.Sprintf("DROP TABLE %s", tableName))
	assert.Nil(t, err)
}

func deleteItemThatDoesNotExist(t *testing.T, ods *OracleDatabase) {
	// Delete the item with a key not in the store.
	deleteReq := &state.DeleteRequest{
		Key: randomKey(),
	}
	err := ods.Delete(deleteReq)
	assert.Nil(t, err)
}

func multiWithSetOnly(t *testing.T, ods *OracleDatabase) {
	var operations []state.TransactionalStateOperation
	var setRequests []state.SetRequest
	for i := 0; i < 3; i++ {
		req := state.SetRequest{
			Key:   randomKey(),
			Value: randomJSON(),
		}
		setRequests = append(setRequests, req)
		operations = append(operations, state.TransactionalStateOperation{
			Operation: state.Upsert,
			Request:   req,
		})
	}

	err := ods.Multi(&state.TransactionalStateRequest{
		Operations: operations,
	})
	assert.Nil(t, err)

	for _, set := range setRequests {
		assert.True(t, storeItemExists(t, set.Key))
		deleteItem(t, ods, set.Key, nil)
	}
}

func multiWithDeleteOnly(t *testing.T, ods *OracleDatabase) {
	var operations []state.TransactionalStateOperation
	var deleteRequests []state.DeleteRequest
	for i := 0; i < 3; i++ {
		req := state.DeleteRequest{Key: randomKey()}

		// Add the item to the database.
		setItem(t, ods, req.Key, randomJSON(), nil) // Add the item to the database.

		// Add the item to a slice of delete requests.
		deleteRequests = append(deleteRequests, req)

		// Add the item to the multi transaction request.
		operations = append(operations, state.TransactionalStateOperation{
			Operation: state.Delete,
			Request:   req,
		})
	}

	err := ods.Multi(&state.TransactionalStateRequest{
		Operations: operations,
	})
	assert.Nil(t, err)

	for _, delete := range deleteRequests {
		assert.False(t, storeItemExists(t, delete.Key))
	}
}

func multiWithDeleteAndSet(t *testing.T, ods *OracleDatabase) {
	var operations []state.TransactionalStateOperation
	var deleteRequests []state.DeleteRequest
	for i := 0; i < 3; i++ {
		req := state.DeleteRequest{Key: randomKey()}

		// Add the item to the database.
		setItem(t, ods, req.Key, randomJSON(), nil) // Add the item to the database.

		// Add the item to a slice of delete requests.
		deleteRequests = append(deleteRequests, req)

		// Add the item to the multi transaction request.
		operations = append(operations, state.TransactionalStateOperation{
			Operation: state.Delete,
			Request:   req,
		})
	}

	// Create the set requests.
	var setRequests []state.SetRequest
	for i := 0; i < 3; i++ {
		req := state.SetRequest{
			Key:   randomKey(),
			Value: randomJSON(),
		}
		setRequests = append(setRequests, req)
		operations = append(operations, state.TransactionalStateOperation{
			Operation: state.Upsert,
			Request:   req,
		})
	}

	err := ods.Multi(&state.TransactionalStateRequest{
		Operations: operations,
	})
	assert.Nil(t, err)

	for _, delete := range deleteRequests {
		assert.False(t, storeItemExists(t, delete.Key))
	}

	for _, set := range setRequests {
		assert.True(t, storeItemExists(t, set.Key))
		deleteItem(t, ods, set.Key, nil)
	}
}

func deleteWithInvalidEtagFails(t *testing.T, ods *OracleDatabase) {
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
	err := ods.Delete(deleteReq)
	assert.NotNil(t, err, "Deleting an item with the wrong etag while enforcing FirstWrite policy should fail")
}

func deleteWithNoKeyFails(t *testing.T, ods *OracleDatabase) {
	deleteReq := &state.DeleteRequest{
		Key: "",
	}
	err := ods.Delete(deleteReq)
	assert.NotNil(t, err)
}

// newItemWithEtagFails creates a new item and also supplies a non existent ETag and requests FirstWrite, which is invalid - expect failure.
func newItemWithEtagFails(t *testing.T, ods *OracleDatabase) {
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

	err := ods.Set(setReq)
	assert.NotNil(t, err)
}

func updateWithOldEtagFails(t *testing.T, ods *OracleDatabase) {
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
	err := ods.Set(setReq)
	assert.NotNil(t, err)
}

func updateAndDeleteWithEtagSucceeds(t *testing.T, ods *OracleDatabase) {
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
	err := ods.Set(setReq)
	assert.Nil(t, err, "Setting the item should be successful")
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
	err = ods.Delete(deleteReq)
	assert.Nil(t, err, "Deleting an item with the right etag while enforcing FirstWrite policy should succeed")

	// Item is not in the data store.
	assert.False(t, storeItemExists(t, key))
}

func updateAndDeleteWithWrongEtagAndNoFirstWriteSucceeds(t *testing.T, ods *OracleDatabase) {
	// Create and retrieve new item.
	key := randomKey()
	value := &fakeItem{Color: "hazel"}
	setItem(t, ods, key, value, nil)
	getResponse, _ := getItem(t, ods, key)
	assert.NotNil(t, getResponse.ETag)

	// Change the value and compare.
	value.Color = "purple"
	someInvalidEtag := "1234581736145"
	setReq := &state.SetRequest{
		Key:   key,
		ETag:  &someInvalidEtag,
		Value: value,
		Options: state.SetStateOption{
			Concurrency: state.LastWrite,
		},
	}
	err := ods.Set(setReq)
	assert.Nil(t, err, "Setting the item should be successful")
	_, updatedItem := getItem(t, ods, key)
	assert.Equal(t, value, updatedItem)

	// Delete.
	deleteReq := &state.DeleteRequest{
		Key:  key,
		ETag: &someInvalidEtag,
		Options: state.DeleteStateOption{
			Concurrency: state.LastWrite,
		},
	}
	err = ods.Delete(deleteReq)
	assert.Nil(t, err, "Deleting an item with the wrong etag but not enforcing FirstWrite policy should succeed")

	// Item is not in the data store.
	assert.False(t, storeItemExists(t, key))
}

// getItemThatDoesNotExist validates the behavior of retrieving an item that does not exist.
func getItemThatDoesNotExist(t *testing.T, ods *OracleDatabase) {
	key := randomKey()
	response, outputObject := getItem(t, ods, key)
	assert.Nil(t, response.Data)

	assert.Equal(t, "", outputObject.Color)
}

// getItemWithNoKey validates that attempting a Get operation without providing a key will return an error.
func getItemWithNoKey(t *testing.T, ods *OracleDatabase) {
	getReq := &state.GetRequest{
		Key: "",
	}

	response, getErr := ods.Get(getReq)
	assert.NotNil(t, getErr)
	assert.Nil(t, response)
}

// setUpdatesTheUpdatedateField proves that the updateddate is set for an update, and not set upon insert.
func setUpdatesTheUpdatedateField(t *testing.T, ods *OracleDatabase) {
	key := randomKey()
	value := &fakeItem{Color: "orange"}
	setItem(t, ods, key, value, nil)
	connectionString := getConnectionString()
	if getWalletLocation() != "" {
		connectionString += "?TRACE FILE=trace.log&SSL=enable&SSL Verify=false&WALLET=" + url.QueryEscape(getWalletLocation())
	}
	db, err := sql.Open("oracle", connectionString)
	assert.Nil(t, err)
	defer db.Close()

	// insertdate should have a value and updatedate should be nil.
	_, insertdate, updatedate := getRowData(t, key)
	assert.NotNil(t, insertdate)
	assert.Equal(t, "", updatedate.String)

	// insertdate should not change, updatedate should have a value.
	value = &fakeItem{Color: "aqua"}
	setItem(t, ods, key, value, nil)
	_, newinsertdate, updatedate := getRowData(t, key)
	assert.Equal(t, insertdate, newinsertdate) // The insertdate should not change.
	assert.NotEqual(t, "", updatedate.String)

	deleteItem(t, ods, key, nil)
}

// setTTLUpdatesExpiry proves that the expirydate is set when a TTL is passed for a key.
func setTTLUpdatesExpiry(t *testing.T, ods *OracleDatabase) {
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

	err := ods.Set(setReq)
	assert.Nil(t, err)
	connectionString := getConnectionString()
	if getWalletLocation() != "" {
		connectionString += "?TRACE FILE=trace.log&SSL=enable&SSL Verify=false&WALLET=" + url.QueryEscape(getWalletLocation())
	}
	db, err := sql.Open("oracle", connectionString)
	assert.Nil(t, err)
	defer db.Close()

	// expirationTime should be set (to a date in the future).
	_, _, expirationTime := getTimesForRow(t, key)

	assert.NotNil(t, expirationTime)
	assert.True(t, expirationTime.Valid, "Expiration Time should have a value after set with TTL value")
	deleteItem(t, ods, key, nil)
}

// setNoTTLUpdatesExpiry proves that the expirydate is reset when a state element with expiration time (TTL) loses TTL upon second set without TTL.
func setNoTTLUpdatesExpiry(t *testing.T, ods *OracleDatabase) {
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
	err := ods.Set(setReq)
	assert.Nil(t, err)
	delete(setReq.Metadata, "ttlInSeconds")
	err = ods.Set(setReq)
	assert.Nil(t, err)
	connectionString := getConnectionString()
	if getWalletLocation() != "" {
		connectionString += "?TRACE FILE=trace.log&SSL=enable&SSL Verify=false&WALLET=" + url.QueryEscape(getWalletLocation())
	}
	db, err := sql.Open("oracle", connectionString)
	assert.Nil(t, err)
	defer db.Close()

	// expirationTime should not be set.
	_, _, expirationTime := getTimesForRow(t, key)

	assert.True(t, !expirationTime.Valid, "Expiration Time should not have a value after first being set with TTL value and then being set without TTL value")
	deleteItem(t, ods, key, nil)
}

// expiredStateCannotBeRead proves that an expired state element can not be read.
func expiredStateCannotBeRead(t *testing.T, ods *OracleDatabase) {
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
	err := ods.Set(setReq)
	assert.Nil(t, err)

	time.Sleep(time.Second * time.Duration(2))
	getResponse, err := ods.Get(&state.GetRequest{Key: key})
	assert.Equal(t, &state.GetResponse{}, getResponse, "Response must be empty")
	assert.NoError(t, err, "Expired element must not be treated as error")

	deleteItem(t, ods, key, nil)
}

// unexpiredStateCanBeRead proves that a state element with TTL - but no yet expired - can be read.
func unexpiredStateCanBeRead(t *testing.T, ods *OracleDatabase) {
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
	err := ods.Set(setReq)
	assert.Nil(t, err)
	_, getValue := getItem(t, ods, key)
	assert.Equal(t, value.Color, getValue.Color, "Response must be as set")
	assert.NoError(t, err, "Unexpired element with future expiration time must not be treated as error")

	deleteItem(t, ods, key, nil)
}

func setItemWithNoKey(t *testing.T, ods *OracleDatabase) {
	setReq := &state.SetRequest{
		Key: "",
	}

	err := ods.Set(setReq)
	assert.NotNil(t, err)
}

func TestParseTTL(t *testing.T) {
	t.Parallel()
	t.Run("TTL Not an integer", func(t *testing.T) {
		t.Parallel()
		ttlInSeconds := "not an integer"
		ttl, err := parseTTL(map[string]string{
			"ttlInSeconds": ttlInSeconds,
		})
		assert.Error(t, err)
		assert.Nil(t, ttl)
	})
	t.Run("TTL specified with wrong key", func(t *testing.T) {
		t.Parallel()
		ttlInSeconds := 12345
		ttl, err := parseTTL(map[string]string{
			"expirationTime": strconv.Itoa(ttlInSeconds),
		})
		assert.NoError(t, err)
		assert.Nil(t, ttl)
	})
	t.Run("TTL is a number", func(t *testing.T) {
		t.Parallel()
		ttlInSeconds := 12345
		ttl, err := parseTTL(map[string]string{
			"ttlInSeconds": strconv.Itoa(ttlInSeconds),
		})
		assert.NoError(t, err)
		assert.Equal(t, *ttl, ttlInSeconds)
	})
	t.Run("TTL not set", func(t *testing.T) {
		t.Parallel()
		ttl, err := parseTTL(map[string]string{})
		assert.NoError(t, err)
		assert.Nil(t, ttl)
	})
}

func testSetItemWithInvalidTTL(t *testing.T, ods *OracleDatabase) {
	setReq := &state.SetRequest{
		Key:   randomKey(),
		Value: &fakeItem{Color: "oceanblue"},
		Metadata: (map[string]string{
			"ttlInSeconds": "XX",
		}),
	}
	err := ods.Set(setReq)
	assert.NotNil(t, err, "Setting a value with a proper key and a incorrect TTL value should be produce an error")
}

func testSetItemWithNegativeTTL(t *testing.T, ods *OracleDatabase) {
	setReq := &state.SetRequest{
		Key:   randomKey(),
		Value: &fakeItem{Color: "oceanblue"},
		Metadata: (map[string]string{
			"ttlInSeconds": "-10",
		}),
	}
	err := ods.Set(setReq)
	assert.NotNil(t, err, "Setting a value with a proper key and a negative (other than -1) TTL value should be produce an error")
}

// Tests valid bulk sets and deletes.
func testBulkSetAndBulkDelete(t *testing.T, ods *OracleDatabase) {
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

	err := ods.BulkSet(setReq)
	assert.Nil(t, err)
	assert.True(t, storeItemExists(t, setReq[0].Key))
	assert.True(t, storeItemExists(t, setReq[1].Key))

	deleteReq := []state.DeleteRequest{
		{
			Key: setReq[0].Key,
		},
		{
			Key: setReq[1].Key,
		},
	}

	err = ods.BulkDelete(deleteReq)
	assert.Nil(t, err)
	assert.False(t, storeItemExists(t, setReq[0].Key))
	assert.False(t, storeItemExists(t, setReq[1].Key))
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
			p := NewOracleDatabaseStateStore(logger).(*OracleDatabase)
			defer p.Close()

			metadata := state.Metadata{
				Base: metadata.Base{Properties: tt.props},
			}

			err := p.Init(metadata)
			if tt.expectedErr == "" {
				assert.Nil(t, err)
			} else {
				assert.NotNil(t, err)
				assert.Equal(t, err.Error(), tt.expectedErr)
			}
		})
	}
}

func getConnectionString() string {
	return os.Getenv(connectionStringEnvKey)
}

func getWalletLocation() string {
	return os.Getenv(oracleWalletLocationEnvKey)
}

func setItem(t *testing.T, ods *OracleDatabase, key string, value interface{}, etag *string) {
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

	err := ods.Set(setReq)
	assert.Nil(t, err)
	itemExists := storeItemExists(t, key)
	assert.True(t, itemExists, "Item should exist after set has been executed ")
}

func getItem(t *testing.T, ods *OracleDatabase, key string) (*state.GetResponse, *fakeItem) {
	getReq := &state.GetRequest{
		Key:     key,
		Options: state.GetStateOption{},
	}

	response, getErr := ods.Get(getReq)
	assert.Nil(t, getErr)
	assert.NotNil(t, response)
	outputObject := &fakeItem{}
	_ = json.Unmarshal(response.Data, outputObject)

	return response, outputObject
}

func deleteItem(t *testing.T, ods *OracleDatabase, key string, etag *string) {
	deleteReq := &state.DeleteRequest{
		Key:     key,
		ETag:    etag,
		Options: state.DeleteStateOption{},
	}

	deleteErr := ods.Delete(deleteReq)
	assert.Nil(t, deleteErr)
	assert.False(t, storeItemExists(t, key), "item should no longer exist after delete has been performed")
}

func storeItemExists(t *testing.T, key string) bool {
	connectionString := getConnectionString()
	if getWalletLocation() != "" {
		connectionString += "?TRACE FILE=trace.log&SSL=enable&SSL Verify=false&WALLET=" + url.QueryEscape(getWalletLocation())
	}
	db, err := sql.Open("oracle", connectionString)
	assert.Nil(t, err)
	defer db.Close()
	var rowCount int32
	statement := fmt.Sprintf(`SELECT count(key) FROM %s WHERE key = :key`, tableName)
	err = db.QueryRow(statement, key).Scan(&rowCount)
	assert.Nil(t, err)
	exists := rowCount > 0
	return exists
}

func getRowData(t *testing.T, key string) (returnValue string, insertdate sql.NullString, updatedate sql.NullString) {
	connectionString := getConnectionString()
	if getWalletLocation() != "" {
		connectionString += "?TRACE FILE=trace.log&SSL=enable&SSL Verify=false&WALLET=" + url.QueryEscape(getWalletLocation())
	}
	db, err := sql.Open("oracle", connectionString)
	assert.Nil(t, err)
	defer db.Close()
	err = db.QueryRow(fmt.Sprintf("SELECT value, creation_time, update_time FROM %s WHERE key = :key", tableName), key).Scan(&returnValue, &insertdate, &updatedate)
	assert.Nil(t, err)

	return returnValue, insertdate, updatedate
}

func getTimesForRow(t *testing.T, key string) (insertdate sql.NullString, updatedate sql.NullString, expirationtime sql.NullString) {
	connectionString := getConnectionString()
	if getWalletLocation() != "" {
		connectionString += "?TRACE FILE=trace.log&SSL=enable&SSL Verify=false&WALLET=" + url.QueryEscape(getWalletLocation())
	}
	db, err := sql.Open("oracle", connectionString)
	assert.Nil(t, err)
	defer db.Close()
	err = db.QueryRow(fmt.Sprintf("SELECT creation_time, update_time, expiration_time FROM %s WHERE key = :key", tableName), key).Scan(&insertdate, &updatedate, &expirationtime)
	assert.Nil(t, err)

	return insertdate, updatedate, expirationtime
}

func randomKey() string {
	return uuid.New().String()
}

func randomJSON() *fakeItem {
	return &fakeItem{Color: randomKey()}
}
