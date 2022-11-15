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
package postgresql

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/state"
	"github.com/dapr/kit/logger"
)

const (
	connectionStringEnvKey = "DAPR_TEST_POSTGRES_CONNSTRING" // Environment variable containing the connection string
)

type fakeItem struct {
	Color string
}

func TestPostgreSQLIntegration(t *testing.T) {
	connectionString := getConnectionString()
	if connectionString == "" {
		t.Skipf("PostgreSQL state integration tests skipped. To enable define the connection string using environment variable '%s' (example 'export %s=\"host=localhost user=postgres password=example port=5432 connect_timeout=10 database=dapr_test\")", connectionStringEnvKey, connectionStringEnvKey)
	}

	t.Run("Test init configurations", func(t *testing.T) {
		testInitConfiguration(t)
	})

	metadata := state.Metadata{
		Base: metadata.Base{Properties: map[string]string{connectionStringKey: connectionString}},
	}

	pgs := NewPostgreSQLStateStore(logger.NewLogger("test")).(*PostgreSQL)
	t.Cleanup(func() {
		defer pgs.Close()
	})

	error := pgs.Init(metadata)
	if error != nil {
		t.Fatal(error)
	}

	t.Run("Create table succeeds", func(t *testing.T) {
		t.Parallel()
		testCreateTable(t, pgs.dbaccess.(*postgresDBAccess))
	})

	t.Run("Get Set Delete one item", func(t *testing.T) {
		t.Parallel()
		setGetUpdateDeleteOneItem(t, pgs)
	})

	t.Run("Get item that does not exist", func(t *testing.T) {
		t.Parallel()
		getItemThatDoesNotExist(t, pgs)
	})

	t.Run("Get item with no key fails", func(t *testing.T) {
		t.Parallel()
		getItemWithNoKey(t, pgs)
	})

	t.Run("Set updates the updatedate field", func(t *testing.T) {
		t.Parallel()
		setUpdatesTheUpdatedateField(t, pgs)
	})

	t.Run("Set item with no key fails", func(t *testing.T) {
		t.Parallel()
		setItemWithNoKey(t, pgs)
	})

	t.Run("Bulk set and bulk delete", func(t *testing.T) {
		t.Parallel()
		testBulkSetAndBulkDelete(t, pgs)
	})

	t.Run("Update and delete with etag succeeds", func(t *testing.T) {
		t.Parallel()
		updateAndDeleteWithEtagSucceeds(t, pgs)
	})

	t.Run("Update with old etag fails", func(t *testing.T) {
		t.Parallel()
		updateWithOldEtagFails(t, pgs)
	})

	t.Run("Insert with etag fails", func(t *testing.T) {
		t.Parallel()
		newItemWithEtagFails(t, pgs)
	})

	t.Run("Delete with invalid etag fails", func(t *testing.T) {
		t.Parallel()
		deleteWithInvalidEtagFails(t, pgs)
	})

	t.Run("Delete item with no key fails", func(t *testing.T) {
		t.Parallel()
		deleteWithNoKeyFails(t, pgs)
	})

	t.Run("Delete an item that does not exist", func(t *testing.T) {
		t.Parallel()
		deleteItemThatDoesNotExist(t, pgs)
	})

	t.Run("Multi with delete and set", func(t *testing.T) {
		t.Parallel()
		multiWithDeleteAndSet(t, pgs)
	})

	t.Run("Multi with delete only", func(t *testing.T) {
		t.Parallel()
		multiWithDeleteOnly(t, pgs)
	})

	t.Run("Multi with set only", func(t *testing.T) {
		t.Parallel()
		multiWithSetOnly(t, pgs)
	})
}

// setGetUpdateDeleteOneItem validates setting one item, getting it, and deleting it.
func setGetUpdateDeleteOneItem(t *testing.T, pgs *PostgreSQL) {
	key := randomKey()
	value := &fakeItem{Color: "yellow"}

	setItem(t, pgs, key, value, nil)

	getResponse, outputObject := getItem(t, pgs, key)
	assert.Equal(t, value, outputObject)

	newValue := &fakeItem{Color: "green"}
	setItem(t, pgs, key, newValue, getResponse.ETag)
	getResponse, outputObject = getItem(t, pgs, key)
	assert.Equal(t, newValue, outputObject)

	deleteItem(t, pgs, key, getResponse.ETag)
}

// testCreateTable tests the ability to create the state table.
func testCreateTable(t *testing.T, dba *postgresDBAccess) {
	tableName := "test_state"

	// Drop the table if it already exists
	exists, err := tableExists(dba.db, tableName)
	assert.Nil(t, err)
	if exists {
		dropTable(t, dba.db, tableName)
	}

	// Create the state table and test for its existence
	err = dba.ensureStateTable(tableName)
	assert.Nil(t, err)
	exists, err = tableExists(dba.db, tableName)
	assert.Nil(t, err)
	assert.True(t, exists)

	// Drop the state table
	dropTable(t, dba.db, tableName)
}

func dropTable(t *testing.T, db *sql.DB, tableName string) {
	_, err := db.Exec(fmt.Sprintf("DROP TABLE %s", tableName))
	assert.Nil(t, err)
}

func deleteItemThatDoesNotExist(t *testing.T, pgs *PostgreSQL) {
	// Delete the item with a key not in the store
	deleteReq := &state.DeleteRequest{
		Key: randomKey(),
	}
	err := pgs.Delete(deleteReq)
	assert.Nil(t, err)
}

func multiWithSetOnly(t *testing.T, pgs *PostgreSQL) {
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

	err := pgs.Multi(&state.TransactionalStateRequest{
		Operations: operations,
	})
	assert.Nil(t, err)

	for _, set := range setRequests {
		assert.True(t, storeItemExists(t, set.Key))
		deleteItem(t, pgs, set.Key, nil)
	}
}

func multiWithDeleteOnly(t *testing.T, pgs *PostgreSQL) {
	var operations []state.TransactionalStateOperation
	var deleteRequests []state.DeleteRequest
	for i := 0; i < 3; i++ {
		req := state.DeleteRequest{Key: randomKey()}

		// Add the item to the database
		setItem(t, pgs, req.Key, randomJSON(), nil) // Add the item to the database

		// Add the item to a slice of delete requests
		deleteRequests = append(deleteRequests, req)

		// Add the item to the multi transaction request
		operations = append(operations, state.TransactionalStateOperation{
			Operation: state.Delete,
			Request:   req,
		})
	}

	err := pgs.Multi(&state.TransactionalStateRequest{
		Operations: operations,
	})
	assert.Nil(t, err)

	for _, delete := range deleteRequests {
		assert.False(t, storeItemExists(t, delete.Key))
	}
}

func multiWithDeleteAndSet(t *testing.T, pgs *PostgreSQL) {
	var operations []state.TransactionalStateOperation
	var deleteRequests []state.DeleteRequest
	for i := 0; i < 3; i++ {
		req := state.DeleteRequest{Key: randomKey()}

		// Add the item to the database
		setItem(t, pgs, req.Key, randomJSON(), nil) // Add the item to the database

		// Add the item to a slice of delete requests
		deleteRequests = append(deleteRequests, req)

		// Add the item to the multi transaction request
		operations = append(operations, state.TransactionalStateOperation{
			Operation: state.Delete,
			Request:   req,
		})
	}

	// Create the set requests
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

	err := pgs.Multi(&state.TransactionalStateRequest{
		Operations: operations,
	})
	assert.Nil(t, err)

	for _, delete := range deleteRequests {
		assert.False(t, storeItemExists(t, delete.Key))
	}

	for _, set := range setRequests {
		assert.True(t, storeItemExists(t, set.Key))
		deleteItem(t, pgs, set.Key, nil)
	}
}

func deleteWithInvalidEtagFails(t *testing.T, pgs *PostgreSQL) {
	// Create new item
	key := randomKey()
	value := &fakeItem{Color: "mauve"}
	setItem(t, pgs, key, value, nil)

	etag := "1234"
	// Delete the item with a fake etag
	deleteReq := &state.DeleteRequest{
		Key:  key,
		ETag: &etag,
	}
	err := pgs.Delete(deleteReq)
	assert.NotNil(t, err)
}

func deleteWithNoKeyFails(t *testing.T, pgs *PostgreSQL) {
	deleteReq := &state.DeleteRequest{
		Key: "",
	}
	err := pgs.Delete(deleteReq)
	assert.NotNil(t, err)
}

// newItemWithEtagFails creates a new item and also supplies an ETag, which is invalid - expect failure.
func newItemWithEtagFails(t *testing.T, pgs *PostgreSQL) {
	value := &fakeItem{Color: "teal"}
	invalidEtag := "12345"

	setReq := &state.SetRequest{
		Key:   randomKey(),
		ETag:  &invalidEtag,
		Value: value,
	}

	err := pgs.Set(setReq)
	assert.NotNil(t, err)
}

func updateWithOldEtagFails(t *testing.T, pgs *PostgreSQL) {
	// Create and retrieve new item
	key := randomKey()
	value := &fakeItem{Color: "gray"}
	setItem(t, pgs, key, value, nil)
	getResponse, _ := getItem(t, pgs, key)
	assert.NotNil(t, getResponse.ETag)
	originalEtag := getResponse.ETag

	// Change the value and get the updated etag
	newValue := &fakeItem{Color: "silver"}
	setItem(t, pgs, key, newValue, originalEtag)
	_, updatedItem := getItem(t, pgs, key)
	assert.Equal(t, newValue, updatedItem)

	// Update again with the original etag - expect udpate failure
	newValue = &fakeItem{Color: "maroon"}
	setReq := &state.SetRequest{
		Key:   key,
		ETag:  originalEtag,
		Value: newValue,
	}
	err := pgs.Set(setReq)
	assert.NotNil(t, err)
}

func updateAndDeleteWithEtagSucceeds(t *testing.T, pgs *PostgreSQL) {
	// Create and retrieve new item
	key := randomKey()
	value := &fakeItem{Color: "hazel"}
	setItem(t, pgs, key, value, nil)
	getResponse, _ := getItem(t, pgs, key)
	assert.NotNil(t, getResponse.ETag)

	// Change the value and compare
	value.Color = "purple"
	setItem(t, pgs, key, value, getResponse.ETag)
	updateResponse, updatedItem := getItem(t, pgs, key)
	assert.Equal(t, value, updatedItem)

	// ETag should change when item is updated
	assert.NotEqual(t, getResponse.ETag, updateResponse.ETag)

	// Delete
	deleteItem(t, pgs, key, updateResponse.ETag)

	// Item is not in the data store
	assert.False(t, storeItemExists(t, key))
}

// getItemThatDoesNotExist validates the behavior of retrieving an item that does not exist.
func getItemThatDoesNotExist(t *testing.T, pgs *PostgreSQL) {
	key := randomKey()
	response, outputObject := getItem(t, pgs, key)
	assert.Nil(t, response.Data)
	assert.Equal(t, "", outputObject.Color)
}

// getItemWithNoKey validates that attempting a Get operation without providing a key will return an error.
func getItemWithNoKey(t *testing.T, pgs *PostgreSQL) {
	getReq := &state.GetRequest{
		Key: "",
	}

	response, getErr := pgs.Get(getReq)
	assert.NotNil(t, getErr)
	assert.Nil(t, response)
}

// setUpdatesTheUpdatedateField proves that the updateddate is set for an update, and not set upon insert.
func setUpdatesTheUpdatedateField(t *testing.T, pgs *PostgreSQL) {
	key := randomKey()
	value := &fakeItem{Color: "orange"}
	setItem(t, pgs, key, value, nil)

	// insertdate should have a value and updatedate should be nil
	_, insertdate, updatedate := getRowData(t, key)
	assert.NotNil(t, insertdate)
	assert.Equal(t, "", updatedate.String)

	// insertdate should not change, updatedate should have a value
	value = &fakeItem{Color: "aqua"}
	setItem(t, pgs, key, value, nil)
	_, newinsertdate, updatedate := getRowData(t, key)
	assert.Equal(t, insertdate, newinsertdate) // The insertdate should not change.
	assert.NotEqual(t, "", updatedate.String)

	deleteItem(t, pgs, key, nil)
}

func setItemWithNoKey(t *testing.T, pgs *PostgreSQL) {
	setReq := &state.SetRequest{
		Key: "",
	}

	err := pgs.Set(setReq)
	assert.NotNil(t, err)
}

// Tests valid bulk sets and deletes.
func testBulkSetAndBulkDelete(t *testing.T, pgs *PostgreSQL) {
	setReq := []state.SetRequest{
		{
			Key:   randomKey(),
			Value: &fakeItem{Color: "blue"},
		},
		{
			Key:   randomKey(),
			Value: &fakeItem{Color: "red"},
		},
	}

	err := pgs.BulkSet(setReq)
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

	err = pgs.BulkDelete(deleteReq)
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
			props:       map[string]string{connectionStringKey: getConnectionString()},
			expectedErr: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := NewPostgreSQLStateStore(logger).(*PostgreSQL)
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

func setItem(t *testing.T, pgs *PostgreSQL, key string, value interface{}, etag *string) {
	setReq := &state.SetRequest{
		Key:   key,
		ETag:  etag,
		Value: value,
	}

	err := pgs.Set(setReq)
	assert.Nil(t, err)
	itemExists := storeItemExists(t, key)
	assert.True(t, itemExists)
}

func getItem(t *testing.T, pgs *PostgreSQL, key string) (*state.GetResponse, *fakeItem) {
	getReq := &state.GetRequest{
		Key:     key,
		Options: state.GetStateOption{},
	}

	response, getErr := pgs.Get(getReq)
	assert.Nil(t, getErr)
	assert.NotNil(t, response)
	outputObject := &fakeItem{}
	_ = json.Unmarshal(response.Data, outputObject)

	return response, outputObject
}

func deleteItem(t *testing.T, pgs *PostgreSQL, key string, etag *string) {
	deleteReq := &state.DeleteRequest{
		Key:     key,
		ETag:    etag,
		Options: state.DeleteStateOption{},
	}

	deleteErr := pgs.Delete(deleteReq)
	assert.Nil(t, deleteErr)
	assert.False(t, storeItemExists(t, key))
}

func storeItemExists(t *testing.T, key string) bool {
	db, err := sql.Open("pgx", getConnectionString())
	assert.Nil(t, err)
	defer db.Close()

	exists := false
	statement := fmt.Sprintf(`SELECT EXISTS (SELECT FROM %s WHERE key = $1)`, defaultTableName)
	err = db.QueryRow(statement, key).Scan(&exists)
	assert.Nil(t, err)

	return exists
}

func getRowData(t *testing.T, key string) (returnValue string, insertdate sql.NullString, updatedate sql.NullString) {
	db, err := sql.Open("pgx", getConnectionString())
	assert.Nil(t, err)
	defer db.Close()

	err = db.QueryRow(fmt.Sprintf("SELECT value, insertdate, updatedate FROM %s WHERE key = $1", defaultTableName), key).Scan(&returnValue, &insertdate, &updatedate)
	assert.Nil(t, err)

	return returnValue, insertdate, updatedate
}

func randomKey() string {
	return uuid.New().String()
}

func randomJSON() *fakeItem {
	return &fakeItem{Color: randomKey()}
}
