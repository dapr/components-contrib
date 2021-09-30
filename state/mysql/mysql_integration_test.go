// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------
package mysql

import (
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"testing"

	"github.com/go-sql-driver/mysql"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	"github.com/dapr/components-contrib/state"
	"github.com/dapr/kit/logger"
)

const (
	// Environment variable containing the connection string.
	connectionStringEnvKey = "DAPR_TEST_MYSQL_CONNSTRING"

	// Set to the path of the PEM file required to connect to MySQL over SSL.
	pemPathEnvKey = "DAPR_TEST_MYSQL_PEMPATH"
)

type fakeItem struct {
	Color string
}

func TestMySQLIntegration(t *testing.T) {
	t.Parallel()

	// When the connection string is not set these tests will simply be skipped.
	// This makes sure the test do not try to run during any CI builds.
	connectionString := getConnectionString("")
	if connectionString == "" {
		t.Skipf(
			`MySQL state integration tests skipped.
			To enable define the connection string
			using environment variable '%s'
			(example 'export %s="root:password@tcp(localhost:3306)/")`,
			connectionStringEnvKey, connectionStringEnvKey)
	}

	t.Run("Test init configurations", func(t *testing.T) {
		testInitConfiguration(t)
	})

	pemPath := getPemPath()

	metadata := state.Metadata{
		Properties: map[string]string{connectionStringKey: connectionString, pemPathKey: pemPath},
	}

	mys := NewMySQLStateStore(logger.NewLogger("test"))
	t.Cleanup(func() {
		defer mys.Close()
	})

	error := mys.Init(metadata)
	if error != nil {
		t.Fatal(error)
	}

	t.Run("Create table succeeds", func(t *testing.T) {
		t.Parallel()
		testCreateTable(t, mys)
	})

	t.Run("Get Set Delete one item", func(t *testing.T) {
		t.Parallel()
		setGetUpdateDeleteOneItem(t, mys)
	})

	t.Run("Get item that does not exist", func(t *testing.T) {
		t.Parallel()
		getItemThatDoesNotExist(t, mys)
	})

	t.Run("Get item with no key fails", func(t *testing.T) {
		t.Parallel()
		getItemWithNoKey(t, mys)
	})

	t.Run("Set updates the updatedate field", func(t *testing.T) {
		t.Parallel()
		setUpdatesTheUpdatedateField(t, mys)
	})

	t.Run("Set item with no key fails", func(t *testing.T) {
		t.Parallel()
		setItemWithNoKey(t, mys)
	})

	t.Run("Bulk set and bulk delete", func(t *testing.T) {
		t.Parallel()
		testBulkSetAndBulkDelete(t, mys)
	})

	t.Run("Update and delete with eTag succeeds", func(t *testing.T) {
		t.Parallel()
		updateAndDeleteWithETagSucceeds(t, mys)
	})

	t.Run("Update with old eTag fails", func(t *testing.T) {
		t.Parallel()
		updateWithOldETagFails(t, mys)
	})

	t.Run("Insert with eTag fails", func(t *testing.T) {
		t.Parallel()
		newItemWithEtagFails(t, mys)
	})

	t.Run("Delete with invalid eTag fails", func(t *testing.T) {
		t.Parallel()
		deleteWithInvalidEtagFails(t, mys)
	})

	t.Run("Delete item with no key fails", func(t *testing.T) {
		t.Parallel()
		deleteWithNoKeyFails(t, mys)
	})

	t.Run("Delete an item that does not exist", func(t *testing.T) {
		t.Parallel()
		deleteItemThatDoesNotExist(t, mys)
	})

	t.Run("Multi with delete and set", func(t *testing.T) {
		t.Parallel()
		multiWithDeleteAndSet(t, mys)
	})

	t.Run("Multi with delete only", func(t *testing.T) {
		t.Parallel()
		multiWithDeleteOnly(t, mys)
	})

	t.Run("Multi with set only", func(t *testing.T) {
		t.Parallel()
		multiWithSetOnly(t, mys)
	})
}

func multiWithSetOnly(t *testing.T, mys *MySQL) {
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

	err := mys.Multi(&state.TransactionalStateRequest{
		Operations: operations,
	})
	assert.Nil(t, err)

	for _, set := range setRequests {
		assert.True(t, storeItemExists(t, set.Key))
		deleteItem(t, mys, set.Key, nil)
	}
}

func multiWithDeleteOnly(t *testing.T, mys *MySQL) {
	var operations []state.TransactionalStateOperation
	var deleteRequests []state.DeleteRequest
	for i := 0; i < 3; i++ {
		req := state.DeleteRequest{Key: randomKey()}

		// Add the item to the database
		setItem(t, mys, req.Key, randomJSON(), nil)

		// Add the item to a slice of delete requests
		deleteRequests = append(deleteRequests, req)

		// Add the item to the multi transaction request
		operations = append(operations, state.TransactionalStateOperation{
			Operation: state.Delete,
			Request:   req,
		})
	}

	err := mys.Multi(&state.TransactionalStateRequest{
		Operations: operations,
	})
	assert.Nil(t, err)

	for _, delete := range deleteRequests {
		assert.False(t, storeItemExists(t, delete.Key))
	}
}

func multiWithDeleteAndSet(t *testing.T, mys *MySQL) {
	var operations []state.TransactionalStateOperation
	var deleteRequests []state.DeleteRequest
	for i := 0; i < 3; i++ {
		req := state.DeleteRequest{Key: randomKey()}

		// Add the item to the database
		setItem(t, mys, req.Key, randomJSON(), nil)

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

	err := mys.Multi(&state.TransactionalStateRequest{
		Operations: operations,
	})
	assert.Nil(t, err)

	for _, delete := range deleteRequests {
		assert.False(t, storeItemExists(t, delete.Key))
	}

	for _, set := range setRequests {
		assert.True(t, storeItemExists(t, set.Key))
		deleteItem(t, mys, set.Key, nil)
	}
}

func deleteItemThatDoesNotExist(t *testing.T, mys *MySQL) {
	// Delete the item with a key not in the store
	deleteReq := &state.DeleteRequest{
		Key: randomKey(),
	}

	err := mys.Delete(deleteReq)
	assert.Nil(t, err)
}

func deleteWithNoKeyFails(t *testing.T, mys *MySQL) {
	deleteReq := &state.DeleteRequest{
		Key: "",
	}

	err := mys.Delete(deleteReq)
	assert.NotNil(t, err)
}

func deleteWithInvalidEtagFails(t *testing.T, mys *MySQL) {
	// Create new item
	key := randomKey()
	value := &fakeItem{Color: "mauve"}
	setItem(t, mys, key, value, nil)

	eTag := "1234"

	// Delete the item with a fake eTag
	deleteReq := &state.DeleteRequest{
		Key:  key,
		ETag: &eTag,
	}

	err := mys.Delete(deleteReq)
	assert.NotNil(t, err)
}

// newItemWithEtagFails creates a new item and also supplies an ETag, which is
// invalid - expect failure.
func newItemWithEtagFails(t *testing.T, mys *MySQL) {
	value := &fakeItem{Color: "teal"}
	invalidETag := "12345"

	setReq := &state.SetRequest{
		Key:   randomKey(),
		ETag:  &invalidETag,
		Value: value,
	}

	err := mys.Set(setReq)
	assert.NotNil(t, err)
}

func updateWithOldETagFails(t *testing.T, mys *MySQL) {
	// Create and retrieve new item
	key := randomKey()
	value := &fakeItem{Color: "gray"}
	setItem(t, mys, key, value, nil)

	getResponse, _ := getItem(t, mys, key)
	assert.NotNil(t, getResponse.ETag)
	originalEtag := getResponse.ETag

	// Change the value and get the updated eTag
	newValue := &fakeItem{Color: "silver"}
	setItem(t, mys, key, newValue, originalEtag)

	_, updatedItem := getItem(t, mys, key)
	assert.Equal(t, newValue, updatedItem)

	// Update again with the original eTag - expect update failure
	newValue = &fakeItem{Color: "maroon"}
	setReq := &state.SetRequest{
		Key:   key,
		ETag:  originalEtag,
		Value: newValue,
	}

	err := mys.Set(setReq)
	assert.NotNil(t, err, "Error was not thrown using old eTag")
}

func updateAndDeleteWithETagSucceeds(t *testing.T, mys *MySQL) {
	// Create and retrieve new item
	key := randomKey()
	value := &fakeItem{Color: "hazel"}
	setItem(t, mys, key, value, nil)
	getResponse, _ := getItem(t, mys, key)
	assert.NotNil(t, getResponse.ETag)

	// Change the value and compare
	value.Color = "purple"
	setItem(t, mys, key, value, getResponse.ETag)
	updateResponse, updatedItem := getItem(t, mys, key)
	assert.Equal(t, value, updatedItem, "Item should have been updated")
	assert.NotEqual(t, getResponse.ETag, updateResponse.ETag,
		"ETag should change when item is updated")

	// Delete
	deleteItem(t, mys, key, updateResponse.ETag)

	assert.False(t, storeItemExists(t, key), "Item is not in the data store")
}

// Tests valid bulk sets and deletes.
func testBulkSetAndBulkDelete(t *testing.T, mys *MySQL) {
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

	err := mys.BulkSet(setReq)
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

	err = mys.BulkDelete(deleteReq)
	assert.Nil(t, err)
	assert.False(t, storeItemExists(t, setReq[0].Key))
	assert.False(t, storeItemExists(t, setReq[1].Key))
}

func setItemWithNoKey(t *testing.T, mys *MySQL) {
	setReq := &state.SetRequest{
		Key: "",
	}

	err := mys.Set(setReq)
	assert.NotNil(t, err, "Error was not nil when setting item with no key.")
}

// setUpdatesTheUpdatedateField proves that the updatedate is set for an
// update, and set upon insert. The updatedate is used as the eTag so must be
// set. It is also auto updated on update by MySQL.
func setUpdatesTheUpdatedateField(t *testing.T, mys *MySQL) {
	key := randomKey()
	value := &fakeItem{Color: "orange"}
	setItem(t, mys, key, value, nil)

	// insertdate and updatedate should have a value
	_, insertdate, updatedate, eTag := getRowData(t, key)
	assert.NotNil(t, insertdate, "insertdate was not set")
	assert.NotNil(t, updatedate, "updatedate was not set")

	// insertdate should not change, updatedate should have a value
	value = &fakeItem{Color: "aqua"}
	setItem(t, mys, key, value, nil)
	_, newinsertdate, _, newETag := getRowData(t, key)
	assert.Equal(t, insertdate, newinsertdate, "InsertDate was changed")
	assert.NotEqual(t, eTag, newETag, "eTag was not updated")

	deleteItem(t, mys, key, nil)
}

// getItemWithNoKey validates that attempting a Get operation without providing
// a key will return an error.
func getItemWithNoKey(t *testing.T, mys *MySQL) {
	getReq := &state.GetRequest{
		Key: "",
	}

	response, getErr := mys.Get(getReq)
	assert.NotNil(t, getErr)
	assert.Nil(t, response)
}

// getItemThatDoesNotExist validates the behavior of retrieving an item that
// does not exist.
func getItemThatDoesNotExist(t *testing.T, mys *MySQL) {
	key := randomKey()
	response, outputObject := getItem(t, mys, key)
	assert.Nil(t, response.Data)
	assert.Equal(t, "", outputObject.Color)
}

// setGetUpdateDeleteOneItem validates setting one item, getting it, and
// deleting it.
func setGetUpdateDeleteOneItem(t *testing.T, mys *MySQL) {
	key := randomKey()
	value := &fakeItem{Color: "yellow"}

	setItem(t, mys, key, value, nil)

	getResponse, outputObject := getItem(t, mys, key)
	assert.Equal(t, value, outputObject)

	newValue := &fakeItem{Color: "green"}
	setItem(t, mys, key, newValue, getResponse.ETag)
	getResponse, outputObject = getItem(t, mys, key)
	assert.Equal(t, newValue, outputObject)

	deleteItem(t, mys, key, getResponse.ETag)
}

// testCreateTable tests the ability to create the state table.
func testCreateTable(t *testing.T, mys *MySQL) {
	tableName := "test_state"

	// Drop the table if it already exists
	exists, err := tableExists(mys.db, tableName)
	assert.Nil(t, err)
	if exists {
		dropTable(t, mys.db, tableName)
	}

	// Create the state table and test for its existence
	// There should be no error
	err = mys.ensureStateTable(tableName)
	assert.Nil(t, err)

	// Now create it and make sure there are no errors
	exists, err = tableExists(mys.db, tableName)
	assert.Nil(t, err)
	assert.True(t, exists)

	// Drop the state table
	dropTable(t, mys.db, tableName)
}

// testInitConfiguration tests valid and invalid config settings.
func testInitConfiguration(t *testing.T) {
	logger := logger.NewLogger("test")

	// define a struct the contain the metadata and create
	// two instances of it in a tests slice
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
			name: "Valid connection string",
			props: map[string]string{
				connectionStringKey: getConnectionString(""),
				pemPathKey:          getPemPath(),
			},
			expectedErr: "",
		},
		{
			name: "Valid table name",
			props: map[string]string{
				connectionStringKey: getConnectionString(""),
				pemPathKey:          getPemPath(),
				tableNameKey:        "stateStore",
			},
			expectedErr: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := NewMySQLStateStore(logger)
			defer p.Close()

			metadata := state.Metadata{
				Properties: tt.props,
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

func dropTable(t *testing.T, db *sql.DB, tableName string) {
	_, err := db.Exec(fmt.Sprintf(
		`DROP TABLE %s;`,
		tableName))
	assert.Nil(t, err)
}

func setItem(t *testing.T, mys *MySQL, key string, value interface{}, eTag *string) {
	setReq := &state.SetRequest{
		Key:   key,
		ETag:  eTag,
		Value: value,
	}

	err := mys.Set(setReq)
	assert.Nil(t, err, "Error setting an item")
	itemExists := storeItemExists(t, key)
	assert.True(t, itemExists, "Item does not exist after being set")
}

func getItem(t *testing.T, mys *MySQL, key string) (*state.GetResponse, *fakeItem) {
	getReq := &state.GetRequest{
		Key:     key,
		Options: state.GetStateOption{},
	}

	response, getErr := mys.Get(getReq)
	assert.Nil(t, getErr)
	assert.NotNil(t, response)
	outputObject := &fakeItem{}
	_ = json.Unmarshal(response.Data, outputObject)

	return response, outputObject
}

func deleteItem(t *testing.T, mys *MySQL, key string, eTag *string) {
	deleteReq := &state.DeleteRequest{
		Key:     key,
		ETag:    eTag,
		Options: state.DeleteStateOption{},
	}

	deleteErr := mys.Delete(deleteReq)
	assert.Nil(t, deleteErr, "There was an error deleting a record")
	assert.False(t, storeItemExists(t, key), "Item still exists after delete")
}

func storeItemExists(t *testing.T, key string) bool {
	db, err := connectToDB(t)
	assert.Nil(t, err)
	defer db.Close()

	exists := false
	statement := fmt.Sprintf(
		`SELECT EXISTS (SELECT * FROM %s WHERE id = ?)`,
		defaultTableName)
	err = db.QueryRow(statement, key).Scan(&exists)
	assert.Nil(t, err)

	return exists
}

func getRowData(t *testing.T, key string) (returnValue string, insertdate sql.NullString, updatedate sql.NullString, eTag string) {
	db, err := connectToDB(t)
	assert.Nil(t, err)
	defer db.Close()

	err = db.QueryRow(fmt.Sprintf(
		`SELECT value, insertdate, updatedate, eTag FROM %s WHERE id = ?`,
		defaultTableName), key).Scan(&returnValue, &insertdate, &updatedate, &eTag)
	assert.Nil(t, err)

	return returnValue, insertdate, updatedate, eTag
}

// Connects to MySQL using SSL if required.
func connectToDB(t *testing.T) (*sql.DB, error) {
	val := getPemPath()
	if val != "" {
		rootCertPool := x509.NewCertPool()
		pem, readErr := ioutil.ReadFile(val)

		assert.Nil(t, readErr, "Could not read PEM file")

		ok := rootCertPool.AppendCertsFromPEM(pem)

		assert.True(t, ok, "failed to append PEM")

		mysql.RegisterTLSConfig("custom", &tls.Config{RootCAs: rootCertPool, MinVersion: tls.VersionTLS12})
	}

	db, err := sql.Open("mysql", getConnectionString(defaultSchemaName))

	return db, err
}

func randomKey() string {
	return uuid.New().String()
}

func randomJSON() *fakeItem {
	return &fakeItem{Color: randomKey()}
}

// Returns the connection string
// The value is read from an environment variable.
func getConnectionString(database string) string {
	connectionString := os.Getenv(connectionStringEnvKey)

	if database != "" {
		parts := strings.Split(connectionString, "/")
		connectionString = fmt.Sprintf("%s/%s%s", parts[0], database, parts[1])
	}

	return connectionString
}

// Returns the full path to the PEM file used to connect to Azure MySQL over
// SSL. The connection string must end with &tls=custom
// The value is read from an environment variable.
func getPemPath() string {
	return os.Getenv(pemPathEnvKey)
}
