package sqlite

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/dapr/components-contrib/state"
	"github.com/dapr/kit/logger"
)

const (
	connectionStringEnvKey = "DAPR_TEST_SQLITE_DATABASE_CONNECTSTRING" // Environment variable containing the connection string.
)

func TestSqliteIntegration(t *testing.T) {
	cgo := os.Getenv("CGO_ENABLED")
	if cgo != "1" {
		t.Skipf("SQLite Database state integration tests skipped. To enable define CGO_ENABLED=1")
	}
	connectionString := getConnectionString()

	t.Run("Test init configurations", func(t *testing.T) {
		testInitConfiguration(t)
	})

	metadata := state.Metadata{
		Properties: map[string]string{
			connectionStringKey: connectionString,
			tableNameKey:        "test_state",
		},
	}

	s := NewSqliteStateStore(logger.NewLogger("test"))
	t.Cleanup(func() {
		defer s.Close()
	})

	if initerror := s.Init(metadata); initerror != nil {
		t.Fatal(initerror)
	}

	t.Run("Create table succeeds", func(t *testing.T) {
		testCreateTable(t, s.dbaccess.(*sqliteDBAccess))
	})

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
	t.Run("Update and Delete with invalid etag and no first write policy enforced succeeds", func(t *testing.T) {
		updateAndDeleteWithWrongEtagAndNoFirstWriteSucceeds(t, s)
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
}

// setGetUpdateDeleteOneItem validates setting one item, getting it, and deleting it.
func setGetUpdateDeleteOneItem(t *testing.T, s *Sqlite) {
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

// testCreateTable tests the ability to create the state table.
func testCreateTable(t *testing.T, dba *sqliteDBAccess) {
	tableName := "test_state_creation"

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

func deleteItemThatDoesNotExist(t *testing.T, s *Sqlite) {
	// Delete the item with a key not in the store.
	deleteReq := &state.DeleteRequest{
		Key: randomKey(),
	}
	err := s.Delete(deleteReq)
	assert.Nil(t, err)
}

func multiWithSetOnly(t *testing.T, s *Sqlite) {
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

	err := s.Multi(&state.TransactionalStateRequest{
		Operations: operations,
	})
	assert.Nil(t, err)

	for _, set := range setRequests {
		assert.True(t, storeItemExists(t, s, set.Key))
		deleteItem(t, s, set.Key, nil)
	}
}

func multiWithDeleteOnly(t *testing.T, s *Sqlite) {
	var operations []state.TransactionalStateOperation
	var deleteRequests []state.DeleteRequest
	for i := 0; i < 3; i++ {
		req := state.DeleteRequest{Key: randomKey()}

		// Add the item to the database.
		setItem(t, s, req.Key, randomJSON(), nil) // Add the item to the database.

		// Add the item to a slice of delete requests.
		deleteRequests = append(deleteRequests, req)

		// Add the item to the multi transaction request.
		operations = append(operations, state.TransactionalStateOperation{
			Operation: state.Delete,
			Request:   req,
		})
	}

	err := s.Multi(&state.TransactionalStateRequest{
		Operations: operations,
	})
	assert.Nil(t, err)

	for _, delete := range deleteRequests {
		assert.False(t, storeItemExists(t, s, delete.Key))
	}
}

func multiWithDeleteAndSet(t *testing.T, s *Sqlite) {
	var operations []state.TransactionalStateOperation
	var deleteRequests []state.DeleteRequest
	for i := 0; i < 3; i++ {
		req := state.DeleteRequest{Key: randomKey()}

		// Add the item to the database.
		setItem(t, s, req.Key, randomJSON(), nil) // Add the item to the database.

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

	err := s.Multi(&state.TransactionalStateRequest{
		Operations: operations,
	})
	assert.Nil(t, err)

	for _, delete := range deleteRequests {
		assert.False(t, storeItemExists(t, s, delete.Key))
	}

	for _, set := range setRequests {
		assert.True(t, storeItemExists(t, s, set.Key))
		deleteItem(t, s, set.Key, nil)
	}
}

func deleteWithInvalidEtagFails(t *testing.T, s *Sqlite) {
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
	err := s.Delete(deleteReq)
	assert.NotNil(t, err, "Deleting an item with the wrong etag while enforcing FirstWrite policy should fail")
}

func deleteWithNoKeyFails(t *testing.T, s *Sqlite) {
	deleteReq := &state.DeleteRequest{
		Key: "",
	}
	err := s.Delete(deleteReq)
	assert.NotNil(t, err)
}

// newItemWithEtagFails creates a new item and also supplies a non existent ETag and requests FirstWrite, which is invalid - expect failure.
func newItemWithEtagFails(t *testing.T, s *Sqlite) {
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

	err := s.Set(setReq)
	assert.NotNil(t, err)
}

func updateWithOldEtagFails(t *testing.T, s *Sqlite) {
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
	err := s.Set(setReq)
	assert.NotNil(t, err)
}

func updateAndDeleteWithEtagSucceeds(t *testing.T, s *Sqlite) {
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
	err := s.Set(setReq)
	assert.Nil(t, err, "Setting the item should be successful")
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
	err = s.Delete(deleteReq)
	assert.Nil(t, err, "Deleting an item with the right etag while enforcing FirstWrite policy should succeed")

	// Item is not in the data store.
	assert.False(t, storeItemExists(t, s, key))
}

func updateAndDeleteWithWrongEtagAndNoFirstWriteSucceeds(t *testing.T, s *Sqlite) {
	// Create and retrieve new item.
	key := randomKey()
	value := &fakeItem{Color: "hazel"}
	setItem(t, s, key, value, nil)
	getResponse, _ := getItem(t, s, key)
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
	err := s.Set(setReq)
	assert.Nil(t, err, "Setting the item should be successful")
	_, updatedItem := getItem(t, s, key)
	assert.Equal(t, value, updatedItem)

	// Delete.
	deleteReq := &state.DeleteRequest{
		Key:  key,
		ETag: &someInvalidEtag,
		Options: state.DeleteStateOption{
			Concurrency: state.LastWrite,
		},
	}
	err = s.Delete(deleteReq)
	assert.Nil(t, err, "Deleting an item with the wrong etag but not enforcing FirstWrite policy should succeed")

	// Item is not in the data store.
	assert.False(t, storeItemExists(t, s, key))
}

// getItemThatDoesNotExist validates the behavior of retrieving an item that does not exist.
func getItemThatDoesNotExist(t *testing.T, s *Sqlite) {
	key := randomKey()
	response, outputObject := getItem(t, s, key)
	assert.Nil(t, response.Data)

	assert.Equal(t, "", outputObject.Color)
}

// getItemWithNoKey validates that attempting a Get operation without providing a key will return an error.
func getItemWithNoKey(t *testing.T, s *Sqlite) {
	getReq := &state.GetRequest{
		Key: "",
	}

	response, getErr := s.Get(getReq)
	assert.NotNil(t, getErr)
	assert.Nil(t, response)
}

// setUpdatesTheUpdatedateField proves that the updateddate is set for an update.
func setUpdatesTheUpdatedateField(t *testing.T, s *Sqlite) {
	key := randomKey()
	value := &fakeItem{Color: "orange"}
	setItem(t, s, key, value, nil)

	// insertdate should have a value and updatedate should be nil.
	_, insertdate, updatedate := getRowData(t, s, key)
	assert.NotNil(t, insertdate)
	assert.Equal(t, insertdate.String, updatedate.String)

	// make sure update time changes from creation.
	time.Sleep(1 * time.Second)

	// insertdate should not change, updatedate should have a value.
	value = &fakeItem{Color: "aqua"}
	setItem(t, s, key, value, nil)
	_, newinsertdate, updatedate := getRowData(t, s, key)
	assert.Equal(t, insertdate, newinsertdate) // The insertdate should not change.
	assert.NotEqual(t, insertdate.String, updatedate.String)

	deleteItem(t, s, key, nil)
}

// setTTLUpdatesExpiry proves that the expirydate is set when a TTL is passed for a key.
func setTTLUpdatesExpiry(t *testing.T, s *Sqlite) {
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

	err := s.Set(setReq)
	assert.Nil(t, err)

	// expirationTime should be set (to a date in the future).
	_, _, expirationTime := getTimesForRow(t, s, key)

	assert.NotNil(t, expirationTime)
	assert.True(t, expirationTime.Valid, "Expiration Time should have a value after set with TTL value")
	deleteItem(t, s, key, nil)
}

// setNoTTLUpdatesExpiry proves that the expirydate is reset when a state element with expiration time (TTL) loses TTL upon second set without TTL.
func setNoTTLUpdatesExpiry(t *testing.T, s *Sqlite) {
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
	err := s.Set(setReq)
	assert.Nil(t, err)
	delete(setReq.Metadata, "ttlInSeconds")
	err = s.Set(setReq)
	assert.Nil(t, err)

	// expirationTime should not be set.
	_, _, expirationTime := getTimesForRow(t, s, key)

	assert.True(t, !expirationTime.Valid, "Expiration Time should not have a value after first being set with TTL value and then being set without TTL value")
	deleteItem(t, s, key, nil)
}

// expiredStateCannotBeRead proves that an expired state element can not be read.
func expiredStateCannotBeRead(t *testing.T, s *Sqlite) {
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
	err := s.Set(setReq)
	assert.Nil(t, err)

	time.Sleep(time.Second * time.Duration(2))
	getResponse, err := s.Get(&state.GetRequest{Key: key})
	assert.Equal(t, &state.GetResponse{}, getResponse, "Response must be empty")
	assert.NoError(t, err, "Expired element must not be treated as error")

	deleteItem(t, s, key, nil)
}

// unexpiredStateCanBeRead proves that a state element with TTL - but no yet expired - can be read.
func unexpiredStateCanBeRead(t *testing.T, s *Sqlite) {
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
	err := s.Set(setReq)
	assert.Nil(t, err)
	_, getValue := getItem(t, s, key)
	assert.Equal(t, value.Color, getValue.Color, "Response must be as set")
	assert.NoError(t, err, "Unexpired element with future expiration time must not be treated as error")

	deleteItem(t, s, key, nil)
}

func setItemWithNoKey(t *testing.T, s *Sqlite) {
	setReq := &state.SetRequest{
		Key: "",
	}

	err := s.Set(setReq)
	assert.NotNil(t, err)
}

func TestParseTTL(t *testing.T) {
	log := logger.NewLogger("parseTTL")
	t.Parallel()
	t.Run("TTL Not an integer", func(t *testing.T) {
		t.Parallel()
		ttlInSeconds := "not an integer"
		ttl, err := parseTTL(map[string]string{
			"ttlInSeconds": ttlInSeconds,
		}, log)
		assert.Error(t, err)
		assert.Nil(t, ttl)
	})
	t.Run("TTL specified with wrong key", func(t *testing.T) {
		t.Parallel()
		ttlInSeconds := 12345
		ttl, err := parseTTL(map[string]string{
			"expirationTime": strconv.Itoa(ttlInSeconds),
		}, log)
		assert.NoError(t, err)
		assert.Nil(t, ttl)
	})
	t.Run("TTL is a number", func(t *testing.T) {
		t.Parallel()
		ttl, err := parseTTL(map[string]string{
			"ttlInSeconds": "12345",
		}, log)
		assert.NoError(t, err)
		assert.Equal(t, *ttl, int64(12345))
	})
	t.Run("TTL not set", func(t *testing.T) {
		t.Parallel()
		ttl, err := parseTTL(map[string]string{}, log)
		assert.NoError(t, err)
		assert.Nil(t, ttl)
	})
}

func testSetItemWithInvalidTTL(t *testing.T, s *Sqlite) {
	setReq := &state.SetRequest{
		Key:   randomKey(),
		Value: &fakeItem{Color: "oceanblue"},
		Metadata: (map[string]string{
			"ttlInSeconds": "XX",
		}),
	}
	err := s.Set(setReq)
	assert.NotNil(t, err, "Setting a value with a proper key and a incorrect TTL value should be produce an error")
}

func testSetItemWithNegativeTTL(t *testing.T, s *Sqlite) {
	setReq := &state.SetRequest{
		Key:   randomKey(),
		Value: &fakeItem{Color: "oceanblue"},
		Metadata: (map[string]string{
			"ttlInSeconds": "-10",
		}),
	}
	err := s.Set(setReq)
	assert.NotNil(t, err, "Setting a value with a proper key and a negative (other than -1) TTL value should be produce an error")
}

// Tests valid bulk sets and deletes.
func testBulkSetAndBulkDelete(t *testing.T, s *Sqlite) {
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

	err := s.BulkSet(setReq)
	assert.Nil(t, err)
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

	err = s.BulkDelete(deleteReq)
	assert.Nil(t, err)
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
			expectedErr: errMissingConnectionString,
		},
		{
			name: "Valid connection string",
			props: map[string]string{
				connectionStringKey: getConnectionString(),
				tableNameKey:        "test_state",
			},
			expectedErr: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := NewSqliteStateStore(logger)
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

func getConnectionString() string {
	s := os.Getenv(connectionStringEnvKey)
	if s == "" {
		// default integration test with in-memory db.
		s = ":memory:"
	}
	return s
}

func setItem(t *testing.T, s *Sqlite, key string, value interface{}, etag *string) {
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

	err := s.Set(setReq)
	assert.Nil(t, err)
	itemExists := storeItemExists(t, s, key)
	assert.True(t, itemExists, "Item should exist after set has been executed ")
}

func getItem(t *testing.T, s *Sqlite, key string) (*state.GetResponse, *fakeItem) {
	getReq := &state.GetRequest{
		Key:     key,
		Options: state.GetStateOption{},
	}

	response, getErr := s.Get(getReq)
	assert.Nil(t, getErr)
	assert.NotNil(t, response)
	outputObject := &fakeItem{}
	_ = json.Unmarshal(response.Data, outputObject)

	return response, outputObject
}

func deleteItem(t *testing.T, s *Sqlite, key string, etag *string) {
	deleteReq := &state.DeleteRequest{
		Key:     key,
		ETag:    etag,
		Options: state.DeleteStateOption{},
	}

	deleteErr := s.Delete(deleteReq)
	assert.Nil(t, deleteErr)
	assert.False(t, storeItemExists(t, s, key), "item should no longer exist after delete has been performed")
}

func storeItemExists(t *testing.T, s *Sqlite, key string) bool {
	dba := s.dbaccess.(*sqliteDBAccess)
	tableName := dba.tableName
	db := dba.db
	var rowCount int32
	stmttpl := "SELECT count(key) FROM %s WHERE key = ?"
	statement := fmt.Sprintf(stmttpl, tableName)
	err := db.QueryRow(statement, key).Scan(&rowCount)
	assert.Nil(t, err)
	exists := rowCount > 0
	return exists
}

func getRowData(t *testing.T, s *Sqlite, key string) (returnValue string, insertdate sql.NullString, updatedate sql.NullString) {
	dba := s.dbaccess.(*sqliteDBAccess)
	tableName := dba.tableName
	db := dba.db
	err := db.QueryRow(fmt.Sprintf("SELECT value, creation_time, update_time FROM %s WHERE key = ?", tableName), key).Scan(
		&returnValue, &insertdate, &updatedate)
	assert.Nil(t, err)

	return returnValue, insertdate, updatedate
}

func getTimesForRow(t *testing.T, s *Sqlite, key string) (insertdate sql.NullString, updatedate sql.NullString, expirationtime sql.NullString) {
	dba := s.dbaccess.(*sqliteDBAccess)
	tableName := dba.tableName
	db := dba.db
	err := db.QueryRow(fmt.Sprintf("SELECT creation_time, update_time, expiration_time FROM %s WHERE key = ?", tableName), key).Scan(&insertdate, &updatedate, &expirationtime)
	assert.Nil(t, err)

	return insertdate, updatedate, expirationtime
}
