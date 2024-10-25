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

package cockroachdb

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	postgresql "github.com/dapr/components-contrib/common/component/postgresql/v1"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/state"
	"github.com/dapr/kit/logger"
)

const (
	connectionStringEnvKey = "DAPR_TEST_COCKROACHDB_CONNSTRING" // Environment variable containing the connection string.
	defaultTableName       = "state"
)

type fakeItem struct {
	Color string `json:"color"`
}

func TestCockroachDBIntegration(t *testing.T) {
	t.Parallel()

	connectionString := getConnectionString()
	if connectionString == "" {
		t.Skipf("CockroachDB state integration tests skipped. To enable define the connection string using environment variable '%s' (example 'export %s=\"host=localhost user=postgres password=example port=5432 connect_timeout=10 database=dapr_test\")", connectionStringEnvKey, connectionStringEnvKey)
	}

	t.Run("Test init configurations", func(t *testing.T) {
		t.Parallel()
		testInitConfiguration(t)
	})

	metadata := state.Metadata{
		Base: metadata.Base{Properties: map[string]string{"connectionString": connectionString}},
	}

	pgs := New(logger.NewLogger("test")).(*postgresql.PostgreSQL)
	t.Cleanup(func() {
		defer pgs.Close()
	})

	if err := pgs.Init(context.Background(), metadata); err != nil {
		t.Fatal(err)
	}

	t.Run("Create table succeeds", func(t *testing.T) {
		t.Parallel()

		testCreateTable(t, pgs.GetDB())
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

	t.Run("Set with TTL should not return after ttl", func(t *testing.T) {
		t.Parallel()
		setWithTTLShouldNotReturnAfterTTL(t, pgs)
	})
}

// setGetUpdateDeleteOneItem validates setting one item, getting it, and deleting it.
func setGetUpdateDeleteOneItem(t *testing.T, pgs *postgresql.PostgreSQL) {
	t.Helper()

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
func testCreateTable(t *testing.T, db *pgxpool.Pool) {
	t.Helper()
	const tableName = "test_state"
	ctx := context.Background()

	// Drop the table if it already exists.
	exists, err := tableExists(ctx, db, tableName)
	require.NoError(t, err)

	if exists {
		dropTable(t, db, tableName)
	}

	// Create the state table and test for its existence.
	err = ensureTables(ctx, db, postgresql.MigrateOptions{
		Logger:            logger.NewLogger("test"),
		StateTableName:    "test_state",
		MetadataTableName: "test_metadata",
	})
	require.NoError(t, err)

	exists, err = tableExists(ctx, db, "test_state")
	require.NoError(t, err)
	assert.True(t, exists)

	exists, err = tableExists(ctx, db, "test_metadata")
	require.NoError(t, err)
	assert.True(t, exists)

	dropTable(t, db, "test_state")
	dropTable(t, db, "test_metadata")
}

func dropTable(t *testing.T, db *pgxpool.Pool, tableName string) {
	t.Helper()

	_, err := db.Exec(context.Background(), "DROP TABLE "+tableName)
	require.NoError(t, err)
}

func deleteItemThatDoesNotExist(t *testing.T, pgs *postgresql.PostgreSQL) {
	t.Helper()

	// Delete the item with a key not in the store.
	deleteReq := &state.DeleteRequest{
		Key:      randomKey(),
		ETag:     nil,
		Metadata: nil,
		Options: state.DeleteStateOption{
			Concurrency: "",
			Consistency: "",
		},
	}
	err := pgs.Delete(context.Background(), deleteReq)
	require.NoError(t, err)
}

func multiWithSetOnly(t *testing.T, pgs *postgresql.PostgreSQL) {
	t.Helper()

	var operations []state.TransactionalStateOperation //nolint:prealloc
	var setRequests []state.SetRequest                 //nolint:prealloc
	for range 3 {
		req := state.SetRequest{
			Key:      randomKey(),
			Value:    randomJSON(),
			ETag:     nil,
			Metadata: nil,
			Options: state.SetStateOption{
				Concurrency: "",
				Consistency: "",
			},
			ContentType: nil,
		}
		setRequests = append(setRequests, req)
		operations = append(operations, req)
	}

	err := pgs.Multi(context.Background(), &state.TransactionalStateRequest{
		Operations: operations,
		Metadata:   nil,
	})
	require.NoError(t, err)

	for _, set := range setRequests {
		assert.True(t, storeItemExists(t, set.Key))
		deleteItem(t, pgs, set.Key, nil)
	}
}

func multiWithDeleteOnly(t *testing.T, pgs *postgresql.PostgreSQL) {
	t.Helper()

	var operations []state.TransactionalStateOperation //nolint:prealloc
	var deleteRequests []state.DeleteRequest           //nolint:prealloc
	for range 3 {
		req := state.DeleteRequest{
			Key:      randomKey(),
			ETag:     nil,
			Metadata: nil,
			Options: state.DeleteStateOption{
				Concurrency: "",
				Consistency: "",
			},
		}

		// Add the item to the database.
		setItem(t, pgs, req.Key, randomJSON(), nil) // Add the item to the database.

		// Add the item to a slice of delete requests.
		deleteRequests = append(deleteRequests, req)

		// Add the item to the multi transaction request.
		operations = append(operations, req)
	}

	err := pgs.Multi(context.Background(), &state.TransactionalStateRequest{
		Operations: operations,
		Metadata:   nil,
	})
	require.NoError(t, err)

	for _, delete := range deleteRequests {
		assert.False(t, storeItemExists(t, delete.Key))
	}
}

func multiWithDeleteAndSet(t *testing.T, pgs *postgresql.PostgreSQL) {
	t.Helper()

	var operations []state.TransactionalStateOperation //nolint:prealloc
	var deleteRequests []state.DeleteRequest           //nolint:prealloc
	for range 3 {
		req := state.DeleteRequest{
			Key:      randomKey(),
			ETag:     nil,
			Metadata: nil,
			Options: state.DeleteStateOption{
				Concurrency: "",
				Consistency: "",
			},
		}

		// Add the item to the database.
		setItem(t, pgs, req.Key, randomJSON(), nil) // Add the item to the database.

		// Add the item to a slice of delete requests.
		deleteRequests = append(deleteRequests, req)

		// Add the item to the multi transaction request.
		operations = append(operations, req)
	}

	// Create the set requests.
	var setRequests []state.SetRequest //nolint:prealloc
	for range 3 {
		req := state.SetRequest{
			Key:      randomKey(),
			Value:    randomJSON(),
			ETag:     nil,
			Metadata: nil,
			Options: state.SetStateOption{
				Concurrency: "",
				Consistency: "",
			},
			ContentType: nil,
		}
		setRequests = append(setRequests, req)
		operations = append(operations, req)
	}

	err := pgs.Multi(context.Background(), &state.TransactionalStateRequest{
		Operations: operations,
		Metadata:   nil,
	})
	require.NoError(t, err)

	for _, delete := range deleteRequests {
		assert.False(t, storeItemExists(t, delete.Key))
	}

	for _, set := range setRequests {
		assert.True(t, storeItemExists(t, set.Key))
		deleteItem(t, pgs, set.Key, nil)
	}
}

func deleteWithInvalidEtagFails(t *testing.T, pgs *postgresql.PostgreSQL) {
	t.Helper()

	// Create new item.
	key := randomKey()
	value := &fakeItem{Color: "mauve"}
	setItem(t, pgs, key, value, nil)

	etag := "1234"
	// Delete the item with a fake etag.
	deleteReq := &state.DeleteRequest{
		Key:      key,
		ETag:     &etag,
		Metadata: nil,
		Options: state.DeleteStateOption{
			Concurrency: "",
			Consistency: "",
		},
	}
	err := pgs.Delete(context.Background(), deleteReq)
	require.Error(t, err)
}

func deleteWithNoKeyFails(t *testing.T, pgs *postgresql.PostgreSQL) {
	t.Helper()

	deleteReq := &state.DeleteRequest{
		Key:      "",
		ETag:     nil,
		Metadata: nil,
		Options: state.DeleteStateOption{
			Concurrency: "",
			Consistency: "",
		},
	}
	err := pgs.Delete(context.Background(), deleteReq)
	require.Error(t, err)
}

// newItemWithEtagFails creates a new item and also supplies an ETag, which is invalid - expect failure.
func newItemWithEtagFails(t *testing.T, pgs *postgresql.PostgreSQL) {
	t.Helper()

	value := &fakeItem{Color: "teal"}
	invalidEtag := "12345"

	setReq := &state.SetRequest{
		Key:      randomKey(),
		ETag:     &invalidEtag,
		Value:    value,
		Metadata: nil,
		Options: state.SetStateOption{
			Concurrency: "",
			Consistency: "",
		},
		ContentType: nil,
	}

	err := pgs.Set(context.Background(), setReq)
	require.Error(t, err)
}

func updateWithOldEtagFails(t *testing.T, pgs *postgresql.PostgreSQL) {
	t.Helper()

	// Create and retrieve new item.
	key := randomKey()
	value := &fakeItem{Color: "gray"}
	setItem(t, pgs, key, value, nil)
	getResponse, _ := getItem(t, pgs, key)
	assert.NotNil(t, getResponse.ETag)
	originalEtag := getResponse.ETag

	// Change the value and get the updated etag.
	newValue := &fakeItem{Color: "silver"}
	setItem(t, pgs, key, newValue, originalEtag)
	_, updatedItem := getItem(t, pgs, key)
	assert.Equal(t, newValue, updatedItem)

	// Update again with the original etag - expect udpate failure.
	newValue = &fakeItem{Color: "maroon"}
	setReq := &state.SetRequest{
		Key:      key,
		ETag:     originalEtag,
		Value:    newValue,
		Metadata: nil,
		Options: state.SetStateOption{
			Concurrency: "",
			Consistency: "",
		},
		ContentType: nil,
	}
	err := pgs.Set(context.Background(), setReq)
	require.Error(t, err)
}

func updateAndDeleteWithEtagSucceeds(t *testing.T, pgs *postgresql.PostgreSQL) {
	t.Helper()

	// Create and retrieve new item.
	key := randomKey()
	value := &fakeItem{Color: "hazel"}
	setItem(t, pgs, key, value, nil)
	getResponse, _ := getItem(t, pgs, key)
	assert.NotNil(t, getResponse.ETag)

	// Change the value and compare.
	value.Color = "purple"
	setItem(t, pgs, key, value, getResponse.ETag)
	updateResponse, updatedItem := getItem(t, pgs, key)
	assert.Equal(t, value, updatedItem)

	// ETag should change when item is updated.
	assert.NotEqual(t, getResponse.ETag, updateResponse.ETag)

	// Delete.
	deleteItem(t, pgs, key, updateResponse.ETag)

	// Item is not in the data store.
	assert.False(t, storeItemExists(t, key))
}

// getItemThatDoesNotExist validates the behavior of retrieving an item that does not exist.
func getItemThatDoesNotExist(t *testing.T, pgs *postgresql.PostgreSQL) {
	t.Helper()

	key := randomKey()
	response, outputObject := getItem(t, pgs, key)
	assert.Nil(t, response.Data)
	assert.Equal(t, "", outputObject.Color)
}

// getItemWithNoKey validates that attempting a Get operation without providing a key will return an error.
func getItemWithNoKey(t *testing.T, pgs *postgresql.PostgreSQL) {
	t.Helper()

	getReq := &state.GetRequest{
		Key:      "",
		Metadata: nil,
		Options: state.GetStateOption{
			Consistency: "",
		},
	}

	response, getErr := pgs.Get(context.Background(), getReq)
	require.Error(t, getErr)
	assert.Nil(t, response)
}

// setUpdatesTheUpdatedateField proves that the updateddate is set for an update, and not set upon insert.
func setUpdatesTheUpdatedateField(t *testing.T, pgs *postgresql.PostgreSQL) {
	t.Helper()

	key := randomKey()
	value := &fakeItem{Color: "orange"}
	setItem(t, pgs, key, value, nil)

	// insertdate should have a value and updatedate should be nil.
	_, insertdate, updatedate := getRowData(t, key)
	assert.NotNil(t, insertdate)
	assert.Equal(t, "", updatedate.String)

	// insertdate should not change, updatedate should have a value.
	value = &fakeItem{Color: "aqua"}
	setItem(t, pgs, key, value, nil)
	_, newinsertdate, updatedate := getRowData(t, key)
	assert.Equal(t, insertdate, newinsertdate) // The insertdate should not change.
	assert.NotEqual(t, "", updatedate.String)

	deleteItem(t, pgs, key, nil)
}

func setItemWithNoKey(t *testing.T, pgs *postgresql.PostgreSQL) {
	t.Helper()

	setReq := &state.SetRequest{
		Key:      "",
		Value:    nil,
		ETag:     nil,
		Metadata: nil,
		Options: state.SetStateOption{
			Concurrency: "",
			Consistency: "",
		},
		ContentType: nil,
	}

	err := pgs.Set(context.Background(), setReq)
	require.Error(t, err)
}

// Tests valid bulk sets and deletes.
func testBulkSetAndBulkDelete(t *testing.T, pgs *postgresql.PostgreSQL) {
	t.Helper()

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

	err := pgs.BulkSet(context.Background(), setReq, state.BulkStoreOpts{})
	require.NoError(t, err)
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

	err = pgs.BulkDelete(context.Background(), deleteReq, state.BulkStoreOpts{})
	require.NoError(t, err)
	assert.False(t, storeItemExists(t, setReq[0].Key))
	assert.False(t, storeItemExists(t, setReq[1].Key))
}

// testInitConfiguration tests valid and invalid config settings.
func testInitConfiguration(t *testing.T) {
	t.Helper()

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
			name:        "Valid connection string",
			props:       map[string]string{"connectionString": getConnectionString()},
			expectedErr: "",
		},
	}

	for _, rowTest := range tests {
		t.Run(rowTest.name, func(t *testing.T) {
			cockroackDB := New(logger).(*postgresql.PostgreSQL)
			defer cockroackDB.Close()

			metadata := state.Metadata{
				Base: metadata.Base{Properties: rowTest.props},
			}

			err := cockroackDB.Init(context.Background(), metadata)
			if rowTest.expectedErr == "" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				assert.Equal(t, rowTest.expectedErr, err.Error())
			}
		})
	}
}

func getConnectionString() string {
	return os.Getenv(connectionStringEnvKey)
}

func setItem(t *testing.T, pgs *postgresql.PostgreSQL, key string, value interface{}, etag *string) {
	t.Helper()

	setReq := &state.SetRequest{
		Key:      key,
		ETag:     etag,
		Value:    value,
		Metadata: map[string]string{},
		Options: state.SetStateOption{
			Concurrency: "",
			Consistency: "",
		},
		ContentType: nil,
	}

	err := pgs.Set(context.Background(), setReq)
	require.NoError(t, err)
	itemExists := storeItemExists(t, key)
	assert.True(t, itemExists)
}

func setWithTTLShouldNotReturnAfterTTL(t *testing.T, pgs *postgresql.PostgreSQL) {
	t.Helper()

	key := randomKey()
	value := &fakeItem{Color: "indigo"}

	setItemWithTTL(t, pgs, key, value, nil, time.Second)

	_, outputObject := getItem(t, pgs, key)
	assert.Equal(t, value, outputObject)

	<-time.After(time.Second * 2)

	getResponse, outputObject := getItem(t, pgs, key)
	assert.Equal(t, new(fakeItem), outputObject)

	newValue := &fakeItem{Color: "green"}
	setItemWithTTL(t, pgs, key, newValue, getResponse.ETag, time.Second)

	getResponse, outputObject = getItem(t, pgs, key)
	assert.Equal(t, newValue, outputObject)

	setItemWithTTL(t, pgs, key, newValue, getResponse.ETag, time.Second*5)
	<-time.After(time.Second * 2)

	getResponse, outputObject = getItem(t, pgs, key)
	assert.Equal(t, newValue, outputObject)

	deleteItem(t, pgs, key, getResponse.ETag)
}

func getItem(t *testing.T, pgs *postgresql.PostgreSQL, key string) (*state.GetResponse, *fakeItem) {
	t.Helper()

	getReq := &state.GetRequest{
		Key: key,
		Options: state.GetStateOption{
			Consistency: "",
		},
		Metadata: map[string]string{},
	}

	response, getErr := pgs.Get(context.Background(), getReq)
	require.NoError(t, getErr)
	assert.NotNil(t, response)
	outputObject := &fakeItem{
		Color: "",
	}
	_ = json.Unmarshal(response.Data, outputObject)

	return response, outputObject
}

func deleteItem(t *testing.T, pgs *postgresql.PostgreSQL, key string, etag *string) {
	t.Helper()

	deleteReq := &state.DeleteRequest{
		Key:  key,
		ETag: etag,
		Options: state.DeleteStateOption{
			Concurrency: "",
			Consistency: "",
		},
		Metadata: map[string]string{},
	}

	deleteErr := pgs.Delete(context.Background(), deleteReq)
	require.NoError(t, deleteErr)
	assert.False(t, storeItemExists(t, key))
}

func storeItemExists(t *testing.T, key string) bool {
	t.Helper()

	databaseConnection, err := sql.Open("pgx", getConnectionString())
	require.NoError(t, err)
	defer databaseConnection.Close()

	exists := false
	statement := fmt.Sprintf(`SELECT EXISTS (SELECT * FROM %s WHERE key = $1)`, defaultTableName)
	err = databaseConnection.QueryRow(statement, key).Scan(&exists)
	require.NoError(t, err)

	return exists
}

func getRowData(t *testing.T, key string) (returnValue string, insertdate sql.NullString, updatedate sql.NullString) {
	t.Helper()

	databaseConnection, err := sql.Open("pgx", getConnectionString())
	require.NoError(t, err)
	defer databaseConnection.Close()

	err = databaseConnection.QueryRow(fmt.Sprintf("SELECT value, insertdate, updatedate FROM %s WHERE key = $1", defaultTableName), key).Scan(&returnValue, &insertdate, &updatedate)
	require.NoError(t, err)

	return returnValue, insertdate, updatedate
}

func setItemWithTTL(t *testing.T, pgs *postgresql.PostgreSQL, key string, value interface{}, etag *string, ttl time.Duration) {
	t.Helper()

	setReq := &state.SetRequest{
		Key:   key,
		ETag:  etag,
		Value: value,
		Metadata: map[string]string{
			"ttlInSeconds": strconv.FormatInt(int64(ttl.Seconds()), 10),
		},
		Options: state.SetStateOption{
			Concurrency: "",
			Consistency: "",
		},
		ContentType: nil,
	}

	err := pgs.Set(context.Background(), setReq)
	require.NoError(t, err)
	itemExists := storeItemExists(t, key)
	assert.True(t, itemExists)
}

func randomKey() string {
	return uuid.New().String()
}

func randomJSON() *fakeItem {
	return &fakeItem{Color: randomKey()}
}
