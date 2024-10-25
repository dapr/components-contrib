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
package mysql

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/state"
	"github.com/dapr/kit/logger"
	"github.com/dapr/kit/ptr"
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

func (f fakeItem) MarshalJSON() ([]byte, error) {
	return json.Marshal(f.Color)
}

func (f *fakeItem) UnmarshalJSON(data []byte) error {
	return json.Unmarshal(data, &f.Color)
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
		// Tests valid and invalid config settings.
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
					keyConnectionString: getConnectionString(""),
					keyPemPath:          getPemPath(),
				},
				expectedErr: "",
			},
			{
				name: "Valid table name",
				props: map[string]string{
					keyConnectionString: getConnectionString(""),
					keyPemPath:          getPemPath(),
					keyTableName:        "stateStore",
				},
				expectedErr: "",
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				p := NewMySQLStateStore(logger).(*MySQL)
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
	})

	pemPath := getPemPath()

	metadata := state.Metadata{
		Base: metadata.Base{Properties: map[string]string{keyConnectionString: connectionString, keyPemPath: pemPath}},
	}

	mys := NewMySQLStateStore(logger.NewLogger("test")).(*MySQL)
	t.Cleanup(func() {
		defer mys.Close()
	})

	error := mys.Init(context.Background(), metadata)
	if error != nil {
		t.Fatal(error)
	}

	t.Run("Create table succeeds", func(t *testing.T) {
		t.Parallel()

		tableName := "test_state"

		// Drop the table if it already exists
		exists, err := tableExists(context.Background(), mys.db, "dapr_state_store", tableName, 10*time.Second)
		require.NoError(t, err)
		if exists {
			dropTable(t, mys.db, tableName)
		}

		// Create the state table and test for its existence
		// There should be no error
		err = mys.ensureStateTable(context.Background(), "dapr_state_store", tableName)
		require.NoError(t, err)

		// Now create it and make sure there are no errors
		exists, err = tableExists(context.Background(), mys.db, "dapr_state_store", tableName, 10*time.Second)
		require.NoError(t, err)
		assert.True(t, exists)

		// Drop the state table
		dropTable(t, mys.db, tableName)
	})

	t.Run("Get Set Delete one item", func(t *testing.T) {
		t.Parallel()

		// Validates setting one item, getting it, and deleting it.
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
	})

	t.Run("Get item that does not exist", func(t *testing.T) {
		t.Parallel()

		// Validates the behavior of retrieving an item that  does not exist.
		key := randomKey()
		response, outputObject := getItem(t, mys, key)
		assert.Nil(t, response.Data)
		assert.Equal(t, "", outputObject.Color)
	})

	t.Run("Get item with no key fails", func(t *testing.T) {
		t.Parallel()

		// Validates that attempting a Get operation without providing a key will return an error.
		getReq := &state.GetRequest{
			Key: "",
		}

		response, getErr := mys.Get(context.Background(), getReq)
		require.Error(t, getErr)
		assert.Nil(t, response)
	})

	t.Run("Set updates the updatedate field", func(t *testing.T) {
		t.Parallel()

		// Proves that the updatedate is set for an
		// update, and set upon insert. The updatedate is used as the eTag so must be
		// set. It is also auto updated on update by MySQL.
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
	})

	t.Run("Set item with no key fails", func(t *testing.T) {
		t.Parallel()

		setReq := &state.SetRequest{
			Key: "",
		}

		err := mys.Set(context.Background(), setReq)
		require.Error(t, err, "Error was not nil when setting item with no key.")
	})

	t.Run("Bulk set and bulk delete", func(t *testing.T) {
		t.Parallel()
		testBulkSetAndBulkDelete(t, mys)
	})

	t.Run("Get and BulkGet with ttl", func(t *testing.T) {
		t.Parallel()
		testGetExpireTime(t, mys)
		testGetBulkExpireTime(t, mys)
	})

	t.Run("Update and delete with eTag succeeds", func(t *testing.T) {
		t.Parallel()

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
	})

	t.Run("Update with old eTag fails", func(t *testing.T) {
		t.Parallel()

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

		err := mys.Set(context.Background(), setReq)
		require.Error(t, err, "Error was not thrown using old eTag")
	})

	t.Run("Insert with eTag fails", func(t *testing.T) {
		t.Parallel()

		value := &fakeItem{Color: "teal"}
		invalidETag := "12345"

		setReq := &state.SetRequest{
			Key:   randomKey(),
			ETag:  &invalidETag,
			Value: value,
		}

		err := mys.Set(context.Background(), setReq)
		require.Error(t, err)
	})

	t.Run("Delete with invalid eTag fails", func(t *testing.T) {
		t.Parallel()

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

		err := mys.Delete(context.Background(), deleteReq)
		require.Error(t, err)
	})

	t.Run("Delete item with no key fails", func(t *testing.T) {
		t.Parallel()

		deleteReq := &state.DeleteRequest{
			Key: "",
		}

		err := mys.Delete(context.Background(), deleteReq)
		require.Error(t, err)
	})

	t.Run("Delete an item that does not exist", func(t *testing.T) {
		t.Parallel()

		// Delete the item with a key not in the store
		deleteReq := &state.DeleteRequest{
			Key: randomKey(),
		}

		err := mys.Delete(context.Background(), deleteReq)
		require.NoError(t, err)
	})

	t.Run("Inserts with first-write-wins", func(t *testing.T) {
		t.Parallel()

		// Insert without an etag should work on new keys
		key := randomKey()
		setReq := &state.SetRequest{
			Key:   key,
			Value: &fakeItem{Color: "teal"},
			Options: state.SetStateOption{
				Concurrency: state.FirstWrite,
			},
		}

		err := mys.Set(context.Background(), setReq)
		require.NoError(t, err)

		// Get the etag
		getResponse, _ := getItem(t, mys, key)
		assert.NotNil(t, getResponse)
		assert.NotNil(t, getResponse.ETag)
		originalEtag := getResponse.ETag

		// Insert without an etag should fail on existing keys
		setReq = &state.SetRequest{
			Key:   key,
			Value: &fakeItem{Color: "gray or grey"},
			Options: state.SetStateOption{
				Concurrency: state.FirstWrite,
			},
		}

		err = mys.Set(context.Background(), setReq)
		require.ErrorContains(t, err, "Duplicate entry")

		// Insert with invalid etag should fail on existing keys
		setReq = &state.SetRequest{
			Key:   key,
			Value: &fakeItem{Color: "pink"},
			ETag:  ptr.Of("no-etag"),
			Options: state.SetStateOption{
				Concurrency: state.FirstWrite,
			},
		}

		err = mys.Set(context.Background(), setReq)
		require.ErrorContains(t, err, "possible etag mismatch")

		// Insert with valid etag should succeed on existing keys
		setReq = &state.SetRequest{
			Key:   key,
			Value: &fakeItem{Color: "scarlet"},
			ETag:  originalEtag,
			Options: state.SetStateOption{
				Concurrency: state.FirstWrite,
			},
		}

		err = mys.Set(context.Background(), setReq)
		require.NoError(t, err)

		// Insert with an etag should fail on new keys
		setReq = &state.SetRequest{
			Key:   randomKey(),
			Value: &fakeItem{Color: "greige"},
			ETag:  ptr.Of("myetag"),
			Options: state.SetStateOption{
				Concurrency: state.FirstWrite,
			},
		}

		err = mys.Set(context.Background(), setReq)
		require.ErrorContains(t, err, "possible etag mismatch")
	})

	t.Run("Multi with delete and set", func(t *testing.T) {
		t.Parallel()

		var operations []state.TransactionalStateOperation
		var deleteRequests []state.DeleteRequest
		for range 3 {
			req := state.DeleteRequest{Key: randomKey()}

			// Add the item to the database
			setItem(t, mys, req.Key, randomJSON(), nil)

			// Add the item to a slice of delete requests
			deleteRequests = append(deleteRequests, req)

			// Add the item to the multi transaction request
			operations = append(operations, req)
		}

		// Create the set requests
		var setRequests []state.SetRequest
		for range 3 {
			req := state.SetRequest{
				Key:   randomKey(),
				Value: randomJSON(),
			}
			setRequests = append(setRequests, req)
			operations = append(operations, req)
		}

		err := mys.Multi(context.Background(), &state.TransactionalStateRequest{
			Operations: operations,
		})
		require.NoError(t, err)

		for _, delete := range deleteRequests {
			assert.False(t, storeItemExists(t, delete.Key))
		}

		for _, set := range setRequests {
			assert.True(t, storeItemExists(t, set.Key))
			deleteItem(t, mys, set.Key, nil)
		}
	})

	t.Run("Multi with delete only", func(t *testing.T) {
		t.Parallel()

		var operations []state.TransactionalStateOperation
		var deleteRequests []state.DeleteRequest
		for range 3 {
			req := state.DeleteRequest{Key: randomKey()}

			// Add the item to the database
			setItem(t, mys, req.Key, randomJSON(), nil)

			// Add the item to a slice of delete requests
			deleteRequests = append(deleteRequests, req)

			// Add the item to the multi transaction request
			operations = append(operations, req)
		}

		err := mys.Multi(context.Background(), &state.TransactionalStateRequest{
			Operations: operations,
		})
		require.NoError(t, err)

		for _, delete := range deleteRequests {
			assert.False(t, storeItemExists(t, delete.Key))
		}
	})

	t.Run("Multi with set only", func(t *testing.T) {
		t.Parallel()

		var operations []state.TransactionalStateOperation
		var setRequests []state.SetRequest
		for range 3 {
			req := state.SetRequest{
				Key:   randomKey(),
				Value: randomJSON(),
			}
			setRequests = append(setRequests, req)
			operations = append(operations, req)
		}

		err := mys.Multi(context.Background(), &state.TransactionalStateRequest{
			Operations: operations,
		})
		require.NoError(t, err)

		for _, set := range setRequests {
			assert.True(t, storeItemExists(t, set.Key))
			deleteItem(t, mys, set.Key, nil)
		}
	})
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

	err := mys.BulkSet(context.Background(), setReq, state.BulkStoreOpts{})
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

	err = mys.BulkDelete(context.Background(), deleteReq, state.BulkStoreOpts{})
	require.NoError(t, err)
	assert.False(t, storeItemExists(t, setReq[0].Key))
	assert.False(t, storeItemExists(t, setReq[1].Key))
}

func testGetExpireTime(t *testing.T, mys *MySQL) {
	key1 := randomKey()
	require.NoError(t, mys.Set(context.Background(), &state.SetRequest{
		Key:   key1,
		Value: "123",
		Metadata: map[string]string{
			"ttlInSeconds": "1000",
		},
	}))

	resp, err := mys.Get(context.Background(), &state.GetRequest{Key: key1})
	require.NoError(t, err)
	assert.Equal(t, `"123"`, string(resp.Data))
	require.Len(t, resp.Metadata, 1)
	expireTime, err := time.Parse(time.RFC3339, resp.Metadata["ttlExpireTime"])
	require.NoError(t, err)
	assert.InDelta(t, time.Now().Add(time.Second*1000).Unix(), expireTime.Unix(), 5)
}

func testGetBulkExpireTime(t *testing.T, mys *MySQL) {
	key1 := randomKey()
	key2 := randomKey()

	require.NoError(t, mys.Set(context.Background(), &state.SetRequest{
		Key:   key1,
		Value: "123",
		Metadata: map[string]string{
			"ttlInSeconds": "1000",
		},
	}))
	require.NoError(t, mys.Set(context.Background(), &state.SetRequest{
		Key:   key2,
		Value: "456",
		Metadata: map[string]string{
			"ttlInSeconds": "2001",
		},
	}))

	resp, err := mys.BulkGet(context.Background(), []state.GetRequest{
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
	assert.InDelta(t, time.Now().Add(time.Second*1000).Unix(), expireTime.Unix(), 5)
	expireTime, err = time.Parse(time.RFC3339, resp[1].Metadata["ttlExpireTime"])
	require.NoError(t, err)
	assert.InDelta(t, time.Now().Add(time.Second*2001).Unix(), expireTime.Unix(), 5)
}

func dropTable(t *testing.T, db *sql.DB, tableName string) {
	_, err := db.Exec(fmt.Sprintf(
		`DROP TABLE %s;`,
		tableName))
	require.NoError(t, err)
}

func setItem(t *testing.T, mys *MySQL, key string, value interface{}, eTag *string) {
	setReq := &state.SetRequest{
		Key:   key,
		ETag:  eTag,
		Value: value,
	}

	err := mys.Set(context.Background(), setReq)
	require.NoError(t, err, "Error setting an item")
	itemExists := storeItemExists(t, key)
	assert.True(t, itemExists, "Item does not exist after being set")
}

func getItem(t *testing.T, mys *MySQL, key string) (*state.GetResponse, *fakeItem) {
	getReq := &state.GetRequest{
		Key:     key,
		Options: state.GetStateOption{},
	}

	response, getErr := mys.Get(context.Background(), getReq)
	require.NoError(t, getErr)
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

	deleteErr := mys.Delete(context.Background(), deleteReq)
	require.NoError(t, deleteErr, "There was an error deleting a record")
	assert.False(t, storeItemExists(t, key), "Item still exists after delete")
}

func storeItemExists(t *testing.T, key string) bool {
	db, err := connectToDB(t)
	require.NoError(t, err)
	defer db.Close()

	exists := false
	statement := fmt.Sprintf(
		`SELECT EXISTS (SELECT * FROM %s WHERE id = ?)`,
		defaultTableName)
	err = db.QueryRow(statement, key).Scan(&exists)
	require.NoError(t, err)

	return exists
}

func getRowData(t *testing.T, key string) (returnValue string, insertdate sql.NullString, updatedate sql.NullString, eTag string) {
	db, err := connectToDB(t)
	require.NoError(t, err)
	defer db.Close()

	err = db.QueryRow(fmt.Sprintf(
		`SELECT value, insertdate, updatedate, eTag FROM %s WHERE id = ?`,
		defaultTableName), key).Scan(&returnValue, &insertdate, &updatedate, &eTag)
	require.NoError(t, err)

	return returnValue, insertdate, updatedate, eTag
}

// Connects to MySQL using SSL if required.
func connectToDB(t *testing.T) (*sql.DB, error) {
	val := getPemPath()
	if val != "" {
		rootCertPool := x509.NewCertPool()
		pem, readErr := os.ReadFile(val)

		require.NoError(t, readErr, "Could not read PEM file")

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
