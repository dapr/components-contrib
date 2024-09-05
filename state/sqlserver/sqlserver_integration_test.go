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

package sqlserver

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	uuid "github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/state"
	"github.com/dapr/kit/logger"
)

const (
	// connectionStringEnvKey defines the key containing the integration test connection string
	// To use docker, server=localhost;user id=sa;password=Pass@Word1;port=1433;
	// To use Azure SQL, server=<your-db-server-name>.database.windows.net;user id=<your-db-user>;port=1433;password=<your-password>;database=dapr_test;.
	connectionStringEnvKey = "DAPR_TEST_SQL_CONNSTRING"
	usersTableName         = "Users"
	beverageTea            = "tea"
	invalidEtag            = "FFFFFFFFFFFFFFFF"
)

type user struct {
	ID               string
	Name             string
	FavoriteBeverage string
}

type userWithPets struct {
	user
	PetsCount int
}

type userWithEtag struct {
	user
	etag string
}

func TestIntegrationCases(t *testing.T) {
	connectionString := os.Getenv(connectionStringEnvKey)
	if connectionString == "" {
		t.Skipf(`SQLServer state integration tests skipped. To enable this test, define the connection string using environment variable '%[1]s' (example 'export %[1]s="server=localhost;user id=sa;password=Pass@Word1;port=1433;")'`, connectionStringEnvKey)
	}

	t.Run("Single operations", testSingleOperations)
	t.Run("Set New Record With Invalid Etag Should Fail", testSetNewRecordWithInvalidEtagShouldFail)
	t.Run("Indexed Properties", testIndexedProperties)
	t.Run("Multi operations", testMultiOperations)
	t.Run("Insert and Update Set Record Dates", testInsertAndUpdateSetRecordDates)
	t.Run("Multiple initializations", testMultipleInitializations)

	// Run concurrent set tests 10 times
	const executions = 10
	for i := range executions {
		t.Run(fmt.Sprintf("Concurrent sets, try #%d", i+1), testConcurrentSets)
	}
}

func getUniqueDBSchema(t *testing.T) string {
	b := make([]byte, 4)
	_, err := io.ReadFull(rand.Reader, b)
	require.NoError(t, err)
	return "v" + hex.EncodeToString(b)
}

func createMetadata(schema string, kt KeyType, indexedProperties string) state.Metadata {
	metadata := state.Metadata{Base: metadata.Base{
		Properties: map[string]string{
			"connectionString": os.Getenv(connectionStringEnvKey),
			"schema":           schema,
			"tableName":        usersTableName,
			"keyType":          string(kt),
			"databaseName":     "dapr_test",
		},
	}}

	if indexedProperties != "" {
		metadata.Properties["indexedProperties"] = indexedProperties
	}

	return metadata
}

// Ensure the database is running
// For docker, use: docker run --name sqlserver -e "ACCEPT_EULA=Y" -e "SA_PASSWORD=Pass@Word1" -p 1433:1433 -d mcr.microsoft.com/mssql/server:2019-GA-ubuntu-16.04.
func getTestStore(t *testing.T, indexedProperties string) *SQLServer {
	return getTestStoreWithKeyType(t, StringKeyType, indexedProperties)
}

func getTestStoreWithKeyType(t *testing.T, kt KeyType, indexedProperties string) *SQLServer {
	schema := getUniqueDBSchema(t)
	metadata := createMetadata(schema, kt, indexedProperties)
	store := &SQLServer{
		logger:          logger.NewLogger("test"),
		migratorFactory: newMigration,
	}
	store.BulkStore = state.NewDefaultBulkStore(store)
	err := store.Init(context.Background(), metadata)
	require.NoError(t, err)

	return store
}

func assertUserExists(t *testing.T, store *SQLServer, key string) (user, string) {
	getRes, err := store.Get(context.Background(), &state.GetRequest{Key: key})
	require.NoError(t, err)
	assert.NotNil(t, getRes)
	assert.NotNil(t, getRes.Data, "No data was returned")
	require.NotNil(t, getRes.ETag)

	var loaded user
	err = json.Unmarshal(getRes.Data, &loaded)
	require.NoError(t, err)

	return loaded, *getRes.ETag
}

func assertLoadedUserIsEqual(t *testing.T, store *SQLServer, key string, expected user) (user, string) {
	loaded, etag := assertUserExists(t, store, key)
	assert.Equal(t, expected.ID, loaded.ID)
	assert.Equal(t, expected.Name, loaded.Name)
	assert.Equal(t, expected.FavoriteBeverage, loaded.FavoriteBeverage)

	return loaded, etag
}

func assertUserDoesNotExist(t *testing.T, store *SQLServer, key string) {
	_, err := store.Get(context.Background(), &state.GetRequest{Key: key})
	require.NoError(t, err)
}

func assertDBQuery(t *testing.T, store *SQLServer, query string, assertReader func(t *testing.T, rows *sql.Rows)) {
	rows, err := store.db.Query(query)
	require.NoError(t, err)
	require.NoError(t, rows.Err())

	defer rows.Close()
	assertReader(t, rows)
}

/* #nosec. */
func assertUserCountIsEqualTo(t *testing.T, store *SQLServer, expected int) {
	tsql := fmt.Sprintf("SELECT count(*) FROM [%s].[%s]", store.metadata.SchemaName, store.metadata.TableName)
	assertDBQuery(t, store, tsql, func(t *testing.T, rows *sql.Rows) {
		assert.True(t, rows.Next())
		var actual int
		err := rows.Scan(&actual)
		require.NoError(t, err)
		assert.Equal(t, expected, actual)
	})
}

type userKeyGenerator interface {
	NextKey() string
}

type numbericKeyGenerator struct {
	seed int32
}

func (n *numbericKeyGenerator) NextKey() string {
	val := atomic.AddInt32(&n.seed, 1)

	return strconv.Itoa(int(val))
}

type uuidKeyGenerator struct{}

func (n uuidKeyGenerator) NextKey() string {
	return uuid.New().String()
}

func testSingleOperations(t *testing.T) {
	invEtag := invalidEtag

	tests := []struct {
		name   string
		kt     KeyType
		keyGen userKeyGenerator
	}{
		{"Single operation string key type", StringKeyType, &numbericKeyGenerator{}},
		{"Single operation integer key type", IntegerKeyType, &numbericKeyGenerator{}},
		{"Single operation uuid key type", UUIDKeyType, &uuidKeyGenerator{}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := getTestStoreWithKeyType(t, test.kt, "")

			john := user{test.keyGen.NextKey(), "John", "Coffee"}

			// Get fails as the item does not exist
			assertUserDoesNotExist(t, store, john.ID)

			// Save and read
			err := store.Set(context.Background(), &state.SetRequest{Key: john.ID, Value: john})
			require.NoError(t, err)
			johnV1, etagFromInsert := assertLoadedUserIsEqual(t, store, john.ID, john)

			// Update with ETAG
			waterJohn := johnV1
			waterJohn.FavoriteBeverage = "Water"
			err = store.Set(context.Background(), &state.SetRequest{Key: waterJohn.ID, Value: waterJohn, ETag: &etagFromInsert})
			require.NoError(t, err)

			// Get updated
			johnV2, _ := assertLoadedUserIsEqual(t, store, waterJohn.ID, waterJohn)

			// Update without ETAG
			noEtagJohn := johnV2
			noEtagJohn.FavoriteBeverage = "No Etag John"
			err = store.Set(context.Background(), &state.SetRequest{Key: noEtagJohn.ID, Value: noEtagJohn})
			require.NoError(t, err)

			// 7. Get updated
			johnV3, _ := assertLoadedUserIsEqual(t, store, noEtagJohn.ID, noEtagJohn)

			// 8. Update with invalid ETAG should fail
			failedJohn := johnV3
			failedJohn.FavoriteBeverage = "Will not work"
			err = store.Set(context.Background(), &state.SetRequest{Key: failedJohn.ID, Value: failedJohn, ETag: &etagFromInsert})
			require.Error(t, err)
			_, etag := assertLoadedUserIsEqual(t, store, johnV3.ID, johnV3)

			// 9. Delete with invalid ETAG should fail
			err = store.Delete(context.Background(), &state.DeleteRequest{Key: johnV3.ID, ETag: &invEtag})
			require.Error(t, err)
			assertLoadedUserIsEqual(t, store, johnV3.ID, johnV3)

			// 10. Delete with valid ETAG
			err = store.Delete(context.Background(), &state.DeleteRequest{Key: johnV2.ID, ETag: &etag})
			require.NoError(t, err)

			assertUserDoesNotExist(t, store, johnV2.ID)
		})
	}
}

func testSetNewRecordWithInvalidEtagShouldFail(t *testing.T) {
	store := getTestStore(t, "")

	u := user{uuid.New().String(), "John", "Coffee"}

	invEtag := invalidEtag
	err := store.Set(context.Background(), &state.SetRequest{Key: u.ID, Value: u, ETag: &invEtag})
	require.Error(t, err)
}

/* #nosec. */
func testIndexedProperties(t *testing.T) {
	store := getTestStore(t, `[{ "column":"FavoriteBeverage", "property":"FavoriteBeverage", "type":"nvarchar(100)"}, { "column":"PetsCount", "property":"PetsCount", "type": "INTEGER"}]`)

	err := store.BulkSet(context.Background(), []state.SetRequest{
		{Key: "1", Value: userWithPets{user{"1", "John", "Coffee"}, 3}},
		{Key: "2", Value: userWithPets{user{"2", "Laura", "Water"}, 1}},
		{Key: "3", Value: userWithPets{user{"3", "Carl", "Beer"}, 0}},
		{Key: "4", Value: userWithPets{user{"4", "Maria", "Wine"}, 100}},
	}, state.BulkStoreOpts{})

	require.NoError(t, err)

	// Check the database for computed columns
	assertDBQuery(t, store, fmt.Sprintf("SELECT count(*) from [%s].[%s] WHERE PetsCount < 3", store.metadata.SchemaName, usersTableName), func(t *testing.T, rows *sql.Rows) {
		assert.True(t, rows.Next())

		var c int
		rows.Scan(&c)
		assert.Equal(t, 2, c)
	})

	// Ensure we can get by beverage
	assertDBQuery(t, store, fmt.Sprintf("SELECT count(*) from [%s].[%s] WHERE FavoriteBeverage = '%s'", store.metadata.SchemaName, usersTableName, "Coffee"), func(t *testing.T, rows *sql.Rows) {
		assert.True(t, rows.Next())

		var c int
		rows.Scan(&c)
		assert.Equal(t, 1, c)
	})
}

func testMultiOperations(t *testing.T) {
	tests := []struct {
		name   string
		kt     KeyType
		keyGen userKeyGenerator
	}{
		{"Multi operations string key type", StringKeyType, &numbericKeyGenerator{}},
		{"Multi operations integer key type", IntegerKeyType, &numbericKeyGenerator{}},
		{"Multi operations uuid key type", UUIDKeyType, &uuidKeyGenerator{}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := getTestStoreWithKeyType(t, test.kt, `[{ "column":"FavoriteBeverage", "property":"FavoriteBeverage", "type":"nvarchar(100)"}]`)

			keyGen := test.keyGen

			initialUsers := []user{
				{keyGen.NextKey(), "John", "Coffee"},
				{keyGen.NextKey(), "Laura", "Water"},
				{keyGen.NextKey(), "Carl", "Beer"},
				{keyGen.NextKey(), "Maria", "Wine"},
				{keyGen.NextKey(), "Mark", "Juice"},
				{keyGen.NextKey(), "Sara", "Soda"},
				{keyGen.NextKey(), "Tony", "Milk"},
				{keyGen.NextKey(), "Hugo", "Juice"},
			}

			// 1. add bulk users
			bulkSet := make([]state.SetRequest, len(initialUsers))
			for i, u := range initialUsers {
				bulkSet[i] = state.SetRequest{Key: u.ID, Value: u}
			}

			err := store.BulkSet(context.Background(), bulkSet, state.BulkStoreOpts{})
			require.NoError(t, err)
			assertUserCountIsEqualTo(t, store, len(initialUsers))

			// Ensure initial users are correctly stored
			loadedUsers := make([]userWithEtag, len(initialUsers))
			for i, u := range initialUsers {
				loaded, etag := assertLoadedUserIsEqual(t, store, u.ID, u)
				loadedWithEtag := userWithEtag{loaded, etag}
				loadedUsers[i] = loadedWithEtag
			}
			totalUsers := len(loadedUsers)

			userIndex := 0
			t.Run("Update and delete without etag should work", func(t *testing.T) {
				toDelete := loadedUsers[userIndex].user
				original := loadedUsers[userIndex+1]
				modified := original.user
				modified.FavoriteBeverage = beverageTea

				localErr := store.Multi(context.Background(), &state.TransactionalStateRequest{
					Operations: []state.TransactionalStateOperation{
						state.DeleteRequest{Key: toDelete.ID},
						state.SetRequest{Key: modified.ID, Value: modified},
					},
				})
				require.NoError(t, localErr)
				assertLoadedUserIsEqual(t, store, modified.ID, modified)
				assertUserDoesNotExist(t, store, toDelete.ID)

				totalUsers--
				assertUserCountIsEqualTo(t, store, totalUsers)

				userIndex += 2
			})

			t.Run("Update, delete and insert should work", func(t *testing.T) {
				toDelete := loadedUsers[userIndex]
				toModify := loadedUsers[userIndex+1]
				toInsert := user{keyGen.NextKey(), "Susan", "Soda"}
				modified := toModify.user
				modified.FavoriteBeverage = beverageTea

				err = store.Multi(context.Background(), &state.TransactionalStateRequest{
					Operations: []state.TransactionalStateOperation{
						state.DeleteRequest{Key: toDelete.ID, ETag: &toDelete.etag},
						state.SetRequest{Key: modified.ID, Value: modified, ETag: &toModify.etag},
						state.SetRequest{Key: toInsert.ID, Value: toInsert},
					},
				})
				require.NoError(t, err)
				assertLoadedUserIsEqual(t, store, modified.ID, modified)
				assertLoadedUserIsEqual(t, store, toInsert.ID, toInsert)
				assertUserDoesNotExist(t, store, toDelete.ID)

				// we added 1 and deleted 1, so totalUsers should have no change
				assertUserCountIsEqualTo(t, store, totalUsers)

				userIndex += 2
			})

			t.Run("Update and delete with etag should work", func(t *testing.T) {
				toDelete := loadedUsers[userIndex]
				toModify := loadedUsers[userIndex+1]
				modified := toModify.user
				modified.FavoriteBeverage = beverageTea

				err = store.Multi(context.Background(), &state.TransactionalStateRequest{
					Operations: []state.TransactionalStateOperation{
						state.DeleteRequest{Key: toDelete.ID, ETag: &toDelete.etag},
						state.SetRequest{Key: modified.ID, Value: modified, ETag: &toModify.etag},
					},
				})
				require.NoError(t, err)
				assertLoadedUserIsEqual(t, store, modified.ID, modified)
				assertUserDoesNotExist(t, store, toDelete.ID)

				totalUsers--
				assertUserCountIsEqualTo(t, store, totalUsers)

				userIndex += 2
			})

			t.Run("Delete fails, should abort insert", func(t *testing.T) {
				toDelete := loadedUsers[userIndex]
				toInsert := user{keyGen.NextKey(), "Wont-be-inserted", "Beer"}

				invEtag := invalidEtag
				err = store.Multi(context.Background(), &state.TransactionalStateRequest{
					Operations: []state.TransactionalStateOperation{
						state.DeleteRequest{Key: toDelete.ID, ETag: &invEtag},
						state.SetRequest{Key: toInsert.ID, Value: toInsert},
					},
				})

				require.Error(t, err)
				assertUserDoesNotExist(t, store, toInsert.ID)
				assertLoadedUserIsEqual(t, store, toDelete.ID, toDelete.user)

				assertUserCountIsEqualTo(t, store, totalUsers)
			})

			t.Run("Delete fails, should abort update", func(t *testing.T) {
				toDelete := loadedUsers[userIndex]
				toModify := loadedUsers[userIndex+1]
				modified := toModify.user
				modified.FavoriteBeverage = beverageTea

				invEtag := invalidEtag
				err = store.Multi(context.Background(), &state.TransactionalStateRequest{
					Operations: []state.TransactionalStateOperation{
						state.DeleteRequest{Key: toDelete.ID, ETag: &invEtag},
						state.SetRequest{Key: modified.ID, Value: modified},
					},
				})
				require.Error(t, err)
				assertLoadedUserIsEqual(t, store, toDelete.ID, toDelete.user)
				assertLoadedUserIsEqual(t, store, toModify.ID, toModify.user)

				assertUserCountIsEqualTo(t, store, totalUsers)
			})

			t.Run("Update fails, should abort delete", func(t *testing.T) {
				toDelete := loadedUsers[userIndex]
				toModify := loadedUsers[userIndex+1]
				modified := toModify.user
				modified.FavoriteBeverage = beverageTea

				invEtag := invalidEtag
				err = store.Multi(context.Background(), &state.TransactionalStateRequest{
					Operations: []state.TransactionalStateOperation{
						state.DeleteRequest{Key: toDelete.ID},
						state.SetRequest{Key: modified.ID, Value: modified, ETag: &invEtag},
					},
				})

				require.Error(t, err)
				assertLoadedUserIsEqual(t, store, toDelete.ID, toDelete.user)
				assertLoadedUserIsEqual(t, store, toModify.ID, toModify.user)

				assertUserCountIsEqualTo(t, store, totalUsers)
			})
		})
	}
}

/* #nosec. */
func testInsertAndUpdateSetRecordDates(t *testing.T) {
	const maxDiffInMs = float64(500)
	store := getTestStore(t, "")

	u := user{"1", "John", "Coffee"}
	err := store.Set(context.Background(), &state.SetRequest{Key: u.ID, Value: u})
	require.NoError(t, err)

	var originalInsertTime time.Time
	getUserTsql := fmt.Sprintf("SELECT [InsertDate], [UpdateDate] from [%s].[%s] WHERE [Key]='%s'", store.metadata.SchemaName, store.metadata.TableName, u.ID)
	assertDBQuery(t, store, getUserTsql, func(t *testing.T, rows *sql.Rows) {
		assert.True(t, rows.Next())

		var insertDate, updateDate sql.NullTime
		localErr := rows.Scan(&insertDate, &updateDate)
		require.NoError(t, localErr)

		assert.True(t, insertDate.Valid)
		insertDiff := float64(time.Now().UTC().Sub(insertDate.Time).Milliseconds())
		assert.LessOrEqual(t, math.Abs(insertDiff), maxDiffInMs)
		assert.False(t, updateDate.Valid)

		originalInsertTime = insertDate.Time
	})

	modified := u
	modified.FavoriteBeverage = beverageTea
	err = store.Set(context.Background(), &state.SetRequest{Key: modified.ID, Value: modified})
	require.NoError(t, err)
	assertDBQuery(t, store, getUserTsql, func(t *testing.T, rows *sql.Rows) {
		assert.True(t, rows.Next())

		var insertDate, updateDate sql.NullTime
		err := rows.Scan(&insertDate, &updateDate)
		require.NoError(t, err)

		assert.True(t, insertDate.Valid)
		assert.Equal(t, originalInsertTime, insertDate.Time)

		assert.True(t, updateDate.Valid)
		updateDiff := float64(time.Now().UTC().Sub(updateDate.Time).Milliseconds())
		assert.LessOrEqual(t, math.Abs(updateDiff), maxDiffInMs)
	})
}

func testConcurrentSets(t *testing.T) {
	const parallelism = 10

	store := getTestStore(t, "")

	u := user{"1", "John", "Coffee"}
	err := store.Set(context.Background(), &state.SetRequest{Key: u.ID, Value: u})
	require.NoError(t, err)

	_, etag := assertLoadedUserIsEqual(t, store, u.ID, u)

	var wc sync.WaitGroup
	start := make(chan bool, parallelism)
	totalErrors := int32(0)
	totalSucceeds := int32(0)
	for range parallelism {
		wc.Add(1)
		go func(id, etag string, start <-chan bool, wc *sync.WaitGroup, store *SQLServer) {
			<-start

			defer wc.Done()

			modified := user{"1", "John", beverageTea}
			err := store.Set(context.Background(), &state.SetRequest{Key: id, Value: modified, ETag: &etag})
			if err != nil {
				atomic.AddInt32(&totalErrors, 1)
			} else {
				atomic.AddInt32(&totalSucceeds, 1)
			}
		}(u.ID, etag, start, &wc, store)
	}

	close(start)
	wc.Wait()

	assert.Equal(t, int32(parallelism-1), totalErrors)
	assert.Equal(t, int32(1), totalSucceeds)
}

func testMultipleInitializations(t *testing.T) {
	tests := []struct {
		name              string
		kt                KeyType
		indexedProperties string
	}{
		{"No indexed properties", StringKeyType, ""},
		{"With indexed properties", StringKeyType, `[{ "column":"FavoriteBeverage", "property":"FavoriteBeverage", "type":"nvarchar(100)"}, { "column":"PetsCount", "property":"PetsCount", "type": "INTEGER"}]`},
		{"No indexed properties uuid key type", UUIDKeyType, ""},
		{"No indexed properties integer key type", IntegerKeyType, ""},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := getTestStoreWithKeyType(t, test.kt, test.indexedProperties)

			store2 := &SQLServer{
				logger:          logger.NewLogger("test"),
				migratorFactory: newMigration,
			}
			store2.BulkStore = state.NewDefaultBulkStore(store2)
			err := store2.Init(context.Background(), createMetadata(store.metadata.SchemaName, test.kt, test.indexedProperties))
			require.NoError(t, err)
		})
	}
}
