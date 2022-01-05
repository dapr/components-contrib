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
	"database/sql"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	uuid "github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/state"
	"github.com/dapr/kit/logger"
)

const (
	// connectionStringEnvKey defines the key containing the integration test connection string
	// To use docker, server=localhost;user id=sa;password=Pass@Word1;port=1433;
	// To use Azure SQL, server=<your-db-server-name>.database.windows.net;user id=<your-db-user>;port=1433;password=<your-password>;database=dapr_test;.
	connectionStringEnvKey        = "DAPR_TEST_SQL_CONNSTRING"
	usersTableName                = "Users"
	beverageTea                   = "tea"
	invalidEtag            string = "FFFFFFFFFFFFFFFF"
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

func getMasterConnectionString() string {
	return os.Getenv(connectionStringEnvKey)
}

func TestIntegrationCases(t *testing.T) {
	connectionString := getMasterConnectionString()
	if connectionString == "" {
		t.Skipf("SQLServer state integration tests skipped. To enable define the connection string using environment variable '%s' (example 'export %s=\"server=localhost;user id=sa;password=Pass@Word1;port=1433;\")", connectionStringEnvKey, connectionStringEnvKey)
	}

	t.Run("Single operations", testSingleOperations)
	t.Run("Set New Record With Invalid Etag Should Fail", testSetNewRecordWithInvalidEtagShouldFail)
	t.Run("Indexed Properties", testIndexedProperties)
	t.Run("Multi operations", testMultiOperations)
	t.Run("Bulk sets", testBulkSet)
	t.Run("Bulk delete", testBulkDelete)
	t.Run("Insert and Update Set Record Dates", testInsertAndUpdateSetRecordDates)
	t.Run("Multiple initializations", testMultipleInitializations)

	// Run concurrent set tests 10 times
	const executions = 10
	for i := 0; i < executions; i++ {
		t.Run(fmt.Sprintf("Concurrent sets, try #%d", i+1), testConcurrentSets)
	}
}

func getUniqueDBSchema() string {
	uuid := uuid.New().String()
	uuid = strings.ReplaceAll(uuid, "-", "")

	return fmt.Sprintf("v%s", uuid)
}

func createMetadata(schema string, kt KeyType, indexedProperties string) state.Metadata {
	metadata := state.Metadata{
		Properties: map[string]string{
			connectionStringKey: getMasterConnectionString(),
			schemaKey:           schema,
			tableNameKey:        usersTableName,
			keyTypeKey:          string(kt),
			databaseNameKey:     "dapr_test",
		},
	}

	if indexedProperties != "" {
		metadata.Properties[indexedPropertiesKey] = indexedProperties
	}

	return metadata
}

// Ensure the database is running
// For docker, use: docker run --name sqlserver -e "ACCEPT_EULA=Y" -e "SA_PASSWORD=Pass@Word1" -p 1433:1433 -d mcr.microsoft.com/mssql/server:2019-GA-ubuntu-16.04.
func getTestStore(t *testing.T, indexedProperties string) *SQLServer {
	return getTestStoreWithKeyType(t, StringKeyType, indexedProperties)
}

func getTestStoreWithKeyType(t *testing.T, kt KeyType, indexedProperties string) *SQLServer {
	schema := getUniqueDBSchema()
	metadata := createMetadata(schema, kt, indexedProperties)
	store := NewSQLServerStateStore(logger.NewLogger("test"))
	err := store.Init(metadata)
	assert.Nil(t, err)

	return store
}

func assertUserExists(t *testing.T, store *SQLServer, key string) (user, string) {
	getRes, err := store.Get(&state.GetRequest{Key: key})
	assert.Nil(t, err)
	assert.NotNil(t, getRes)
	assert.NotNil(t, getRes.Data, "No data was returned")
	require.NotNil(t, getRes.ETag)

	var loaded user
	err = json.Unmarshal(getRes.Data, &loaded)
	assert.Nil(t, err)

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
	_, err := store.Get(&state.GetRequest{Key: key})
	assert.Nil(t, err)
}

func assertDBQuery(t *testing.T, store *SQLServer, query string, assertReader func(t *testing.T, rows *sql.Rows)) {
	db, err := sql.Open("sqlserver", store.connectionString)
	assert.Nil(t, err)
	defer db.Close()

	rows, err := db.Query(query)
	assert.Nil(t, err)
	assert.Nil(t, rows.Err())

	defer rows.Close()
	assertReader(t, rows)
}

/* #nosec. */
func assertUserCountIsEqualTo(t *testing.T, store *SQLServer, expected int) {
	tsql := fmt.Sprintf("SELECT count(*) FROM [%s].[%s]", store.schema, store.tableName)
	assertDBQuery(t, store, tsql, func(t *testing.T, rows *sql.Rows) {
		assert.True(t, rows.Next())
		var actual int
		err := rows.Scan(&actual)
		assert.Nil(t, err)
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
			err := store.Set(&state.SetRequest{Key: john.ID, Value: john})
			assert.Nil(t, err)
			johnV1, etagFromInsert := assertLoadedUserIsEqual(t, store, john.ID, john)

			// Update with ETAG
			waterJohn := johnV1
			waterJohn.FavoriteBeverage = "Water"
			err = store.Set(&state.SetRequest{Key: waterJohn.ID, Value: waterJohn, ETag: &etagFromInsert})
			assert.Nil(t, err)

			// Get updated
			johnV2, _ := assertLoadedUserIsEqual(t, store, waterJohn.ID, waterJohn)

			// Update without ETAG
			noEtagJohn := johnV2
			noEtagJohn.FavoriteBeverage = "No Etag John"
			err = store.Set(&state.SetRequest{Key: noEtagJohn.ID, Value: noEtagJohn})
			assert.Nil(t, err)

			// 7. Get updated
			johnV3, _ := assertLoadedUserIsEqual(t, store, noEtagJohn.ID, noEtagJohn)

			// 8. Update with invalid ETAG should fail
			failedJohn := johnV3
			failedJohn.FavoriteBeverage = "Will not work"
			err = store.Set(&state.SetRequest{Key: failedJohn.ID, Value: failedJohn, ETag: &etagFromInsert})
			assert.NotNil(t, err)
			_, etag := assertLoadedUserIsEqual(t, store, johnV3.ID, johnV3)

			// 9. Delete with invalid ETAG should fail
			err = store.Delete(&state.DeleteRequest{Key: johnV3.ID, ETag: &invEtag})
			assert.NotNil(t, err)
			assertLoadedUserIsEqual(t, store, johnV3.ID, johnV3)

			// 10. Delete with valid ETAG
			err = store.Delete(&state.DeleteRequest{Key: johnV2.ID, ETag: &etag})
			assert.Nil(t, err)

			assertUserDoesNotExist(t, store, johnV2.ID)
		})
	}
}

func testSetNewRecordWithInvalidEtagShouldFail(t *testing.T) {
	store := getTestStore(t, "")

	u := user{uuid.New().String(), "John", "Coffee"}

	invEtag := invalidEtag
	err := store.Set(&state.SetRequest{Key: u.ID, Value: u, ETag: &invEtag})
	assert.NotNil(t, err)
}

/* #nosec. */
func testIndexedProperties(t *testing.T) {
	store := getTestStore(t, `[{ "column":"FavoriteBeverage", "property":"FavoriteBeverage", "type":"nvarchar(100)"}, { "column":"PetsCount", "property":"PetsCount", "type": "INTEGER"}]`)

	err := store.BulkSet([]state.SetRequest{
		{Key: "1", Value: userWithPets{user{"1", "John", "Coffee"}, 3}},
		{Key: "2", Value: userWithPets{user{"2", "Laura", "Water"}, 1}},
		{Key: "3", Value: userWithPets{user{"3", "Carl", "Beer"}, 0}},
		{Key: "4", Value: userWithPets{user{"4", "Maria", "Wine"}, 100}},
	})

	assert.Nil(t, err)

	// Check the database for computed columns
	assertDBQuery(t, store, fmt.Sprintf("SELECT count(*) from [%s].[%s] WHERE PetsCount < 3", store.schema, usersTableName), func(t *testing.T, rows *sql.Rows) {
		assert.True(t, rows.Next())

		var c int
		rows.Scan(&c)
		assert.Equal(t, 2, c)
	})

	// Ensure we can get by beverage
	assertDBQuery(t, store, fmt.Sprintf("SELECT count(*) from [%s].[%s] WHERE FavoriteBeverage = '%s'", store.schema, usersTableName, "Coffee"), func(t *testing.T, rows *sql.Rows) {
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

			err := store.BulkSet(bulkSet)
			assert.Nil(t, err)
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

				localErr := store.Multi(&state.TransactionalStateRequest{
					Operations: []state.TransactionalStateOperation{
						{Operation: state.Delete, Request: state.DeleteRequest{Key: toDelete.ID}},
						{Operation: state.Upsert, Request: state.SetRequest{Key: modified.ID, Value: modified}},
					},
				})
				assert.Nil(t, localErr)
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

				err = store.Multi(&state.TransactionalStateRequest{
					Operations: []state.TransactionalStateOperation{
						{Operation: state.Delete, Request: state.DeleteRequest{Key: toDelete.ID, ETag: &toDelete.etag}},
						{Operation: state.Upsert, Request: state.SetRequest{Key: modified.ID, Value: modified, ETag: &toModify.etag}},
						{Operation: state.Upsert, Request: state.SetRequest{Key: toInsert.ID, Value: toInsert}},
					},
				})
				assert.Nil(t, err)
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

				err = store.Multi(&state.TransactionalStateRequest{
					Operations: []state.TransactionalStateOperation{
						{Operation: state.Delete, Request: state.DeleteRequest{Key: toDelete.ID, ETag: &toDelete.etag}},
						{Operation: state.Upsert, Request: state.SetRequest{Key: modified.ID, Value: modified, ETag: &toModify.etag}},
					},
				})
				assert.Nil(t, err)
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
				err = store.Multi(&state.TransactionalStateRequest{
					Operations: []state.TransactionalStateOperation{
						{Operation: state.Delete, Request: state.DeleteRequest{Key: toDelete.ID, ETag: &invEtag}},
						{Operation: state.Upsert, Request: state.SetRequest{Key: toInsert.ID, Value: toInsert}},
					},
				})

				assert.NotNil(t, err)
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
				err = store.Multi(&state.TransactionalStateRequest{
					Operations: []state.TransactionalStateOperation{
						{Operation: state.Delete, Request: state.DeleteRequest{Key: toDelete.ID, ETag: &invEtag}},
						{Operation: state.Upsert, Request: state.SetRequest{Key: modified.ID, Value: modified}},
					},
				})
				assert.NotNil(t, err)
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
				err = store.Multi(&state.TransactionalStateRequest{
					Operations: []state.TransactionalStateOperation{
						{Operation: state.Delete, Request: state.DeleteRequest{Key: toDelete.ID}},
						{Operation: state.Upsert, Request: state.SetRequest{Key: modified.ID, Value: modified, ETag: &invEtag}},
					},
				})

				assert.NotNil(t, err)
				assertLoadedUserIsEqual(t, store, toDelete.ID, toDelete.user)
				assertLoadedUserIsEqual(t, store, toModify.ID, toModify.user)

				assertUserCountIsEqualTo(t, store, totalUsers)
			})
		})
	}
}

func testBulkSet(t *testing.T) {
	tests := []struct {
		name   string
		kt     KeyType
		keyGen userKeyGenerator
	}{
		{"Bulk set string key type", StringKeyType, &numbericKeyGenerator{}},
		{"Bulk set integer key type", IntegerKeyType, &numbericKeyGenerator{}},
		{"Bulk set uuid key type", UUIDKeyType, &uuidKeyGenerator{}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := getTestStoreWithKeyType(t, test.kt, "")
			keyGen := test.keyGen

			initialUsers := []user{
				{keyGen.NextKey(), "John", "Coffee"},
				{keyGen.NextKey(), "Laura", "Water"},
				{keyGen.NextKey(), "Carl", "Beer"},
			}

			totalUsers := 0
			userIndex := 0

			t.Run("Add initial users", func(t *testing.T) {
				sets := make([]state.SetRequest, len(initialUsers))
				for i, u := range initialUsers {
					sets[i] = state.SetRequest{Key: u.ID, Value: u}
				}

				err := store.BulkSet(sets)
				assert.Nil(t, err)
				totalUsers = len(sets)
				assertUserCountIsEqualTo(t, store, totalUsers)
			})

			t.Run("Add 1, update 1 with valid etag", func(t *testing.T) {
				toModify, toModifyETag := assertUserExists(t, store, initialUsers[userIndex].ID)
				modified := toModify
				modified.FavoriteBeverage = beverageTea
				toInsert := user{keyGen.NextKey(), "Maria", "Wine"}

				err := store.BulkSet([]state.SetRequest{
					{Key: modified.ID, Value: modified, ETag: &toModifyETag},
					{Key: toInsert.ID, Value: toInsert},
				})
				assert.Nil(t, err)
				assertLoadedUserIsEqual(t, store, modified.ID, modified)
				assertLoadedUserIsEqual(t, store, toInsert.ID, toInsert)
				totalUsers++
				assertUserCountIsEqualTo(t, store, totalUsers)

				userIndex++
			})

			t.Run("Add 1, update 1 without etag", func(t *testing.T) {
				toModify := initialUsers[userIndex]
				modified := toModify
				modified.FavoriteBeverage = beverageTea
				toInsert := user{keyGen.NextKey(), "Tony", "Milk"}

				err := store.BulkSet([]state.SetRequest{
					{Key: modified.ID, Value: modified},
					{Key: toInsert.ID, Value: toInsert},
				})
				assert.Nil(t, err)
				assertLoadedUserIsEqual(t, store, modified.ID, modified)
				assertLoadedUserIsEqual(t, store, toInsert.ID, toInsert)
				totalUsers++
				assertUserCountIsEqualTo(t, store, totalUsers)

				userIndex++
			})

			t.Run("Failed upsert due to etag should be aborted", func(t *testing.T) {
				toInsert1 := user{keyGen.NextKey(), "Ted1", "Beer"}
				toInsert2 := user{keyGen.NextKey(), "Ted2", "Beer"}
				toModify := initialUsers[userIndex]
				modified := toModify
				modified.FavoriteBeverage = beverageTea

				invEtag := invalidEtag
				sets := []state.SetRequest{
					{Key: toInsert1.ID, Value: toInsert1},
					{Key: toInsert2.ID, Value: toInsert2},
					{Key: modified.ID, Value: modified, ETag: &invEtag},
				}

				err := store.BulkSet(sets)
				assert.NotNil(t, err)
				assertUserCountIsEqualTo(t, store, totalUsers)
				assertUserDoesNotExist(t, store, toInsert1.ID)
				assertUserDoesNotExist(t, store, toInsert2.ID)
				assertLoadedUserIsEqual(t, store, modified.ID, toModify)
				assertUserCountIsEqualTo(t, store, totalUsers)
			})
		})
	}
}

func testBulkDelete(t *testing.T) {
	tests := []struct {
		name   string
		kt     KeyType
		keyGen userKeyGenerator
	}{
		{"Bulk delete string key type", StringKeyType, &numbericKeyGenerator{}},
		{"Bulk delete integer key type", IntegerKeyType, &numbericKeyGenerator{}},
		{"Bulk delete uuid key type", UUIDKeyType, &uuidKeyGenerator{}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := getTestStoreWithKeyType(t, test.kt, "")
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

			sets := make([]state.SetRequest, len(initialUsers))
			for i, u := range initialUsers {
				sets[i] = state.SetRequest{Key: u.ID, Value: u}
			}
			err := store.BulkSet(sets)
			assert.Nil(t, err)
			totalUsers := len(initialUsers)
			assertUserCountIsEqualTo(t, store, totalUsers)

			userIndex := 0

			t.Run("Delete 2 items without etag should work", func(t *testing.T) {
				deleted1 := initialUsers[userIndex].ID
				deleted2 := initialUsers[userIndex+1].ID
				err := store.BulkDelete([]state.DeleteRequest{
					{Key: deleted1},
					{Key: deleted2},
				})
				assert.Nil(t, err)
				totalUsers -= 2
				assertUserCountIsEqualTo(t, store, totalUsers)
				assertUserDoesNotExist(t, store, deleted1)
				assertUserDoesNotExist(t, store, deleted2)

				userIndex += 2
			})

			t.Run("Delete 2 items with etag should work", func(t *testing.T) {
				deleted1, deleted1Etag := assertUserExists(t, store, initialUsers[userIndex].ID)
				deleted2, deleted2Etag := assertUserExists(t, store, initialUsers[userIndex+1].ID)

				err := store.BulkDelete([]state.DeleteRequest{
					{Key: deleted1.ID, ETag: &deleted1Etag},
					{Key: deleted2.ID, ETag: &deleted2Etag},
				})
				assert.Nil(t, err)
				totalUsers -= 2
				assertUserCountIsEqualTo(t, store, totalUsers)
				assertUserDoesNotExist(t, store, deleted1.ID)
				assertUserDoesNotExist(t, store, deleted2.ID)

				userIndex += 2
			})

			t.Run("Delete with/without etag should work", func(t *testing.T) {
				deleted1, deleted1Etag := assertUserExists(t, store, initialUsers[userIndex].ID)
				deleted2 := initialUsers[userIndex+1]

				err := store.BulkDelete([]state.DeleteRequest{
					{Key: deleted1.ID, ETag: &deleted1Etag},
					{Key: deleted2.ID},
				})
				assert.Nil(t, err)
				totalUsers -= 2
				assertUserCountIsEqualTo(t, store, totalUsers)
				assertUserDoesNotExist(t, store, deleted1.ID)
				assertUserDoesNotExist(t, store, deleted2.ID)

				userIndex += 2
			})

			t.Run("Failed delete due to etag should be aborted", func(t *testing.T) {
				deleted1, deleted1Etag := assertUserExists(t, store, initialUsers[userIndex].ID)
				deleted2 := initialUsers[userIndex+1]

				invEtag := invalidEtag
				err := store.BulkDelete([]state.DeleteRequest{
					{Key: deleted1.ID, ETag: &deleted1Etag},
					{Key: deleted2.ID, ETag: &invEtag},
				})
				assert.NotNil(t, err)
				assert.NotNil(t, err)
				assertUserCountIsEqualTo(t, store, totalUsers)
				assertUserExists(t, store, deleted1.ID)
				assertUserExists(t, store, deleted2.ID)
			})
		})
	}
}

/* #nosec. */
func testInsertAndUpdateSetRecordDates(t *testing.T) {
	const maxDiffInMs = float64(500)
	store := getTestStore(t, "")

	u := user{"1", "John", "Coffee"}
	err := store.Set(&state.SetRequest{Key: u.ID, Value: u})
	assert.Nil(t, err)

	var originalInsertTime time.Time
	getUserTsql := fmt.Sprintf("SELECT [InsertDate], [UpdateDate] from [%s].[%s] WHERE [Key]='%s'", store.schema, store.tableName, u.ID)
	assertDBQuery(t, store, getUserTsql, func(t *testing.T, rows *sql.Rows) {
		assert.True(t, rows.Next())

		var insertDate, updateDate sql.NullTime
		localErr := rows.Scan(&insertDate, &updateDate)
		assert.Nil(t, localErr)

		assert.True(t, insertDate.Valid)
		insertDiff := float64(time.Now().UTC().Sub(insertDate.Time).Milliseconds())
		assert.LessOrEqual(t, math.Abs(insertDiff), maxDiffInMs)
		assert.False(t, updateDate.Valid)

		originalInsertTime = insertDate.Time
	})

	modified := u
	modified.FavoriteBeverage = beverageTea
	err = store.Set(&state.SetRequest{Key: modified.ID, Value: modified})
	assert.Nil(t, err)
	assertDBQuery(t, store, getUserTsql, func(t *testing.T, rows *sql.Rows) {
		assert.True(t, rows.Next())

		var insertDate, updateDate sql.NullTime
		err := rows.Scan(&insertDate, &updateDate)
		assert.Nil(t, err)

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
	err := store.Set(&state.SetRequest{Key: u.ID, Value: u})
	assert.Nil(t, err)

	_, etag := assertLoadedUserIsEqual(t, store, u.ID, u)

	var wc sync.WaitGroup
	start := make(chan bool, parallelism)
	totalErrors := int32(0)
	totalSucceeds := int32(0)
	for i := 0; i < parallelism; i++ {
		wc.Add(1)
		go func(id, etag string, start <-chan bool, wc *sync.WaitGroup, store *SQLServer) {
			<-start

			defer wc.Done()

			modified := user{"1", "John", beverageTea}
			err := store.Set(&state.SetRequest{Key: id, Value: modified, ETag: &etag})
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

			store2 := NewSQLServerStateStore(logger.NewLogger("test"))
			assert.Nil(t, store2.Init(createMetadata(store.schema, test.kt, test.indexedProperties)))
		})
	}
}
