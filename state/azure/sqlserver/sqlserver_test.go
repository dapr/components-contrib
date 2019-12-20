package sqlserver

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dapr/components-contrib/state"

	uuid "github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

const (
	masterConnectionString = "server=localhost;user id=sa;password=Pass@Word1;port=1433;"
	connectionString       = "server=localhost;user id=sa;password=Pass@Word1;port=1433;database=dapr_test;"
	usersTableName         = "Users"
	invalidEtag            = "FFFFFFFFFFFFFFFF"
)

var initDatabaseExec sync.Once

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

func getUniqueDBSchema() string {
	uuid := uuid.New().String()
	uuid = strings.ReplaceAll(uuid, "-", "")
	return fmt.Sprintf("v%s", uuid)
}

func createMetadata(schema string, kt KeyType, indexedProperties string) state.Metadata {
	metadata := state.Metadata{
		Properties: map[string]string{
			connectionStringKey: connectionString,
			schemaKey:           schema,
			tableNameKey:        usersTableName,
			keyTypeKey:          string(kt),
		},
	}

	if indexedProperties != "" {
		metadata.Properties[indexedPropertiesKey] = indexedProperties
	}

	return metadata
}

// Ensure the database is running
// For docker use: docker run --name sqlserver -e "ACCEPT_EULA=Y" -e "SA_PASSWORD=Pass@Word1" -p 1433:1433 -d mcr.microsoft.com/mssql/server:2019-GA-ubuntu-16.04
func getTestStore(t *testing.T, indexedProperties string) *StateStore {
	return getTestStoreWithKeyType(t, StringKeyType, indexedProperties)
}

func getTestStoreWithKeyType(t *testing.T, kt KeyType, indexedProperties string) *StateStore {
	ensureDBIsValid(t)
	schema := getUniqueDBSchema()
	metadata := createMetadata(schema, kt, indexedProperties)
	store := NewSQLServerStore()
	err := store.Init(metadata)
	assert.Nil(t, err)

	return store
}

func ensureDBIsValid(t *testing.T) {

	initDatabaseExec.Do(func() {
		// Check the database for computed columns
		db, err := sql.Open("sqlserver", masterConnectionString)
		assert.Nil(t, err)
		defer db.Close()

		_, err = db.Exec(`
IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = N'dapr_test')
	CREATE DATABASE [dapr_test]`)
		assert.Nil(t, err)
	})
}

func assertUserExists(t *testing.T, store *StateStore, key string) (user, string) {
	getRes, err := store.Get(&state.GetRequest{Key: key})
	assert.Nil(t, err)
	assert.NotNil(t, getRes)
	assert.NotNil(t, getRes.Data)
	assert.NotEmpty(t, getRes.ETag)

	var loaded user
	err = json.Unmarshal(getRes.Data, &loaded)
	assert.Nil(t, err)

	return loaded, getRes.ETag
}

func assertLoadedUserIsEqual(t *testing.T, store *StateStore, key string, expected user) (user, string) {
	loaded, etag := assertUserExists(t, store, key)
	assert.Equal(t, expected.ID, loaded.ID)
	assert.Equal(t, expected.Name, loaded.Name)
	assert.Equal(t, expected.FavoriteBeverage, loaded.FavoriteBeverage)

	return loaded, etag
}

func assertUserDoesNotExist(t *testing.T, store *StateStore, key string) {
	_, err := store.Get(&state.GetRequest{Key: key})
	assert.NotNil(t, err)
}

func assertDBQuery(t *testing.T, store *StateStore, query string, assertReader func(t *testing.T, rows *sql.Rows)) {
	db, err := sql.Open("sqlserver", store.connectionString)
	assert.Nil(t, err)
	defer db.Close()

	rows, err := db.Query(query)
	assert.Nil(t, err)

	defer rows.Close()
	assertReader(t, rows)
}

func assertUserCountIsEqualTo(t *testing.T, store *StateStore, expected int) {
	tsql := fmt.Sprintf("SELECT count(*) FROM [%s].[%s]", store.schema, store.tableName)
	assertDBQuery(t, store, tsql, func(t *testing.T, rows *sql.Rows) {
		assert.True(t, rows.Next())
		var actual int
		err := rows.Scan(&actual)
		assert.Nil(t, err)
		assert.Equal(t, expected, actual)
	})
}

func TestInvalidConfiguration(t *testing.T) {

	tests := []struct {
		name  string
		props map[string]string
	}{
		{
			name:  "Empty",
			props: map[string]string{},
		},
		{
			name:  "Empty connection string",
			props: map[string]string{connectionStringKey: ""},
		},
		{
			name:  "Empty table name ",
			props: map[string]string{connectionStringKey: connectionString},
		},
		{
			name:  "Invalid maxKeyLength value ",
			props: map[string]string{connectionStringKey: connectionString, tableNameKey: "test", keyLengthKey: "aa"},
		},
		{
			name:  "Invalid indexes properties ",
			props: map[string]string{connectionStringKey: connectionString, tableNameKey: "test", indexedPropertiesKey: "no_json"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sqlStore := NewSQLServerStore()

			metadata := state.Metadata{
				Properties: tt.props,
			}

			err := sqlStore.Init(metadata)
			assert.NotNil(t, err)
		})
	}
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

type uuidKeyGenerator struct {
}

func (n uuidKeyGenerator) NextKey() string {
	return uuid.New().String()
}

func TestSingleOperations(t *testing.T) {

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
			err = store.Set(&state.SetRequest{Key: waterJohn.ID, Value: waterJohn, ETag: etagFromInsert})
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
			err = store.Set(&state.SetRequest{Key: failedJohn.ID, Value: failedJohn, ETag: etagFromInsert})
			assert.NotNil(t, err)
			_, etag := assertLoadedUserIsEqual(t, store, johnV3.ID, johnV3)

			// 9. Delete with invalid ETAG should fail
			err = store.Delete(&state.DeleteRequest{Key: johnV3.ID, ETag: invalidEtag})
			assert.NotNil(t, err)
			assertLoadedUserIsEqual(t, store, johnV3.ID, johnV3)

			// 10. Delete with valid ETAG
			err = store.Delete(&state.DeleteRequest{Key: johnV2.ID, ETag: etag})
			assert.Nil(t, err)

			assertUserDoesNotExist(t, store, johnV2.ID)
		})
	}

}

func TestSetNewRecordWithInvalidEtagShouldFail(t *testing.T) {
	store := getTestStore(t, "")

	u := user{uuid.New().String(), "John", "Coffee"}

	err := store.Set(&state.SetRequest{Key: u.ID, Value: u, ETag: invalidEtag})
	assert.NotNil(t, err)
}

func TestIndexedProperties(t *testing.T) {

	store := getTestStore(t, `[{ "column":"FavoriteBeverage", "property":"FavoriteBeverage", "type":"nvarchar(100)"}, { "column":"PetsCount", "property":"PetsCount", "type": "INTEGER"}]`)

	err := store.BulkSet([]state.SetRequest{
		state.SetRequest{Key: "1", Value: userWithPets{user{"1", "John", "Coffee"}, 3}},
		state.SetRequest{Key: "2", Value: userWithPets{user{"2", "Laura", "Water"}, 1}},
		state.SetRequest{Key: "3", Value: userWithPets{user{"3", "Carl", "Beer"}, 0}},
		state.SetRequest{Key: "4", Value: userWithPets{user{"4", "Maria", "Wine"}, 100}},
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

func TestMultiOperations(t *testing.T) {

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
				user{keyGen.NextKey(), "John", "Coffee"},
				user{keyGen.NextKey(), "Laura", "Water"},
				user{keyGen.NextKey(), "Carl", "Beer"},
				user{keyGen.NextKey(), "Maria", "Wine"},
				user{keyGen.NextKey(), "Mark", "Juice"},
				user{keyGen.NextKey(), "Sara", "Soda"},
				user{keyGen.NextKey(), "Tony", "Milk"},
				user{keyGen.NextKey(), "Hugo", "Juice"},
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
				modified.FavoriteBeverage = "tea"

				err := store.Multi([]state.TransactionalRequest{
					state.TransactionalRequest{Operation: state.Delete, Request: state.DeleteRequest{Key: toDelete.ID}},
					state.TransactionalRequest{Operation: state.Upsert, Request: state.SetRequest{Key: modified.ID, Value: modified}},
				})
				assert.Nil(t, err)
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
				modified.FavoriteBeverage = "tea"

				err = store.Multi([]state.TransactionalRequest{
					state.TransactionalRequest{Operation: state.Delete, Request: state.DeleteRequest{Key: toDelete.ID, ETag: toDelete.etag}},
					state.TransactionalRequest{Operation: state.Upsert, Request: state.SetRequest{Key: modified.ID, Value: modified, ETag: toModify.etag}},
					state.TransactionalRequest{Operation: state.Upsert, Request: state.SetRequest{Key: toInsert.ID, Value: toInsert}},
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
				modified.FavoriteBeverage = "tea"

				err = store.Multi([]state.TransactionalRequest{
					state.TransactionalRequest{Operation: state.Delete, Request: state.DeleteRequest{Key: toDelete.ID, ETag: toDelete.etag}},
					state.TransactionalRequest{Operation: state.Upsert, Request: state.SetRequest{Key: modified.ID, Value: modified, ETag: toModify.etag}},
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

				err = store.Multi([]state.TransactionalRequest{
					state.TransactionalRequest{Operation: state.Delete, Request: state.DeleteRequest{Key: toDelete.ID, ETag: invalidEtag}},
					state.TransactionalRequest{Operation: state.Upsert, Request: state.SetRequest{Key: toInsert.ID, Value: toInsert}},
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
				modified.FavoriteBeverage = "tea"

				err = store.Multi([]state.TransactionalRequest{
					state.TransactionalRequest{Operation: state.Delete, Request: state.DeleteRequest{Key: toDelete.ID, ETag: invalidEtag}},
					state.TransactionalRequest{Operation: state.Upsert, Request: state.SetRequest{Key: modified.ID, Value: modified}},
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
				modified.FavoriteBeverage = "tea"

				err = store.Multi([]state.TransactionalRequest{
					state.TransactionalRequest{Operation: state.Delete, Request: state.DeleteRequest{Key: toDelete.ID}},
					state.TransactionalRequest{Operation: state.Upsert, Request: state.SetRequest{Key: modified.ID, Value: modified, ETag: invalidEtag}},
				})

				assert.NotNil(t, err)
				assertLoadedUserIsEqual(t, store, toDelete.ID, toDelete.user)
				assertLoadedUserIsEqual(t, store, toModify.ID, toModify.user)

				assertUserCountIsEqualTo(t, store, totalUsers)
			})
		})
	}
}

func TestBulkSet(t *testing.T) {

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
				user{keyGen.NextKey(), "John", "Coffee"},
				user{keyGen.NextKey(), "Laura", "Water"},
				user{keyGen.NextKey(), "Carl", "Beer"},
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
				modified.FavoriteBeverage = "tea"
				toInsert := user{keyGen.NextKey(), "Maria", "Wine"}

				err := store.BulkSet([]state.SetRequest{
					state.SetRequest{Key: modified.ID, Value: modified, ETag: toModifyETag},
					state.SetRequest{Key: toInsert.ID, Value: toInsert},
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
				modified.FavoriteBeverage = "tea"
				toInsert := user{keyGen.NextKey(), "Tony", "Milk"}

				err := store.BulkSet([]state.SetRequest{
					state.SetRequest{Key: modified.ID, Value: modified},
					state.SetRequest{Key: toInsert.ID, Value: toInsert},
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
				modified.FavoriteBeverage = "tea"

				sets := []state.SetRequest{
					state.SetRequest{Key: toInsert1.ID, Value: toInsert1},
					state.SetRequest{Key: toInsert2.ID, Value: toInsert2},
					state.SetRequest{Key: modified.ID, Value: modified, ETag: invalidEtag},
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

func TestBulkDelete(t *testing.T) {

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
				user{keyGen.NextKey(), "John", "Coffee"},
				user{keyGen.NextKey(), "Laura", "Water"},
				user{keyGen.NextKey(), "Carl", "Beer"},
				user{keyGen.NextKey(), "Maria", "Wine"},
				user{keyGen.NextKey(), "Mark", "Juice"},
				user{keyGen.NextKey(), "Sara", "Soda"},
				user{keyGen.NextKey(), "Tony", "Milk"},
				user{keyGen.NextKey(), "Hugo", "Juice"},
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
					state.DeleteRequest{Key: deleted1},
					state.DeleteRequest{Key: deleted2},
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
					state.DeleteRequest{Key: deleted1.ID, ETag: deleted1Etag},
					state.DeleteRequest{Key: deleted2.ID, ETag: deleted2Etag},
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
					state.DeleteRequest{Key: deleted1.ID, ETag: deleted1Etag},
					state.DeleteRequest{Key: deleted2.ID},
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

				err := store.BulkDelete([]state.DeleteRequest{
					state.DeleteRequest{Key: deleted1.ID, ETag: deleted1Etag},
					state.DeleteRequest{Key: deleted2.ID, ETag: invalidEtag},
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

func TestInsertAndUpdateSetRecordDates(t *testing.T) {
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
		err := rows.Scan(&insertDate, &updateDate)
		assert.Nil(t, err)

		assert.True(t, insertDate.Valid)
		var insertDiff = float64(time.Now().UTC().Sub(insertDate.Time).Milliseconds())
		assert.LessOrEqual(t, math.Abs(insertDiff), maxDiffInMs)
		assert.False(t, updateDate.Valid)

		originalInsertTime = insertDate.Time
	})

	modified := u
	modified.FavoriteBeverage = "tea"
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
		var updateDiff = float64(time.Now().UTC().Sub(updateDate.Time).Milliseconds())
		assert.LessOrEqual(t, math.Abs(updateDiff), maxDiffInMs)
	})
}

func TestConcurrentSets(t *testing.T) {

	const executions = 10
	const parallelism = 10

	for i := 0; i < executions; i++ {

		t.Run(fmt.Sprintf("Only a single writer, try #%d", i+1), func(t *testing.T) {
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
				go func(id, etag string, start <-chan bool, wc *sync.WaitGroup, store *StateStore) {
					<-start

					defer wc.Done()

					modified := user{"1", "John", "tea"}
					err := store.Set(&state.SetRequest{Key: id, Value: modified, ETag: etag})
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
		})
	}
}

func TestMultipleInitializations(t *testing.T) {

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

			store2 := NewSQLServerStore()
			assert.Nil(t, store2.Init(createMetadata(store.schema, test.kt, test.indexedProperties)))
		})
	}
}
