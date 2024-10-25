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
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/state"
	"github.com/dapr/components-contrib/state/utils"
	"github.com/dapr/kit/logger"
)

const (
	fakeConnectionString = "not a real connection"
	keyTableName         = "tableName"
	keyConnectionString  = "connectionString"
	keySchemaName        = "schemaName"
)

func TestEnsureStateSchemaHandlesShortConnectionString(t *testing.T) {
	// Arrange
	m, _ := mockDatabase(t)
	defer m.mySQL.Close()

	m.mySQL.schemaName = "theSchema"
	m.mySQL.connectionString = "theUser:thePassword@/"

	rows := sqlmock.NewRows([]string{"exists"}).AddRow(1)
	m.mock1.ExpectQuery("SELECT EXISTS").WillReturnRows(rows)

	// Act
	m.mySQL.ensureStateSchema(context.Background())

	// Assert
	assert.Equal(t, "theUser:thePassword@/theSchema", m.mySQL.connectionString)
}

func TestFinishInitHandlesSchemaExistsError(t *testing.T) {
	// Arrange
	m, _ := mockDatabase(t)
	defer m.mySQL.Close()

	expectedErr := errors.New("existsError")
	m.mock1.ExpectQuery("SELECT EXISTS").WillReturnError(expectedErr)

	// Act
	actualErr := m.mySQL.finishInit(context.Background(), m.mySQL.db)

	// Assert
	require.Error(t, actualErr, "now error returned")
	assert.Equal(t, "existsError", actualErr.Error(), "wrong error")
}

func TestFinishInitHandlesDatabaseCreateError(t *testing.T) {
	// Arrange
	m, _ := mockDatabase(t)
	defer m.mySQL.Close()

	rows := sqlmock.NewRows([]string{"exists"}).AddRow(0)
	m.mock1.ExpectQuery("SELECT EXISTS").WillReturnRows(rows)

	expectedErr := errors.New("createDatabaseError")
	m.mock1.ExpectExec("CREATE DATABASE").WillReturnError(expectedErr)

	// Act
	actualErr := m.mySQL.finishInit(context.Background(), m.mySQL.db)

	// Assert
	require.Error(t, actualErr, "now error returned")
	assert.Equal(t, "createDatabaseError", actualErr.Error(), "wrong error")
}

func TestFinishInitHandlesPingError(t *testing.T) {
	// Arrange
	m, _ := mockDatabase(t)
	defer m.mySQL.Close()

	m.factory.openCount = 1

	// See if Schema exists
	rows := sqlmock.NewRows([]string{"exists"}).AddRow(1)
	m.mock1.ExpectQuery("SELECT EXISTS").WillReturnRows(rows)

	m.mock1.ExpectClose()

	expectedErr := errors.New("pingError")
	m.mock2.ExpectPing().WillReturnError(expectedErr)

	// Act
	actualErr := m.mySQL.finishInit(context.Background(), m.mySQL.db)

	// Assert
	require.Error(t, actualErr, "now error returned")
	assert.Equal(t, "pingError", actualErr.Error(), "wrong error")
}

// Verifies that finishInit can handle an error from its call to
// ensureStateTable. The code should not attempt to create the table. Because
// there is no m.mock1.ExpectExec if the code attempts to execute the create
// table commnad this test will fail.
func TestFinishInitHandlesTableExistsError(t *testing.T) {
	// Arrange
	m, _ := mockDatabase(t)
	defer m.mySQL.Close()

	m.factory.openCount = 1

	// See if Schema exists
	rows := sqlmock.NewRows([]string{"exists"}).AddRow(1)
	m.mock1.ExpectQuery("SELECT EXISTS").WillReturnRows(rows)
	m.mock1.ExpectClose()

	// Execute use command
	m.mock2.ExpectPing()
	m.mock2.ExpectQuery("SELECT EXISTS").WillReturnError(errors.New("tableExistsError"))

	// Act
	err := m.mySQL.finishInit(context.Background(), m.mySQL.db)

	// Assert
	require.Error(t, err, "no error returned")
	assert.Equal(t, "tableExistsError", err.Error(), "tableExists did not return err")
}

func TestClosingDatabaseTwiceReturnsNil(t *testing.T) {
	// Arrange
	m, _ := mockDatabase(t)
	m.mySQL.Close()
	m.mySQL.db = nil

	// Act
	err := m.mySQL.Close()

	// Assert
	require.NoError(t, err, "error returned")
}

func TestMultiCommitSetsAndDeletes(t *testing.T) {
	// Arrange
	m, _ := mockDatabase(t)
	defer m.mySQL.Close()

	m.mock1.ExpectBegin()
	m.mock1.ExpectExec("REPLACE INTO").WillReturnResult(sqlmock.NewResult(0, 1))
	m.mock1.ExpectExec("DELETE FROM").WillReturnResult(sqlmock.NewResult(0, 1))
	m.mock1.ExpectCommit()

	request := state.TransactionalStateRequest{
		Operations: []state.TransactionalStateOperation{
			createSetRequest(),
			createDeleteRequest(),
		},
		Metadata: map[string]string{},
	}

	// Act
	err := m.mySQL.Multi(context.Background(), &request)

	// Assert
	require.NoError(t, err, "error returned")
}

func TestSetHandlesOptionsError(t *testing.T) {
	// Arrange
	m, _ := mockDatabase(t)
	defer m.mySQL.Close()

	request := createSetRequest()

	request.Options.Consistency = "Invalid"

	// Act
	err := m.mySQL.Set(context.Background(), &request)

	// Assert
	require.Error(t, err)
}

func TestSetHandlesNoKey(t *testing.T) {
	// Arrange
	m, _ := mockDatabase(t)
	defer m.mySQL.Close()

	request := createSetRequest()
	request.Key = ""

	// Act
	err := m.mySQL.Set(context.Background(), &request)

	// Assert
	require.Error(t, err)
	assert.Equal(t, "missing key in set operation", err.Error(), "wrong error returned")
}

func TestSetHandlesUpdate(t *testing.T) {
	// Arrange
	m, _ := mockDatabase(t)
	defer m.mySQL.Close()

	m.mock1.ExpectExec("UPDATE state").WillReturnResult(sqlmock.NewResult(1, 1))

	eTag := "946af56e"

	request := createSetRequest()
	request.ETag = &eTag

	// Act
	err := m.mySQL.Set(context.Background(), &request)

	// Assert
	require.NoError(t, err)
}

func TestSetHandlesErr(t *testing.T) {
	// Arrange
	m, _ := mockDatabase(t)
	defer m.mySQL.Close()

	t.Run("error occurs when insert", func(t *testing.T) {
		m.mock1.ExpectExec("REPLACE INTO state").WillReturnError(errors.New("error"))
		request := createSetRequest()

		// Act
		err := m.mySQL.Set(context.Background(), &request)

		// Assert
		require.Error(t, err)
		assert.Equal(t, "error", err.Error())
	})

	t.Run("insert on conflict", func(t *testing.T) {
		m.mock1.ExpectExec("REPLACE INTO state").WillReturnResult(sqlmock.NewResult(1, 2))
		request := createSetRequest()

		// Act
		err := m.mySQL.Set(context.Background(), &request)

		// Assert
		require.NoError(t, err)
	})

	t.Run("no rows effected error", func(t *testing.T) {
		m.mock1.ExpectExec("UPDATE state").WillReturnResult(sqlmock.NewResult(1, 0))

		eTag := "illegal etag"
		request := createSetRequest()
		request.ETag = &eTag

		// Act
		err := m.mySQL.Set(context.Background(), &request)

		// Assert
		require.Error(t, err)
		assert.IsType(t, &state.ETagError{}, err)
		assert.Equal(t, state.ETagMismatch, err.(*state.ETagError).Kind())
	})
}

// Verifies that MySQL passes through to myDBAccess.
func TestMySQLDeleteHandlesNoKey(t *testing.T) {
	// Arrange
	m, _ := mockDatabase(t)
	request := createDeleteRequest()
	request.Key = ""

	// Act
	err := m.mySQL.Delete(context.Background(), &request)

	// Asset
	require.Error(t, err)
	assert.Equal(t, "missing key in delete operation", err.Error(), "wrong error returned")
}

func TestDeleteWithETag(t *testing.T) {
	// Arrange
	m, _ := mockDatabase(t)
	defer m.mySQL.Close()

	m.mock1.ExpectExec("DELETE FROM").WillReturnResult(sqlmock.NewResult(0, 1))

	eTag := "946af562"
	request := createDeleteRequest()
	request.ETag = &eTag

	// Act
	err := m.mySQL.Delete(context.Background(), &request)

	// Assert
	require.NoError(t, err)
}

func TestDeleteWithErr(t *testing.T) {
	// Arrange
	m, _ := mockDatabase(t)
	defer m.mySQL.Close()

	t.Run("error occurs when delete", func(t *testing.T) {
		m.mock1.ExpectExec("DELETE FROM").WillReturnError(errors.New("error"))

		request := createDeleteRequest()

		// Act
		err := m.mySQL.Delete(context.Background(), &request)

		// Assert
		require.Error(t, err)
		assert.Equal(t, "error", err.Error())
	})

	t.Run("etag mismatch", func(t *testing.T) {
		m.mock1.ExpectExec("DELETE FROM").WillReturnResult(sqlmock.NewResult(0, 0))

		eTag := "946af563"
		request := createDeleteRequest()
		request.ETag = &eTag

		// Act
		err := m.mySQL.Delete(context.Background(), &request)

		// Assert
		require.Error(t, err)
		assert.IsType(t, &state.ETagError{}, err)
		assert.Equal(t, state.ETagMismatch, err.(*state.ETagError).Kind())
	})
}

func TestGetHandlesNoRows(t *testing.T) {
	// Arrange
	m, _ := mockDatabase(t)
	defer m.mySQL.Close()

	m.mock1.ExpectQuery("SELECT id").WillReturnRows(sqlmock.NewRows([]string{"UnitTest", "value", "eTag"}))

	request := &state.GetRequest{
		Key: "UnitTest",
	}

	// Act
	response, err := m.mySQL.Get(context.Background(), request)

	// Assert
	require.NoError(t, err, "returned error")
	assert.NotNil(t, response, "did not return empty response")
}

func TestGetHandlesNoKey(t *testing.T) {
	// Arrange
	m, _ := mockDatabase(t)
	defer m.mySQL.Close()

	request := &state.GetRequest{
		Key: "",
	}

	// Act
	response, err := m.mySQL.Get(context.Background(), request)

	// Assert
	require.Error(t, err, "returned error")
	assert.Equal(t, "missing key in get operation", err.Error(), "wrong error returned")
	assert.Nil(t, response, "returned response")
}

func TestGetHandlesGenericError(t *testing.T) {
	// Arrange
	m, _ := mockDatabase(t)
	defer m.mySQL.Close()

	m.mock1.ExpectQuery("").WillReturnError(errors.New("generic error"))

	request := &state.GetRequest{
		Key: "UnitTest",
	}

	// Act
	response, err := m.mySQL.Get(context.Background(), request)

	// Assert
	require.Error(t, err)
	assert.Nil(t, response)
}

func TestGetSucceeds(t *testing.T) {
	// Arrange
	m, _ := mockDatabase(t)
	defer m.mySQL.Close()

	t.Run("has json type", func(t *testing.T) {
		rows := sqlmock.NewRows([]string{"id", "value", "eTag", "isbinary", "expiredate"}).AddRow("UnitTest", "{}", "946af56e", false, "")
		m.mock1.ExpectQuery(`SELECT id, value, eTag, isbinary, IFNULL\(expiredate, ""\) FROM state WHERE id = ?`).WillReturnRows(rows)

		request := &state.GetRequest{
			Key: "UnitTest",
		}

		// Act
		response, err := m.mySQL.Get(context.Background(), request)

		// Assert
		require.NoError(t, err)
		assert.NotNil(t, response)
		assert.Equal(t, "{}", string(response.Data))
		assert.NotContains(t, response.Metadata, state.GetRespMetaKeyTTLExpireTime)
	})

	t.Run("has binary type and expiredate", func(t *testing.T) {
		now := time.UnixMilli(20001).UTC()

		value, _ := utils.Marshal(base64.StdEncoding.EncodeToString([]byte("abcdefg")), json.Marshal)
		rows := sqlmock.NewRows([]string{"id", "value", "eTag", "isbinary", "expiredate"}).AddRow("UnitTest", value, "946af56e", true, now.Format(time.DateTime))
		m.mock1.ExpectQuery(`SELECT id, value, eTag, isbinary, IFNULL\(expiredate, ""\) FROM state WHERE id = ?`).WillReturnRows(rows)

		request := &state.GetRequest{
			Key: "UnitTest",
		}

		// Act
		response, err := m.mySQL.Get(context.Background(), request)

		// Assert
		require.NoError(t, err)
		assert.NotNil(t, response)
		assert.Equal(t, "abcdefg", string(response.Data))
		assert.Contains(t, response.Metadata, state.GetRespMetaKeyTTLExpireTime)
		assert.Equal(t, "1970-01-01T00:00:20Z", response.Metadata[state.GetRespMetaKeyTTLExpireTime])
	})
}

// Verifies that the correct query is executed to test if the table
// already exists in the database or not.
func TestTableExists(t *testing.T) {
	// Arrange
	m, _ := mockDatabase(t)
	defer m.mySQL.Close()

	// Return a result that indicates that the table already exists in the
	// database.
	rows := sqlmock.NewRows([]string{"exists"}).AddRow(1)
	m.mock1.ExpectQuery("SELECT EXISTS").WillReturnRows(rows)

	// Act
	actual, err := tableExists(context.Background(), m.mySQL.db, "dapr_state_store", "store", 10*time.Second)

	// Assert
	require.NoError(t, err, `error was returned`)
	assert.True(t, actual, `table does not exists`)
}

// Verifies that the code returns an error if the create table command fails.
func TestEnsureStateTableHandlesCreateTableError(t *testing.T) {
	// Arrange
	m, _ := mockDatabase(t)
	defer m.mySQL.Close()

	rows := sqlmock.NewRows([]string{"exists"}).AddRow(0)
	m.mock1.ExpectQuery("SELECT EXISTS").WillReturnRows(rows)
	m.mock1.ExpectExec("CREATE TABLE").WillReturnError(errors.New("CreateTableError"))

	// Act
	err := m.mySQL.ensureStateTable(context.Background(), "dapr_state_store", "state")

	// Assert
	require.Error(t, err, "no error returned")
	assert.Equal(t, "CreateTableError", err.Error(), "wrong error returned")
}

// Verifies that ensureStateTable creates the table when tableExists returns
// false.
func TestEnsureStateTableCreatesTable(t *testing.T) {
	// Arrange
	m, _ := mockDatabase(t)
	defer m.mySQL.Close()

	// Return exists = 0 when Select Exists is called to indicate the table
	// does not already exist.
	rows := sqlmock.NewRows([]string{"exists"}).AddRow(0)
	m.mock1.ExpectQuery("SELECT EXISTS").WillReturnRows(rows)
	m.mock1.ExpectExec("CREATE TABLE").WillReturnResult(sqlmock.NewResult(1, 1))
	rows = sqlmock.NewRows([]string{"exists"}).AddRow(1)
	m.mock1.ExpectQuery("SELECT count(/*)").WillReturnRows(rows)
	m.mock1.ExpectExec("CREATE PROCEDURE").WillReturnResult(sqlmock.NewResult(1, 1))

	// Act
	err := m.mySQL.ensureStateTable(context.Background(), "dapr_state_store", "state")

	// Assert
	require.NoError(t, err)
}

// Verify that the call to MySQL init get passed through
// to the DbAccess instance.
func TestInitReturnsErrorOnNoConnectionString(t *testing.T) {
	// Arrange
	t.Parallel()
	m, _ := mockDatabase(t)
	metadata := &state.Metadata{
		Base: metadata.Base{Properties: map[string]string{keyConnectionString: ""}},
	}

	// Act
	err := m.mySQL.Init(context.Background(), *metadata)

	// Assert
	require.Error(t, err)
	assert.Equal(t, defaultTableName, m.mySQL.tableName, "table name did not default")
}

func TestInitReturnsErrorOnFailOpen(t *testing.T) {
	// Arrange
	t.Parallel()
	m, _ := mockDatabase(t)
	metadata := &state.Metadata{
		Base: metadata.Base{Properties: map[string]string{keyConnectionString: fakeConnectionString}},
	}
	m.mock1.ExpectQuery("SELECT EXISTS").WillReturnError(sql.ErrConnDone)

	// Act
	err := m.mySQL.Init(context.Background(), *metadata)

	// Assert
	require.Error(t, err)
}

func TestInitHandlesRegisterTLSConfigError(t *testing.T) {
	// Arrange
	t.Parallel()
	m, _ := mockDatabase(t)
	m.factory.registerErr = errors.New("registerTLSConfigError")

	metadata := &state.Metadata{
		Base: metadata.Base{
			Properties: map[string]string{
				keyPemPath:          "./ssl.pem",
				keyTableName:        "stateStore",
				keyConnectionString: fakeConnectionString,
			},
		},
	}

	// Act
	err := m.mySQL.Init(context.Background(), *metadata)

	// Assert
	require.Error(t, err)
	assert.Equal(t, "registerTLSConfigError", err.Error(), "wrong error")
}

func TestInitSetsTableName(t *testing.T) {
	// Arrange
	t.Parallel()
	m, _ := mockDatabase(t)
	metadata := &state.Metadata{
		Base: metadata.Base{Properties: map[string]string{keyConnectionString: "", keyTableName: "stateStore"}},
	}

	// Act
	err := m.mySQL.Init(context.Background(), *metadata)

	// Assert
	require.Error(t, err)
	assert.Equal(t, "stateStore", m.mySQL.tableName, "table name did not default")
}

func TestInitInvalidTableName(t *testing.T) {
	// Arrange
	t.Parallel()
	m, _ := mockDatabase(t)
	metadata := &state.Metadata{
		Base: metadata.Base{Properties: map[string]string{keyConnectionString: "", keyTableName: "🙃"}},
	}

	// Act
	err := m.mySQL.Init(context.Background(), *metadata)

	// Assert
	require.ErrorContains(t, err, "table name '🙃' is not valid")
}

func TestInitSetsSchemaName(t *testing.T) {
	// Arrange
	t.Parallel()
	m, _ := mockDatabase(t)
	metadata := &state.Metadata{
		Base: metadata.Base{Properties: map[string]string{keyConnectionString: "", keySchemaName: "stateStoreSchema"}},
	}

	// Act
	err := m.mySQL.Init(context.Background(), *metadata)

	// Assert
	require.Error(t, err)
	assert.Equal(t, "stateStoreSchema", m.mySQL.schemaName, "table name did not default")
}

func TestInitInvalidSchemaName(t *testing.T) {
	// Arrange
	t.Parallel()
	m, _ := mockDatabase(t)
	metadata := &state.Metadata{
		Base: metadata.Base{Properties: map[string]string{keyConnectionString: "", keySchemaName: "?"}},
	}

	// Act
	err := m.mySQL.Init(context.Background(), *metadata)

	// Assert
	require.ErrorContains(t, err, "schema name '?' is not valid")
}

func TestMultiWithNoRequestsDoesNothing(t *testing.T) {
	// Arrange
	t.Parallel()
	m, _ := mockDatabase(t)
	var ops []state.TransactionalStateOperation

	// no operations expected
	m.mock1.ExpectBegin()
	m.mock1.ExpectCommit()

	// Act
	err := m.mySQL.Multi(context.Background(), &state.TransactionalStateRequest{
		Operations: ops,
	})

	// Assert
	require.NoError(t, err)
}

func TestClosingMySQLWithNilDba(t *testing.T) {
	// Arrange
	t.Parallel()
	m, _ := mockDatabase(t)
	m.mySQL.Close()

	m.mySQL.db = nil

	// Act
	err := m.mySQL.Close()

	// Assert
	require.NoError(t, err)
}

func TestValidSetRequest(t *testing.T) {
	// Arrange
	t.Parallel()
	m, _ := mockDatabase(t)

	t.Run("single op", func(t *testing.T) {
		ops := []state.TransactionalStateOperation{
			createSetRequest(),
		}

		m.mock1.ExpectExec("REPLACE INTO").WillReturnResult(sqlmock.NewResult(0, 1))

		// Act
		err := m.mySQL.Multi(context.Background(), &state.TransactionalStateRequest{
			Operations: ops,
		})

		// Assert
		require.NoError(t, err)
	})

	t.Run("multiple ops", func(t *testing.T) {
		ops := []state.TransactionalStateOperation{
			createSetRequest(),
			createSetRequest(),
		}

		m.mock1.ExpectBegin()
		m.mock1.ExpectExec("REPLACE INTO").WillReturnResult(sqlmock.NewResult(0, 1))
		m.mock1.ExpectExec("REPLACE INTO").WillReturnResult(sqlmock.NewResult(0, 1))
		m.mock1.ExpectCommit()

		// Act
		err := m.mySQL.Multi(context.Background(), &state.TransactionalStateRequest{
			Operations: ops,
		})

		// Assert
		require.NoError(t, err)
	})
}

func TestInvalidMultiSetRequestNoKey(t *testing.T) {
	// Arrange
	t.Parallel()
	m, _ := mockDatabase(t)
	ops := []state.TransactionalStateOperation{
		state.SetRequest{
			// empty key is not valid for Upsert operation
			Key:   "",
			Value: "value1",
		},
	}

	// Act
	err := m.mySQL.Multi(context.Background(), &state.TransactionalStateRequest{
		Operations: ops,
	})

	// Assert
	require.Error(t, err)
}

func TestValidMultiDeleteRequest(t *testing.T) {
	// Arrange
	t.Parallel()
	m, _ := mockDatabase(t)

	t.Run("single op", func(t *testing.T) {
		ops := []state.TransactionalStateOperation{
			createDeleteRequest(),
		}

		m.mock1.ExpectExec("DELETE FROM").WillReturnResult(sqlmock.NewResult(0, 1))

		// Act
		err := m.mySQL.Multi(context.Background(), &state.TransactionalStateRequest{
			Operations: ops,
		})

		// Assert
		require.NoError(t, err)
	})

	t.Run("multiple ops", func(t *testing.T) {
		ops := []state.TransactionalStateOperation{
			createDeleteRequest(),
			createDeleteRequest(),
		}

		m.mock1.ExpectBegin()
		m.mock1.ExpectExec("DELETE FROM").WillReturnResult(sqlmock.NewResult(0, 1))
		m.mock1.ExpectExec("DELETE FROM").WillReturnResult(sqlmock.NewResult(0, 1))
		m.mock1.ExpectCommit()

		// Act
		err := m.mySQL.Multi(context.Background(), &state.TransactionalStateRequest{
			Operations: ops,
		})

		// Assert
		require.NoError(t, err)
	})
}

func TestInvalidMultiDeleteRequestNoKey(t *testing.T) {
	// Arrange
	t.Parallel()
	m, _ := mockDatabase(t)
	ops := []state.TransactionalStateOperation{
		state.DeleteRequest{
			// empty key is not valid for Delete operation
			Key: "",
		},
	}

	// Act
	err := m.mySQL.Multi(context.Background(), &state.TransactionalStateRequest{
		Operations: ops,
	})

	// Assert
	require.Error(t, err)
}

func TestMultiOperationOrder(t *testing.T) {
	// Arrange
	t.Parallel()
	m, _ := mockDatabase(t)

	// In a transaction with multiple operations,
	// the order of operations must be respected.
	ops := []state.TransactionalStateOperation{
		state.SetRequest{Key: "k1", Value: "v1"},
		state.DeleteRequest{Key: "k1"},
		state.SetRequest{Key: "k2", Value: "v2"},
	}

	// expected to run the operations in sequence
	m.mock1.ExpectBegin()
	m.mock1.ExpectExec("REPLACE INTO").WillReturnResult(sqlmock.NewResult(0, 1))
	m.mock1.ExpectExec("DELETE FROM").WithArgs("k1").WillReturnResult(sqlmock.NewResult(0, 1))
	m.mock1.ExpectExec("REPLACE INTO").WillReturnResult(sqlmock.NewResult(0, 1))
	m.mock1.ExpectCommit()

	// Act
	err := m.mySQL.Multi(context.Background(), &state.TransactionalStateRequest{
		Operations: ops,
	})

	// Assert
	require.NoError(t, err)

	err = m.mock1.ExpectationsWereMet()
	require.NoError(t, err)
}

func createSetRequest() state.SetRequest {
	return state.SetRequest{
		Key:   randomKey(),
		Value: randomJSON(),
	}
}

func createDeleteRequest() state.DeleteRequest {
	return state.DeleteRequest{
		Key: randomKey(),
	}
}

type mocks struct {
	mySQL   *MySQL
	db      *sql.DB
	mock1   sqlmock.Sqlmock
	mock2   sqlmock.Sqlmock
	factory *fakeMySQLFactory
}

// Returns a MySQL and an extra sql.DB for test that have to close the first
// db returned in the MySQL instance.
func mockDatabase(t *testing.T) (*mocks, error) {
	db1, mock1, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}

	db2, mock2, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}

	fake := newFakeMySQLFactory(db1, db2, nil, nil)
	logger := logger.NewLogger("test")
	mys := newMySQLStateStore(logger, fake)
	mys.db = db1
	mys.tableName = "state"
	mys.connectionString = "theUser:thePassword@/theDBName"

	return &mocks{
		db:      db2,
		mySQL:   mys,
		mock1:   mock1,
		mock2:   mock2,
		factory: fake,
	}, err
}

// Fake for unit testings the part that uses the factory
// to open the database and register the pem file.
type fakeMySQLFactory struct {
	openCount   int
	openErr     error
	registerErr error
	db1         *sql.DB
	db2         *sql.DB
}

// startCount is used to set openCount. openCount is used to determine
// which fake to open. In the normal flow of code this would go from 1
// to 2. However, in some tests they assume the factory open has already
// been called so for those test set startCount to 1.
func newFakeMySQLFactory(db1 *sql.DB, db2 *sql.DB, registerErr, openErr error) *fakeMySQLFactory {
	return &fakeMySQLFactory{
		db1:         db1,
		db2:         db2,
		openErr:     openErr,
		registerErr: registerErr,
	}
}

func (f *fakeMySQLFactory) Open(connectionString string) (*sql.DB, error) {
	f.openCount++

	if f.openCount == 1 {
		return f.db1, f.openErr
	}

	return f.db2, f.openErr
}

func (f *fakeMySQLFactory) RegisterTLSConfig(pemPath string) error {
	return f.registerErr
}

func TestValidIdentifier(t *testing.T) {
	tests := []struct {
		name string
		arg  string
		want bool
	}{
		{name: "empty string", arg: "", want: false},
		{name: "valid characters only", arg: "acz_039_AZS", want: true},
		{name: "invalid ASCII characters 1", arg: "$", want: false},
		{name: "invalid ASCII characters 2", arg: "*", want: false},
		{name: "invalid ASCII characters 3", arg: "hello world", want: false},
		{name: "non-ASCII characters", arg: "🙃", want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := validIdentifier(tt.arg); got != tt.want {
				t.Errorf("validIdentifier() = %v, want %v", got, tt.want)
			}
		})
	}
}
