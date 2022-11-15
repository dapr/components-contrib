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
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"

	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/state"
	"github.com/dapr/components-contrib/state/utils"
	"github.com/dapr/kit/logger"
)

const (
	fakeConnectionString = "not a real connection"
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
	m.mySQL.ensureStateSchema()

	// Assert
	assert.Equal(t, "theUser:thePassword@/theSchema", m.mySQL.connectionString)
}

func TestFinishInitHandlesSchemaExistsError(t *testing.T) {
	// Arrange
	m, _ := mockDatabase(t)
	defer m.mySQL.Close()

	expectedErr := fmt.Errorf("existsError")
	m.mock1.ExpectQuery("SELECT EXISTS").WillReturnError(expectedErr)

	// Act
	actualErr := m.mySQL.finishInit(m.mySQL.db)

	// Assert
	assert.NotNil(t, actualErr, "now error returned")
	assert.Equal(t, "existsError", actualErr.Error(), "wrong error")
}

func TestFinishInitHandlesDatabaseCreateError(t *testing.T) {
	// Arrange
	m, _ := mockDatabase(t)
	defer m.mySQL.Close()

	rows := sqlmock.NewRows([]string{"exists"}).AddRow(0)
	m.mock1.ExpectQuery("SELECT EXISTS").WillReturnRows(rows)

	expectedErr := fmt.Errorf("createDatabaseError")
	m.mock1.ExpectExec("CREATE DATABASE").WillReturnError(expectedErr)

	// Act
	actualErr := m.mySQL.finishInit(m.mySQL.db)

	// Assert
	assert.NotNil(t, actualErr, "now error returned")
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

	expectedErr := fmt.Errorf("pingError")
	m.mock2.ExpectPing().WillReturnError(expectedErr)

	// Act
	actualErr := m.mySQL.finishInit(m.mySQL.db)

	// Assert
	assert.NotNil(t, actualErr, "now error returned")
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
	m.mock2.ExpectQuery("SELECT EXISTS").WillReturnError(fmt.Errorf("tableExistsError"))

	// Act
	err := m.mySQL.finishInit(m.mySQL.db)

	// Assert
	assert.NotNil(t, err, "no error returned")
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
	assert.Nil(t, err, "error returned")
}

func TestExecuteMultiCannotBeginTransaction(t *testing.T) {
	// Arrange
	m, _ := mockDatabase(t)
	defer m.mySQL.Close()

	m.mock1.ExpectBegin().WillReturnError(fmt.Errorf("beginError"))

	// Act
	err := m.mySQL.Multi(nil)

	// Assert
	assert.NotNil(t, err, "no error returned")
	assert.Equal(t, "beginError", err.Error(), "wrong error returned")
}

func TestMySQLBulkDeleteRollbackDeletes(t *testing.T) {
	// Arrange
	m, _ := mockDatabase(t)
	defer m.mySQL.Close()

	m.mock1.ExpectBegin()
	m.mock1.ExpectExec("DELETE FROM").WillReturnError(fmt.Errorf("deleteError"))
	m.mock1.ExpectRollback()

	deletes := []state.DeleteRequest{createDeleteRequest()}

	// Act
	err := m.mySQL.BulkDelete(deletes)

	// Assert
	assert.NotNil(t, err, "no error returned")
	assert.Equal(t, "deleteError", err.Error(), "wrong error returned")
}

func TestMySQLBulkSetRollbackSets(t *testing.T) {
	// Arrange
	m, _ := mockDatabase(t)
	defer m.mySQL.Close()

	m.mock1.ExpectBegin()
	m.mock1.ExpectExec("INSERT INTO").WillReturnError(fmt.Errorf("setError"))
	m.mock1.ExpectRollback()

	sets := []state.SetRequest{createSetRequest()}

	// Act
	err := m.mySQL.BulkSet(sets)

	// Assert
	assert.NotNil(t, err, "no error returned")
	assert.Equal(t, "setError", err.Error(), "wrong error returned")
}

func TestExecuteMultiCommitSetsAndDeletes(t *testing.T) {
	// Arrange
	m, _ := mockDatabase(t)
	defer m.mySQL.Close()

	m.mock1.ExpectBegin()
	m.mock1.ExpectExec("INSERT INTO").WillReturnResult(sqlmock.NewResult(0, 1))
	m.mock1.ExpectExec("DELETE FROM").WillReturnResult(sqlmock.NewResult(0, 1))
	m.mock1.ExpectCommit()

	setOperation := state.TransactionalStateOperation{
		Request:   createSetRequest(),
		Operation: state.Upsert,
	}

	deleteOperation := state.TransactionalStateOperation{
		Request:   createDeleteRequest(),
		Operation: state.Delete,
	}

	request := state.TransactionalStateRequest{
		Operations: []state.TransactionalStateOperation{setOperation, deleteOperation},
		Metadata:   map[string]string{},
	}

	// Act
	err := m.mySQL.Multi(&request)

	// Assert
	assert.Nil(t, err, "error returned")
}

func TestSetHandlesOptionsError(t *testing.T) {
	// Arrange
	m, _ := mockDatabase(t)
	defer m.mySQL.Close()

	request := createSetRequest()

	request.Options.Consistency = "Invalid"

	// Act
	err := m.mySQL.Set(&request)

	// Assert
	assert.NotNil(t, err)
}

func TestSetHandlesNoKey(t *testing.T) {
	// Arrange
	m, _ := mockDatabase(t)
	defer m.mySQL.Close()

	request := createSetRequest()
	request.Key = ""

	// Act
	err := m.mySQL.Set(&request)

	// Assert
	assert.NotNil(t, err)
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
	err := m.mySQL.Set(&request)

	// Assert
	assert.Nil(t, err)
}

func TestSetHandlesErr(t *testing.T) {
	// Arrange
	m, _ := mockDatabase(t)
	defer m.mySQL.Close()

	t.Run("error occurs when update with tag", func(t *testing.T) {
		m.mock1.ExpectExec("UPDATE state").WillReturnError(errors.New("error"))

		eTag := "946af561"
		request := createSetRequest()
		request.ETag = &eTag

		// Act
		err := m.mySQL.Set(&request)

		// Assert
		assert.NotNil(t, err)
		assert.IsType(t, &state.ETagError{}, err)
		assert.Equal(t, err.(*state.ETagError).Kind(), state.ETagMismatch)
	})

	t.Run("error occurs when insert", func(t *testing.T) {
		m.mock1.ExpectExec("INSERT INTO state").WillReturnError(errors.New("error"))
		request := createSetRequest()

		// Act
		err := m.mySQL.Set(&request)

		// Assert
		assert.NotNil(t, err)
		assert.Equal(t, "error", err.Error())
	})

	t.Run("insert on conflict", func(t *testing.T) {
		m.mock1.ExpectExec("INSERT INTO state").WillReturnResult(sqlmock.NewResult(1, 2))
		request := createSetRequest()

		// Act
		err := m.mySQL.Set(&request)

		// Assert
		assert.Nil(t, err)
	})

	t.Run("too many rows error", func(t *testing.T) {
		m.mock1.ExpectExec("INSERT INTO state").WillReturnResult(sqlmock.NewResult(1, 3))
		request := createSetRequest()

		// Act
		err := m.mySQL.Set(&request)

		// Assert
		assert.NotNil(t, err)
	})

	t.Run("no rows effected error", func(t *testing.T) {
		m.mock1.ExpectExec("UPDATE state").WillReturnResult(sqlmock.NewResult(1, 0))

		eTag := "illegal etag"
		request := createSetRequest()
		request.ETag = &eTag

		// Act
		err := m.mySQL.Set(&request)

		// Assert
		assert.NotNil(t, err)
		assert.IsType(t, &state.ETagError{}, err)
		assert.Equal(t, err.(*state.ETagError).Kind(), state.ETagMismatch)
	})
}

// Verifies that MySQL passes through to myDBAccess.
func TestMySQLDeleteHandlesNoKey(t *testing.T) {
	// Arrange
	m, _ := mockDatabase(t)
	request := createDeleteRequest()
	request.Key = ""

	// Act
	err := m.mySQL.Delete(&request)

	// Asset
	assert.NotNil(t, err)
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
	err := m.mySQL.Delete(&request)

	// Assert
	assert.Nil(t, err)
}

func TestDeleteWithErr(t *testing.T) {
	// Arrange
	m, _ := mockDatabase(t)
	defer m.mySQL.Close()

	t.Run("error occurs when delete", func(t *testing.T) {
		m.mock1.ExpectExec("DELETE FROM").WillReturnError(errors.New("error"))

		request := createDeleteRequest()

		// Act
		err := m.mySQL.Delete(&request)

		// Assert
		assert.NotNil(t, err)
		assert.Equal(t, "error", err.Error())
	})

	t.Run("etag mismatch", func(t *testing.T) {
		m.mock1.ExpectExec("DELETE FROM").WillReturnResult(sqlmock.NewResult(0, 0))

		eTag := "946af563"
		request := createDeleteRequest()
		request.ETag = &eTag

		// Act
		err := m.mySQL.Delete(&request)

		// Assert
		assert.NotNil(t, err)
		assert.IsType(t, &state.ETagError{}, err)
		assert.Equal(t, err.(*state.ETagError).Kind(), state.ETagMismatch)
	})
}

func TestGetHandlesNoRows(t *testing.T) {
	// Arrange
	m, _ := mockDatabase(t)
	defer m.mySQL.Close()

	m.mock1.ExpectQuery("SELECT value").WillReturnRows(sqlmock.NewRows([]string{"value", "eTag"}))

	request := &state.GetRequest{
		Key: "UnitTest",
	}

	// Act
	response, err := m.mySQL.Get(request)

	// Assert
	assert.Nil(t, err, "returned error")
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
	response, err := m.mySQL.Get(request)

	// Assert
	assert.NotNil(t, err, "returned error")
	assert.Equal(t, "missing key in get operation", err.Error(), "wrong error returned")
	assert.Nil(t, response, "returned response")
}

func TestGetHandlesGenericError(t *testing.T) {
	// Arrange
	m, _ := mockDatabase(t)
	defer m.mySQL.Close()

	m.mock1.ExpectQuery("").WillReturnError(fmt.Errorf("generic error"))

	request := &state.GetRequest{
		Key: "UnitTest",
	}

	// Act
	response, err := m.mySQL.Get(request)

	// Assert
	assert.NotNil(t, err)
	assert.Nil(t, response)
}

func TestGetSucceeds(t *testing.T) {
	// Arrange
	m, _ := mockDatabase(t)
	defer m.mySQL.Close()

	t.Run("has json type", func(t *testing.T) {
		rows := sqlmock.NewRows([]string{"value", "eTag", "isbinary"}).AddRow("{}", "946af56e", false)
		m.mock1.ExpectQuery("SELECT value, eTag, isbinary FROM state WHERE id = ?").WillReturnRows(rows)

		request := &state.GetRequest{
			Key: "UnitTest",
		}

		// Act
		response, err := m.mySQL.Get(request)

		// Assert
		assert.Nil(t, err)
		assert.NotNil(t, response)
		assert.Equal(t, "{}", string(response.Data))
	})

	t.Run("has binary type", func(t *testing.T) {
		value, _ := utils.Marshal(base64.StdEncoding.EncodeToString([]byte("abcdefg")), json.Marshal)
		rows := sqlmock.NewRows([]string{"value", "eTag", "isbinary"}).AddRow(value, "946af56e", true)
		m.mock1.ExpectQuery("SELECT value, eTag, isbinary FROM state WHERE id = ?").WillReturnRows(rows)

		request := &state.GetRequest{
			Key: "UnitTest",
		}

		// Act
		response, err := m.mySQL.Get(request)

		// Assert
		assert.Nil(t, err)
		assert.NotNil(t, response)
		assert.Equal(t, "abcdefg", string(response.Data))
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
	actual, err := tableExists(m.mySQL.db, "store", 10*time.Second)

	// Assert
	assert.Nil(t, err, `error was returned`)
	assert.True(t, actual, `table does not exists`)
}

// Verifies that the code returns an error if the create table command fails.
func TestEnsureStateTableHandlesCreateTableError(t *testing.T) {
	// Arrange
	m, _ := mockDatabase(t)
	defer m.mySQL.Close()

	rows := sqlmock.NewRows([]string{"exists"}).AddRow(0)
	m.mock1.ExpectQuery("SELECT EXISTS").WillReturnRows(rows)
	m.mock1.ExpectExec("CREATE TABLE").WillReturnError(fmt.Errorf("CreateTableError"))

	// Act
	err := m.mySQL.ensureStateTable("state")

	// Assert
	assert.NotNil(t, err, "no error returned")
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

	// Act
	err := m.mySQL.ensureStateTable("state")

	// Assert
	assert.Nil(t, err)
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
	err := m.mySQL.Init(*metadata)

	// Assert
	assert.NotNil(t, err)
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
	err := m.mySQL.Init(*metadata)

	// Assert
	assert.NotNil(t, err)
}

func TestInitHandlesRegisterTLSConfigError(t *testing.T) {
	// Arrange
	t.Parallel()
	m, _ := mockDatabase(t)
	m.factory.registerErr = fmt.Errorf("registerTLSConfigError")

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
	err := m.mySQL.Init(*metadata)

	// Assert
	assert.NotNil(t, err)
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
	err := m.mySQL.Init(*metadata)

	// Assert
	assert.NotNil(t, err)
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
	err := m.mySQL.Init(*metadata)

	// Assert
	assert.ErrorContains(t, err, "table name '🙃' is not valid")
}

func TestInitSetsSchemaName(t *testing.T) {
	// Arrange
	t.Parallel()
	m, _ := mockDatabase(t)
	metadata := &state.Metadata{
		Base: metadata.Base{Properties: map[string]string{keyConnectionString: "", keySchemaName: "stateStoreSchema"}},
	}

	// Act
	err := m.mySQL.Init(*metadata)

	// Assert
	assert.NotNil(t, err)
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
	err := m.mySQL.Init(*metadata)

	// Assert
	assert.ErrorContains(t, err, "schema name '?' is not valid")
}

// This state store does not support BulkGet so it must return false and
// nil nil.
func TestBulkGetReturnsNil(t *testing.T) {
	// Arrange
	t.Parallel()
	m, _ := mockDatabase(t)

	// Act
	supported, response, err := m.mySQL.BulkGet(nil)

	// Assert
	assert.Nil(t, err, `returned err`)
	assert.Nil(t, response, `returned response`)
	assert.False(t, supported, `returned supported`)
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
	err := m.mySQL.Multi(&state.TransactionalStateRequest{
		Operations: ops,
	})

	// Assert
	assert.Nil(t, err)
}

func TestInvalidMultiAction(t *testing.T) {
	// Arrange
	t.Parallel()
	m, _ := mockDatabase(t)
	var ops []state.TransactionalStateOperation

	ops = append(ops, state.TransactionalStateOperation{
		Operation: "Something invalid",
		Request:   createSetRequest(),
	})

	// Act
	err := m.mySQL.Multi(&state.TransactionalStateRequest{
		Operations: ops,
	})

	// Assert
	assert.NotNil(t, err)
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
	assert.Nil(t, err)
}

func TestValidSetRequest(t *testing.T) {
	// Arrange
	t.Parallel()
	m, _ := mockDatabase(t)
	var ops []state.TransactionalStateOperation

	ops = append(ops, state.TransactionalStateOperation{
		Operation: state.Upsert,
		Request:   createSetRequest(),
	})

	m.mock1.ExpectBegin()
	m.mock1.ExpectExec("INSERT INTO").WillReturnResult(sqlmock.NewResult(0, 1))
	m.mock1.ExpectCommit()

	// Act
	err := m.mySQL.Multi(&state.TransactionalStateRequest{
		Operations: ops,
	})

	// Assert
	assert.Nil(t, err)
}

func TestInvalidMultiSetRequest(t *testing.T) {
	// Arrange
	t.Parallel()
	m, _ := mockDatabase(t)
	var ops []state.TransactionalStateOperation

	ops = append(ops, state.TransactionalStateOperation{
		Operation: state.Upsert,
		// Delete request is not valid for Upsert operation
		Request: createDeleteRequest(),
	})

	// Act
	err := m.mySQL.Multi(&state.TransactionalStateRequest{
		Operations: ops,
	})

	// Assert
	assert.NotNil(t, err)
}

func TestInvalidMultiSetRequestNoKey(t *testing.T) {
	// Arrange
	t.Parallel()
	m, _ := mockDatabase(t)
	var ops []state.TransactionalStateOperation

	ops = append(ops, state.TransactionalStateOperation{
		Operation: state.Upsert,
		Request: state.SetRequest{
			// empty key is not valid for Upsert operation
			Key:   "",
			Value: "value1",
		},
	})

	// Act
	err := m.mySQL.Multi(&state.TransactionalStateRequest{
		Operations: ops,
	})

	// Assert
	assert.NotNil(t, err)
}

func TestValidMultiDeleteRequest(t *testing.T) {
	// Arrange
	t.Parallel()
	m, _ := mockDatabase(t)
	var ops []state.TransactionalStateOperation

	m.mock1.ExpectBegin()
	m.mock1.ExpectExec("DELETE FROM").WillReturnResult(sqlmock.NewResult(0, 1))
	m.mock1.ExpectCommit()

	ops = append(ops, state.TransactionalStateOperation{
		Operation: state.Delete,
		Request:   createDeleteRequest(),
	})

	// Act
	err := m.mySQL.Multi(&state.TransactionalStateRequest{
		Operations: ops,
	})

	// Assert
	assert.Nil(t, err)
}

func TestInvalidMultiDeleteRequest(t *testing.T) {
	// Arrange
	t.Parallel()
	m, _ := mockDatabase(t)
	var ops []state.TransactionalStateOperation

	ops = append(ops, state.TransactionalStateOperation{
		Operation: state.Delete,
		// Set request is not valid for Delete operation
		Request: createSetRequest(),
	})

	// Act
	err := m.mySQL.Multi(&state.TransactionalStateRequest{
		Operations: ops,
	})

	// Assert
	assert.NotNil(t, err)
}

func TestInvalidMultiDeleteRequestNoKey(t *testing.T) {
	// Arrange
	t.Parallel()
	m, _ := mockDatabase(t)
	var ops []state.TransactionalStateOperation

	ops = append(ops, state.TransactionalStateOperation{
		Operation: state.Delete,
		Request: state.DeleteRequest{
			// empty key is not valid for Delete operation
			Key: "",
		},
	})

	// Act
	err := m.mySQL.Multi(&state.TransactionalStateRequest{
		Operations: ops,
	})

	// Assert
	assert.NotNil(t, err)
}

func TestMultiOperationOrder(t *testing.T) {
	// Arrange
	t.Parallel()
	m, _ := mockDatabase(t)
	var ops []state.TransactionalStateOperation

	// In a transaction with multiple operations,
	// the order of operations must be respected.
	ops = append(ops,
		state.TransactionalStateOperation{
			Operation: state.Upsert,
			Request:   state.SetRequest{Key: "k1", Value: "v1"},
		},
		state.TransactionalStateOperation{
			Operation: state.Delete,
			Request:   state.DeleteRequest{Key: "k1"},
		},
		state.TransactionalStateOperation{
			Operation: state.Upsert,
			Request:   state.SetRequest{Key: "k2", Value: "v2"},
		},
	)

	// expected to run the operations in sequence
	m.mock1.ExpectBegin()
	m.mock1.ExpectExec("INSERT INTO").WillReturnResult(sqlmock.NewResult(0, 1))
	m.mock1.ExpectExec("DELETE FROM").WithArgs("k1").WillReturnResult(sqlmock.NewResult(0, 1))
	m.mock1.ExpectExec("INSERT INTO").WillReturnResult(sqlmock.NewResult(0, 1))
	m.mock1.ExpectCommit()

	// Act
	err := m.mySQL.Multi(&state.TransactionalStateRequest{
		Operations: ops,
	})

	// Assert
	assert.Nil(t, err)

	err = m.mock1.ExpectationsWereMet()
	assert.Nil(t, err)
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
