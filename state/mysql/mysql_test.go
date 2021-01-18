// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------
package mysql

import (
	"fmt"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/dapr/components-contrib/state"
	"github.com/dapr/dapr/pkg/logger"
	"github.com/stretchr/testify/assert"
)

const (
	fakeConnectionString = "not a real connection"
)

func TestFinishInitHandlesOpenError(t *testing.T) {
	// Arrange
	mys, _, _ := mockDatabase(t)
	defer mys.Close()

	// Act
	err := mys.finishInit(mys.db, fmt.Errorf("failed to open database"))

	// Assert
	assert.NotNil(t, err, "now error returned")
	assert.Equal(t, "failed to open database", err.Error(), "wrong error")
}

func TestFinishInitHandlesPingError(t *testing.T) {
	// Arrange
	mys, mock, _ := mockDatabase(t)
	defer mys.Close()

	err := fmt.Errorf("pingError")
	mock.ExpectPing().WillReturnError(err)

	// Act
	expectedErr := mys.finishInit(mys.db, nil)

	// Assert
	assert.NotNil(t, expectedErr, "now error returned")
	assert.Equal(t, "pingError", expectedErr.Error(), "wrong error")
}

// Verifies that finishInit can handle an error from its call to
// ensureStateTable. The code should not attempt to create the table. Because
// there is no mock.ExpectExec if the code attempts to execute the create
// table commnad this test will fail.
func TestFinishInitHandlesTableExistsError(t *testing.T) {
	// Arrange
	mys, mock, _ := mockDatabase(t)
	defer mys.Close()

	mock.ExpectPing()
	mock.ExpectQuery("SELECT EXISTS").WillReturnError(fmt.Errorf("tableExistsError"))

	// Act
	err := mys.finishInit(mys.db, nil)

	// Assert
	assert.NotNil(t, err, "no error returned")
	assert.Equal(t, "tableExistsError", err.Error(), "tableExists did not return err")
}

func TestClosingDatabaseTwiceReturnsNil(t *testing.T) {
	// Arrange
	mys, _, _ := mockDatabase(t)
	mys.Close()
	mys.db = nil

	// Act
	err := mys.Close()

	// Assert
	assert.Nil(t, err, "error returned")
}

func TestExecuteMultiCannotBeginTransaction(t *testing.T) {
	// Arrange
	mys, mock, _ := mockDatabase(t)
	defer mys.Close()

	mock.ExpectBegin().WillReturnError(fmt.Errorf("beginError"))

	// Act
	err := mys.executeMulti(nil, nil)

	// Assert
	assert.NotNil(t, err, "no error returned")
	assert.Equal(t, "beginError", err.Error(), "wrong error returned")
}

func TestMySQLBulkDeleteRollbackDeletes(t *testing.T) {
	// Arrange
	mys, mock, _ := mockDatabase(t)
	defer mys.Close()

	mock.ExpectBegin()
	mock.ExpectExec("DELETE FROM").WillReturnError(fmt.Errorf("deleteError"))
	mock.ExpectRollback()

	deletes := []state.DeleteRequest{createDeleteRequest()}

	// Act
	err := mys.BulkDelete(deletes)

	// Assert
	assert.NotNil(t, err, "no error returned")
	assert.Equal(t, "deleteError", err.Error(), "wrong error returned")
}

func TestMySQLBulkSetRollbackSets(t *testing.T) {
	// Arrange
	mys, mock, _ := mockDatabase(t)
	defer mys.Close()

	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO").WillReturnError(fmt.Errorf("setError"))
	mock.ExpectRollback()

	sets := []state.SetRequest{createSetRequest()}

	// Act
	err := mys.BulkSet(sets)

	// Assert
	assert.NotNil(t, err, "no error returned")
	assert.Equal(t, "setError", err.Error(), "wrong error returned")
}

func TestExecuteMultiCommitSetsAndDeletes(t *testing.T) {
	// Arrange
	mys, mock, _ := mockDatabase(t)
	defer mys.Close()

	mock.ExpectBegin()
	mock.ExpectExec("DELETE FROM").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	sets := []state.SetRequest{createSetRequest()}
	deletes := []state.DeleteRequest{createDeleteRequest()}

	// Act
	err := mys.executeMulti(sets, deletes)

	// Assert
	assert.Nil(t, err, "error returned")
}

func TestSetHandlesOptionsError(t *testing.T) {
	// Arrange
	mys, _, _ := mockDatabase(t)
	defer mys.Close()

	request := createSetRequest()

	request.Options.Consistency = "Invalid"

	// Act
	err := mys.setValue(&request)

	// Assert
	assert.NotNil(t, err)
}

func TestSetHandlesNoKey(t *testing.T) {
	// Arrange
	mys, _, _ := mockDatabase(t)
	defer mys.Close()

	request := createSetRequest()
	request.Key = ""

	// Act
	err := mys.Set(&request)

	// Assert
	assert.NotNil(t, err)
	assert.Equal(t, "missing key in set operation", err.Error(), "wrong error returned")
}

func TestSetHandlesUpdate(t *testing.T) {
	// Arrange
	mys, mock, _ := mockDatabase(t)
	defer mys.Close()

	mock.ExpectExec("UPDATE state").WillReturnResult(sqlmock.NewResult(1, 1))

	eTag := "946af56e"

	request := createSetRequest()
	request.ETag = &eTag

	// Act
	err := mys.setValue(&request)

	// Assert
	assert.Nil(t, err)
}

// Verifies that MySQL passes through to myDBAccess
func TestMySQLDeleteHandlesNoKey(t *testing.T) {
	// Arrange
	mys, _, _ := mockDatabase(t)
	request := createDeleteRequest()
	request.Key = ""

	// Act
	err := mys.Delete(&request)

	// Asset
	assert.NotNil(t, err)
	assert.Equal(t, "missing key in delete operation", err.Error(), "wrong error returned")
}

func TestDeleteWithETag(t *testing.T) {
	// Arrange
	mys, mock, _ := mockDatabase(t)
	defer mys.Close()

	mock.ExpectExec("DELETE FROM").WillReturnResult(sqlmock.NewResult(0, 1))

	eTag := "946af56e"

	request := createDeleteRequest()
	request.ETag = &eTag

	// Act
	err := mys.deleteValue(&request)

	// Assert
	assert.Nil(t, err)
}

func TestReturnNDBResultsRowsAffectedReturnsError(t *testing.T) {
	// Arrange
	mys, _, _ := mockDatabase(t)
	defer mys.Close()

	request := &fakeSQLRequest{
		rowsAffected: 3,
		lastInsertID: 0,
		err:          fmt.Errorf("RowAffectedError"),
	}

	// Act
	err := mys.returnNDBResults(request, nil, 2)

	// Assert
	assert.NotNil(t, err)
	assert.Equal(t, "RowAffectedError", err.Error())
}

func TestReturnNDBResultsNoRows(t *testing.T) {
	// Arrange
	mys, _, _ := mockDatabase(t)
	defer mys.Close()

	request := &fakeSQLRequest{
		rowsAffected: 0,
		lastInsertID: 0,
	}

	// Act
	err := mys.returnNDBResults(request, nil, 2)

	// Assert
	assert.NotNil(t, err)
	assert.Equal(t, "database operation failed: no rows match given key and eTag", err.Error())
}

func TestReturnNDBResultsTooManyRows(t *testing.T) {
	// Arrange
	mys, _, _ := mockDatabase(t)
	defer mys.Close()

	request := &fakeSQLRequest{
		rowsAffected: 3,
		lastInsertID: 0,
	}

	// Act
	err := mys.returnNDBResults(request, nil, 2)

	// Assert
	assert.NotNil(t, err)
	assert.Equal(t, "database operation failed: more than 2 row affected, expected 2, actual 3", err.Error())
}

func TestGetHandlesNoRows(t *testing.T) {
	// Arrange
	mys, mock, _ := mockDatabase(t)
	defer mys.Close()

	mock.ExpectQuery("SELECT value").WillReturnRows(sqlmock.NewRows([]string{"value", "eTag"}))

	request := &state.GetRequest{
		Key: "UnitTest",
	}

	// Act
	response, err := mys.Get(request)

	// Assert
	assert.Nil(t, err, "returned error")
	assert.NotNil(t, response, "did not return empty response")
}

func TestGetHandlesNoKey(t *testing.T) {
	// Arrange
	mys, _, _ := mockDatabase(t)
	defer mys.Close()

	request := &state.GetRequest{
		Key: "",
	}

	// Act
	response, err := mys.Get(request)

	// Assert
	assert.NotNil(t, err, "returned error")
	assert.Equal(t, "missing key in get operation", err.Error(), "wrong error returned")
	assert.Nil(t, response, "returned response")
}

func TestGetHandlesGenericError(t *testing.T) {
	// Arrange
	mys, mock, _ := mockDatabase(t)
	defer mys.Close()

	mock.ExpectQuery("").WillReturnError(fmt.Errorf("generic error"))

	request := &state.GetRequest{
		Key: "UnitTest",
	}

	// Act
	response, err := mys.Get(request)

	// Assert
	assert.NotNil(t, err)
	assert.Nil(t, response)
}

func TestGetSucceeds(t *testing.T) {
	// Arrange
	mys, mock, _ := mockDatabase(t)
	defer mys.Close()

	rows := sqlmock.NewRows([]string{"value", "eTag"}).AddRow("{}", "946af56e")
	mock.ExpectQuery("SELECT value, eTag FROM state WHERE id = ?").WillReturnRows(rows)

	request := &state.GetRequest{
		Key: "UnitTest",
	}

	// Act
	response, err := mys.Get(request)

	// Assert
	assert.Nil(t, err)
	assert.NotNil(t, response)
}

// Verifies that the correct query is executed to test if the table
// already exists in the database or not.
func TestTableExists(t *testing.T) {
	// Arrange
	mys, mock, _ := mockDatabase(t)
	defer mys.Close()

	// Return a result that indicates that the table already exists in the
	// database.
	rows := sqlmock.NewRows([]string{"exists"}).AddRow(1)
	mock.ExpectQuery("SELECT EXISTS").WillReturnRows(rows)

	// Act
	actual, err := tableExists(mys.db, "store")

	// Assert
	assert.Nil(t, err, `error was returned`)
	assert.True(t, actual, `table does not exists`)
}

// Verifies that the code returns an error if the create table command fails
func TestEnsureStateTableHandlesCreateTableError(t *testing.T) {
	// Arrange
	mys, mock, _ := mockDatabase(t)
	defer mys.Close()

	rows := sqlmock.NewRows([]string{"exists"}).AddRow(0)
	mock.ExpectQuery("SELECT EXISTS").WillReturnRows(rows)
	mock.ExpectExec("CREATE TABLE").WillReturnError(fmt.Errorf("CreateTableError"))

	// Act
	err := mys.ensureStateTable("state")

	// Assert
	assert.NotNil(t, err, "no error returned")
	assert.Equal(t, "CreateTableError", err.Error(), "wrong error returned")
}

// Verifies that ensureStateTable creates the table when tableExists returns
// false.
func TestEnsureStateTableCreatesTable(t *testing.T) {
	// Arrange
	mys, mock, _ := mockDatabase(t)
	defer mys.Close()

	// Return exists = 0 when Select Exists is called to indicate the table
	// does not already exist.
	rows := sqlmock.NewRows([]string{"exists"}).AddRow(0)
	mock.ExpectQuery("SELECT EXISTS").WillReturnRows(rows)
	mock.ExpectExec("CREATE TABLE").WillReturnResult(sqlmock.NewResult(1, 1))

	// Act
	err := mys.ensureStateTable("state")

	// Assert
	assert.Nil(t, err)
}

// Verify that the call to MySQL init get passed through
// to the DbAccess instance
func TestInitRunsDBAccessInit(t *testing.T) {
	// Arrange
	t.Parallel()
	mys, _, _ := mockDatabase(t)
	metadata := &state.Metadata{
		Properties: map[string]string{connectionStringKey: ""},
	}

	// Act
	err := mys.Init(*metadata)

	// Assert
	assert.NotNil(t, err)
	assert.Equal(t, defaultTableName, mys.tableName, "table name did not default")
}

func TestInitReturnsErrorOnFailOpen(t *testing.T) {
	// Arrange
	t.Parallel()
	mys, _, _ := mockDatabase(t)
	metadata := &state.Metadata{
		Properties: map[string]string{connectionStringKey: fakeConnectionString},
	}

	// Act
	err := mys.Init(*metadata)

	// Assert
	assert.NotNil(t, err)
}

func TestInitSetsTableName(t *testing.T) {
	// Arrange
	t.Parallel()
	mys, _, _ := mockDatabase(t)
	metadata := &state.Metadata{
		Properties: map[string]string{connectionStringKey: "", tableNameKey: "stateStore"},
	}

	// Act
	err := mys.Init(*metadata)

	// Assert
	assert.NotNil(t, err)
	assert.Equal(t, "stateStore", mys.tableName, "table name did not default")
}

// This state store does not support BulkGet so it must return false and
// nil nil
func TestBulkGetReturnsNil(t *testing.T) {
	// Arrange
	t.Parallel()
	mys, _, _ := mockDatabase(t)

	// Act
	supported, response, err := mys.BulkGet(nil)

	// Assert
	assert.Nil(t, err, `returned err`)
	assert.Nil(t, response, `returned response`)
	assert.False(t, supported, `returned supported`)
}

func TestMultiWithNoRequestsReturnsNil(t *testing.T) {
	// Arrange
	t.Parallel()
	mys, _, _ := mockDatabase(t)
	var ops []state.TransactionalStateOperation

	// Act
	err := mys.Multi(&state.TransactionalStateRequest{
		Operations: ops,
	})

	// Assert
	assert.Nil(t, err)
}

func TestInvalidMultiAction(t *testing.T) {
	// Arrange
	t.Parallel()
	mys, _, _ := mockDatabase(t)
	var ops []state.TransactionalStateOperation

	ops = append(ops, state.TransactionalStateOperation{
		Operation: "Something invalid",
		Request:   createSetRequest(),
	})

	// Act
	err := mys.Multi(&state.TransactionalStateRequest{
		Operations: ops,
	})

	// Assert
	assert.NotNil(t, err)
}

func TestClosingMySQLWithNilDba(t *testing.T) {
	// Arrange
	t.Parallel()
	mys, _, _ := mockDatabase(t)
	mys.Close()

	mys.db = nil

	// Act
	err := mys.Close()

	// Assert
	assert.Nil(t, err)
}

func TestValidSetRequest(t *testing.T) {
	// Arrange
	t.Parallel()
	mys, mock, _ := mockDatabase(t)
	var ops []state.TransactionalStateOperation

	ops = append(ops, state.TransactionalStateOperation{
		Operation: state.Upsert,
		Request:   createSetRequest(),
	})

	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	// Act
	err := mys.Multi(&state.TransactionalStateRequest{
		Operations: ops,
	})

	// Assert
	assert.Nil(t, err)
}

func TestInvalidMultiSetRequest(t *testing.T) {
	// Arrange
	t.Parallel()
	mys, _, _ := mockDatabase(t)
	var ops []state.TransactionalStateOperation

	ops = append(ops, state.TransactionalStateOperation{
		Operation: state.Upsert,
		// Delete request is not valid for Upsert operation
		Request: createDeleteRequest(),
	})

	// Act
	err := mys.Multi(&state.TransactionalStateRequest{
		Operations: ops,
	})

	// Assert
	assert.NotNil(t, err)
}

func TestValidMultiDeleteRequest(t *testing.T) {
	// Arrange
	t.Parallel()
	mys, mock, _ := mockDatabase(t)
	var ops []state.TransactionalStateOperation

	mock.ExpectBegin()
	mock.ExpectExec("DELETE FROM").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	ops = append(ops, state.TransactionalStateOperation{
		Operation: state.Delete,
		Request:   createDeleteRequest(),
	})

	// Act
	err := mys.Multi(&state.TransactionalStateRequest{
		Operations: ops,
	})

	// Assert
	assert.Nil(t, err)
}

func TestInvalidMultiDeleteRequest(t *testing.T) {
	// Arrange
	t.Parallel()
	mys, _, _ := mockDatabase(t)
	var ops []state.TransactionalStateOperation

	ops = append(ops, state.TransactionalStateOperation{
		Operation: state.Delete,
		// Set request is not valid for Delete operation
		Request: createSetRequest(),
	})

	// Act
	err := mys.Multi(&state.TransactionalStateRequest{
		Operations: ops,
	})

	// Assert
	assert.NotNil(t, err)
}

type fakeSQLRequest struct {
	lastInsertID int64
	rowsAffected int64
	err          error
}

func (f *fakeSQLRequest) LastInsertId() (int64, error) {
	return f.lastInsertID, f.err
}

func (f *fakeSQLRequest) RowsAffected() (int64, error) {
	return f.rowsAffected, f.err
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

func mockDatabase(t *testing.T) (*MySQL, sqlmock.Sqlmock, error) {
	db, mock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))

	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}

	logger := logger.NewLogger("test")
	mys := NewMySQLStateStore(logger)
	mys.db = db
	mys.tableName = "state"

	return mys, mock, err
}
