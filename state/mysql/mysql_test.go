// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------
package mysql

import (
	"database/sql"
	"fmt"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/dapr/components-contrib/state"
	"github.com/dapr/kit/logger"
	"github.com/stretchr/testify/assert"
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
	actualErr := m.mySQL.finishInit(m.mySQL.db, nil)

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
	actualErr := m.mySQL.finishInit(m.mySQL.db, nil)

	// Assert
	assert.NotNil(t, actualErr, "now error returned")
	assert.Equal(t, "createDatabaseError", actualErr.Error(), "wrong error")
}

func TestFinishInitHandlesOpenError(t *testing.T) {
	// Arrange
	m, _ := mockDatabase(t)
	defer m.mySQL.Close()

	// Act
	err := m.mySQL.finishInit(m.mySQL.db, fmt.Errorf("failed to open database"))

	// Assert
	assert.NotNil(t, err, "now error returned")
	assert.Equal(t, "failed to open database", err.Error(), "wrong error")
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
	actualErr := m.mySQL.finishInit(m.mySQL.db, nil)

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
	err := m.mySQL.finishInit(m.mySQL.db, nil)

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
	err := m.mySQL.executeMulti(nil, nil)

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
	m.mock1.ExpectExec("DELETE FROM").WillReturnResult(sqlmock.NewResult(0, 1))
	m.mock1.ExpectExec("INSERT INTO").WillReturnResult(sqlmock.NewResult(0, 1))
	m.mock1.ExpectCommit()

	sets := []state.SetRequest{createSetRequest()}
	deletes := []state.DeleteRequest{createDeleteRequest()}

	// Act
	err := m.mySQL.executeMulti(sets, deletes)

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
	err := m.mySQL.setValue(&request)

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
	err := m.mySQL.setValue(&request)

	// Assert
	assert.Nil(t, err)
}

// Verifies that MySQL passes through to myDBAccess
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

	eTag := "946af56e"

	request := createDeleteRequest()
	request.ETag = &eTag

	// Act
	err := m.mySQL.deleteValue(&request)

	// Assert
	assert.Nil(t, err)
}

func TestReturnNDBResultsRowsAffectedReturnsError(t *testing.T) {
	// Arrange
	m, _ := mockDatabase(t)
	defer m.mySQL.Close()

	request := &fakeSQLRequest{
		rowsAffected: 3,
		lastInsertID: 0,
		err:          fmt.Errorf("RowAffectedError"),
	}

	// Act
	err := m.mySQL.returnNDBResults(request, nil, 2)

	// Assert
	assert.NotNil(t, err)
	assert.Equal(t, "RowAffectedError", err.Error())
}

func TestReturnNDBResultsNoRows(t *testing.T) {
	// Arrange
	m, _ := mockDatabase(t)
	defer m.mySQL.Close()

	request := &fakeSQLRequest{
		rowsAffected: 0,
		lastInsertID: 0,
	}

	// Act
	err := m.mySQL.returnNDBResults(request, nil, 2)

	// Assert
	assert.NotNil(t, err)
	assert.Equal(t, "rows affected error: no rows match given key and eTag", err.Error())
}

func TestReturnNDBResultsTooManyRows(t *testing.T) {
	// Arrange
	m, _ := mockDatabase(t)
	defer m.mySQL.Close()

	request := &fakeSQLRequest{
		rowsAffected: 3,
		lastInsertID: 0,
	}

	// Act
	err := m.mySQL.returnNDBResults(request, nil, 2)

	// Assert
	assert.NotNil(t, err)
	assert.Equal(t, "rows affected error: more than 2 row affected, expected 2, actual 3", err.Error())
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

	rows := sqlmock.NewRows([]string{"value", "eTag"}).AddRow("{}", "946af56e")
	m.mock1.ExpectQuery("SELECT value, eTag FROM state WHERE id = ?").WillReturnRows(rows)

	request := &state.GetRequest{
		Key: "UnitTest",
	}

	// Act
	response, err := m.mySQL.Get(request)

	// Assert
	assert.Nil(t, err)
	assert.NotNil(t, response)
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
	actual, err := tableExists(m.mySQL.db, "store")

	// Assert
	assert.Nil(t, err, `error was returned`)
	assert.True(t, actual, `table does not exists`)
}

// Verifies that the code returns an error if the create table command fails
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
// to the DbAccess instance
func TestInitReturnsErrorOnNoConnectionString(t *testing.T) {
	// Arrange
	t.Parallel()
	m, _ := mockDatabase(t)
	metadata := &state.Metadata{
		Properties: map[string]string{connectionStringKey: ""},
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
		Properties: map[string]string{connectionStringKey: fakeConnectionString},
	}

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
		Properties: map[string]string{
			pemPathKey:          "./ssl.pem",
			tableNameKey:        "stateStore",
			connectionStringKey: fakeConnectionString,
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
		Properties: map[string]string{connectionStringKey: "", tableNameKey: "stateStore"},
	}

	// Act
	err := m.mySQL.Init(*metadata)

	// Assert
	assert.NotNil(t, err)
	assert.Equal(t, "stateStore", m.mySQL.tableName, "table name did not default")
}

func TestInitSetsSchemaName(t *testing.T) {
	// Arrange
	t.Parallel()
	m, _ := mockDatabase(t)
	metadata := &state.Metadata{
		Properties: map[string]string{connectionStringKey: "", schemaNameKey: "stateStoreSchema"},
	}

	// Act
	err := m.mySQL.Init(*metadata)

	// Assert
	assert.NotNil(t, err)
	assert.Equal(t, "stateStoreSchema", m.mySQL.schemaName, "table name did not default")
}

// This state store does not support BulkGet so it must return false and
// nil nil
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

func TestMultiWithNoRequestsReturnsNil(t *testing.T) {
	// Arrange
	t.Parallel()
	m, _ := mockDatabase(t)
	var ops []state.TransactionalStateOperation

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
