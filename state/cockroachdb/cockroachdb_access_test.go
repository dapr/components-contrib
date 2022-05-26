/*
Copyright 2022 The Dapr Authors
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
	"database/sql"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"

	"github.com/dapr/components-contrib/state"
	"github.com/dapr/kit/logger"
)

type mocks struct {
	db       *sql.DB
	mock     sqlmock.Sqlmock
	roachDba *cockroachDBAccess
}

func TestGetSetWithWrongType(t *testing.T) {
	t.Parallel()
	operation := state.TransactionalStateOperation{
		Operation: state.Delete,
		Request:   state.DeleteRequest{}, // Delete request is not valid for getSets
	}

	_, err := getSet(operation)
	assert.NotNil(t, err)
}

func TestGetSetWithNoKey(t *testing.T) {
	t.Parallel()
	operation := state.TransactionalStateOperation{
		Operation: state.Upsert,
		Request:   state.SetRequest{Value: "value1"}, // Set request with no key is invalid
	}

	_, err := getSet(operation)
	assert.NotNil(t, err)
}

func TestGetSetValid(t *testing.T) {
	t.Parallel()
	operation := state.TransactionalStateOperation{
		Operation: state.Upsert,
		Request:   state.SetRequest{Key: "key1", Value: "value1"},
	}

	set, err := getSet(operation)
	assert.Nil(t, err)
	assert.Equal(t, "key1", set.Key)
}

func TestGetDeleteWithWrongType(t *testing.T) {
	t.Parallel()
	operation := state.TransactionalStateOperation{
		Operation: state.Upsert,
		Request:   state.SetRequest{Value: "value1"}, // Set request is not valid for getDeletes
	}

	_, err := getDelete(operation)
	assert.NotNil(t, err)
}

func TestGetDeleteWithNoKey(t *testing.T) {
	t.Parallel()
	operation := state.TransactionalStateOperation{
		Operation: state.Delete,
		Request:   state.DeleteRequest{}, // Delete request with no key is invalid
	}

	_, err := getDelete(operation)
	assert.NotNil(t, err)
}

func TestGetDeleteValid(t *testing.T) {
	t.Parallel()
	operation := state.TransactionalStateOperation{
		Operation: state.Delete,
		Request:   state.DeleteRequest{Key: "key1"},
	}

	delete, err := getDelete(operation)
	assert.Nil(t, err)
	assert.Equal(t, "key1", delete.Key)
}

func TestMultiWithNoRequests(t *testing.T) {
	// Arrange
	m, _ := mockDatabase(t)
	defer m.db.Close()

	m.mock.ExpectBegin()
	m.mock.ExpectCommit()

	var operations []state.TransactionalStateOperation

	// Act
	err := m.roachDba.ExecuteMulti(&state.TransactionalStateRequest{
		Operations: operations,
	})

	// Assert
	assert.Nil(t, err)
}

func TestInvalidMultiInvalidAction(t *testing.T) {
	// Arrange
	m, _ := mockDatabase(t)
	defer m.db.Close()

	m.mock.ExpectBegin()
	m.mock.ExpectRollback()

	var operations []state.TransactionalStateOperation

	operations = append(operations, state.TransactionalStateOperation{
		Operation: "Something invalid",
		Request:   createSetRequest(),
	})

	// Act
	err := m.roachDba.ExecuteMulti(&state.TransactionalStateRequest{
		Operations: operations,
	})

	// Assert
	assert.NotNil(t, err)
}

func TestValidSetRequest(t *testing.T) {
	// Arrange
	m, _ := mockDatabase(t)
	defer m.db.Close()

	m.mock.ExpectBegin()
	m.mock.ExpectExec("INSERT INTO").WillReturnResult(sqlmock.NewResult(1, 1))
	m.mock.ExpectCommit()

	var operations []state.TransactionalStateOperation

	operations = append(operations, state.TransactionalStateOperation{
		Operation: state.Upsert,
		Request:   createSetRequest(),
	})

	// Act
	err := m.roachDba.ExecuteMulti(&state.TransactionalStateRequest{
		Operations: operations,
	})

	// Assert
	assert.Nil(t, err)
}

func TestInvalidMultiSetRequest(t *testing.T) {
	// Arrange
	m, _ := mockDatabase(t)
	defer m.db.Close()

	m.mock.ExpectBegin()
	m.mock.ExpectRollback()

	var operations []state.TransactionalStateOperation

	operations = append(operations, state.TransactionalStateOperation{
		Operation: state.Upsert,
		Request:   createDeleteRequest(), // Delete request is not valid for Upsert operation
	})

	// Act
	err := m.roachDba.ExecuteMulti(&state.TransactionalStateRequest{
		Operations: operations,
	})

	// Assert
	assert.NotNil(t, err)
}

func TestInvalidMultiSetRequestNoKey(t *testing.T) {
	// Arrange
	m, _ := mockDatabase(t)
	defer m.db.Close()

	m.mock.ExpectBegin()
	m.mock.ExpectRollback()

	var operations []state.TransactionalStateOperation

	operations = append(operations, state.TransactionalStateOperation{
		Operation: state.Upsert,
		Request:   state.SetRequest{Value: "value1"}, // Set request without key is not valid for Upsert operation
	})

	// Act
	err := m.roachDba.ExecuteMulti(&state.TransactionalStateRequest{
		Operations: operations,
	})

	// Assert
	assert.NotNil(t, err)
}

func TestValidMultiDeleteRequest(t *testing.T) {
	// Arrange
	m, _ := mockDatabase(t)
	defer m.db.Close()

	m.mock.ExpectBegin()
	m.mock.ExpectExec("DELETE FROM").WillReturnResult(sqlmock.NewResult(1, 1))
	m.mock.ExpectCommit()

	var operations []state.TransactionalStateOperation

	operations = append(operations, state.TransactionalStateOperation{
		Operation: state.Delete,
		Request:   createDeleteRequest(),
	})

	// Act
	err := m.roachDba.ExecuteMulti(&state.TransactionalStateRequest{
		Operations: operations,
	})

	// Assert
	assert.Nil(t, err)
}

func TestInvalidMultiDeleteRequest(t *testing.T) {
	// Arrange
	m, _ := mockDatabase(t)
	defer m.db.Close()

	m.mock.ExpectBegin()
	m.mock.ExpectRollback()

	var operations []state.TransactionalStateOperation

	operations = append(operations, state.TransactionalStateOperation{
		Operation: state.Delete,
		Request:   createSetRequest(), // Set request is not valid for Delete operation
	})

	// Act
	err := m.roachDba.ExecuteMulti(&state.TransactionalStateRequest{
		Operations: operations,
	})

	// Assert
	assert.NotNil(t, err)
}

func TestInvalidMultiDeleteRequestNoKey(t *testing.T) {
	// Arrange
	m, _ := mockDatabase(t)
	defer m.db.Close()

	m.mock.ExpectBegin()
	m.mock.ExpectRollback()

	var operations []state.TransactionalStateOperation

	operations = append(operations, state.TransactionalStateOperation{
		Operation: state.Delete,
		Request:   state.DeleteRequest{}, // Delete request without key is not valid for Delete operation
	})

	// Act
	err := m.roachDba.ExecuteMulti(&state.TransactionalStateRequest{
		Operations: operations,
	})

	// Assert
	assert.NotNil(t, err)
}

func TestMultiOperationOrder(t *testing.T) {
	// Arrange
	m, _ := mockDatabase(t)
	defer m.db.Close()

	m.mock.ExpectBegin()
	m.mock.ExpectExec("INSERT INTO").WillReturnResult(sqlmock.NewResult(1, 1))
	m.mock.ExpectExec("DELETE FROM").WillReturnResult(sqlmock.NewResult(1, 1))
	m.mock.ExpectCommit()

	var operations []state.TransactionalStateOperation

	operations = append(operations,
		state.TransactionalStateOperation{
			Operation: state.Upsert,
			Request:   state.SetRequest{Key: "key1", Value: "value1"},
		},
		state.TransactionalStateOperation{
			Operation: state.Delete,
			Request:   state.DeleteRequest{Key: "key1"},
		},
	)

	// Act
	err := m.roachDba.ExecuteMulti(&state.TransactionalStateRequest{
		Operations: operations,
	})

	// Assert
	assert.Nil(t, err)
}

func TestInvalidBulkSetNoKey(t *testing.T) {
	// Arrange
	m, _ := mockDatabase(t)
	defer m.db.Close()

	m.mock.ExpectBegin()
	m.mock.ExpectRollback()

	var sets []state.SetRequest

	sets = append(sets, state.SetRequest{ // Set request without key is not valid for Set operation
		Value: "value1",
	})

	// Act
	err := m.roachDba.BulkSet(sets)

	// Assert
	assert.NotNil(t, err)
}

func TestInvalidBulkSetEmptyValue(t *testing.T) {
	// Arrange
	m, _ := mockDatabase(t)
	defer m.db.Close()

	m.mock.ExpectBegin()
	m.mock.ExpectRollback()

	var sets []state.SetRequest

	sets = append(sets, state.SetRequest{ // Set request without value is not valid for Set operation
		Key:   "key1",
		Value: "",
	})

	// Act
	err := m.roachDba.BulkSet(sets)

	// Assert
	assert.NotNil(t, err)
}

func TestValidBulkSet(t *testing.T) {
	// Arrange
	m, _ := mockDatabase(t)
	defer m.db.Close()

	m.mock.ExpectBegin()
	m.mock.ExpectExec("INSERT INTO").WillReturnResult(sqlmock.NewResult(1, 1))
	m.mock.ExpectCommit()

	var sets []state.SetRequest

	sets = append(sets, state.SetRequest{
		Key:   "key1",
		Value: "value1",
	})

	// Act
	err := m.roachDba.BulkSet(sets)

	// Assert
	assert.Nil(t, err)
}

func TestInvalidBulkDeleteNoKey(t *testing.T) {
	// Arrange
	m, _ := mockDatabase(t)
	defer m.db.Close()

	m.mock.ExpectBegin()
	m.mock.ExpectRollback()

	var deletes []state.DeleteRequest

	deletes = append(deletes, state.DeleteRequest{ // Delete request without key is not valid for Delete operation
		Key: "",
	})

	// Act
	err := m.roachDba.BulkDelete(deletes)

	// Assert
	assert.NotNil(t, err)
}

func TestValidBulkDelete(t *testing.T) {
	// Arrange
	m, _ := mockDatabase(t)
	defer m.db.Close()

	m.mock.ExpectBegin()
	m.mock.ExpectExec("DELETE FROM").WillReturnResult(sqlmock.NewResult(1, 1))
	m.mock.ExpectCommit()

	var deletes []state.DeleteRequest

	deletes = append(deletes, state.DeleteRequest{
		Key: "key1",
	})

	// Act
	err := m.roachDba.BulkDelete(deletes)

	// Assert
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

func mockDatabase(t *testing.T) (*mocks, error) {
	logger := logger.NewLogger("test")

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}

	dba := &cockroachDBAccess{
		logger: logger,
		db:     db,
	}

	return &mocks{
		db:       db,
		mock:     mock,
		roachDba: dba,
	}, err
}
