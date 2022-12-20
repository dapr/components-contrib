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
package postgresql

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	pgxmock "github.com/pashagolub/pgxmock/v2"
	"github.com/stretchr/testify/assert"

	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/state"
	"github.com/dapr/kit/logger"
)

type mocks struct {
	db    pgxmock.PgxPoolIface
	pgDba *PostgresDBAccess
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
	assert.NoError(t, err)
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
	assert.NoError(t, err)
	assert.Equal(t, "key1", delete.Key)
}

func TestMultiWithNoRequests(t *testing.T) {
	// Arrange
	m, _ := mockDatabase(t)
	defer m.db.Close()

	m.db.ExpectBegin()
	m.db.ExpectCommit()
	// There's also a rollback called after a commit, which is expected and will not have effect
	m.db.ExpectRollback()

	var operations []state.TransactionalStateOperation

	// Act
	err := m.pgDba.ExecuteMulti(context.Background(), &state.TransactionalStateRequest{
		Operations: operations,
	})

	// Assert
	assert.NoError(t, err)
}

func TestInvalidMultiInvalidAction(t *testing.T) {
	// Arrange
	m, _ := mockDatabase(t)
	defer m.db.Close()

	m.db.ExpectBegin()
	m.db.ExpectRollback()

	var operations []state.TransactionalStateOperation

	operations = append(operations, state.TransactionalStateOperation{
		Operation: "Something invalid",
		Request:   createSetRequest(),
	})

	// Act
	err := m.pgDba.ExecuteMulti(context.Background(), &state.TransactionalStateRequest{
		Operations: operations,
	})

	// Assert
	assert.NotNil(t, err)
}

func TestValidSetRequest(t *testing.T) {
	// Arrange
	m, _ := mockDatabase(t)
	defer m.db.Close()

	setReq := createSetRequest()
	operations := []state.TransactionalStateOperation{
		{Operation: state.Upsert, Request: setReq},
	}
	val, _ := json.Marshal(setReq.Value)

	m.db.ExpectBegin()
	m.db.ExpectExec("INSERT INTO").
		WithArgs(setReq.Key, string(val), false).
		WillReturnResult(pgxmock.NewResult("INSERT", 1))
	m.db.ExpectCommit()
	// There's also a rollback called after a commit, which is expected and will not have effect
	m.db.ExpectRollback()

	// Act
	err := m.pgDba.ExecuteMulti(context.Background(), &state.TransactionalStateRequest{
		Operations: operations,
	})

	// Assert
	assert.NoError(t, err)
}

func TestInvalidMultiSetRequest(t *testing.T) {
	// Arrange
	m, _ := mockDatabase(t)
	defer m.db.Close()

	m.db.ExpectBegin()
	m.db.ExpectRollback()

	operations := []state.TransactionalStateOperation{
		{
			Operation: state.Upsert,
			Request:   createDeleteRequest(), // Delete request is not valid for Upsert operation
		},
	}

	// Act
	err := m.pgDba.ExecuteMulti(context.Background(), &state.TransactionalStateRequest{
		Operations: operations,
	})

	// Assert
	assert.NotNil(t, err)
}

func TestInvalidMultiSetRequestNoKey(t *testing.T) {
	// Arrange
	m, _ := mockDatabase(t)
	defer m.db.Close()

	m.db.ExpectBegin()
	m.db.ExpectRollback()

	var operations []state.TransactionalStateOperation

	operations = append(operations, state.TransactionalStateOperation{
		Operation: state.Upsert,
		Request:   state.SetRequest{Value: "value1"}, // Set request without key is not valid for Upsert operation
	})

	// Act
	err := m.pgDba.ExecuteMulti(context.Background(), &state.TransactionalStateRequest{
		Operations: operations,
	})

	// Assert
	assert.NotNil(t, err)
}

func TestValidMultiDeleteRequest(t *testing.T) {
	// Arrange
	m, _ := mockDatabase(t)
	defer m.db.Close()

	deleteReq := createDeleteRequest()
	operations := []state.TransactionalStateOperation{
		{Operation: state.Delete, Request: deleteReq},
	}

	m.db.ExpectBegin()
	m.db.ExpectExec("DELETE FROM").
		WithArgs(deleteReq.Key).
		WillReturnResult(pgxmock.NewResult("DELETE", 1))
	m.db.ExpectCommit()
	// There's also a rollback called after a commit, which is expected and will not have effect
	m.db.ExpectRollback()

	// Act
	err := m.pgDba.ExecuteMulti(context.Background(), &state.TransactionalStateRequest{
		Operations: operations,
	})

	// Assert
	assert.NoError(t, err)
}

func TestInvalidMultiDeleteRequest(t *testing.T) {
	// Arrange
	m, _ := mockDatabase(t)
	defer m.db.Close()

	m.db.ExpectBegin()
	m.db.ExpectRollback()

	var operations []state.TransactionalStateOperation

	operations = append(operations, state.TransactionalStateOperation{
		Operation: state.Delete,
		Request:   createSetRequest(), // Set request is not valid for Delete operation
	})

	// Act
	err := m.pgDba.ExecuteMulti(context.Background(), &state.TransactionalStateRequest{
		Operations: operations,
	})

	// Assert
	assert.NotNil(t, err)
}

func TestInvalidMultiDeleteRequestNoKey(t *testing.T) {
	// Arrange
	m, _ := mockDatabase(t)
	defer m.db.Close()

	m.db.ExpectBegin()
	m.db.ExpectRollback()

	var operations []state.TransactionalStateOperation

	operations = append(operations, state.TransactionalStateOperation{
		Operation: state.Delete,
		Request:   state.DeleteRequest{}, // Delete request without key is not valid for Delete operation
	})

	// Act
	err := m.pgDba.ExecuteMulti(context.Background(), &state.TransactionalStateRequest{
		Operations: operations,
	})

	// Assert
	assert.NotNil(t, err)
}

func TestMultiOperationOrder(t *testing.T) {
	// Arrange
	m, _ := mockDatabase(t)
	defer m.db.Close()

	operations := []state.TransactionalStateOperation{
		{
			Operation: state.Upsert,
			Request:   state.SetRequest{Key: "key1", Value: "value1"},
		},
		{
			Operation: state.Delete,
			Request:   state.DeleteRequest{Key: "key1"},
		},
	}

	m.db.ExpectBegin()
	m.db.ExpectExec("INSERT INTO").
		WithArgs("key1", `"value1"`, false).
		WillReturnResult(pgxmock.NewResult("INSERT", 1))
	m.db.ExpectExec("DELETE FROM").
		WithArgs("key1").
		WillReturnResult(pgxmock.NewResult("DELETE", 1))
	m.db.ExpectCommit()
	// There's also a rollback called after a commit, which is expected and will not have effect
	m.db.ExpectRollback()

	// Act
	err := m.pgDba.ExecuteMulti(context.Background(), &state.TransactionalStateRequest{
		Operations: operations,
	})

	// Assert
	assert.NoError(t, err)
}

func TestInvalidBulkSetNoKey(t *testing.T) {
	// Arrange
	m, _ := mockDatabase(t)
	defer m.db.Close()

	m.db.ExpectBegin()
	m.db.ExpectRollback()

	sets := []state.SetRequest{
		// Set request without key is not valid for Set operation
		{Value: "value1"},
	}

	// Act
	err := m.pgDba.BulkSet(context.Background(), sets)

	// Assert
	assert.NotNil(t, err)
}

func TestInvalidBulkSetEmptyValue(t *testing.T) {
	// Arrange
	m, _ := mockDatabase(t)
	defer m.db.Close()

	m.db.ExpectBegin()
	m.db.ExpectRollback()

	sets := []state.SetRequest{
		// Set request without value is not valid for Set operation
		{Key: "key1", Value: "value1"},
	}

	// Act
	err := m.pgDba.BulkSet(context.Background(), sets)

	// Assert
	assert.NotNil(t, err)
}

func TestValidBulkSet(t *testing.T) {
	// Arrange
	m, _ := mockDatabase(t)
	defer m.db.Close()

	sets := []state.SetRequest{
		{
			Key:   "key1",
			Value: "value1",
		},
	}

	m.db.ExpectBegin()
	m.db.ExpectExec("INSERT INTO").
		WithArgs("key1", `"value1"`, false).
		WillReturnResult(pgxmock.NewResult("INSERT", 1))
	m.db.ExpectCommit()
	// There's also a rollback called after a commit, which is expected and will not have effect
	m.db.ExpectRollback()

	// Act
	err := m.pgDba.BulkSet(context.Background(), sets)

	// Assert
	assert.NoError(t, err)
}

func TestInvalidBulkDeleteNoKey(t *testing.T) {
	// Arrange
	m, _ := mockDatabase(t)
	defer m.db.Close()

	m.db.ExpectBegin()
	m.db.ExpectRollback()

	var deletes []state.DeleteRequest

	deletes = append(deletes, state.DeleteRequest{ // Delete request without key is not valid for Delete operation
		Key: "",
	})

	// Act
	err := m.pgDba.BulkDelete(context.Background(), deletes)

	// Assert
	assert.NotNil(t, err)
}

func TestValidBulkDelete(t *testing.T) {
	// Arrange
	m, _ := mockDatabase(t)
	defer m.db.Close()

	deletes := []state.DeleteRequest{
		{Key: "key1"},
	}

	m.db.ExpectBegin()
	m.db.ExpectExec("DELETE FROM").
		WithArgs("key1").
		WillReturnResult(pgxmock.NewResult("DELETE", 1))
	m.db.ExpectCommit()
	// There's also a rollback called after a commit, which is expected and will not have effect
	m.db.ExpectRollback()

	// Act
	err := m.pgDba.BulkDelete(context.Background(), deletes)

	// Assert
	assert.NoError(t, err)
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

	db, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}

	dba := &PostgresDBAccess{
		logger: logger,
		db:     db,
	}

	return &mocks{
		db:    db,
		pgDba: dba,
	}, err
}

func TestParseMetadata(t *testing.T) {
	t.Run("missing connection string", func(t *testing.T) {
		p := &PostgresDBAccess{}
		props := map[string]string{}

		err := p.ParseMetadata(state.Metadata{Base: metadata.Base{Properties: props}})
		assert.Error(t, err)
		assert.ErrorIs(t, err, errMissingConnectionString)
	})

	t.Run("has connection string", func(t *testing.T) {
		p := &PostgresDBAccess{}
		props := map[string]string{
			"connectionString": "foo",
		}

		err := p.ParseMetadata(state.Metadata{Base: metadata.Base{Properties: props}})
		assert.NoError(t, err)
	})

	t.Run("default table name", func(t *testing.T) {
		p := &PostgresDBAccess{}
		props := map[string]string{
			"connectionString": "foo",
		}

		err := p.ParseMetadata(state.Metadata{Base: metadata.Base{Properties: props}})
		assert.NoError(t, err)
		assert.Equal(t, p.metadata.TableName, defaultTableName)
	})

	t.Run("custom table name", func(t *testing.T) {
		p := &PostgresDBAccess{}
		props := map[string]string{
			"connectionString": "foo",
			"tableName":        "mytable",
		}

		err := p.ParseMetadata(state.Metadata{Base: metadata.Base{Properties: props}})
		assert.NoError(t, err)
		assert.Equal(t, p.metadata.TableName, "mytable")
	})

	t.Run("default cleanupIntervalInSeconds", func(t *testing.T) {
		p := &PostgresDBAccess{}
		props := map[string]string{
			"connectionString": "foo",
		}

		err := p.ParseMetadata(state.Metadata{Base: metadata.Base{Properties: props}})
		assert.NoError(t, err)
		_ = assert.NotNil(t, p.cleanupInterval) &&
			assert.Equal(t, *p.cleanupInterval, defaultCleanupInternal*time.Second)
	})

	t.Run("invalid cleanupIntervalInSeconds", func(t *testing.T) {
		p := &PostgresDBAccess{}
		props := map[string]string{
			"connectionString":         "foo",
			"cleanupIntervalInSeconds": "NaN",
		}

		err := p.ParseMetadata(state.Metadata{Base: metadata.Base{Properties: props}})
		assert.Error(t, err)
	})

	t.Run("positive cleanupIntervalInSeconds", func(t *testing.T) {
		p := &PostgresDBAccess{}
		props := map[string]string{
			"connectionString":         "foo",
			"cleanupIntervalInSeconds": "42",
		}

		err := p.ParseMetadata(state.Metadata{Base: metadata.Base{Properties: props}})
		assert.NoError(t, err)
		_ = assert.NotNil(t, p.cleanupInterval) &&
			assert.Equal(t, *p.cleanupInterval, 42*time.Second)
	})

	t.Run("zero cleanupIntervalInSeconds", func(t *testing.T) {
		p := &PostgresDBAccess{}
		props := map[string]string{
			"connectionString":         "foo",
			"cleanupIntervalInSeconds": "0",
		}

		err := p.ParseMetadata(state.Metadata{Base: metadata.Base{Properties: props}})
		assert.NoError(t, err)
		assert.Nil(t, p.cleanupInterval)
	})
}
