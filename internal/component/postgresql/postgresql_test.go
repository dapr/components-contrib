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

	"github.com/google/uuid"
	pgxmock "github.com/pashagolub/pgxmock/v2"
	"github.com/stretchr/testify/assert"

	pginterfaces "github.com/dapr/components-contrib/internal/component/postgresql/interfaces"
	"github.com/dapr/components-contrib/state"
	"github.com/dapr/kit/logger"
)

type mocks struct {
	db pgxmock.PgxPoolIface
	pg *PostgreSQL
}

type fakeItem struct {
	Color string
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
	err := m.pg.Multi(context.Background(), &state.TransactionalStateRequest{
		Operations: operations,
	})

	// Assert
	assert.NoError(t, err)
}

func TestValidSetRequest(t *testing.T) {
	// Arrange
	m, _ := mockDatabase(t)
	defer m.db.Close()

	setReq := createSetRequest()
	operations := []state.TransactionalStateOperation{setReq}
	val, _ := json.Marshal(setReq.Value)

	m.db.ExpectBegin()
	m.db.ExpectExec("INSERT INTO").
		WithArgs(setReq.Key, string(val), false).
		WillReturnResult(pgxmock.NewResult("INSERT", 1))
	m.db.ExpectCommit()
	// There's also a rollback called after a commit, which is expected and will not have effect
	m.db.ExpectRollback()

	// Act
	err := m.pg.Multi(context.Background(), &state.TransactionalStateRequest{
		Operations: operations,
	})

	// Assert
	assert.NoError(t, err)
}

func TestInvalidMultiSetRequestNoKey(t *testing.T) {
	// Arrange
	m, _ := mockDatabase(t)
	defer m.db.Close()

	m.db.ExpectBegin()
	m.db.ExpectRollback()

	operations := []state.TransactionalStateOperation{
		state.SetRequest{Value: "value1"}, // Set request without key is not valid for Upsert operation
	}

	// Act
	err := m.pg.Multi(context.Background(), &state.TransactionalStateRequest{
		Operations: operations,
	})

	// Assert
	assert.Error(t, err)
}

func TestValidMultiDeleteRequest(t *testing.T) {
	// Arrange
	m, _ := mockDatabase(t)
	defer m.db.Close()

	deleteReq := createDeleteRequest()
	operations := []state.TransactionalStateOperation{deleteReq}

	m.db.ExpectBegin()
	m.db.ExpectExec("DELETE FROM").
		WithArgs(deleteReq.Key).
		WillReturnResult(pgxmock.NewResult("DELETE", 1))
	m.db.ExpectCommit()
	// There's also a rollback called after a commit, which is expected and will not have effect
	m.db.ExpectRollback()

	// Act
	err := m.pg.Multi(context.Background(), &state.TransactionalStateRequest{
		Operations: operations,
	})

	// Assert
	assert.NoError(t, err)
}

func TestInvalidMultiDeleteRequestNoKey(t *testing.T) {
	// Arrange
	m, _ := mockDatabase(t)
	defer m.db.Close()

	m.db.ExpectBegin()
	m.db.ExpectRollback()

	operations := []state.TransactionalStateOperation{state.DeleteRequest{}} // Delete request without key is not valid for Delete operation

	// Act
	err := m.pg.Multi(context.Background(), &state.TransactionalStateRequest{
		Operations: operations,
	})

	// Assert
	assert.Error(t, err)
}

func TestMultiOperationOrder(t *testing.T) {
	// Arrange
	m, _ := mockDatabase(t)
	defer m.db.Close()

	operations := []state.TransactionalStateOperation{
		state.SetRequest{Key: "key1", Value: "value1"},
		state.DeleteRequest{Key: "key1"},
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
	err := m.pg.Multi(context.Background(), &state.TransactionalStateRequest{
		Operations: operations,
	})

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

	dba := &PostgreSQL{
		metadata: pgMetadata{
			TableName: "state",
			Timeout:   30 * time.Second,
		},
		logger: logger,
		db:     db,
		migrateFn: func(context.Context, pginterfaces.PGXPoolConn, MigrateOptions) error {
			return nil
		},
		setQueryFn: func(*state.SetRequest, SetQueryOptions) string {
			return `INSERT INTO state
					(key, value, isbinary, expiredate)
				VALUES
					($1, $2, $3, NULL)`
		},
	}

	return &mocks{
		db: db,
		pg: dba,
	}, err
}

func randomKey() string {
	return uuid.New().String()
}

func randomJSON() *fakeItem {
	return &fakeItem{Color: randomKey()}
}
