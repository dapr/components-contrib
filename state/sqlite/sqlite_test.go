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

package sqlite

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/state"
	"github.com/dapr/kit/logger"
)

const (
	fakeConnectionString = "not a real connection"
)

// Fake implementation of interface oracledatabase.dbaccess.
type fakeDBaccess struct {
	logger       logger.Logger
	pingExecuted bool
	initExecuted bool
	setExecuted  bool
	getExecuted  bool
}

func (m *fakeDBaccess) Ping(ctx context.Context) error {
	m.pingExecuted = true
	return nil
}

func (m *fakeDBaccess) Init(ctx context.Context, metadata state.Metadata) error {
	m.initExecuted = true

	return nil
}

func (m *fakeDBaccess) Set(ctx context.Context, req *state.SetRequest) error {
	m.setExecuted = true

	return nil
}

func (m *fakeDBaccess) Get(ctx context.Context, req *state.GetRequest) (*state.GetResponse, error) {
	m.getExecuted = true

	return nil, nil
}

func (m *fakeDBaccess) Delete(ctx context.Context, req *state.DeleteRequest) error {
	return nil
}

func (m *fakeDBaccess) ExecuteMulti(ctx context.Context, reqs []state.TransactionalStateOperation) error {
	return nil
}

func (m *fakeDBaccess) Close() error {
	return nil
}

// Proves that the Init method runs the init method.
func TestInitRunsDBAccessInit(t *testing.T) {
	t.Parallel()
	ods, fake := createSqliteWithFake(t)
	ods.Ping()
	assert.True(t, fake.initExecuted)
}

func TestMultiWithNoRequestsReturnsNil(t *testing.T) {
	t.Parallel()
	var operations []state.TransactionalStateOperation
	ods := createSqlite(t)
	err := ods.Multi(context.Background(), &state.TransactionalStateRequest{
		Operations: operations,
	})
	assert.NoError(t, err)
}

func TestValidSetRequest(t *testing.T) {
	t.Parallel()
	var operations []state.TransactionalStateOperation

	operations = append(operations, state.TransactionalStateOperation{
		Operation: state.Upsert,
		Request:   createSetRequest(),
	})

	ods := createSqlite(t)
	err := ods.Multi(context.Background(), &state.TransactionalStateRequest{
		Operations: operations,
	})
	assert.NoError(t, err)
}

func TestValidMultiDeleteRequest(t *testing.T) {
	t.Parallel()
	var operations []state.TransactionalStateOperation

	operations = append(operations, state.TransactionalStateOperation{
		Operation: state.Delete,
		Request:   createDeleteRequest(),
	})

	ods := createSqlite(t)
	err := ods.Multi(context.Background(), &state.TransactionalStateRequest{
		Operations: operations,
	})
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

func createSqliteWithFake(t *testing.T) (*SQLiteStore, *fakeDBaccess) {
	ods := createSqlite(t)
	fake := ods.dbaccess.(*fakeDBaccess)
	return ods, fake
}

// Proves that the Ping method runs the ping method.
func TestPingRunsDBAccessPing(t *testing.T) {
	t.Parallel()
	odb, fake := createSqliteWithFake(t)
	odb.Ping()
	assert.True(t, fake.pingExecuted)
}

func createSqlite(t *testing.T) *SQLiteStore {
	logger := logger.NewLogger("test")

	dba := &fakeDBaccess{
		logger: logger,
	}

	odb := newSQLiteStateStore(logger, dba)
	assert.NotNil(t, odb)

	metadata := &state.Metadata{
		Base: metadata.Base{
			Properties: map[string]string{
				"connectionString": fakeConnectionString,
			},
		},
	}

	err := odb.Init(context.Background(), *metadata)

	assert.NoError(t, err)
	assert.NotNil(t, odb.dbaccess)

	return odb
}

func randomKey() string {
	return uuid.New().String()
}

type fakeItem struct {
	Color string
}

func randomJSON() *fakeItem {
	return &fakeItem{Color: randomKey()}
}
