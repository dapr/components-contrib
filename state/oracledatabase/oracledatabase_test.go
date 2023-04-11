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
package oracledatabase

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/state"
	"github.com/dapr/kit/logger"
)

const (
	fakeConnectionString = "not a real connection"
)

// GetDBAccess is used in tests only, returns the dbaccess property.
func (o *OracleDatabase) GetDBAccess() *oracleDatabaseAccess {
	return o.dbaccess.(*oracleDatabaseAccess)
}

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

func (m *fakeDBaccess) ExecuteMulti(parentCtx context.Context, reqs []state.TransactionalStateOperation) error {
	return nil
}

func (m *fakeDBaccess) Close() error {
	return nil
}

// Proves that the Init method runs the init method.
func TestInitRunsDBAccessInit(t *testing.T) {
	t.Parallel()
	ods, fake := createOracleDatabaseWithFake(t)
	ods.Ping(context.Background())
	assert.True(t, fake.initExecuted)
}

func TestMultiWithNoRequestsReturnsNil(t *testing.T) {
	t.Parallel()
	var operations []state.TransactionalStateOperation
	ods := createOracleDatabase(t)
	err := ods.Multi(context.Background(), &state.TransactionalStateRequest{
		Operations: operations,
	})
	assert.NoError(t, err)
}

func TestValidSetRequest(t *testing.T) {
	t.Parallel()

	ods := createOracleDatabase(t)
	err := ods.Multi(context.Background(), &state.TransactionalStateRequest{
		Operations: []state.TransactionalStateOperation{
			createSetRequest(),
		},
	})
	assert.NoError(t, err)
}

func TestValidMultiDeleteRequest(t *testing.T) {
	t.Parallel()

	ods := createOracleDatabase(t)
	err := ods.Multi(context.Background(), &state.TransactionalStateRequest{
		Operations: []state.TransactionalStateOperation{
			createDeleteRequest(),
		},
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

func createOracleDatabaseWithFake(t *testing.T) (*OracleDatabase, *fakeDBaccess) {
	ods := createOracleDatabase(t)
	fake := ods.dbaccess.(*fakeDBaccess)
	return ods, fake
}

// Proves that the Ping method runs the ping method.
func TestPingRunsDBAccessPing(t *testing.T) {
	t.Parallel()
	odb, fake := createOracleDatabaseWithFake(t)
	odb.Ping(context.Background())
	assert.True(t, fake.pingExecuted)
}

func createOracleDatabase(t *testing.T) *OracleDatabase {
	logger := logger.NewLogger("test")

	dba := &fakeDBaccess{
		logger: logger,
	}

	odb := newOracleDatabaseStateStore(logger, dba)
	assert.NotNil(t, odb)

	metadata := &state.Metadata{
		Base: metadata.Base{Properties: map[string]string{connectionStringKey: fakeConnectionString}},
	}

	err := odb.Init(context.Background(), *metadata)

	assert.NoError(t, err)
	assert.NotNil(t, odb.dbaccess)

	return odb
}
