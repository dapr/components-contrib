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
package oracledatabase

import (
	"testing"

	"github.com/stretchr/testify/assert"

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

func (m *fakeDBaccess) Ping() error {
	m.pingExecuted = true
	return nil
}

func (m *fakeDBaccess) Init(metadata state.Metadata) error {
	m.initExecuted = true

	return nil
}

func (m *fakeDBaccess) Set(req *state.SetRequest) error {
	m.setExecuted = true

	return nil
}

func (m *fakeDBaccess) Get(req *state.GetRequest) (*state.GetResponse, error) {
	m.getExecuted = true

	return nil, nil
}

func (m *fakeDBaccess) Delete(req *state.DeleteRequest) error {
	return nil
}

func (m *fakeDBaccess) ExecuteMulti(sets []state.SetRequest, deletes []state.DeleteRequest) error {
	return nil
}

func (m *fakeDBaccess) Close() error {
	return nil
}

// Proves that the Init method runs the init method.
func TestInitRunsDBAccessInit(t *testing.T) {
	t.Parallel()
	ods, fake := createOracleDatabaseWithFake(t)
	ods.Ping()
	assert.True(t, fake.initExecuted)
}

func TestMultiWithNoRequestsReturnsNil(t *testing.T) {
	t.Parallel()
	var operations []state.TransactionalStateOperation
	ods := createOracleDatabase(t)
	err := ods.Multi(&state.TransactionalStateRequest{
		Operations: operations,
	})
	assert.Nil(t, err)
}

func TestInvalidMultiAction(t *testing.T) {
	t.Parallel()
	var operations []state.TransactionalStateOperation

	operations = append(operations, state.TransactionalStateOperation{
		Operation: "Something invalid",
		Request:   createSetRequest(),
	})

	ods := createOracleDatabase(t)
	err := ods.Multi(&state.TransactionalStateRequest{
		Operations: operations,
	})
	assert.NotNil(t, err)
}

func TestValidSetRequest(t *testing.T) {
	t.Parallel()
	var operations []state.TransactionalStateOperation

	operations = append(operations, state.TransactionalStateOperation{
		Operation: state.Upsert,
		Request:   createSetRequest(),
	})

	ods := createOracleDatabase(t)
	err := ods.Multi(&state.TransactionalStateRequest{
		Operations: operations,
	})
	assert.Nil(t, err)
}

func TestInvalidMultiSetRequest(t *testing.T) {
	t.Parallel()
	var operations []state.TransactionalStateOperation

	operations = append(operations, state.TransactionalStateOperation{
		Operation: state.Upsert,
		Request:   createDeleteRequest(), // Delete request is not valid for Upsert operation.
	})

	ods := createOracleDatabase(t)
	err := ods.Multi(&state.TransactionalStateRequest{
		Operations: operations,
	})
	assert.NotNil(t, err)
}

func TestValidMultiDeleteRequest(t *testing.T) {
	t.Parallel()
	var operations []state.TransactionalStateOperation

	operations = append(operations, state.TransactionalStateOperation{
		Operation: state.Delete,
		Request:   createDeleteRequest(),
	})

	ods := createOracleDatabase(t)
	err := ods.Multi(&state.TransactionalStateRequest{
		Operations: operations,
	})
	assert.Nil(t, err)
}

func TestInvalidMultiDeleteRequest(t *testing.T) {
	t.Parallel()
	var operations []state.TransactionalStateOperation

	operations = append(operations, state.TransactionalStateOperation{
		Operation: state.Delete,
		Request:   createSetRequest(), // Set request is not valid for Delete operation.
	})

	ods := createOracleDatabase(t)
	err := ods.Multi(&state.TransactionalStateRequest{
		Operations: operations,
	})
	assert.NotNil(t, err)
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
	odb.Ping()
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
		Properties: map[string]string{connectionStringKey: fakeConnectionString},
	}

	err := odb.Init(*metadata)

	assert.Nil(t, err)
	assert.NotNil(t, odb.dbaccess)

	return odb
}
