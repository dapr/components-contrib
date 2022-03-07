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

package cockroachdb

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/dapr/components-contrib/state"
	"github.com/dapr/kit/logger"
)

const (
	fakeConnectionString = "not a real connection"
)

// Fake implementation of interface postgressql.dbaccess.
type fakeDBaccess struct {
	logger       logger.Logger
	initExecuted bool
	setExecuted  bool
	getExecuted  bool
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

	return &state.GetResponse{
		Data:        nil,
		ETag:        nil,
		Metadata:    nil,
		ContentType: nil,
	}, nil
}

func (m *fakeDBaccess) Delete(req *state.DeleteRequest) error {
	return nil
}

func (m *fakeDBaccess) ExecuteMulti(sets []state.SetRequest, deletes []state.DeleteRequest) error {
	return nil
}

func (m *fakeDBaccess) Query(req *state.QueryRequest) (*state.QueryResponse, error) {
	return &state.QueryResponse{
		Results:  nil,
		Token:    "",
		Metadata: nil,
	}, nil
}

func (m *fakeDBaccess) Close() error {
	return nil
}

func (m *fakeDBaccess) Ping() error {
	return nil
}

// Proves that the Init method runs the init method.
func TestInitRunsDBAccessInit(t *testing.T) {
	t.Parallel()
	_, fake := createCockroachDBWithFake(t)
	assert.True(t, fake.initExecuted)
}

func TestMultiWithNoRequestsReturnsNil(t *testing.T) {
	t.Parallel()
	var operations []state.TransactionalStateOperation
	pgs := createCockroachDB(t)
	err := pgs.Multi(&state.TransactionalStateRequest{
		Operations: operations,
		Metadata:   nil,
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

	pgs := createCockroachDB(t)
	err := pgs.Multi(&state.TransactionalStateRequest{
		Operations: operations,
		Metadata:   nil,
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

	pgs := createCockroachDB(t)
	err := pgs.Multi(&state.TransactionalStateRequest{
		Operations: operations,
		Metadata:   nil,
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

	pgs := createCockroachDB(t)
	err := pgs.Multi(&state.TransactionalStateRequest{
		Operations: operations,
		Metadata:   nil,
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

	pgs := createCockroachDB(t)
	err := pgs.Multi(&state.TransactionalStateRequest{
		Operations: operations,
		Metadata:   nil,
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

	pgs := createCockroachDB(t)
	err := pgs.Multi(&state.TransactionalStateRequest{
		Operations: operations,
		Metadata:   nil,
	})
	assert.NotNil(t, err)
}

func createSetRequest() state.SetRequest {
	return state.SetRequest{
		Key:      randomKey(),
		Value:    randomJSON(),
		ETag:     nil,
		Metadata: nil,
		Options: state.SetStateOption{
			Concurrency: "",
			Consistency: "",
		},
		ContentType: nil,
	}
}

func createDeleteRequest() state.DeleteRequest {
	return state.DeleteRequest{
		Key:      randomKey(),
		ETag:     nil,
		Metadata: nil,
		Options: state.DeleteStateOption{
			Concurrency: "",
			Consistency: "",
		},
	}
}

func createCockroachDBWithFake(t *testing.T) (*CockroachDB, *fakeDBaccess) {
	t.Helper()

	pgs := createCockroachDB(t)
	fake, ok := pgs.dbaccess.(*fakeDBaccess)
	assert.True(t, ok)

	return pgs, fake
}

func createCockroachDB(t *testing.T) *CockroachDB {
	t.Helper()

	logger := logger.NewLogger("test")

	dba := &fakeDBaccess{
		logger:       logger,
		initExecuted: false,
		setExecuted:  false,
		getExecuted:  false,
	}

	pgs := internalNew(logger, dba)
	assert.NotNil(t, pgs)

	metadata := &state.Metadata{
		Properties: map[string]string{connectionStringKey: fakeConnectionString},
	}

	err := pgs.Init(*metadata)

	assert.Nil(t, err)
	assert.NotNil(t, pgs.dbaccess)

	return pgs
}
