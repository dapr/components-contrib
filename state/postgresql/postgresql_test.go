// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------
package postgresql

import (
	"testing"

	"github.com/dapr/components-contrib/state"
	"github.com/dapr/dapr/pkg/logger"
	"github.com/stretchr/testify/assert"
)

const (
	fakeConnectionString = "not a real connection"
)

// Fake implementation of interface postgressql.dbaccess
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

// Proves that the Init method runs the init method
func TestInitRunsDBAccessInit(t *testing.T) {
	t.Parallel()
	_, fake := createPostgreSQLWithFake(t)
	assert.True(t, fake.initExecuted)
}

func TestMultiWithNoRequestsReturnsNil(t *testing.T) {
	t.Parallel()
	var multiRequest []state.TransactionalRequest
	pgs := createPostgreSQL(t)
	err := pgs.Multi(multiRequest)
	assert.Nil(t, err)
}

func TestInvalidMultiAction(t *testing.T) {
	t.Parallel()
	var multiRequest []state.TransactionalRequest

	multiRequest = append(multiRequest, state.TransactionalRequest{
		Operation: "Something invalid",
		Request:   createSetRequest(),
	})

	pgs := createPostgreSQL(t)
	err := pgs.Multi(multiRequest)
	assert.NotNil(t, err)
}

func TestValidSetRequest(t *testing.T) {
	t.Parallel()
	var multiRequest []state.TransactionalRequest

	multiRequest = append(multiRequest, state.TransactionalRequest{
		Operation: state.Upsert,
		Request:   createSetRequest(),
	})

	pgs := createPostgreSQL(t)
	err := pgs.Multi(multiRequest)
	assert.Nil(t, err)
}

func TestInvalidMultiSetRequest(t *testing.T) {
	t.Parallel()
	var multiRequest []state.TransactionalRequest

	multiRequest = append(multiRequest, state.TransactionalRequest{
		Operation: state.Upsert,
		Request:   createDeleteRequest(), // Delete request is not valid for Upsert operation
	})

	pgs := createPostgreSQL(t)
	err := pgs.Multi(multiRequest)
	assert.NotNil(t, err)
}

func TestValidMultiDeleteRequest(t *testing.T) {
	t.Parallel()
	var multiRequest []state.TransactionalRequest

	multiRequest = append(multiRequest, state.TransactionalRequest{
		Operation: state.Delete,
		Request:   createDeleteRequest(),
	})

	pgs := createPostgreSQL(t)
	err := pgs.Multi(multiRequest)
	assert.Nil(t, err)
}

func TestInvalidMultiDeleteRequest(t *testing.T) {
	t.Parallel()
	var multiRequest []state.TransactionalRequest

	multiRequest = append(multiRequest, state.TransactionalRequest{
		Operation: state.Delete,
		Request:   createSetRequest(), // Set request is not valid for Delete operation
	})

	pgs := createPostgreSQL(t)
	err := pgs.Multi(multiRequest)
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

func createPostgreSQLWithFake(t *testing.T) (*PostgreSQL, *fakeDBaccess) {
	pgs := createPostgreSQL(t)
	fake := pgs.dbaccess.(*fakeDBaccess)
	return pgs, fake
}

func createPostgreSQL(t *testing.T) *PostgreSQL {
	logger := logger.NewLogger("test")

	dba := &fakeDBaccess{
		logger: logger,
	}

	pgs := newPostgreSQLStateStore(logger, dba)
	assert.NotNil(t, pgs)

	metadata := &state.Metadata{
		Properties: map[string]string{connectionStringKey: fakeConnectionString},
	}

	err := pgs.Init(*metadata)

	assert.Nil(t, err)
	assert.NotNil(t, pgs.dbaccess)

	return pgs
}
