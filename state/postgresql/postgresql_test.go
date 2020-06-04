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
	metadata     state.Metadata
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
	logger := logger.NewLogger("test")

	dba := &fakeDBaccess{
		logger: logger,
	}

	store := newPostgreSQLStateStore(logger, dba)
	assert.NotNil(t,store)

	metadata := &state.Metadata{
		Properties: map[string]string{connectionStringKey: fakeConnectionString},
	}

	err := store.Init(*metadata)

	assert.Nil(t, err)
	assert.NotNil(t, store.dbaccess)
	fake := store.dbaccess.(*fakeDBaccess)
	assert.True(t, fake.initExecuted)
}
