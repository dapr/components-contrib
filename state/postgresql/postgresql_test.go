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

func (m *fakeDBaccess) Logger() logger.Logger {
	return m.logger
}

func (m *fakeDBaccess) Metadata() state.Metadata {
	return m.metadata
}

func (m *fakeDBaccess) Close() error {
	return nil
}

// Proves that the Init method runs the init method
func TestInitRunsDBAccessInit(t *testing.T) {
	p := createNewStoreWithFakes()
	metadata := &state.Metadata{
		Properties: map[string]string{connectionStringKey: fakeConnectionString},
	}

	err := p.Init(*metadata)

	assert.Nil(t, err)
	assert.NotNil(t, p.dbaccess)
	fake := p.dbaccess.(*fakeDBaccess)
	assert.True(t, fake.initExecuted)
}

// Proves that PostgreSQL implements state.Store.
func TestPostgreSQLImplementsStore(t *testing.T) {
	assert.NotNil(t, createNewStoreWithFakes())
}

// Creates a new instance of PostreSQL with fakes to prevent real database calls.
func createNewStoreWithFakes() *PostgreSQL {
	logger := logger.NewLogger("test")
	return newPostgreSQLStateStore(logger, &fakeDBaccess{
		logger: logger,
	})
}
