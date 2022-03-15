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
	logger         logger.Logger
	initExecuted   bool
	setExecuted    bool
	getExecuted    bool
	deleteExecuted bool
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
	m.deleteExecuted = true

	return nil
}

func (m *fakeDBaccess) BulkSet(req []state.SetRequest) error {
	return nil
}

func (m *fakeDBaccess) BulkDelete(req []state.DeleteRequest) error {
	return nil
}

func (m *fakeDBaccess) ExecuteMulti(req *state.TransactionalStateRequest) error {
	return nil
}

func (m *fakeDBaccess) Query(req *state.QueryRequest) (*state.QueryResponse, error) {
	return nil, nil
}

func (m *fakeDBaccess) Close() error {
	return nil
}

// Proves that the Init method runs the init method.
func TestInitRunsDBAccessInit(t *testing.T) {
	t.Parallel()
	_, fake := createPostgreSQLWithFake(t)
	assert.True(t, fake.initExecuted)
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
