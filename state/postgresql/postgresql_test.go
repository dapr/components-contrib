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

const(
	fakeConnectionString = "not a real connection"
)

type fakeDBaccess struct {
	initExecuted bool
}

func (m *fakeDBaccess) Init() (error){
	m.initExecuted = true
	return nil;
}

// Proves that the Init method runs the init method
func TestInitRunsDBAccessInit(t *testing.T) {

	p := createNewStoreWithFakes()
	metadata := state.Metadata{
		Properties: map[string]string{connectionStringKey: fakeConnectionString},
	}

	err := p.Init(metadata)
	
	assert.Nil(t, err)
	assert.NotNil(t, p.dbaccess)
	fake := p.dbaccess.(*fakeDBaccess)
	assert.True(t, fake.initExecuted)
}

// Creates a new instance of PostreSQL with fakes to prevent real database calls.
func createNewStoreWithFakes() PostgreSQL {
	return *NewPostgreSQLStateStore(logger.NewLogger("test"), &fakeDBaccess{})
}
