// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package postgresql

import (
	//"fmt"
	"github.com/dapr/components-contrib/state"
	"github.com/dapr/dapr/pkg/logger"
)

// PostgreSQL state store
type PostgreSQL struct {
	logger		logger.Logger
	dbaccess	dbAccess
}

// NewPostgreSQLStateStore creates a new instance of a PostgreSQL state store
func NewPostgreSQLStateStore(dba dbAccess) *PostgreSQL {
	return &PostgreSQL{
		logger:		dba.Logger(),
		dbaccess:	dba,
	}
}

// Init initializes the SQL server state store
func (p *PostgreSQL) Init(metadata *state.Metadata) error {
	return p.dbaccess.Init(metadata)
}

// Delete removes an entity from the store
func (p *PostgreSQL) Delete(req *state.DeleteRequest) error {
	return nil
}

// BulkDelete removes multiple entries from the store
func (p *PostgreSQL) BulkDelete(req []state.DeleteRequest) error {
	for _, re := range req {
		err := p.Delete(&re)
		if err != nil {
			return err
		}
	}

	return nil
}

// Get returns an entity from store
func (p *PostgreSQL) Get(req *state.GetRequest) (*state.GetResponse, error) {
	return p.dbaccess.Get(req)
}

// Set adds/updates an entity on store
func (p *PostgreSQL) Set(req *state.SetRequest) error {
	return p.dbaccess.Set(req);
}

// BulkSet adds/updates multiple entities on store
func (p *PostgreSQL) BulkSet(req []state.SetRequest) error {
	for _, s := range req {
		err := p.Set(&s)
		if err != nil {
			return err
		}
	}

	return nil
}
