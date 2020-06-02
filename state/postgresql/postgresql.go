// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package postgresql

import (
	"github.com/dapr/components-contrib/state"
	"github.com/dapr/dapr/pkg/logger"
)

// PostgreSQL state store
type PostgreSQL struct {
	logger   logger.Logger
	dbaccess dbAccess
}

// NewPostgreSQLStateStore creates a new instance of PostgreSQL state store
func NewPostgreSQLStateStore(logger logger.Logger) *PostgreSQL {
	dba := newPostgresDBAccess(logger)
	return newPostgreSQLStateStore(logger, dba)
}

// newPostgreSQLStateStore creates a newPostgreSQLStateStore instance of a PostgreSQL state store.
// This unexported constructor allows injecting a dbAccess instance for unit testing.
func newPostgreSQLStateStore(logger logger.Logger, dba dbAccess) *PostgreSQL {
	return &PostgreSQL{
		logger:   logger,
		dbaccess: dba,
	}
}

// Init initializes the SQL server state store
func (p *PostgreSQL) Init(metadata state.Metadata) error {
	return p.dbaccess.Init(metadata)
}

// Delete removes an entity from the store
func (p *PostgreSQL) Delete(req *state.DeleteRequest) error {
	return p.dbaccess.Delete(req)
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
	return p.dbaccess.Set(req)
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

// Close implements io.Closer
func (p *PostgreSQL) Close() error {
	if p.dbaccess != nil {
		return p.dbaccess.Close()
	}

	return nil
}
