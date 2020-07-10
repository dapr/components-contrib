// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package postgresql

import (
	"fmt"

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
	return p.dbaccess.ExecuteMulti(nil, req)
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
	return p.dbaccess.ExecuteMulti(req, nil)
}

// Multi handles multiple transactions. Implements TransactionalStore.
func (p *PostgreSQL) Multi(reqs []state.TransactionalRequest) error {
	var deletes []state.DeleteRequest
	var sets []state.SetRequest
	for _, req := range reqs {
		switch req.Operation {
		case state.Upsert:
			if setReq, ok := req.Request.(state.SetRequest); ok {
				sets = append(sets, setReq)
			} else {
				return fmt.Errorf("expecting set request")
			}

		case state.Delete:
			if delReq, ok := req.Request.(state.DeleteRequest); ok {
				deletes = append(deletes, delReq)
			} else {
				return fmt.Errorf("expecting delete request")
			}

		default:
			return fmt.Errorf("unsupported operation: %s", req.Operation)
		}
	}

	if len(sets) > 0 || len(deletes) > 0 {
		return p.dbaccess.ExecuteMulti(sets, deletes)
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
