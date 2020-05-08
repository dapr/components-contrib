// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package postgresql

import (
	"fmt"
	"database/sql"
	"github.com/dapr/components-contrib/state"
	"github.com/dapr/dapr/pkg/logger"
)

const (
	connectionStringKey = "connectionString"

	errMissingConnectionString = "missing connection string"
)

// PostgresDBAccess implements dbaccess
type PostgresDBAccess struct {
	logger				logger.Logger
	metadata			state.Metadata
	db					*sql.DB
	connectionString	string
	tableName			string
	schema				string
	bulkDeleteCommand	string
	upsertCommand		string
	getCommand			string
	deleteCommand		string
}

// NewPostgresDBAccess creates a new instance of postgresAccess
func NewPostgresDBAccess (logger logger.Logger, metadata state.Metadata) *PostgresDBAccess {
	return &PostgresDBAccess{
		logger: logger,
		metadata: metadata,
	}
}

// Init sets up PostgreSQL connection and ensures that the state table exists
func (p *PostgresDBAccess) Init() (error){
	if val, ok := p.metadata.Properties[connectionStringKey]; ok && val != "" {
		p.connectionString = val
	} else {
		return fmt.Errorf(errMissingConnectionString)
	}

	db, err := sql.Open("pgx", p.connectionString)
	if err != nil {
		return err
	}

	p.db = db
			
	return nil;
}