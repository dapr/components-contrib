// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

// PostgreSQL implementation notes:
// - sql.DB methods provide limited support for parameter substitution, so in 
//   some cases fmt.Sprintf is used to replace values, e.g. table names.
// - TODO: Insert/Update time
// - TODO: ETag for concurrency
// - TODO: Would the connection string change after Init is called?

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
	tableName = "state"
)

// PostgresDBAccess implements dbaccess
type PostgresDBAccess struct {
	logger				logger.Logger
	db					*sql.DB
	connectionString	string
}

// NewPostgresDBAccess creates a new instance of postgresAccess
func NewPostgresDBAccess (logger logger.Logger) *PostgresDBAccess {
	return &PostgresDBAccess{
		logger: logger,
	}
}

// Logger returns an instance of logger.Logger
func (p *PostgresDBAccess) Logger() logger.Logger {
	return p.logger
}

// Init sets up PostgreSQL connection and ensures that the state table exists
func (p *PostgresDBAccess) Init(metadata *state.Metadata) (error) {
	if val, ok := metadata.Properties[connectionStringKey]; ok && val != "" {
		p.connectionString = val
	} else {
		return fmt.Errorf(errMissingConnectionString)
	}

	db, err := sql.Open("pgx", p.connectionString)
	if err != nil {
		return err
	}

	p.db = db
	
	pingErr := db.Ping()
	if(pingErr != nil) {
		return pingErr
	}

	err = p.ensureStateTable()
	if err != nil {
		return err
	}
			
	return nil;
}

// Set makes an insert or update to the database.
func (p *PostgresDBAccess) Set(req *state.SetRequest) (error) {
	
	// Sprintf is required for table name because sql.DB does not substitue parameters for table names.
	result, err := p.db.Exec(fmt.Sprintf(
		`INSERT INTO %s (key, value) VALUES ($1, $2)
		 ON CONFLICT (key) DO UPDATE SET value = $2;`, 
				tableName), req.Key, req.Value)

	p.logger.Debug(result)
	return err
}

// Get returns data from the database.
func (p *PostgresDBAccess) Get(req *state.GetRequest) (*state.GetResponse, error) {
	
	var value string
	err := p.db.QueryRow("SELECT value FROM state WHERE key = $1", req.Key).Scan(&value)
	if err != nil {
		return nil, err
	}

	response := &state.GetResponse{
		Data: []byte(value),
		ETag: "",
		Metadata: req.Metadata,
	}

	return response, nil
}

func (p *PostgresDBAccess) ensureStateTable() (error) {
	var exists bool = false
	err := p.db.QueryRow("SELECT EXISTS (SELECT FROM pg_tables where tablename = $1)", tableName).Scan(&exists)
	if err != nil {
		return err
	}

	if !exists {
		createTable := fmt.Sprintf(`CREATE TABLE %s (
									key varchar(200) NOT NULL PRIMARY KEY,
									value json NOT NULL);`, tableName)
		_, err = p.db.Exec(createTable)
		if err != nil {
			return err
		}	
	}

	return nil
}
