// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

// PostgreSQL implementation notes:
// - TODO: ETag for concurrency
// - TODO: Implement consistency flags
// - TODO: Implement transactions
// - TODO: Rename the dbaccess interface and the variable that stores it
// - TODO: Run linter
// - TODO: Verify that postgresql naming conventions were followed
// - TODO: Implement benchmark tests
// - TODO: Find out constraints on key size

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
	metadata 			state.Metadata
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

// Metadata returns an instance of logger.Logger
func (p *PostgresDBAccess) Metadata() state.Metadata {
	return p.metadata
}

// Init sets up PostgreSQL connection and ensures that the state table exists
func (p *PostgresDBAccess) Init(metadata state.Metadata) (error) {
	p.metadata = metadata

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
	
	// Sprintf is required for table name because sql.DB does not substitue parameters for table names,
	// however, sql.DB parameter substitution is also used to prevent injection attacks.
	_, err := p.db.Exec(fmt.Sprintf(
		`INSERT INTO %s (key, value) VALUES ($1, $2)
		 ON CONFLICT (key) DO UPDATE SET value = $2, updatedate = NOW();`, 
				tableName), req.Key, req.Value)

	return err
}

// Get returns data from the database. If data does not exist for the key an empty state.GetResponse will be returned.
func (p *PostgresDBAccess) Get(req *state.GetRequest) (*state.GetResponse, error) {
	
	var value string
	err := p.db.QueryRow(fmt.Sprintf("SELECT value FROM %s WHERE key = $1", tableName), req.Key).Scan(&value)
	if err != nil {
		// If no rows exist, return an empty response, otherwise return the error.
		if err == sql.ErrNoRows {
			return &state.GetResponse{}, nil
		}
		return nil, err
	}
	
	response := &state.GetResponse{
		Data: []byte(value),
		ETag: "",
		Metadata: req.Metadata,
	}

	return response, nil
}

// Delete removes an item from the state store.
func (p *PostgresDBAccess) Delete(req *state.DeleteRequest) (error) {

	_, err := p.db.Exec("DELETE FROM state WHERE key = $1", req.Key)
	if err != nil {
		return err
	}

	return nil
} 

// Close implements io.Close
func (p *PostgresDBAccess) Close() error {
	if p.db != nil {
		return p.db.Close()
	}

	return nil
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
									value json NOT NULL,
									insertdate TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
									updatedate TIMESTAMP WITH TIME ZONE NULL);`, tableName)
		_, err = p.db.Exec(createTable)
		if err != nil {
			return err
		}	
	}

	return nil
}
