// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package postgresql

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"

	"github.com/agrea/ptr"

	"github.com/dapr/components-contrib/state"
	"github.com/dapr/components-contrib/state/utils"
	"github.com/dapr/kit/logger"

	// Blank import for the underlying PostgreSQL driver.
	_ "github.com/jackc/pgx/v4/stdlib"
)

const (
	connectionStringKey        = "connectionString"
	errMissingConnectionString = "missing connection string"
	tableName                  = "state"
)

// postgresDBAccess implements dbaccess.
type postgresDBAccess struct {
	logger           logger.Logger
	metadata         state.Metadata
	db               *sql.DB
	connectionString string
}

// newPostgresDBAccess creates a new instance of postgresAccess.
func newPostgresDBAccess(logger logger.Logger) *postgresDBAccess {
	logger.Debug("Instantiating new PostgreSQL state store")

	return &postgresDBAccess{
		logger: logger,
	}
}

// Init sets up PostgreSQL connection and ensures that the state table exists.
func (p *postgresDBAccess) Init(metadata state.Metadata) error {
	p.logger.Debug("Initializing PostgreSQL state store")
	p.metadata = metadata

	if val, ok := metadata.Properties[connectionStringKey]; ok && val != "" {
		p.connectionString = val
	} else {
		p.logger.Error("Missing postgreSQL connection string")

		return fmt.Errorf(errMissingConnectionString)
	}

	db, err := sql.Open("pgx", p.connectionString)
	if err != nil {
		p.logger.Error(err)

		return err
	}

	p.db = db

	pingErr := db.Ping()
	if pingErr != nil {
		return pingErr
	}

	err = p.ensureStateTable(tableName)
	if err != nil {
		return err
	}

	return nil
}

// Set makes an insert or update to the database.
func (p *postgresDBAccess) Set(req *state.SetRequest) error {
	return state.SetWithOptions(p.setValue, req)
}

// setValue is an internal implementation of set to enable passing the logic to state.SetWithRetries as a func.
func (p *postgresDBAccess) setValue(req *state.SetRequest) error {
	p.logger.Debug("Setting state value in PostgreSQL")

	err := state.CheckRequestOptions(req.Options)
	if err != nil {
		return err
	}

	if req.Key == "" {
		return fmt.Errorf("missing key in set operation")
	}

	// Convert to json string
	bt, _ := utils.Marshal(req.Value, json.Marshal)
	value := string(bt)

	var result sql.Result

	// Sprintf is required for table name because sql.DB does not substitute parameters for table names.
	// Other parameters use sql.DB parameter substitution.
	if req.ETag == nil {
		result, err = p.db.Exec(fmt.Sprintf(
			`INSERT INTO %s (key, value) VALUES ($1, $2)
			ON CONFLICT (key) DO UPDATE SET value = $2, updatedate = NOW();`,
			tableName), req.Key, value)
	} else {
		// Convert req.ETag to integer for postgres compatibility
		var etag int
		etag, err = strconv.Atoi(*req.ETag)
		if err != nil {
			return state.NewETagError(state.ETagInvalid, err)
		}

		// When an etag is provided do an update - no insert
		result, err = p.db.Exec(fmt.Sprintf(
			`UPDATE %s SET value = $1, updatedate = NOW() 
			 WHERE key = $2 AND xmin = $3;`,
			tableName), value, req.Key, etag)
	}

	return p.returnSingleDBResult(result, err)
}

// Get returns data from the database. If data does not exist for the key an empty state.GetResponse will be returned.
func (p *postgresDBAccess) Get(req *state.GetRequest) (*state.GetResponse, error) {
	p.logger.Debug("Getting state value from PostgreSQL")
	if req.Key == "" {
		return nil, fmt.Errorf("missing key in get operation")
	}

	var value string
	var etag int
	err := p.db.QueryRow(fmt.Sprintf("SELECT value, xmin as etag FROM %s WHERE key = $1", tableName), req.Key).Scan(&value, &etag)
	if err != nil {
		// If no rows exist, return an empty response, otherwise return the error.
		if err == sql.ErrNoRows {
			return &state.GetResponse{}, nil
		}

		return nil, err
	}

	response := &state.GetResponse{
		Data:     []byte(value),
		ETag:     ptr.String(strconv.Itoa(etag)),
		Metadata: req.Metadata,
	}

	return response, nil
}

// Delete removes an item from the state store.
func (p *postgresDBAccess) Delete(req *state.DeleteRequest) error {
	return state.DeleteWithOptions(p.deleteValue, req)
}

// deleteValue is an internal implementation of delete to enable passing the logic to state.DeleteWithRetries as a func.
func (p *postgresDBAccess) deleteValue(req *state.DeleteRequest) error {
	p.logger.Debug("Deleting state value from PostgreSQL")
	if req.Key == "" {
		return fmt.Errorf("missing key in delete operation")
	}

	var result sql.Result
	var err error

	if req.ETag == nil {
		result, err = p.db.Exec("DELETE FROM state WHERE key = $1", req.Key)
	} else {
		// Convert req.ETag to integer for postgres compatibility
		etag, conversionError := strconv.Atoi(*req.ETag)
		if conversionError != nil {
			return state.NewETagError(state.ETagInvalid, err)
		}

		result, err = p.db.Exec("DELETE FROM state WHERE key = $1 and xmin = $2", req.Key, etag)
	}

	return p.returnSingleDBResult(result, err)
}

func (p *postgresDBAccess) ExecuteMulti(sets []state.SetRequest, deletes []state.DeleteRequest) error {
	p.logger.Debug("Executing multiple PostgreSQL operations")
	tx, err := p.db.Begin()
	if err != nil {
		return err
	}

	if len(deletes) > 0 {
		for _, d := range deletes {
			da := d // Fix for gosec  G601: Implicit memory aliasing in for loop.
			err = p.Delete(&da)
			if err != nil {
				tx.Rollback()

				return err
			}
		}
	}

	if len(sets) > 0 {
		for _, s := range sets {
			sa := s // Fix for gosec  G601: Implicit memory aliasing in for loop.
			err = p.Set(&sa)
			if err != nil {
				tx.Rollback()

				return err
			}
		}
	}

	err = tx.Commit()

	return err
}

// Verifies that the sql.Result affected only one row and no errors exist.
func (p *postgresDBAccess) returnSingleDBResult(result sql.Result, err error) error {
	if err != nil {
		p.logger.Debug(err)

		return err
	}

	rowsAffected, resultErr := result.RowsAffected()

	if resultErr != nil {
		p.logger.Error(resultErr)

		return resultErr
	}

	if rowsAffected == 0 {
		noRowsErr := state.NewETagError(state.ETagMismatch, err)
		p.logger.Error(noRowsErr)

		return noRowsErr
	}

	if rowsAffected > 1 {
		tooManyRowsErr := errors.New("database operation failed: more than one row affected, expected one")
		p.logger.Error(tooManyRowsErr)

		return tooManyRowsErr
	}

	return nil
}

// Close implements io.Close.
func (p *postgresDBAccess) Close() error {
	if p.db != nil {
		return p.db.Close()
	}

	return nil
}

func (p *postgresDBAccess) ensureStateTable(stateTableName string) error {
	exists, err := tableExists(p.db, stateTableName)
	if err != nil {
		return err
	}

	if !exists {
		p.logger.Info("Creating PostgreSQL state table")
		createTable := fmt.Sprintf(`CREATE TABLE %s (
									key text NOT NULL PRIMARY KEY,
									value json NOT NULL,
									insertdate TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
									updatedate TIMESTAMP WITH TIME ZONE NULL);`, stateTableName)
		_, err = p.db.Exec(createTable)
		if err != nil {
			return err
		}
	}

	return nil
}

func tableExists(db *sql.DB, tableName string) (bool, error) {
	var exists bool = false
	err := db.QueryRow("SELECT EXISTS (SELECT FROM pg_tables where tablename = $1)", tableName).Scan(&exists)

	return exists, err
}
