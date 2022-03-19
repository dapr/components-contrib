/*
Copyright 2022 The Dapr Authors
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

package cockroachdb

import (
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"

	"github.com/agrea/ptr"

	"github.com/dapr/components-contrib/state"
	"github.com/dapr/components-contrib/state/query"
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

// cockroachDBAccess implements dbaccess.
type cockroachDBAccess struct {
	logger           logger.Logger
	metadata         state.Metadata
	db               *sql.DB
	connectionString string
}

// newCockroachDBAccess creates a new instance of cockroachDBAccess.
func newCockroachDBAccess(logger logger.Logger) *cockroachDBAccess {
	logger.Debug("Instantiating new CockroachDB state store")

	return &cockroachDBAccess{
		logger: logger,
		metadata: state.Metadata{
			Properties: map[string]string{},
		},
		db:               nil,
		connectionString: "",
	}
}

// Init sets up CockroachDB connection and ensures that the state table exists.
func (p *cockroachDBAccess) Init(metadata state.Metadata) error {
	p.logger.Debug("Initializing CockroachDB state store")

	p.metadata = metadata

	if val, ok := metadata.Properties[connectionStringKey]; ok && val != "" {
		p.connectionString = val
	} else {
		p.logger.Error("Missing CockroachDB connection string")

		return fmt.Errorf(errMissingConnectionString)
	}

	databaseConn, err := sql.Open("pgx", p.connectionString)
	if err != nil {
		p.logger.Error(err)

		return err
	}

	p.db = databaseConn

	if err = databaseConn.Ping(); err != nil {
		return err
	}

	if err = p.ensureStateTable(tableName); err != nil {
		return err
	}

	return nil
}

// Set makes an insert or update to the database.
func (p *cockroachDBAccess) Set(req *state.SetRequest) error {
	return state.SetWithOptions(p.setValue, req)
}

// setValue is an internal implementation of set to enable passing the logic to state.SetWithRetries as a func.
func (p *cockroachDBAccess) setValue(req *state.SetRequest) error {
	p.logger.Debug("Setting state value in CockroachDB")

	value, isBinary, err := validateAndReturnValue(req)
	if err != nil {
		return err
	}

	var result sql.Result

	// Sprintf is required for table name because sql.DB does not substitute parameters for table names.
	// Other parameters use sql.DB parameter substitution.
	if req.ETag == nil {
		result, err = p.db.Exec(fmt.Sprintf(
			`INSERT INTO %s (key, value, isbinary, etag) VALUES ($1, $2, $3, 1)
			ON CONFLICT (key) DO UPDATE SET value = $2, isbinary = $3, updatedate = NOW(), etag = EXCLUDED.etag + 1;`,
			tableName), req.Key, value, isBinary)
	} else {
		var etag64 uint64
		etag64, err = strconv.ParseUint(*req.ETag, 10, 32)
		if err != nil {
			return state.NewETagError(state.ETagInvalid, err)
		}
		etag := uint32(etag64)

		// When an etag is provided do an update - no insert.
		result, err = p.db.Exec(fmt.Sprintf(
			`UPDATE %s SET value = $1, isbinary = $2, updatedate = NOW(), etag = etag + 1
			 WHERE key = $3 AND etag = $4;`,
			tableName), value, isBinary, req.Key, etag)
	}

	if err != nil {
		if req.ETag != nil && *req.ETag != "" {
			return state.NewETagError(state.ETagMismatch, err)
		}

		return err
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}

	if rows != 1 {
		return fmt.Errorf("no item was updated")
	}

	return nil
}

// Get returns data from the database. If data does not exist for the key an empty state.GetResponse will be returned.
func (p *cockroachDBAccess) Get(req *state.GetRequest) (*state.GetResponse, error) {
	p.logger.Debug("Getting state value from CockroachDB")
	if req.Key == "" {
		return nil, fmt.Errorf("missing key in get operation")
	}

	var value string
	var isBinary bool
	var etag int
	err := p.db.QueryRow(fmt.Sprintf("SELECT value, isbinary, etag FROM %s WHERE key = $1", tableName), req.Key).Scan(&value, &isBinary, &etag)
	if err != nil {
		// If no rows exist, return an empty response, otherwise return the error.
		if errors.Is(err, sql.ErrNoRows) {
			return &state.GetResponse{
				Data:        nil,
				ETag:        nil,
				Metadata:    nil,
				ContentType: nil,
			}, nil
		}

		return nil, err
	}

	if isBinary {
		var dataS string
		var data []byte

		if err = json.Unmarshal([]byte(value), &dataS); err != nil {
			return nil, err
		}

		if data, err = base64.StdEncoding.DecodeString(dataS); err != nil {
			return nil, err
		}

		return &state.GetResponse{
			Data:        data,
			ETag:        ptr.String(strconv.Itoa(etag)),
			Metadata:    req.Metadata,
			ContentType: nil,
		}, nil
	}

	return &state.GetResponse{
		Data:        []byte(value),
		ETag:        ptr.String(strconv.Itoa(etag)),
		Metadata:    req.Metadata,
		ContentType: nil,
	}, nil
}

// Delete removes an item from the state store.
func (p *cockroachDBAccess) Delete(req *state.DeleteRequest) error {
	return state.DeleteWithOptions(p.deleteValue, req)
}

// deleteValue is an internal implementation of delete to enable passing the logic to state.DeleteWithRetries as a func.
func (p *cockroachDBAccess) deleteValue(req *state.DeleteRequest) error {
	p.logger.Debug("Deleting state value from CockroachDB")
	if req.Key == "" {
		return fmt.Errorf("missing key in delete operation")
	}

	var result sql.Result
	var err error

	if req.ETag == nil {
		result, err = p.db.Exec("DELETE FROM state WHERE key = $1", req.Key)
	} else {
		var etag64 uint64
		etag64, err = strconv.ParseUint(*req.ETag, 10, 32)
		if err != nil {
			return state.NewETagError(state.ETagInvalid, err)
		}
		etag := uint32(etag64)

		result, err = p.db.Exec("DELETE FROM state WHERE key = $1 and etag = $2", req.Key, etag)
	}

	if err != nil {
		return err
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}

	if rows != 1 && req.ETag != nil && *req.ETag != "" {
		return state.NewETagError(state.ETagMismatch, nil)
	}

	return nil
}

func (p *cockroachDBAccess) ExecuteMulti(sets []state.SetRequest, deletes []state.DeleteRequest) error {
	p.logger.Debug("Executing multiple CockroachDB operations")
	transaction, err := p.db.Begin()
	if err != nil {
		return err
	}

	if len(deletes) > 0 {
		for _, d := range deletes {
			da := d // Fix for gosec  G601: Implicit memory aliasing in for loop.
			err = p.Delete(&da)
			if err != nil {
				transaction.Rollback()

				return err
			}
		}
	}

	if len(sets) > 0 {
		for _, s := range sets {
			sa := s // Fix for gosec  G601: Implicit memory aliasing in for loop.
			err = p.Set(&sa)
			if err != nil {
				transaction.Rollback()

				return err
			}
		}
	}

	err = transaction.Commit()

	return err
}

// Query executes a query against store.
func (p *cockroachDBAccess) Query(req *state.QueryRequest) (*state.QueryResponse, error) {
	p.logger.Debug("Getting query value from CockroachDB")

	stateQuery := &Query{
		query:  "",
		params: []interface{}{},
		limit:  0,
		skip:   ptr.Int64(0),
	}
	qbuilder := query.NewQueryBuilder(stateQuery)
	if err := qbuilder.BuildQuery(&req.Query); err != nil {
		return &state.QueryResponse{
			Results:  []state.QueryItem{},
			Token:    "",
			Metadata: map[string]string{},
		}, err
	}

	p.logger.Debug("Query: " + stateQuery.query)

	data, token, err := stateQuery.execute(p.logger, p.db)
	if err != nil {
		return &state.QueryResponse{
			Results:  []state.QueryItem{},
			Token:    "",
			Metadata: map[string]string{},
		}, err
	}

	return &state.QueryResponse{
		Results:  data,
		Token:    token,
		Metadata: map[string]string{},
	}, nil
}

// Ping implements database ping.
func (p *cockroachDBAccess) Ping() error {
	return p.db.Ping()
}

// Close implements io.Close.
func (p *cockroachDBAccess) Close() error {
	if p.db != nil {
		return p.db.Close()
	}

	return nil
}

func (p *cockroachDBAccess) ensureStateTable(stateTableName string) error {
	exists, err := tableExists(p.db, stateTableName)
	if err != nil {
		return err
	}

	if !exists {
		p.logger.Info("Creating CockroachDB state table")
		createTable := fmt.Sprintf(`CREATE TABLE %s (
									key text NOT NULL PRIMARY KEY,
									value jsonb NOT NULL,
									isbinary boolean NOT NULL,
									etag INT,
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
	exists := false
	err := db.QueryRow("SELECT EXISTS (SELECT * FROM pg_tables where tablename = $1)", tableName).Scan(&exists)

	return exists, err
}

func validateAndReturnValue(request *state.SetRequest) (value string, isBinary bool, err error) {
	err = state.CheckRequestOptions(request.Options)
	if err != nil {
		return "", false, err
	}

	if request.Key == "" {
		return "", false, fmt.Errorf("missing key in set operation")
	}

	if v, ok := request.Value.(string); ok && v == "" {
		return "", false, fmt.Errorf("empty string is not allowed in set operation")
	}

	requestValue := request.Value
	byteArray, isBinary := request.Value.([]uint8)
	if isBinary {
		requestValue = base64.StdEncoding.EncodeToString(byteArray)
	}

	// Convert to json string.
	bt, _ := utils.Marshal(requestValue, json.Marshal)

	return string(bt), isBinary, nil
}
