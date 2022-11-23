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
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/state"
	"github.com/dapr/components-contrib/state/query"
	"github.com/dapr/components-contrib/state/utils"
	"github.com/dapr/kit/logger"
	"github.com/dapr/kit/ptr"

	// Blank import for the underlying PostgreSQL driver.
	_ "github.com/jackc/pgx/v5/stdlib"
)

const (
	connectionStringKey        = "connectionString"
	errMissingConnectionString = "missing connection string"
	defaultTableName           = "state"
)

// postgresDBAccess implements dbaccess.
type postgresDBAccess struct {
	logger           logger.Logger
	metadata         postgresMetadataStruct
	db               *sql.DB
	connectionString string
	tableName        string
}

// newPostgresDBAccess creates a new instance of postgresAccess.
func newPostgresDBAccess(logger logger.Logger) *postgresDBAccess {
	logger.Debug("Instantiating new PostgreSQL state store")

	return &postgresDBAccess{
		logger: logger,
	}
}

type postgresMetadataStruct struct {
	ConnectionString      string
	ConnectionMaxIdleTime time.Duration
	TableName             string
}

// Init sets up PostgreSQL connection and ensures that the state table exists.
func (p *postgresDBAccess) Init(meta state.Metadata) error {
	p.logger.Debug("Initializing PostgreSQL state store")
	m := postgresMetadataStruct{
		TableName: defaultTableName,
	}
	err := metadata.DecodeMetadata(meta.Properties, &m)
	if err != nil {
		return err
	}
	p.metadata = m

	if m.ConnectionString == "" {
		p.logger.Error("Missing postgreSQL connection string")

		return errors.New(errMissingConnectionString)
	}
	p.connectionString = m.ConnectionString

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

	p.db.SetConnMaxIdleTime(m.ConnectionMaxIdleTime)
	if err != nil {
		return err
	}

	err = p.ensureStateTable(m.TableName)
	if err != nil {
		return err
	}
	p.tableName = m.TableName

	return nil
}

// Set makes an insert or update to the database.
func (p *postgresDBAccess) Set(req *state.SetRequest) error {
	p.logger.Debug("Setting state value in PostgreSQL")

	err := state.CheckRequestOptions(req.Options)
	if err != nil {
		return err
	}

	if req.Key == "" {
		return errors.New("missing key in set operation")
	}

	if v, ok := req.Value.(string); ok && v == "" {
		return errors.New("empty string is not allowed in set operation")
	}

	v := req.Value
	byteArray, isBinary := req.Value.([]uint8)
	if isBinary {
		v = base64.StdEncoding.EncodeToString(byteArray)
	}

	// Convert to json string
	bt, _ := utils.Marshal(v, json.Marshal)
	value := string(bt)

	var result sql.Result

	// Sprintf is required for table name because sql.DB does not substitute parameters for table names.
	// Other parameters use sql.DB parameter substitution.
	if req.Options.Concurrency == state.FirstWrite && (req.ETag == nil || *req.ETag == "") {
		result, err = p.db.Exec(fmt.Sprintf(
			`INSERT INTO %s (key, value, isbinary) VALUES ($1, $2, $3);`,
			p.tableName), req.Key, value, isBinary)
	} else if req.ETag == nil || *req.ETag == "" {
		result, err = p.db.Exec(fmt.Sprintf(
			`INSERT INTO %s (key, value, isbinary) VALUES ($1, $2, $3)
			ON CONFLICT (key) DO UPDATE SET value = $2, isbinary = $3, updatedate = NOW();`,
			p.tableName), req.Key, value, isBinary)
	} else {
		// Convert req.ETag to uint32 for postgres XID compatibility
		var etag64 uint64
		etag64, err = strconv.ParseUint(*req.ETag, 10, 32)
		if err != nil {
			return state.NewETagError(state.ETagInvalid, err)
		}
		etag := uint32(etag64)

		// When an etag is provided do an update - no insert
		result, err = p.db.Exec(fmt.Sprintf(
			`UPDATE %s SET value = $1, isbinary = $2, updatedate = NOW()
			 WHERE key = $3 AND xmin = $4;`,
			p.tableName), value, isBinary, req.Key, etag)
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
		return errors.New("no item was updated")
	}

	return nil
}

func (p *postgresDBAccess) BulkSet(req []state.SetRequest) error {
	p.logger.Debug("Executing BulkSet request")
	tx, err := p.db.Begin()
	if err != nil {
		return err
	}

	if len(req) > 0 {
		for _, s := range req {
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

// Get returns data from the database. If data does not exist for the key an empty state.GetResponse will be returned.
func (p *postgresDBAccess) Get(req *state.GetRequest) (*state.GetResponse, error) {
	p.logger.Debug("Getting state value from PostgreSQL")
	if req.Key == "" {
		return nil, errors.New("missing key in get operation")
	}

	var (
		value    []byte
		isBinary bool
		etag     uint64 // Postgres uses uint32, but FormatUint requires uint64, so using uint64 directly to avoid re-allocations
	)
	err := p.db.QueryRow(fmt.Sprintf("SELECT value, isbinary, xmin as etag FROM %s WHERE key = $1", p.tableName), req.Key).Scan(&value, &isBinary, &etag)
	if err != nil {
		// If no rows exist, return an empty response, otherwise return the error.
		if err == sql.ErrNoRows {
			return &state.GetResponse{}, nil
		}
		return nil, err
	}

	if isBinary {
		var (
			s    string
			data []byte
		)

		if err = json.Unmarshal(value, &s); err != nil {
			return nil, err
		}

		if data, err = base64.StdEncoding.DecodeString(s); err != nil {
			return nil, err
		}

		return &state.GetResponse{
			Data:     data,
			ETag:     ptr.Of(strconv.FormatUint(etag, 10)),
			Metadata: req.Metadata,
		}, nil
	}

	return &state.GetResponse{
		Data:     value,
		ETag:     ptr.Of(strconv.FormatUint(etag, 10)),
		Metadata: req.Metadata,
	}, nil
}

// Delete removes an item from the state store.
func (p *postgresDBAccess) Delete(req *state.DeleteRequest) (err error) {
	p.logger.Debug("Deleting state value from PostgreSQL")
	if req.Key == "" {
		return errors.New("missing key in delete operation")
	}

	var result sql.Result

	if req.ETag == nil || *req.ETag == "" {
		result, err = p.db.Exec("DELETE FROM state WHERE key = $1", req.Key)
	} else {
		// Convert req.ETag to uint32 for postgres XID compatibility
		var etag64 uint64
		etag64, err = strconv.ParseUint(*req.ETag, 10, 32)
		if err != nil {
			return state.NewETagError(state.ETagInvalid, err)
		}
		etag := uint32(etag64)

		result, err = p.db.Exec("DELETE FROM state WHERE key = $1 and xmin = $2", req.Key, etag)
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

func (p *postgresDBAccess) BulkDelete(req []state.DeleteRequest) error {
	p.logger.Debug("Executing BulkDelete request")
	tx, err := p.db.Begin()
	if err != nil {
		return err
	}

	if len(req) > 0 {
		for i := range req {
			err = p.Delete(&req[i])
			if err != nil {
				tx.Rollback()
				return err
			}
		}
	}

	err = tx.Commit()

	return err
}

func (p *postgresDBAccess) ExecuteMulti(request *state.TransactionalStateRequest) error {
	p.logger.Debug("Executing PostgreSQL transaction")

	tx, err := p.db.Begin()
	if err != nil {
		return err
	}

	for _, o := range request.Operations {
		switch o.Operation {
		case state.Upsert:
			var setReq state.SetRequest

			setReq, err = getSet(o)
			if err != nil {
				tx.Rollback()
				return err
			}

			err = p.Set(&setReq)
			if err != nil {
				tx.Rollback()
				return err
			}

		case state.Delete:
			var delReq state.DeleteRequest

			delReq, err = getDelete(o)
			if err != nil {
				tx.Rollback()
				return err
			}

			err = p.Delete(&delReq)
			if err != nil {
				tx.Rollback()
				return err
			}

		default:
			tx.Rollback()
			return fmt.Errorf("unsupported operation: %s", o.Operation)
		}
	}

	err = tx.Commit()

	return err
}

// Query executes a query against store.
func (p *postgresDBAccess) Query(req *state.QueryRequest) (*state.QueryResponse, error) {
	p.logger.Debug("Getting query value from PostgreSQL")
	q := &Query{
		query:     "",
		params:    []interface{}{},
		tableName: p.tableName,
	}
	qbuilder := query.NewQueryBuilder(q)
	if err := qbuilder.BuildQuery(&req.Query); err != nil {
		return &state.QueryResponse{}, err
	}
	data, token, err := q.execute(p.logger, p.db)
	if err != nil {
		return &state.QueryResponse{}, err
	}

	return &state.QueryResponse{
		Results: data,
		Token:   token,
	}, nil
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
									value jsonb NOT NULL,
									isbinary boolean NOT NULL,
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
	err := db.QueryRow("SELECT EXISTS (SELECT FROM pg_tables where tablename = $1)", tableName).Scan(&exists)

	return exists, err
}

// Returns the set requests.
func getSet(req state.TransactionalStateOperation) (state.SetRequest, error) {
	setReq, ok := req.Request.(state.SetRequest)
	if !ok {
		return setReq, errors.New("expecting set request")
	}

	if setReq.Key == "" {
		return setReq, errors.New("missing key in upsert operation")
	}

	return setReq, nil
}

// Returns the delete requests.
func getDelete(req state.TransactionalStateOperation) (state.DeleteRequest, error) {
	delReq, ok := req.Request.(state.DeleteRequest)
	if !ok {
		return delReq, errors.New("expecting delete request")
	}

	if delReq.Key == "" {
		return delReq, errors.New("missing key in upsert operation")
	}

	return delReq, nil
}
