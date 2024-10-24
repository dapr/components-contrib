/*
Copyright 2023 The Dapr Authors
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

package sqlite

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	// Blank import for the underlying SQLite Driver.
	_ "modernc.org/sqlite"

	"github.com/dapr/components-contrib/common/authentication/sqlite"
	commonsql "github.com/dapr/components-contrib/common/component/sql"
	sqltransactions "github.com/dapr/components-contrib/common/component/sql/transactions"
	"github.com/dapr/components-contrib/state"
	stateutils "github.com/dapr/components-contrib/state/utils"
	"github.com/dapr/kit/logger"
)

// DBAccess is a private interface which enables unit testing of SQLite.
type DBAccess interface {
	Init(ctx context.Context, metadata state.Metadata) error
	Ping(ctx context.Context) error
	Set(ctx context.Context, req *state.SetRequest) error
	Get(ctx context.Context, req *state.GetRequest) (*state.GetResponse, error)
	Delete(ctx context.Context, req *state.DeleteRequest) error
	BulkGet(ctx context.Context, req []state.GetRequest) ([]state.BulkGetResponse, error)
	ExecuteMulti(ctx context.Context, reqs []state.TransactionalStateOperation) error
	Close() error
}

// Interface for both sql.DB and sql.Tx
type querier interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// sqliteDBAccess implements DBAccess.
type sqliteDBAccess struct {
	logger   logger.Logger
	metadata sqliteMetadataStruct
	db       *sql.DB
	gc       commonsql.GarbageCollector
}

// newSqliteDBAccess creates a new instance of sqliteDbAccess.
func newSqliteDBAccess(logger logger.Logger) *sqliteDBAccess {
	return &sqliteDBAccess{
		logger: logger,
	}
}

// Init sets up SQLite Database connection and ensures that the state table
// exists.
func (a *sqliteDBAccess) Init(ctx context.Context, md state.Metadata) error {
	err := a.metadata.InitWithMetadata(md)
	if err != nil {
		return err
	}

	connString, err := a.metadata.GetConnectionString(a.logger, sqlite.GetConnectionStringOpts{})
	if err != nil {
		// Already logged
		return err
	}

	a.db, err = sql.Open("sqlite", connString)
	if err != nil {
		return fmt.Errorf("failed to create connection: %w", err)
	}

	// If the database is in-memory, we can't have more than 1 open connection
	if a.metadata.IsInMemoryDB() {
		a.db.SetMaxOpenConns(1)
	}

	err = a.Ping(ctx)
	if err != nil {
		return fmt.Errorf("failed to ping: %w", err)
	}

	// Performs migrations
	err = performMigrations(ctx, a.db, a.logger, migrationOptions{
		StateTableName:    a.metadata.TableName,
		MetadataTableName: a.metadata.MetadataTableName,
	})
	if err != nil {
		return fmt.Errorf("failed to perform migrations: %w", err)
	}

	// Init the background GC
	err = a.initGC()
	if err != nil {
		return err
	}

	return nil
}

func (a *sqliteDBAccess) initGC() (err error) {
	a.gc, err = commonsql.ScheduleGarbageCollector(commonsql.GCOptions{
		Logger: a.logger,
		UpdateLastCleanupQuery: func(arg any) (string, any) {
			return fmt.Sprintf(`INSERT INTO %s (key, value)
				VALUES ('last-cleanup', CURRENT_TIMESTAMP)
				ON CONFLICT (key)
				DO UPDATE SET value = CURRENT_TIMESTAMP
					WHERE (unixepoch(CURRENT_TIMESTAMP) - unixepoch(value)) * 1000 > ?;`,
				a.metadata.MetadataTableName,
			), arg
		},
		DeleteExpiredValuesQuery: fmt.Sprintf(`DELETE FROM %s
		WHERE
			expiration_time IS NOT NULL
			AND expiration_time < CURRENT_TIMESTAMP`,
			a.metadata.TableName,
		),
		CleanupInterval: a.metadata.CleanupInterval,
		DB:              commonsql.AdaptDatabaseSQLConn(a.db),
	})
	return err
}

func (a *sqliteDBAccess) CleanupExpired() error {
	return a.gc.CleanupExpired()
}

func (a *sqliteDBAccess) Ping(parentCtx context.Context) error {
	ctx, cancel := context.WithTimeout(parentCtx, a.metadata.Timeout)
	err := a.db.PingContext(ctx)
	cancel()
	return err
}

func (a *sqliteDBAccess) Get(parentCtx context.Context, req *state.GetRequest) (*state.GetResponse, error) {
	if req.Key == "" {
		return nil, errors.New("missing key in get operation")
	}

	// Concatenation is required for table name because sql.DB does not substitute parameters for table names
	//nolint:gosec
	stmt := `SELECT key, value, is_binary, etag, expiration_time FROM ` + a.metadata.TableName + `
		WHERE
			key = ?
			AND (expiration_time IS NULL OR expiration_time > CURRENT_TIMESTAMP)`
	ctx, cancel := context.WithTimeout(parentCtx, a.metadata.Timeout)
	defer cancel()
	row := a.db.QueryRowContext(ctx, stmt, req.Key)
	_, value, etag, expireTime, err := readRow(row)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return &state.GetResponse{}, nil
		}
		return nil, err
	}

	var metadata map[string]string
	if expireTime != nil {
		metadata = map[string]string{
			state.GetRespMetaKeyTTLExpireTime: expireTime.UTC().Format(time.RFC3339),
		}
	}

	return &state.GetResponse{
		Data:     value,
		ETag:     etag,
		Metadata: metadata,
	}, nil
}

func (a *sqliteDBAccess) BulkGet(parentCtx context.Context, req []state.GetRequest) ([]state.BulkGetResponse, error) {
	if len(req) == 0 {
		return []state.BulkGetResponse{}, nil
	}

	// SQLite doesn't support passing an array for an IN clause, so we need to build a custom query
	inClause := strings.Repeat("?,", len(req))
	inClause = inClause[:(len(inClause) - 1)]
	params := make([]any, len(req))
	for i, r := range req {
		params[i] = r.Key
	}

	// Concatenation is required for table name because sql.DB does not substitute parameters for table names
	//nolint:gosec
	stmt := `SELECT key, value, is_binary, etag, expiration_time FROM ` + a.metadata.TableName + `
		WHERE
			key IN (` + inClause + `)
			AND (expiration_time IS NULL OR expiration_time > CURRENT_TIMESTAMP)`
	ctx, cancel := context.WithTimeout(parentCtx, a.metadata.Timeout)
	defer cancel()
	rows, err := a.db.QueryContext(ctx, stmt, params...)
	if err != nil {
		return nil, err
	}

	var n int
	res := make([]state.BulkGetResponse, len(req))
	foundKeys := make(map[string]struct{}, len(req))
	for ; rows.Next(); n++ {
		if n >= len(req) {
			// Sanity check to prevent panics, which should never happen
			return nil, fmt.Errorf("query returned more records than expected (expected %d)", len(req))
		}

		r := state.BulkGetResponse{}
		var expireTime *time.Time
		r.Key, r.Data, r.ETag, expireTime, err = readRow(rows)
		if err != nil {
			r.Error = err.Error()
		}
		if expireTime != nil {
			r.Metadata = map[string]string{
				state.GetRespMetaKeyTTLExpireTime: expireTime.UTC().Format(time.RFC3339),
			}
		}
		res[n] = r
		foundKeys[r.Key] = struct{}{}
	}

	// Populate missing keys with empty values
	// This is to ensure consistency with the other state stores that implement BulkGet as a loop over Get, and with the Get method
	if len(foundKeys) < len(req) {
		var ok bool
		for _, r := range req {
			_, ok = foundKeys[r.Key]
			if !ok {
				if n >= len(req) {
					// Sanity check to prevent panics, which should never happen
					return nil, fmt.Errorf("query returned more records than expected (expected %d)", len(req))
				}
				res[n] = state.BulkGetResponse{
					Key: r.Key,
				}
				n++
			}
		}
	}

	return res[:n], nil
}

func readRow(row interface{ Scan(dest ...any) error }) (string, []byte, *string, *time.Time, error) {
	var (
		key        string
		value      []byte
		isBinary   bool
		etag       string
		expire     sql.NullTime
		expireTime *time.Time
	)
	err := row.Scan(&key, &value, &isBinary, &etag, &expire)
	if err != nil {
		return key, nil, nil, nil, err
	}

	if expire.Valid {
		expireTime = &expire.Time
	}

	if isBinary {
		var n int
		data := make([]byte, len(value))
		n, err = base64.StdEncoding.Decode(data, value)
		if err != nil {
			return key, nil, nil, nil, fmt.Errorf("failed to decode binary data: %w", err)
		}
		return key, data[:n], &etag, expireTime, nil
	}

	return key, value, &etag, expireTime, nil
}

func (a *sqliteDBAccess) Set(ctx context.Context, req *state.SetRequest) error {
	return a.doSet(ctx, a.db, req)
}

func (a *sqliteDBAccess) doSet(parentCtx context.Context, db querier, req *state.SetRequest) error {
	err := state.CheckRequestOptions(req.Options)
	if err != nil {
		return err
	}

	if req.Key == "" {
		return errors.New("missing key in set option")
	}

	// TTL
	var ttlSeconds int
	ttl, ttlerr := stateutils.ParseTTL(req.Metadata)
	if ttlerr != nil {
		return fmt.Errorf("error parsing TTL: %w", ttlerr)
	}
	if ttl != nil {
		ttlSeconds = *ttl
	}

	// Encode the value
	var requestValue string
	byteArray, isBinary := req.Value.([]uint8)
	if isBinary {
		requestValue = base64.StdEncoding.EncodeToString(byteArray)
	} else {
		var bt []byte
		bt, err = json.Marshal(req.Value)
		if err != nil {
			return err
		}
		requestValue = string(bt)
	}

	// New ETag
	etagObj, err := uuid.NewRandom()
	if err != nil {
		return err
	}
	newEtag := etagObj.String()

	// Also resets expiration time in case of an update
	expiration := "NULL"
	if ttlSeconds > 0 {
		expiration = "DATETIME(CURRENT_TIMESTAMP, '+" + strconv.Itoa(ttlSeconds) + " seconds')"
	}

	// Only check for etag if FirstWrite specified (ref oracledatabaseaccess)
	ctx, cancel := context.WithTimeout(context.Background(), a.metadata.Timeout)
	defer cancel()

	// Sprintf is required for table name because sql.DB does not substitute parameters for table names.
	// And the same is for DATETIME function's seconds parameter (which is from an integer anyways).
	switch {
	case !req.HasETag() && req.Options.Concurrency == state.FirstWrite:
		// If the operation uses first-write concurrency, we need to handle the special case of a row that has expired but hasn't been garbage collected yet
		// In this case, the row should be considered as if it were deleted
		stmt := `WITH a AS (
				SELECT
					?, ?, ?, ?, ` + expiration + `, CURRENT_TIMESTAMP
				WHERE NOT EXISTS (
					SELECT 1
					FROM ` + a.metadata.TableName + `
					WHERE key = ?
						AND (expiration_time IS NULL OR expiration_time > CURRENT_TIMESTAMP)
				)
			)
			INSERT OR REPLACE INTO ` + a.metadata.TableName + `
			SELECT * FROM a
			RETURNING 1`
		var num int
		err = db.QueryRowContext(ctx, stmt, req.Key, requestValue, isBinary, newEtag, req.Key).Scan(&num)
		if err != nil {
			// If no row was returned, it means no row was updated, so we had an etag failure
			if errors.Is(err, sql.ErrNoRows) {
				return state.NewETagError(state.ETagMismatch, nil)
			}
			return err
		}

	case !req.HasETag():
		stmt := "INSERT OR REPLACE INTO " + a.metadata.TableName + `
				(key, value, is_binary, etag, update_time, expiration_time)
			VALUES(?, ?, ?, ?, CURRENT_TIMESTAMP, ` + expiration + `)`
		_, err = db.ExecContext(ctx, stmt, req.Key, requestValue, isBinary, newEtag)
		if err != nil {
			return err
		}

	default:
		stmt := `UPDATE ` + a.metadata.TableName + ` SET
				value = ?,
				etag = ?,
				is_binary = ?,
				update_time = CURRENT_TIMESTAMP,
				expiration_time = ` + expiration + `
			WHERE
				key = ?
				AND etag = ?
				AND (expiration_time IS NULL OR expiration_time > CURRENT_TIMESTAMP)`
		var res sql.Result
		res, err = db.ExecContext(ctx, stmt, requestValue, newEtag, isBinary, req.Key, *req.ETag)
		if err != nil {
			return err
		}

		rows, err := res.RowsAffected()
		if err != nil {
			return err
		}
		if rows == 0 {
			// 0 affected rows means etag failure
			return state.NewETagError(state.ETagMismatch, nil)
		}
	}

	return nil
}

func (a *sqliteDBAccess) Delete(ctx context.Context, req *state.DeleteRequest) error {
	return a.doDelete(ctx, a.db, req)
}

func (a *sqliteDBAccess) ExecuteMulti(parentCtx context.Context, reqs []state.TransactionalStateOperation) error {
	// If there's only 1 operation, skip starting a transaction
	switch len(reqs) {
	case 0:
		return nil
	case 1:
		return a.execMultiOperation(parentCtx, reqs[0], a.db)
	default:
		_, err := sqltransactions.ExecuteInTransaction(parentCtx, a.logger, a.db, func(ctx context.Context, tx *sql.Tx) (r struct{}, err error) {
			for _, op := range reqs {
				err = a.execMultiOperation(parentCtx, op, tx)
				if err != nil {
					return r, err
				}
			}
			return r, nil
		})
		return err
	}
}

func (a *sqliteDBAccess) execMultiOperation(parentCtx context.Context, op state.TransactionalStateOperation, db querier) (err error) {
	switch req := op.(type) {
	case state.SetRequest:
		return a.doSet(parentCtx, db, &req)
	case state.DeleteRequest:
		return a.doDelete(parentCtx, db, &req)
	default:
		return fmt.Errorf("unsupported operation: %s", op.Operation())
	}
}

// Close implements io.Closer.
func (a *sqliteDBAccess) Close() (err error) {
	errs := make([]error, 0)

	if a.gc != nil {
		err = a.gc.Close()
		if err != nil {
			errs = append(errs, err)
		}
	}

	if a.db != nil {
		err = a.db.Close()
		if err != nil {
			errs = append(errs, err)
		}
	}

	return errors.Join(errs...)
}

func (a *sqliteDBAccess) doDelete(parentCtx context.Context, db querier, req *state.DeleteRequest) error {
	err := state.CheckRequestOptions(req.Options)
	if err != nil {
		return err
	}

	if req.Key == "" {
		return errors.New("missing key in delete operation")
	}

	ctx, cancel := context.WithTimeout(parentCtx, a.metadata.Timeout)
	defer cancel()
	var result sql.Result
	if !req.HasETag() {
		// Concatenation is required for table name because sql.DB does not substitute parameters for table names.
		result, err = db.ExecContext(ctx, "DELETE FROM "+a.metadata.TableName+" WHERE key = ?",
			req.Key)
	} else {
		// Concatenation is required for table name because sql.DB does not substitute parameters for table names.
		result, err = db.ExecContext(ctx, "DELETE FROM "+a.metadata.TableName+" WHERE key = ? AND etag = ?",
			req.Key, *req.ETag)
	}

	if err != nil {
		return err
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}

	if rows == 0 && req.ETag != nil && *req.ETag != "" {
		return state.NewETagError(state.ETagMismatch, nil)
	}

	return nil
}

// GetConnection returns the database connection object.
// This is primarily used for tests.
func (a *sqliteDBAccess) GetConnection() *sql.DB {
	return a.db
}

// GetCleanupInterval returns the cleanupInterval property.
// This is primarily used for tests.
func (a *sqliteDBAccess) GetCleanupInterval() time.Duration {
	return a.metadata.CleanupInterval
}
