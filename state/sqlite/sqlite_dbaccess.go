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
	"net/url"
	"strings"
	"time"

	"github.com/google/uuid"

	// Blank import for the underlying SQLite Driver.
	_ "modernc.org/sqlite"

	"github.com/dapr/components-contrib/state"
	stateutils "github.com/dapr/components-contrib/state/utils"
	"github.com/dapr/kit/logger"
)

// DBAccess is a private interface which enables unit testing of SQLite.
type DBAccess interface {
	Init(metadata state.Metadata) error
	Ping(ctx context.Context) error
	Set(ctx context.Context, req *state.SetRequest) error
	Get(ctx context.Context, req *state.GetRequest) (*state.GetResponse, error)
	Delete(ctx context.Context, req *state.DeleteRequest) error
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
	ctx      context.Context
	cancel   context.CancelFunc
}

// newSqliteDBAccess creates a new instance of sqliteDbAccess.
func newSqliteDBAccess(logger logger.Logger) *sqliteDBAccess {
	return &sqliteDBAccess{
		logger: logger,
	}
}

// Init sets up SQLite Database connection and ensures that the state table
// exists.
func (a *sqliteDBAccess) Init(md state.Metadata) error {
	err := a.metadata.InitWithMetadata(md)
	if err != nil {
		return err
	}

	db, err := sql.Open("sqlite", a.getConnectionString())
	if err != nil {
		a.logger.Error(err)
		return err
	}

	a.db = db
	a.ctx, a.cancel = context.WithCancel(context.Background())

	err = a.Ping(a.ctx)
	if err != nil {
		return err
	}

	err = a.ensureStateTable(a.ctx, a.metadata.TableName)
	if err != nil {
		return err
	}

	a.scheduleCleanupExpiredData()

	return nil
}

func (a *sqliteDBAccess) getConnectionString() string {
	// Get the "query string" from the connection string if present
	idx := strings.IndexRune(a.metadata.ConnectionString, '?')
	var qs url.Values
	if idx > 0 {
		qs, _ = url.ParseQuery(a.metadata.ConnectionString[(idx + 1):])
	}
	if len(qs) == 0 {
		qs = make(url.Values, 2)
	}

	// We do not want to override a _txlock if set, but we'll show a warning if it's not "immediate"
	if len(qs["_txlock"]) > 0 {
		// Keep the first value only
		qs["_txlock"] = []string{
			strings.ToLower(qs["_txlock"][0]),
		}
		if qs["_txlock"][0] != "immediate" {
			a.logger.Warn("Database connection is being created with a _txlock different from the recommended value 'immediate'")
		}
	} else {
		qs["_txlock"] = []string{"immediate"}
	}

	// Add pragma values
	if len(qs["_pragma"]) == 0 {
		qs["_pragma"] = make([]string, 0, 1)
	}
	if a.metadata.busyTimeout > 0 {
		qs["_pragma"] = append(qs["_pragma"], fmt.Sprintf("busy_timeout(%d)", a.metadata.busyTimeout.Milliseconds()))
	}

	// Build the final connection string
	connString := a.metadata.ConnectionString
	if idx > 0 {
		connString = connString[:idx]
	}
	connString += "?" + qs.Encode()

	// If the connection string doesn't begin with "file:", add the prefix
	if !strings.HasPrefix(connString, "file:") {
		a.logger.Info("prefix 'file:' added to the connection string")
		connString = "file:" + connString
	}

	return connString
}

func (a *sqliteDBAccess) Ping(parentCtx context.Context) error {
	ctx, cancel := context.WithTimeout(parentCtx, a.metadata.timeout)
	err := a.db.PingContext(ctx)
	cancel()
	return err
}

func (a *sqliteDBAccess) Get(parentCtx context.Context, req *state.GetRequest) (*state.GetResponse, error) {
	if req.Key == "" {
		return nil, errors.New("missing key in get operation")
	}
	var (
		value    []byte
		isBinary bool
		etag     string
	)

	// Sprintf is required for table name because sql.DB does not substitute parameters for table names
	//nolint:gosec
	stmt := fmt.Sprintf(
		`SELECT value, is_binary, etag FROM %s
		WHERE
			key = ?
			AND (expiration_time IS NULL OR expiration_time > CURRENT_TIMESTAMP)`,
		a.metadata.TableName)
	ctx, cancel := context.WithTimeout(parentCtx, a.metadata.timeout)
	err := a.db.QueryRowContext(ctx, stmt, req.Key).
		Scan(&value, &isBinary, &etag)
	cancel()
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return &state.GetResponse{}, nil
		}
		return nil, err
	}

	if isBinary {
		var n int
		data := make([]byte, len(value))
		n, err = base64.StdEncoding.Decode(data, value)
		if err != nil {
			return nil, err
		}
		return &state.GetResponse{
			Data:     data[:n],
			ETag:     &etag,
			Metadata: req.Metadata,
		}, nil
	}

	return &state.GetResponse{
		Data:     value,
		ETag:     &etag,
		Metadata: req.Metadata,
	}, nil
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

	if v, ok := req.Value.(string); ok && v == "" {
		return fmt.Errorf("empty string is not allowed in set operation")
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
		expiration = fmt.Sprintf("DATETIME(CURRENT_TIMESTAMP, '+%d seconds')", ttlSeconds)
	}

	// Only check for etag if FirstWrite specified (ref oracledatabaseaccess)
	var res sql.Result
	// Sprintf is required for table name because sql.DB does not substitute parameters for table names.
	// And the same is for DATETIME function's seconds parameter (which is from an integer anyways).
	if req.ETag == nil || *req.ETag == "" {
		var op string
		if req.Options.Concurrency == state.FirstWrite {
			op = "INSERT"
		} else {
			op = "INSERT OR REPLACE"
		}
		stmt := fmt.Sprintf(
			`%s INTO %s
				(key, value, is_binary, etag, update_time, expiration_time)
			VALUES(?, ?, ?, ?, CURRENT_TIMESTAMP, %s)`,
			op, a.metadata.TableName, expiration,
		)
		ctx, cancel := context.WithTimeout(context.Background(), a.metadata.timeout)
		res, err = db.ExecContext(ctx, stmt, req.Key, requestValue, isBinary, newEtag, req.Key)
		cancel()
	} else {
		stmt := fmt.Sprintf(
			`UPDATE %s SET
				value = ?,
				etag = ?,
				is_binary = ?,
				update_time = CURRENT_TIMESTAMP,
				expiration_time = %s
			WHERE
				key = ?
				AND eTag = ?`,
			a.metadata.TableName, expiration,
		)
		ctx, cancel := context.WithTimeout(context.Background(), a.metadata.timeout)
		res, err = db.ExecContext(ctx, stmt, requestValue, newEtag, isBinary, req.Key, *req.ETag)
		cancel()
	}

	if err != nil {
		return err
	}

	rows, err := res.RowsAffected()
	if err != nil {
		return err
	}

	if rows == 0 {
		if req.ETag != nil && *req.ETag != "" {
			return state.NewETagError(state.ETagMismatch, nil)
		}
		return errors.New("no item was updated")
	}

	return nil
}

func (a *sqliteDBAccess) Delete(ctx context.Context, req *state.DeleteRequest) error {
	return a.doDelete(ctx, a.db, req)
}

func (a *sqliteDBAccess) ExecuteMulti(parentCtx context.Context, reqs []state.TransactionalStateOperation) error {
	tx, err := a.db.BeginTx(parentCtx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	for _, req := range reqs {
		switch req.Operation {
		case state.Upsert:
			if setReq, ok := req.Request.(state.SetRequest); ok {
				err = a.doSet(parentCtx, tx, &setReq)
				if err != nil {
					return err
				}
			} else {
				return fmt.Errorf("expecting set request")
			}
		case state.Delete:
			if delReq, ok := req.Request.(state.DeleteRequest); ok {
				err = a.doDelete(parentCtx, tx, &delReq)
				if err != nil {
					return err
				}
			} else {
				return fmt.Errorf("expecting delete request")
			}
		default:
			// Do nothing
		}
	}
	return tx.Commit()
}

// Close implements io.Close.
func (a *sqliteDBAccess) Close() error {
	if a.cancel != nil {
		a.cancel()
	}
	if a.db != nil {
		_ = a.db.Close()
	}
	return nil
}

// Create table if not exists.
func (a *sqliteDBAccess) ensureStateTable(parentCtx context.Context, stateTableName string) error {
	exists, err := a.tableExists(parentCtx)
	if err != nil || exists {
		return err
	}

	a.logger.Infof("Creating SQLite state table '%s'", stateTableName)

	ctx, cancel := context.WithTimeout(parentCtx, a.metadata.timeout)
	defer cancel()

	tx, err := a.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	stmt := fmt.Sprintf(
		`CREATE TABLE %s (
			key TEXT NOT NULL PRIMARY KEY,
			value TEXT NOT NULL,
			is_binary BOOLEAN NOT NULL,
			etag TEXT NOT NULL,
			expiration_time TIMESTAMP DEFAULT NULL,
			update_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
		)`,
		stateTableName,
	)
	_, err = tx.Exec(stmt)
	if err != nil {
		return err
	}

	stmt = fmt.Sprintf(`CREATE INDEX idx_%s_expiration_time ON %s (expiration_time)`, stateTableName, stateTableName)
	_, err = tx.Exec(stmt)
	if err != nil {
		return err
	}

	return tx.Commit()
}

// Check if table exists.
func (a *sqliteDBAccess) tableExists(parentCtx context.Context) (bool, error) {
	ctx, cancel := context.WithTimeout(parentCtx, a.metadata.timeout)
	defer cancel()

	var exists string
	// Returns 1 or 0 as a string if the table exists or not.
	const q = `SELECT EXISTS (
		SELECT name FROM sqlite_master WHERE type='table' AND name = ?
	) AS 'exists'`
	err := a.db.QueryRowContext(ctx, q, a.metadata.TableName).Scan(&exists)
	return exists == "1", err
}

func (a *sqliteDBAccess) doDelete(parentCtx context.Context, db querier, req *state.DeleteRequest) error {
	err := state.CheckRequestOptions(req.Options)
	if err != nil {
		return err
	}

	if req.Key == "" {
		return fmt.Errorf("missing key in delete operation")
	}

	var result sql.Result
	if req.ETag == nil || *req.ETag == "" {
		// Sprintf is required for table name because sql.DB does not substitute parameters for table names.
		stmt := fmt.Sprintf("DELETE FROM %s WHERE key = ?", a.metadata.TableName)
		ctx, cancel := context.WithTimeout(parentCtx, a.metadata.timeout)
		result, err = db.ExecContext(ctx, stmt, req.Key)
		cancel()
	} else {
		// Sprintf is required for table name because sql.DB does not substitute parameters for table names.
		stmt := fmt.Sprintf("DELETE FROM %s WHERE key = ? AND etag = ?", a.metadata.TableName)
		ctx, cancel := context.WithTimeout(parentCtx, a.metadata.timeout)
		result, err = db.ExecContext(ctx, stmt, req.Key, *req.ETag)
		cancel()
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

func (a *sqliteDBAccess) scheduleCleanupExpiredData() {
	if a.metadata.cleanupInterval <= 0 {
		return
	}

	a.logger.Infof("Schedule expired data clean up every %v", a.metadata.cleanupInterval)

	ticker := time.NewTicker(a.metadata.cleanupInterval)
	go func() {
		for {
			select {
			case <-ticker.C:
				a.cleanupTimeout()
			case <-a.ctx.Done():
				return
			}
		}
	}()
}

func (a *sqliteDBAccess) cleanupTimeout() {
	ctx, cancel := context.WithTimeout(a.ctx, a.metadata.timeout)
	defer cancel()

	tx, err := a.db.BeginTx(ctx, nil)
	if err != nil {
		a.logger.Errorf("Error removing expired data: failed to begin transaction: %v", err)
		return
	}
	defer tx.Rollback()

	// Sprintf is required for table name because sql.DB does not substitute parameters for table names
	//nolint:gosec
	stmt := fmt.Sprintf(
		`DELETE FROM %s
		WHERE
			expiration_time IS NOT NULL
			AND expiration_time < CURRENT_TIMESTAMP`,
		a.metadata.TableName,
	)
	res, err := tx.Exec(stmt)
	if err != nil {
		a.logger.Errorf("Error removing expired data: failed to execute query: %v", err)
		return
	}

	cleaned, err := res.RowsAffected()
	if err != nil {
		a.logger.Errorf("Error removing expired data: failed to count affected rows: %v", err)
		return
	}

	err = tx.Commit()
	if err != nil {
		a.logger.Errorf("Error removing expired data: failed to commit transaction: %v", err)
		return
	}

	a.logger.Debugf("Removed %d expired rows", cleaned)
}
