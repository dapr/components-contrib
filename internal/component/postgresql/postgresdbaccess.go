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
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/dapr/components-contrib/state"
	"github.com/dapr/components-contrib/state/query"
	stateutils "github.com/dapr/components-contrib/state/utils"
	"github.com/dapr/kit/logger"
	"github.com/dapr/kit/ptr"
)

var errMissingConnectionString = errors.New("missing connection string")

// Interface that applies to *pgxpool.Pool.
// We need this to be able to mock the connection in tests.
type PGXPoolConn interface {
	Begin(context.Context) (pgx.Tx, error)
	BeginTx(ctx context.Context, txOptions pgx.TxOptions) (pgx.Tx, error)
	Exec(context.Context, string, ...interface{}) (pgconn.CommandTag, error)
	Query(context.Context, string, ...interface{}) (pgx.Rows, error)
	QueryRow(context.Context, string, ...interface{}) pgx.Row
	Ping(context.Context) error
	Close()
}

// PostgresDBAccess implements dbaccess.
type PostgresDBAccess struct {
	logger   logger.Logger
	metadata postgresMetadataStruct
	db       PGXPoolConn

	migrateFn  func(context.Context, PGXPoolConn, MigrateOptions) error
	setQueryFn func(*state.SetRequest, SetQueryOptions) string
	etagColumn string

	closeCh chan struct{}
	closed  atomic.Bool
	wg      sync.WaitGroup
}

// newPostgresDBAccess creates a new instance of postgresAccess.
func newPostgresDBAccess(logger logger.Logger, opts Options) *PostgresDBAccess {
	logger.Debug("Instantiating new Postgres state store")

	return &PostgresDBAccess{
		logger:     logger,
		closeCh:    make(chan struct{}),
		migrateFn:  opts.MigrateFn,
		setQueryFn: opts.SetQueryFn,
		etagColumn: opts.ETagColumn,
	}
}

// Init sets up Postgres connection and ensures that the state table exists.
func (p *PostgresDBAccess) Init(ctx context.Context, meta state.Metadata) error {
	p.logger.Debug("Initializing Postgres state store")

	err := p.metadata.InitWithMetadata(meta)
	if err != nil {
		p.logger.Errorf("Failed to parse metadata: %v", err)
		return err
	}

	config, err := pgxpool.ParseConfig(p.metadata.ConnectionString)
	if err != nil {
		err = fmt.Errorf("failed to parse connection string: %w", err)
		p.logger.Error(err)
		return err
	}
	if p.metadata.ConnectionMaxIdleTime > 0 {
		config.MaxConnIdleTime = p.metadata.ConnectionMaxIdleTime
	}

	connCtx, connCancel := context.WithTimeout(ctx, p.metadata.timeout)
	p.db, err = pgxpool.NewWithConfig(connCtx, config)
	connCancel()
	if err != nil {
		err = fmt.Errorf("failed to connect to the database: %w", err)
		p.logger.Error(err)
		return err
	}

	pingCtx, pingCancel := context.WithTimeout(ctx, p.metadata.timeout)
	err = p.db.Ping(pingCtx)
	pingCancel()
	if err != nil {
		err = fmt.Errorf("failed to ping the database: %w", err)
		p.logger.Error(err)
		return err
	}

	if err = p.migrateFn(ctx, p.db, MigrateOptions{
		Logger:            p.logger,
		StateTableName:    p.metadata.TableName,
		MetadataTableName: p.metadata.MetadataTableName,
	}); err != nil {
		return err
	}

	p.ScheduleCleanupExpiredData(ctx)

	return nil
}

func (p *PostgresDBAccess) GetDB() *pgxpool.Pool {
	// We can safely cast to *pgxpool.Pool because this method is never used in unit tests where we mock the DB
	return p.db.(*pgxpool.Pool)
}

// Set makes an insert or update to the database.
func (p *PostgresDBAccess) Set(ctx context.Context, req *state.SetRequest) error {
	return p.doSet(ctx, p.db, req)
}

func (p *PostgresDBAccess) doSet(parentCtx context.Context, db dbquerier, req *state.SetRequest) error {
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
	bt, _ := stateutils.Marshal(v, json.Marshal)
	value := string(bt)

	// TTL
	var ttlSeconds int
	ttl, ttlerr := stateutils.ParseTTL(req.Metadata)
	if ttlerr != nil {
		return fmt.Errorf("error parsing TTL: %w", ttlerr)
	}
	if ttl != nil {
		ttlSeconds = *ttl
	}

	var (
		queryExpiredate string
		params          []any
	)

	if req.ETag == nil || *req.ETag == "" {
		params = []any{req.Key, value, isBinary}
	} else {
		var etag64 uint64
		etag64, err = strconv.ParseUint(*req.ETag, 10, 32)
		if err != nil {
			return state.NewETagError(state.ETagInvalid, err)
		}
		params = []any{req.Key, value, isBinary, uint32(etag64)}
	}

	if ttlSeconds > 0 {
		queryExpiredate = "CURRENT_TIMESTAMP + interval '" + strconv.Itoa(ttlSeconds) + " seconds'"
	} else {
		queryExpiredate = "NULL"
	}

	query := p.setQueryFn(req, SetQueryOptions{
		TableName:       p.metadata.TableName,
		ExpireDateValue: queryExpiredate,
	})

	result, err := db.Exec(parentCtx, query, params...)
	if err != nil {
		if req.ETag != nil && *req.ETag != "" {
			return state.NewETagError(state.ETagMismatch, err)
		}
		return err
	}

	if result.RowsAffected() != 1 {
		return errors.New("no item was updated")
	}

	return nil
}

func (p *PostgresDBAccess) BulkSet(parentCtx context.Context, req []state.SetRequest) error {
	tx, err := p.beginTx(parentCtx)
	if err != nil {
		return err
	}
	defer p.rollbackTx(parentCtx, tx, "BulkSet")

	if len(req) > 0 {
		for i := range req {
			err = p.doSet(parentCtx, tx, &req[i])
			if err != nil {
				return err
			}
		}
	}

	ctx, cancel := context.WithTimeout(parentCtx, p.metadata.timeout)
	err = tx.Commit(ctx)
	cancel()
	if err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// Get returns data from the database. If data does not exist for the key an empty state.GetResponse will be returned.
func (p *PostgresDBAccess) Get(parentCtx context.Context, req *state.GetRequest) (*state.GetResponse, error) {
	if req.Key == "" {
		return nil, errors.New("missing key in get operation")
	}

	var (
		value    []byte
		isBinary bool
		etag     pgtype.Int8
	)
	query := `SELECT
			value, isbinary, %[1]s AS etag
		FROM %[2]s
			WHERE
				key = $1
				AND (expiredate IS NULL OR expiredate >= CURRENT_TIMESTAMP)`
	err := p.db.QueryRow(parentCtx, fmt.Sprintf(query, p.etagColumn, p.metadata.TableName), req.Key).
		Scan(&value, &isBinary, &etag)
	if err != nil {
		// If no rows exist, return an empty response, otherwise return the error.
		if err == pgx.ErrNoRows {
			return &state.GetResponse{}, nil
		}
		return nil, err
	}

	var etagS *string
	if etag.Valid {
		etagS = ptr.Of(strconv.FormatInt(etag.Int64, 10))
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
			Data: data,
			ETag: etagS,
		}, nil
	}

	return &state.GetResponse{
		Data: value,
		ETag: etagS,
	}, nil
}

// Delete removes an item from the state store.
func (p *PostgresDBAccess) Delete(ctx context.Context, req *state.DeleteRequest) (err error) {
	return p.doDelete(ctx, p.db, req)
}

func (p *PostgresDBAccess) doDelete(parentCtx context.Context, db dbquerier, req *state.DeleteRequest) (err error) {
	if req.Key == "" {
		return errors.New("missing key in delete operation")
	}

	var result pgconn.CommandTag
	if req.ETag == nil || *req.ETag == "" {
		result, err = db.Exec(parentCtx, "DELETE FROM state WHERE key = $1", req.Key)
	} else {
		// Convert req.ETag to uint32 for postgres XID compatibility
		var etag64 uint64
		etag64, err = strconv.ParseUint(*req.ETag, 10, 32)
		if err != nil {
			return state.NewETagError(state.ETagInvalid, err)
		}

		result, err = db.Exec(parentCtx, "DELETE FROM state WHERE key = $1 AND $2 = "+p.etagColumn, req.Key, uint32(etag64))
	}

	if err != nil {
		return err
	}

	rows := result.RowsAffected()
	if rows != 1 && req.ETag != nil && *req.ETag != "" {
		return state.NewETagError(state.ETagMismatch, nil)
	}

	return nil
}

func (p *PostgresDBAccess) BulkDelete(parentCtx context.Context, req []state.DeleteRequest) error {
	tx, err := p.beginTx(parentCtx)
	if err != nil {
		return err
	}
	defer p.rollbackTx(parentCtx, tx, "BulkDelete")

	if len(req) > 0 {
		for i := range req {
			err = p.doDelete(parentCtx, tx, &req[i])
			if err != nil {
				return err
			}
		}
	}

	ctx, cancel := context.WithTimeout(parentCtx, p.metadata.timeout)
	err = tx.Commit(ctx)
	cancel()
	if err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

func (p *PostgresDBAccess) ExecuteMulti(parentCtx context.Context, request *state.TransactionalStateRequest) error {
	tx, err := p.beginTx(parentCtx)
	if err != nil {
		return err
	}
	defer p.rollbackTx(parentCtx, tx, "ExecMulti")

	for _, o := range request.Operations {
		switch o.Operation {
		case state.Upsert:
			var setReq state.SetRequest
			setReq, err = getSet(o)
			if err != nil {
				return err
			}

			err = p.doSet(parentCtx, tx, &setReq)
			if err != nil {
				return err
			}

		case state.Delete:
			var delReq state.DeleteRequest
			delReq, err = getDelete(o)
			if err != nil {
				return err
			}

			err = p.doDelete(parentCtx, tx, &delReq)
			if err != nil {
				return err
			}

		default:
			return fmt.Errorf("unsupported operation: %s", o.Operation)
		}
	}

	ctx, cancel := context.WithTimeout(parentCtx, p.metadata.timeout)
	err = tx.Commit(ctx)
	cancel()
	if err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// Query executes a query against store.
func (p *PostgresDBAccess) Query(parentCtx context.Context, req *state.QueryRequest) (*state.QueryResponse, error) {
	q := &Query{
		query:      "",
		params:     []any{},
		tableName:  p.metadata.TableName,
		etagColumn: p.etagColumn,
	}
	qbuilder := query.NewQueryBuilder(q)
	if err := qbuilder.BuildQuery(&req.Query); err != nil {
		return &state.QueryResponse{}, err
	}
	data, token, err := q.execute(parentCtx, p.logger, p.db)
	if err != nil {
		return &state.QueryResponse{}, err
	}

	return &state.QueryResponse{
		Results: data,
		Token:   token,
	}, nil
}

func (p *PostgresDBAccess) ScheduleCleanupExpiredData(ctx context.Context) {
	if p.metadata.cleanupInterval == nil || *p.metadata.cleanupInterval <= 0 || p.closed.Load() {
		return
	}

	p.logger.Infof("Schedule expired data clean up every %d seconds", int(p.metadata.cleanupInterval.Seconds()))

	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		ticker := time.NewTicker(*p.metadata.cleanupInterval)
		defer ticker.Stop()

		var err error
		for {
			select {
			case <-ticker.C:
				err = p.CleanupExpired(ctx)
				if err != nil {
					p.logger.Errorf("Error removing expired data: %v", err)
				}
			case <-ctx.Done():
				p.logger.Debug("Stopped background cleanup of expired data")
				return
			case <-p.closeCh:
				p.logger.Debug("Stopping background because PostgresDBAccess is closing")
				return
			}
		}
	}()
}

func (p *PostgresDBAccess) CleanupExpired(ctx context.Context) error {
	// Check if the last iteration was too recent
	// This performs an atomic operation, so allows coordination with other daprd processes too
	canContinue, err := p.UpdateLastCleanup(ctx, p.db, *p.metadata.cleanupInterval)
	if err != nil {
		// Log errors only
		p.logger.Warnf("Failed to read last cleanup time from database: %v", err)
	}
	if !canContinue {
		p.logger.Debug("Last cleanup was performed too recently")
		return nil
	}

	// Note we're not using the transaction here as we don't want this to be rolled back half-way or to lock the table unnecessarily
	// Need to use fmt.Sprintf because we can't parametrize a table name
	// Note we are not setting a timeout here as this query can take a "long" time, especially if there's no index on expiredate
	stmt := fmt.Sprintf(`DELETE FROM %s WHERE expiredate IS NOT NULL AND expiredate < CURRENT_TIMESTAMP`, p.metadata.TableName)
	res, err := p.db.Exec(ctx, stmt)
	if err != nil {
		return fmt.Errorf("failed to execute query: %w", err)
	}

	p.logger.Infof("Removed %d expired rows", res.RowsAffected())
	return nil
}

// UpdateLastCleanup sets the 'last-cleanup' value only if it's less than cleanupInterval.
// Returns true if the row was updated, which means that the cleanup can proceed.
func (p *PostgresDBAccess) UpdateLastCleanup(ctx context.Context, db dbquerier, cleanupInterval time.Duration) (bool, error) {
	queryCtx, cancel := context.WithTimeout(ctx, p.metadata.timeout)
	res, err := db.Exec(queryCtx,
		fmt.Sprintf(`INSERT INTO %[1]s (key, value)
			VALUES ('last-cleanup', CURRENT_TIMESTAMP::text)
			ON CONFLICT (key)
			DO UPDATE SET value = CURRENT_TIMESTAMP::text
				WHERE (EXTRACT('epoch' FROM CURRENT_TIMESTAMP - %[1]s.value::timestamp with time zone) * 1000)::bigint > $1`,
			p.metadata.MetadataTableName),
		cleanupInterval.Milliseconds()-100, // Subtract 100ms for some buffer
	)
	cancel()
	if err != nil {
		return false, fmt.Errorf("failed to execute query: %w", err)
	}

	n := res.RowsAffected()
	return n > 0, nil
}

// Close implements io.Close.
func (p *PostgresDBAccess) Close() error {
	if p.closed.CompareAndSwap(false, true) {
		close(p.closeCh)
	}

	if p.db != nil {
		p.db.Close()
		p.db = nil
	}

	p.wg.Wait()

	return nil
}

// GetCleanupInterval returns the cleanupInterval property.
// This is primarily used for tests.
func (p *PostgresDBAccess) GetCleanupInterval() *time.Duration {
	return p.metadata.cleanupInterval
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

// Internal function that begins a transaction.
func (p *PostgresDBAccess) beginTx(parentCtx context.Context) (pgx.Tx, error) {
	ctx, cancel := context.WithTimeout(parentCtx, p.metadata.timeout)
	tx, err := p.db.Begin(ctx)
	cancel()
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	return tx, nil
}

// Internal function that rolls back a transaction.
// Normally called as a deferred function in methods that use transactions.
// In case of errors, they are logged but not actioned upon.
func (p *PostgresDBAccess) rollbackTx(parentCtx context.Context, tx pgx.Tx, methodName string) {
	rollbackCtx, rollbackCancel := context.WithTimeout(parentCtx, p.metadata.timeout)
	rollbackErr := tx.Rollback(rollbackCtx)
	rollbackCancel()
	if rollbackErr != nil && !errors.Is(rollbackErr, pgx.ErrTxClosed) {
		p.logger.Errorf("Failed to rollback transaction in %s: %v", methodName, rollbackErr)
	}
}
