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
	"reflect"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	pginterfaces "github.com/dapr/components-contrib/internal/component/postgresql/interfaces"
	internalsql "github.com/dapr/components-contrib/internal/component/sql"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/state"
	"github.com/dapr/components-contrib/state/query"
	stateutils "github.com/dapr/components-contrib/state/utils"
	"github.com/dapr/kit/logger"
	"github.com/dapr/kit/ptr"
)

// PostgreSQL state store.
type PostgreSQL struct {
	state.BulkStore

	logger   logger.Logger
	metadata pgMetadata
	db       pginterfaces.PGXPoolConn

	gc internalsql.GarbageCollector

	migrateFn     func(context.Context, pginterfaces.PGXPoolConn, MigrateOptions) error
	setQueryFn    func(*state.SetRequest, SetQueryOptions) string
	etagColumn    string
	enableAzureAD bool
}

type Options struct {
	MigrateFn     func(context.Context, pginterfaces.PGXPoolConn, MigrateOptions) error
	SetQueryFn    func(*state.SetRequest, SetQueryOptions) string
	ETagColumn    string
	EnableAzureAD bool
}

type MigrateOptions struct {
	Logger            logger.Logger
	StateTableName    string
	MetadataTableName string
}

type SetQueryOptions struct {
	TableName       string
	ExpireDateValue string
}

// NewPostgreSQLStateStore creates a new instance of PostgreSQL state store.
func NewPostgreSQLStateStore(logger logger.Logger, opts Options) state.Store {
	s := newPostgreSQLStateStore(logger, opts)
	s.BulkStore = state.NewDefaultBulkStore(s)
	return s
}

// newPostgreSQLStateStore creates a newPostgreSQLStateStore instance of a PostgreSQL state store.
func newPostgreSQLStateStore(logger logger.Logger, opts Options) *PostgreSQL {
	return &PostgreSQL{
		logger:        logger,
		migrateFn:     opts.MigrateFn,
		setQueryFn:    opts.SetQueryFn,
		etagColumn:    opts.ETagColumn,
		enableAzureAD: opts.EnableAzureAD,
	}
}

// Init sets up Postgres connection and performs migrations.
func (p *PostgreSQL) Init(ctx context.Context, meta state.Metadata) error {
	err := p.metadata.InitWithMetadata(meta, p.enableAzureAD)
	if err != nil {
		p.logger.Errorf("Failed to parse metadata: %v", err)
		return err
	}

	config, err := p.metadata.GetPgxPoolConfig()
	if err != nil {
		p.logger.Error(err)
		return err
	}

	connCtx, connCancel := context.WithTimeout(ctx, p.metadata.Timeout)
	p.db, err = pgxpool.NewWithConfig(connCtx, config)
	connCancel()
	if err != nil {
		err = fmt.Errorf("failed to connect to the database: %w", err)
		p.logger.Error(err)
		return err
	}

	pingCtx, pingCancel := context.WithTimeout(ctx, p.metadata.Timeout)
	err = p.db.Ping(pingCtx)
	pingCancel()
	if err != nil {
		err = fmt.Errorf("failed to ping the database: %w", err)
		p.logger.Error(err)
		return err
	}

	err = p.migrateFn(ctx, p.db, MigrateOptions{
		Logger:            p.logger,
		StateTableName:    p.metadata.TableName,
		MetadataTableName: p.metadata.MetadataTableName,
	})
	if err != nil {
		return err
	}

	if p.metadata.CleanupInterval != nil {
		gc, err := internalsql.ScheduleGarbageCollector(internalsql.GCOptions{
			Logger: p.logger,
			UpdateLastCleanupQuery: func(arg any) (string, any) {
				return fmt.Sprintf(
					`INSERT INTO %[1]s (key, value)
				VALUES ('last-cleanup', CURRENT_TIMESTAMP::text)
				ON CONFLICT (key)
				DO UPDATE SET value = CURRENT_TIMESTAMP::text
					WHERE (EXTRACT('epoch' FROM CURRENT_TIMESTAMP - %[1]s.value::timestamp with time zone) * 1000)::bigint > $1`,
					p.metadata.MetadataTableName,
				), arg
			},
			DeleteExpiredValuesQuery: fmt.Sprintf(
				`DELETE FROM %s WHERE expiredate IS NOT NULL AND expiredate < CURRENT_TIMESTAMP`,
				p.metadata.TableName,
			),
			CleanupInterval: *p.metadata.CleanupInterval,
			DB:              internalsql.AdaptPgxConn(p.db),
		})
		if err != nil {
			return err
		}
		p.gc = gc
	}

	return nil
}

// Features returns the features available in this state store.
func (p *PostgreSQL) Features() []state.Feature {
	return []state.Feature{
		state.FeatureETag,
		state.FeatureTransactional,
		state.FeatureQueryAPI,
		state.FeatureTTL,
	}
}

func (p *PostgreSQL) GetDB() *pgxpool.Pool {
	// We can safely cast to *pgxpool.Pool because this method is never used in unit tests where we mock the DB
	return p.db.(*pgxpool.Pool)
}

// Set makes an insert or update to the database.
func (p *PostgreSQL) Set(ctx context.Context, req *state.SetRequest) error {
	return p.doSet(ctx, p.db, req)
}

func (p *PostgreSQL) doSet(parentCtx context.Context, db pginterfaces.DBQuerier, req *state.SetRequest) error {
	err := state.CheckRequestOptions(req.Options)
	if err != nil {
		return err
	}

	if req.Key == "" {
		return errors.New("missing key in set operation")
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

	if !req.HasETag() {
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
		return err
	}
	if result.RowsAffected() != 1 {
		if req.HasETag() {
			return state.NewETagError(state.ETagMismatch, nil)
		}
		return errors.New("no item was updated")
	}

	return nil
}

// Get returns data from the database. If data does not exist for the key an empty state.GetResponse will be returned.
func (p *PostgreSQL) Get(parentCtx context.Context, req *state.GetRequest) (*state.GetResponse, error) {
	if req.Key == "" {
		return nil, errors.New("missing key in get operation")
	}

	query := `SELECT
			key, value, isbinary, ` + p.etagColumn + ` AS etag, expiredate
		FROM ` + p.metadata.TableName + `
			WHERE
				key = $1
				AND (expiredate IS NULL OR expiredate >= CURRENT_TIMESTAMP)`
	ctx, cancel := context.WithTimeout(parentCtx, p.metadata.Timeout)
	defer cancel()
	row := p.db.QueryRow(ctx, query, req.Key)
	_, value, etag, expireTime, err := readRow(row)
	if err != nil {
		// If no rows exist, return an empty response, otherwise return the error.
		if errors.Is(err, pgx.ErrNoRows) {
			return &state.GetResponse{}, nil
		}
		return nil, err
	}

	resp := &state.GetResponse{
		Data: value,
		ETag: etag,
	}

	if expireTime != nil {
		resp.Metadata = map[string]string{
			state.GetRespMetaKeyTTLExpireTime: expireTime.UTC().Format(time.RFC3339),
		}
	}

	return resp, nil
}

func (p *PostgreSQL) BulkGet(parentCtx context.Context, req []state.GetRequest, _ state.BulkGetOpts) ([]state.BulkGetResponse, error) {
	if len(req) == 0 {
		return []state.BulkGetResponse{}, nil
	}

	// Get all keys
	keys := make([]string, len(req))
	for i, r := range req {
		keys[i] = r.Key
	}

	// Execute the query
	query := `SELECT
			key, value, isbinary, ` + p.etagColumn + ` AS etag, expiredate
		FROM ` + p.metadata.TableName + `
			WHERE
				key = ANY($1)
				AND (expiredate IS NULL OR expiredate >= CURRENT_TIMESTAMP)`
	ctx, cancel := context.WithTimeout(parentCtx, p.metadata.Timeout)
	defer cancel()
	rows, err := p.db.Query(ctx, query, keys)
	if err != nil {
		// If no rows exist, return an empty response, otherwise return the error.
		if errors.Is(err, pgx.ErrNoRows) {
			return []state.BulkGetResponse{}, nil
		}
		return nil, err
	}

	// Scan all rows
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

func readRow(row pgx.Row) (key string, value []byte, etagS *string, expireTime *time.Time, err error) {
	var (
		isBinary bool
		etag     pgtype.Int8
		expT     pgtype.Timestamp
	)
	err = row.Scan(&key, &value, &isBinary, &etag, &expT)
	if err != nil {
		return key, nil, nil, nil, err
	}

	if etag.Valid {
		etagS = ptr.Of(strconv.FormatInt(etag.Int64, 10))
	}

	if expT.Valid {
		expireTime = &expT.Time
	}

	if isBinary {
		var (
			s    string
			data []byte
		)

		err = json.Unmarshal(value, &s)
		if err != nil {
			return key, nil, nil, nil, fmt.Errorf("failed to unmarshal JSON data: %w", err)
		}

		data, err = base64.StdEncoding.DecodeString(s)
		if err != nil {
			return key, nil, nil, nil, fmt.Errorf("failed to decode base64 data: %w", err)
		}

		return key, data, etagS, expireTime, nil
	}

	return key, value, etagS, expireTime, nil
}

// Delete removes an item from the state store.
func (p *PostgreSQL) Delete(ctx context.Context, req *state.DeleteRequest) (err error) {
	return p.doDelete(ctx, p.db, req)
}

func (p *PostgreSQL) doDelete(parentCtx context.Context, db pginterfaces.DBQuerier, req *state.DeleteRequest) (err error) {
	if req.Key == "" {
		return errors.New("missing key in delete operation")
	}

	ctx, cancel := context.WithTimeout(parentCtx, p.metadata.Timeout)
	defer cancel()
	var result pgconn.CommandTag
	if !req.HasETag() {
		result, err = db.Exec(ctx, "DELETE FROM "+p.metadata.TableName+" WHERE key = $1", req.Key)
	} else {
		// Convert req.ETag to uint32 for postgres XID compatibility
		var etag64 uint64
		etag64, err = strconv.ParseUint(*req.ETag, 10, 32)
		if err != nil {
			return state.NewETagError(state.ETagInvalid, err)
		}

		result, err = db.Exec(ctx, "DELETE FROM "+p.metadata.TableName+" WHERE key = $1 AND $2 = "+p.etagColumn, req.Key, uint32(etag64))
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

func (p *PostgreSQL) Multi(parentCtx context.Context, request *state.TransactionalStateRequest) error {
	tx, err := p.beginTx(parentCtx)
	if err != nil {
		return err
	}
	defer p.rollbackTx(parentCtx, tx, "ExecMulti")

	for _, o := range request.Operations {
		switch x := o.(type) {
		case state.SetRequest:
			err = p.doSet(parentCtx, tx, &x)
			if err != nil {
				return err
			}

		case state.DeleteRequest:
			err = p.doDelete(parentCtx, tx, &x)
			if err != nil {
				return err
			}

		default:
			return fmt.Errorf("unsupported operation: %s", o.Operation())
		}
	}

	ctx, cancel := context.WithTimeout(parentCtx, p.metadata.Timeout)
	err = tx.Commit(ctx)
	cancel()
	if err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// Query executes a query against store.
func (p *PostgreSQL) Query(parentCtx context.Context, req *state.QueryRequest) (*state.QueryResponse, error) {
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

func (p *PostgreSQL) CleanupExpired() error {
	if p.gc != nil {
		return p.gc.CleanupExpired()
	}
	return nil
}

// Close implements io.Close.
func (p *PostgreSQL) Close() error {
	if p.db != nil {
		p.db.Close()
		p.db = nil
	}

	if p.gc != nil {
		return p.gc.Close()
	}

	return nil
}

// GetCleanupInterval returns the cleanupInterval property.
// This is primarily used for tests.
func (p *PostgreSQL) GetCleanupInterval() *time.Duration {
	return p.metadata.CleanupInterval
}

// Internal function that begins a transaction.
func (p *PostgreSQL) beginTx(parentCtx context.Context) (pgx.Tx, error) {
	ctx, cancel := context.WithTimeout(parentCtx, p.metadata.Timeout)
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
func (p *PostgreSQL) rollbackTx(parentCtx context.Context, tx pgx.Tx, methodName string) {
	rollbackCtx, rollbackCancel := context.WithTimeout(parentCtx, p.metadata.Timeout)
	rollbackErr := tx.Rollback(rollbackCtx)
	rollbackCancel()
	if rollbackErr != nil && !errors.Is(rollbackErr, pgx.ErrTxClosed) {
		p.logger.Errorf("Failed to rollback transaction in %s: %v", methodName, rollbackErr)
	}
}

func (p *PostgreSQL) GetComponentMetadata() (metadataInfo metadata.MetadataMap) {
	metadataStruct := pgMetadata{}
	metadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, metadata.StateStoreType)
	return
}
