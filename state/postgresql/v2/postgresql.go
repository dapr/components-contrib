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

package postgresql

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	awsAuth "github.com/dapr/components-contrib/common/authentication/aws"
	pgauth "github.com/dapr/components-contrib/common/authentication/postgresql"
	pginterfaces "github.com/dapr/components-contrib/common/component/postgresql/interfaces"
	pgtransactions "github.com/dapr/components-contrib/common/component/postgresql/transactions"
	sqlinternal "github.com/dapr/components-contrib/common/component/sql"
	pgmigrations "github.com/dapr/components-contrib/common/component/sql/migrations/postgres"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/state"
	stateutils "github.com/dapr/components-contrib/state/utils"
	"github.com/dapr/kit/logger"
)

// PostgreSQL state store.
type PostgreSQL struct {
	state.BulkStore

	logger   logger.Logger
	metadata pgMetadata
	db       pginterfaces.PGXPoolConn

	gc sqlinternal.GarbageCollector

	enableAzureAD bool
	enableAWSIAM  bool

	awsAuthProvider awsAuth.Provider
}

type Options struct {
	// Disables support for authenticating with Azure AD
	// This should be set to "false" when targeting different databases than PostgreSQL (such as CockroachDB)
	NoAzureAD bool

	// Disables support for authenticating with AWS IAM
	// This should be set to "false" when targeting different databases than PostgreSQL (such as CockroachDB)
	NoAWSIAM bool
}

// NewPostgreSQLStateStore creates a new instance of PostgreSQL state store v2 with the default options.
// The v2 of the component uses a different format for storing data, always in a BYTEA column, which is more efficient than the JSONB column used in v1.
// Additionally, v2 uses random UUIDs for etags instead of the xmin column, expanding support to all Postgres-compatible databases such as CockroachDB, etc.
func NewPostgreSQLStateStore(logger logger.Logger) state.Store {
	return NewPostgreSQLStateStoreWithOptions(logger, Options{})
}

// NewPostgreSQLStateStoreWithOptions creates a new instance of PostgreSQL state store with options.
func NewPostgreSQLStateStoreWithOptions(logger logger.Logger, opts Options) state.Store {
	s := &PostgreSQL{
		logger:        logger,
		enableAzureAD: !opts.NoAzureAD,
		enableAWSIAM:  !opts.NoAWSIAM,
	}
	s.BulkStore = state.NewDefaultBulkStore(s)
	return s
}

// Init sets up Postgres connection and performs migrations
func (p *PostgreSQL) Init(ctx context.Context, meta state.Metadata) (err error) {
	opts := pgauth.InitWithMetadataOpts{
		AzureADEnabled: p.enableAzureAD,
		AWSIAMEnabled:  p.enableAWSIAM,
	}

	err = p.metadata.InitWithMetadata(meta, opts)
	if err != nil {
		return err
	}

	config, err := p.metadata.GetPgxPoolConfig()
	if err != nil {
		return err
	}

	if opts.AWSIAMEnabled && p.metadata.UseAWSIAM {
		opts, validateErr := p.metadata.BuildAwsIamOptions(p.logger, meta.Properties)
		if validateErr != nil {
			return fmt.Errorf("failed to validate AWS IAM authentication fields: %w", validateErr)
		}

		var provider awsAuth.Provider
		provider, err = awsAuth.NewProvider(ctx, *opts, awsAuth.GetConfig(*opts))
		if err != nil {
			return err
		}
		p.awsAuthProvider = provider
		p.awsAuthProvider.UpdatePostgres(ctx, config)
	}

	connCtx, connCancel := context.WithTimeout(ctx, p.metadata.Timeout)
	defer connCancel()
	p.db, err = pgxpool.NewWithConfig(connCtx, config)
	if err != nil {
		err = fmt.Errorf("failed to connect to the database: %w", err)
		return err
	}

	pingCtx, pingCancel := context.WithTimeout(ctx, p.metadata.Timeout)
	defer pingCancel()
	err = p.db.Ping(pingCtx)
	if err != nil {
		err = fmt.Errorf("failed to ping the database: %w", err)
		return err
	}

	// Migrate schema
	err = p.performMigrations(ctx)
	if err != nil {
		return err
	}

	if p.metadata.CleanupInterval != nil {
		gc, err := sqlinternal.ScheduleGarbageCollector(sqlinternal.GCOptions{
			Logger: p.logger,
			UpdateLastCleanupQuery: func(arg any) (string, any) {
				return fmt.Sprintf(
					`INSERT INTO %[1]s (key, value)
				VALUES ('last-cleanup-state-v2-%[2]s', now()::text)
				ON CONFLICT (key)
				DO UPDATE SET value = now()::text
					WHERE (EXTRACT('epoch' FROM now() - %[1]s.value::timestamp with time zone) * 1000)::bigint > $1`,
					p.metadata.MetadataTableName,
					p.metadata.TablePrefix,
				), arg
			},
			DeleteExpiredValuesQuery: fmt.Sprintf(
				`DELETE FROM %s WHERE expires_at IS NOT NULL AND expires_at < now()`,
				p.metadata.TableName(pgTableState),
			),
			CleanupInterval: *p.metadata.CleanupInterval,
			DB:              sqlinternal.AdaptPgxConn(p.db),
		})
		if err != nil {
			return err
		}
		p.gc = gc
	}

	return nil
}

func (p *PostgreSQL) performMigrations(ctx context.Context) error {
	m := pgmigrations.Migrations{
		DB:                p.db,
		Logger:            p.logger,
		MetadataTableName: p.metadata.MetadataTableName,
		MetadataKey:       "migrations-state-v2-" + p.metadata.TablePrefix,
	}

	stateTable := p.metadata.TableName(pgTableState)

	return m.Perform(ctx, []sqlinternal.MigrationFn{
		// Migration 1: create the table for state
		func(ctx context.Context) error {
			p.logger.Infof("Creating state table: '%s'", stateTable)
			_, err := p.db.Exec(ctx,
				fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %[1]s (
  key text NOT NULL PRIMARY KEY,
  value bytea NOT NULL,
  etag uuid NOT NULL DEFAULT gen_random_uuid(),
  created_at timestamp with time zone NOT NULL DEFAULT now(),
  updated_at timestamp with time zone,
  expires_at timestamp with time zone
);
	
CREATE INDEX ON %[1]s (expires_at);
`, stateTable),
			)
			if err != nil {
				// Check if the error is about a duplicate key constraint violation.
				// Note: This can occur due to a race of multiple sidecars trying to run the table creation within their own transactions.
				// It's then a race to see who actually gets to create the table, and who gets the unique constraint violation error.
				var pgErr *pgconn.PgError
				if errors.As(err, &pgErr) && pgErr.Code == pgerrcode.UniqueViolation {
					p.logger.Debugf("ignoring PostgreSQL duplicate key error for table '%s'", stateTable)
				} else {
					return fmt.Errorf("failed to create state table: '%s', %v", stateTable, err)
				}
			}
			return nil
		},

		// Migration 2: add row_id (identity), backfill deterministically, enforce uniqueness (schema-safe)
		func(ctx context.Context) error {
			p.logger.Infof("Ensuring row_id (identity) exists on '%s'", stateTable)

			// Resolve schema + table and a safely quoted FQ table name
			var schema, table, fqtnQI, regName string
			if err := p.db.QueryRow(ctx, `
        SELECT n.nspname,
               c.relname,
               format('%I.%I', n.nspname, c.relname) AS fqtn_quoted,
               n.nspname || '.' || c.relname         AS reg_name
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.oid = to_regclass($1)
    `, stateTable).Scan(&schema, &table, &fqtnQI, &regName); err != nil || schema == "" || table == "" {
				if err == nil {
					err = fmt.Errorf("table %q not found", stateTable)
				}
				return fmt.Errorf("resolve table OID: %w", err)
			}

			// 1) Add column if missing
			var hasCol bool
			if err := p.db.QueryRow(ctx, `
        SELECT EXISTS (
          SELECT 1
          FROM pg_attribute
          WHERE attrelid = to_regclass($1)
            AND attname  = 'row_id'
            AND NOT attisdropped
        )`, stateTable).Scan(&hasCol); err != nil {
				return fmt.Errorf("introspect row_id: %w", err)
			}
			if !hasCol {
				if _, err := p.db.Exec(ctx, `ALTER TABLE `+fqtnQI+` ADD COLUMN row_id BIGINT`); err != nil {
					return fmt.Errorf("add row_id column: %w", err)
				}
			}

			// 2) Backfill NULLs deterministically (created_at, key)
			var nulls int64
			if err := p.db.QueryRow(ctx, `SELECT COUNT(*) FROM `+fqtnQI+` WHERE row_id IS NULL`).Scan(&nulls); err != nil {
				return fmt.Errorf("count NULL row_id: %w", err)
			}
			if nulls > 0 {
				if _, err := p.db.Exec(ctx, `
WITH ranked AS (
  SELECT key, ROW_NUMBER() OVER (ORDER BY created_at ASC, key ASC) AS rn
  FROM `+fqtnQI+`
  WHERE row_id IS NULL
)
UPDATE `+fqtnQI+` AS t
SET row_id = r.rn
FROM ranked r
WHERE r.key = t.key
`); err != nil {
					return fmt.Errorf("backfill row_id: %w", err)
				}
			}

			// 3) Enforce NOT NULL
			if _, err := p.db.Exec(ctx, `ALTER TABLE `+fqtnQI+` ALTER COLUMN row_id SET NOT NULL`); err != nil {
				return fmt.Errorf("set NOT NULL: %w", err)
			}

			// 4) Turn row_id into an identity column if not already
			var isIdentity bool
			if err := p.db.QueryRow(ctx, `
        SELECT EXISTS (
          SELECT 1
          FROM information_schema.columns
          WHERE table_schema = $1
            AND table_name   = $2
            AND column_name  = 'row_id'
            AND is_identity  = 'YES'
        )`, schema, table).Scan(&isIdentity); err != nil {
				return fmt.Errorf("check identity: %w", err)
			}
			if !isIdentity {
				if _, err := p.db.Exec(ctx, `ALTER TABLE `+fqtnQI+` ALTER COLUMN row_id ADD GENERATED BY DEFAULT AS IDENTITY`); err != nil {
					return fmt.Errorf("add identity: %w", err)
				}
			}

			// 5) Align the identity sequence to MAX(row_id)+1
			var seqName sql.NullString
			if err := p.db.QueryRow(ctx, `SELECT pg_get_serial_sequence($1, 'row_id')`, regName).Scan(&seqName); err != nil {
				return fmt.Errorf("get identity sequence: %w", err)
			}
			if seqName.Valid && seqName.String != "" {
				if _, err := p.db.Exec(ctx, `
            SELECT setval($1, COALESCE((SELECT MAX(row_id) FROM `+fqtnQI+`), 0) + 1, false)
        `, seqName.String); err != nil {
					return fmt.Errorf("set identity sequence value: %w", err)
				}
			}

			// 6) Unique index on row_id — schema-qualified and quoted
			idxNameQI := quoteIdent(table + "_row_id_uidx")
			if _, err := p.db.Exec(ctx,
				`CREATE UNIQUE INDEX IF NOT EXISTS `+idxNameQI+` ON `+fqtnQI+` (row_id)`); err != nil {
				return fmt.Errorf("create unique index on row_id: %w", err)
			}

			return nil
		},
	})
}

// Features returns the features available in this state store.
func (p *PostgreSQL) Features() []state.Feature {
	return []state.Feature{
		state.FeatureETag,
		state.FeatureTransactional,
		state.FeatureTTL,
		state.FeatureKeysLike,
	}
}

func (p *PostgreSQL) GetDB() *pgxpool.Pool {
	// We can safely cast to *pgxpool.Pool because this method is never used in unit tests where we mock the DB
	return p.db.(*pgxpool.Pool)
}

// Set makes an insert or update to the database.
func (p *PostgreSQL) Set(ctx context.Context, req *state.SetRequest) error {
	if req == nil {
		return errors.New("request object is nil")
	}
	return p.doSet(ctx, p.db, *req)
}

func (p *PostgreSQL) doSet(parentCtx context.Context, db pginterfaces.DBQuerier, req state.SetRequest) error {
	if req.Key == "" {
		return errors.New("missing key in set operation")
	}

	err := state.CheckRequestOptions(req.Options)
	if err != nil {
		return err
	}

	// If the value is a byte slice, accept it as-is; otherwise, encode to JSON
	var value []byte
	switch x := req.Value.(type) {
	case []byte:
		value = x
	default:
		value, err = json.Marshal(x)
		if err != nil {
			return fmt.Errorf("failed to marshal to JSON: %w", err)
		}
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

	var (
		queryExpiresAt string
		params         []any
	)

	if req.HasETag() {
		// Check if the etag is valid
		var etag uuid.UUID
		etag, err = uuid.Parse(*req.ETag)
		if err != nil {
			// Return an etag mismatch error right away if the etag is invalid
			return state.NewETagError(state.ETagMismatch, err)
		}

		params = []any{req.Key, value, etag.String()}
	} else {
		params = []any{req.Key, value}
	}

	if ttlSeconds > 0 {
		queryExpiresAt = "now() + interval '" + strconv.Itoa(ttlSeconds) + " seconds'"
	} else {
		queryExpiresAt = "NULL"
	}

	// Sprintf is required for table name because the driver does not substitute parameters for table names.
	var query string
	if !req.HasETag() {
		// We do an upsert in both cases, even when concurrency is first-write, because the row may exist but be expired (and not yet garbage collected)
		// The difference is that with concurrency as first-write, we'll update the row only if it's expired
		var whereClause string
		if req.Options.Concurrency == state.FirstWrite {
			whereClause = " WHERE (t.expires_at IS NOT NULL AND t.expires_at < now())"
		}

		query = `
INSERT INTO ` + p.metadata.TableName(pgTableState) + ` AS t
  (key, value, etag, expires_at)
VALUES
  ($1, $2, gen_random_uuid(),` + queryExpiresAt + `)
ON CONFLICT (key)
DO UPDATE SET
  value = $2,
  updated_at = now(),
  etag = gen_random_uuid(),
  expires_at = ` + queryExpiresAt + whereClause
	} else {
		// When an etag is provided do an update - no insert.
		query = `
UPDATE ` + p.metadata.TableName(pgTableState) + `
SET
  value = $2,
  updated_at = now(),
  etag = gen_random_uuid(),
  expires_at = ` + queryExpiresAt + `
WHERE
  key = $1
  AND etag = $3
  AND (expires_at IS NULL OR expires_at >= now());`
	}

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

	var (
		value      []byte
		etag       *string
		expireTime *time.Time
	)
	query := `
SELECT
  value, etag, expires_at
FROM ` + p.metadata.TableName(pgTableState) + `
WHERE
  key = $1
  AND (expires_at IS NULL OR expires_at >= now())`

	ctx, cancel := context.WithTimeout(parentCtx, p.metadata.Timeout)
	defer cancel()
	row := p.db.QueryRow(ctx, query, req.Key)
	err := row.Scan(&value, &etag, &expireTime)
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
	query := `
SELECT
  key, value, etag, expires_at
FROM ` + p.metadata.TableName(pgTableState) + `
WHERE
  key = ANY($1)
  AND (expires_at IS NULL OR expires_at >= now())`
	ctx, cancel := context.WithTimeout(parentCtx, p.metadata.Timeout)
	defer cancel()
	rows, err := p.db.Query(ctx, query, keys)
	if err != nil {
		return nil, err
	}

	// Scan all rows
	var n int
	res := make([]state.BulkGetResponse, len(req))
	foundKeys := make(map[string]struct{}, len(req))
	for rows.Next() {
		if n >= len(req) {
			// Sanity check to prevent panics, which should never happen
			return nil, fmt.Errorf("query returned more records than expected (expected %d)", len(req))
		}

		r := state.BulkGetResponse{}
		var expireTime *time.Time
		err = rows.Scan(&r.Key, &r.Data, &r.ETag, &expireTime)
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
		n++
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

// Delete removes an item from the state store.
func (p *PostgreSQL) Delete(ctx context.Context, req *state.DeleteRequest) error {
	if req == nil {
		return errors.New("request object is nil")
	}
	return p.doDelete(ctx, p.db, *req)
}

func (p *PostgreSQL) doDelete(parentCtx context.Context, db pginterfaces.DBQuerier, req state.DeleteRequest) (err error) {
	if req.Key == "" {
		return errors.New("missing key in delete operation")
	}

	ctx, cancel := context.WithTimeout(parentCtx, p.metadata.Timeout)
	defer cancel()
	var result pgconn.CommandTag
	if req.HasETag() {
		// Check if the etag is valid
		var etag uuid.UUID
		etag, err = uuid.Parse(*req.ETag)
		if err != nil {
			// Return an etag mismatch error right away if the etag is invalid
			return state.NewETagError(state.ETagMismatch, err)
		}

		result, err = db.Exec(ctx, "DELETE FROM "+p.metadata.TableName(pgTableState)+" WHERE key = $1 AND etag = $2", req.Key, etag)
	} else {
		result, err = db.Exec(ctx, "DELETE FROM "+p.metadata.TableName(pgTableState)+" WHERE key = $1", req.Key)
	}
	if err != nil {
		return err
	}

	rows := result.RowsAffected()
	if rows != 1 && req.HasETag() {
		return state.NewETagError(state.ETagMismatch, nil)
	}

	return nil
}

func (p *PostgreSQL) Multi(parentCtx context.Context, request *state.TransactionalStateRequest) error {
	if request == nil {
		return nil
	}

	// If there's only 1 operation, skip starting a transaction
	switch len(request.Operations) {
	case 0:
		return nil
	case 1:
		return p.execMultiOperation(parentCtx, request.Operations[0], p.db)
	default:
		_, err := pgtransactions.ExecuteInTransaction[struct{}](parentCtx, p.logger, p.db, p.metadata.Timeout, func(ctx context.Context, tx pgx.Tx) (res struct{}, err error) {
			for _, op := range request.Operations {
				err = p.execMultiOperation(ctx, op, tx)
				if err != nil {
					return res, err
				}
			}

			return res, nil
		})
		return err
	}
}

func (p *PostgreSQL) execMultiOperation(ctx context.Context, op state.TransactionalStateOperation, db pginterfaces.DBQuerier) error {
	switch x := op.(type) {
	case state.SetRequest:
		return p.doSet(ctx, db, x)
	case state.DeleteRequest:
		return p.doDelete(ctx, db, x)
	default:
		return fmt.Errorf("unsupported operation: %s", op.Operation())
	}
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

	errs := make([]error, 2)
	if p.gc != nil {
		errs[0] = p.gc.Close()
	}

	if p.awsAuthProvider != nil {
		errs[1] = p.awsAuthProvider.Close()
	}
	return errors.Join(errs...)
}

// GetCleanupInterval returns the cleanupInterval property.
// This is primarily used for tests.
func (p *PostgreSQL) GetCleanupInterval() *time.Duration {
	return p.metadata.CleanupInterval
}

func (p *PostgreSQL) GetComponentMetadata() (metadataInfo metadata.MetadataMap) {
	metadataStruct := pgMetadata{}
	metadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, metadata.StateStoreType)
	return
}

func (p *PostgreSQL) KeysLike(ctx context.Context, req *state.KeysLikeRequest) (*state.KeysLikeResponse, error) {
	// 1) Validate pattern
	if len(req.Pattern) == 0 {
		return nil, state.ErrKeysLikeEmptyPattern
	}

	where := []string{
		"key LIKE $1",
		"(expires_at IS NULL OR expires_at > now())",
	}
	args := []any{req.Pattern}

	// 2) Continue strictly AFTER the last returned row_id of prev page
	if req.ContinuationToken != nil && *req.ContinuationToken != "" {
		rid, err := strconv.ParseInt(*req.ContinuationToken, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid continue token: %w", err)
		}
		where = append(where, fmt.Sprintf("row_id > $%d", len(args)+1))
		args = append(args, rid)
	}

	orderClause := " ORDER BY row_id ASC"

	limitClause := ""
	var pageSize uint32
	if req.PageSize != nil && *req.PageSize > 0 {
		pageSize = *req.PageSize
		// fetch one extra to detect "has next"
		limitClause = fmt.Sprintf(" LIMIT $%d", len(args)+1)
		args = append(args, pageSize+1)
	}

	query := fmt.Sprintf(`
		SELECT key, row_id
		FROM %s
		WHERE %s%s%s
	`, p.metadata.TableName(pgTableState), strings.Join(where, " AND "), orderClause, limitClause)

	rows, err := p.db.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	type rec struct {
		key   string
		rowID int64
	}
	recs := make([]rec, 0, 256)

	for rows.Next() {
		var k string
		var rid int64
		if err := rows.Scan(&k, &rid); err != nil {
			return nil, err
		}
		recs = append(recs, rec{key: k, rowID: rid})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	resp := &state.KeysLikeResponse{
		Keys: make([]string, 0, len(recs)),
	}

	// 3) If we over-fetched, token must be LAST returned record (index pageSize-1)
	//nolint:gosec
	if pageSize > 0 && uint32(len(recs)) > pageSize {
		lastReturned := recs[pageSize-1]
		tok := strconv.FormatInt(lastReturned.rowID, 10)
		resp.ContinuationToken = &tok
		recs = recs[:pageSize]
	}

	for _, r := range recs {
		resp.Keys = append(resp.Keys, r.key)
	}

	return resp, nil
}

func quoteIdent(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}
