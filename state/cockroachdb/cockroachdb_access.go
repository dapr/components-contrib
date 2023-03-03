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
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dapr/kit/logger"
	"github.com/dapr/kit/ptr"
	"github.com/dapr/kit/retry"

	// Blank import for the underlying PostgreSQL driver.
	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/state"
	"github.com/dapr/components-contrib/state/query"
	"github.com/dapr/components-contrib/state/utils"
)

const (
	cleanupIntervalKey           = "cleanupIntervalInSeconds"
	connectionStringKey          = "connectionString"
	errMissingConnectionString   = "missing connection string"
	defaultTableName             = "state"
	defaultMetadataTableName     = "dapr_metadata"
	defaultMaxConnectionAttempts = 5 // A bad driver connection error can occur inside the sql code so this essentially allows for more retries since the sql code does not allow that to be changed
	defaultCleanupInterval       = time.Hour
)

// CockroachDBAccess implements dbaccess.
type CockroachDBAccess struct {
	logger           logger.Logger
	metadata         cockroachDBMetadata
	db               *sql.DB
	connectionString string
	closeCh          chan struct{}
	closed           atomic.Bool
	wg               sync.WaitGroup
}

type cockroachDBMetadata struct {
	ConnectionString      string
	TableName             string
	MetadataTableName     string
	CleanupInterval       *time.Duration
	MaxConnectionAttempts *int
}

// Interface that contains methods for querying.
type dbquerier interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

// newCockroachDBAccess creates a new instance of CockroachDBAccess.
func newCockroachDBAccess(logger logger.Logger) *CockroachDBAccess {
	logger.Debug("Instantiating new CockroachDB state store")

	return &CockroachDBAccess{
		logger:           logger,
		metadata:         cockroachDBMetadata{},
		db:               nil,
		connectionString: "",
		closeCh:          make(chan struct{}),
	}
}

func parseMetadata(meta state.Metadata) (*cockroachDBMetadata, error) {
	m := cockroachDBMetadata{
		CleanupInterval: ptr.Of(defaultCleanupInterval),
	}
	if err := metadata.DecodeMetadata(meta.Properties, &m); err != nil {
		return nil, err
	}

	if m.ConnectionString == "" {
		return nil, errors.New(errMissingConnectionString)
	}

	if len(m.TableName) == 0 {
		m.TableName = defaultTableName
	}

	if len(m.MetadataTableName) == 0 {
		m.MetadataTableName = defaultMetadataTableName
	}

	// Cleanup interval
	s, ok := meta.Properties[cleanupIntervalKey]
	if ok && s != "" {
		cleanupIntervalInSec, err := strconv.ParseInt(s, 10, 0)
		if err != nil {
			return nil, fmt.Errorf("invalid value for '%s': %s", cleanupIntervalKey, s)
		}

		// Non-positive value from meta means disable auto cleanup.
		if cleanupIntervalInSec > 0 {
			m.CleanupInterval = ptr.Of(time.Duration(cleanupIntervalInSec) * time.Second)
		} else {
			m.CleanupInterval = nil
		}
	}

	return &m, nil
}

// Init sets up CockroachDB connection and ensures that the state table exists.
func (c *CockroachDBAccess) Init(ctx context.Context, metadata state.Metadata) error {
	c.logger.Debug("Initializing CockroachDB state store")

	meta, err := parseMetadata(metadata)
	if err != nil {
		return err
	}
	c.metadata = *meta

	if c.metadata.ConnectionString == "" {
		c.logger.Error("Missing CockroachDB connection string")

		return fmt.Errorf(errMissingConnectionString)
	} else {
		c.connectionString = c.metadata.ConnectionString
	}

	databaseConn, err := sql.Open("pgx", c.connectionString)
	if err != nil {
		c.logger.Error(err)

		return err
	}

	c.db = databaseConn

	if err = databaseConn.PingContext(ctx); err != nil {
		return err
	}

	if err = c.ensureStateTable(ctx, c.metadata.TableName); err != nil {
		return err
	}

	if err = c.ensureMetadataTable(ctx, c.metadata.MetadataTableName); err != nil {
		return err
	}

	// Ensure that a connection to the database is actually established
	if err = c.Ping(ctx); err != nil {
		return err
	}

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		c.scheduleCleanup(ctx)
	}()

	return nil
}

// Set makes an insert or update to the database.
func (c *CockroachDBAccess) Set(ctx context.Context, req *state.SetRequest) error {
	return c.set(ctx, c.db, req)
}

func (c *CockroachDBAccess) set(ctx context.Context, d dbquerier, req *state.SetRequest) error {
	c.logger.Debug("Setting state value in CockroachDB")

	value, isBinary, err := validateAndReturnValue(req)
	if err != nil {
		return err
	}

	// TTL
	var ttlSeconds int
	ttl, ttlerr := utils.ParseTTL(req.Metadata)
	if ttlerr != nil {
		return fmt.Errorf("error parsing TTL: %w", ttlerr)
	}
	if ttl != nil {
		ttlSeconds = *ttl
	}

	var (
		query    string
		ttlQuery string
		params   []any
	)

	// Sprintf is required for table name because sql.DB does not substitute parameters for table names.
	// Other parameters use sql.DB parameter substitution.
	if req.ETag == nil {
		query = `
INSERT INTO %[1]s
  (key, value, isbinary, etag, expiredate)
VALUES
  ($1, $2, $3, 1, %[2]s)
ON CONFLICT (key) DO UPDATE SET
  value = $2,
  isbinary = $3,
  updatedate = NOW(),
  etag = EXCLUDED.etag + 1,
  expiredate = %[2]s
;`
		params = []any{req.Key, value, isBinary}
	} else {
		var etag64 uint64
		etag64, err = strconv.ParseUint(*req.ETag, 10, 32)
		if err != nil {
			return state.NewETagError(state.ETagInvalid, err)
		}
		etag := uint32(etag64)

		// When an etag is provided do an update - no insert.
		query = `
UPDATE %[1]s
SET
  value = $1,
  isbinary = $2,
  updatedate = NOW(),
  etag = etag + 1,
  expiredate = %[2]s
WHERE
  key = $3 AND etag = $4
;`
		params = []any{value, isBinary, req.Key, etag}
	}

	if ttlSeconds > 0 {
		ttlQuery = "CURRENT_TIMESTAMP + INTERVAL '" + strconv.Itoa(ttlSeconds) + " seconds'"
	} else {
		ttlQuery = "NULL"
	}

	result, err := d.ExecContext(ctx, fmt.Sprintf(query, c.metadata.TableName, ttlQuery), params...)
	if err != nil {
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

func (c *CockroachDBAccess) BulkSet(ctx context.Context, req []state.SetRequest) error {
	c.logger.Debug("Executing BulkSet request")
	tx, err := c.db.Begin()
	if err != nil {
		return err
	}

	if len(req) > 0 {
		for _, s := range req {
			sa := s // Fix for gosec  G601: Implicit memory aliasing in for loop.
			err = c.set(ctx, tx, &sa)
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
func (c *CockroachDBAccess) Get(ctx context.Context, req *state.GetRequest) (*state.GetResponse, error) {
	return c.get(ctx, c.db, req)
}

func (c *CockroachDBAccess) get(ctx context.Context, d dbquerier, req *state.GetRequest) (*state.GetResponse, error) {
	c.logger.Debug("Getting state value from CockroachDB")

	if req.Key == "" {
		return nil, fmt.Errorf("missing key in get operation")
	}

	var value string
	var isBinary bool
	var etag int
	query := `
SELECT
  value, isbinary, etag
FROM %s
WHERE
  key = $1
	AND (expiredate IS NULL OR expiredate > CURRENT_TIMESTAMP)
;`
	err := d.QueryRowContext(ctx, fmt.Sprintf(query, c.metadata.TableName), req.Key).Scan(&value, &isBinary, &etag)
	if err != nil {
		// If no rows exist, return an empty response, otherwise return the error.
		if errors.Is(err, sql.ErrNoRows) {
			return &state.GetResponse{}, nil
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
			ETag:        ptr.Of(strconv.Itoa(etag)),
			Metadata:    req.Metadata,
			ContentType: nil,
		}, nil
	}

	return &state.GetResponse{
		Data:        []byte(value),
		ETag:        ptr.Of(strconv.Itoa(etag)),
		Metadata:    req.Metadata,
		ContentType: nil,
	}, nil
}

// Delete removes an item from the state store.
func (c *CockroachDBAccess) Delete(ctx context.Context, req *state.DeleteRequest) error {
	return c.delete(ctx, c.db, req)
}

func (c *CockroachDBAccess) delete(ctx context.Context, d dbquerier, req *state.DeleteRequest) error {
	c.logger.Debug("Deleting state value from CockroachDB")

	if req.Key == "" {
		return fmt.Errorf("missing key in delete operation")
	}

	var result sql.Result
	var err error

	if req.ETag == nil {
		result, err = d.ExecContext(ctx, "DELETE FROM state WHERE key = $1", req.Key)
	} else {
		var etag64 uint64
		etag64, err = strconv.ParseUint(*req.ETag, 10, 32)
		if err != nil {
			return state.NewETagError(state.ETagInvalid, err)
		}
		etag := uint32(etag64)

		result, err = d.ExecContext(ctx, "DELETE FROM state WHERE key = $1 and etag = $2", req.Key, etag)
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

func (c *CockroachDBAccess) BulkDelete(ctx context.Context, req []state.DeleteRequest) error {
	c.logger.Debug("Executing BulkDelete request")
	tx, err := c.db.Begin()
	if err != nil {
		return err
	}

	if len(req) > 0 {
		for _, d := range req {
			da := d // Fix for gosec  G601: Implicit memory aliasing in for loop.
			err = c.delete(ctx, tx, &da)
			if err != nil {
				tx.Rollback()

				return err
			}
		}
	}

	err = tx.Commit()

	return err
}

func (c *CockroachDBAccess) ExecuteMulti(ctx context.Context, request *state.TransactionalStateRequest) error {
	c.logger.Debug("Executing CockroachDB transaction")

	tx, err := c.db.Begin()
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

			err = c.set(ctx, tx, &setReq)
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

			err = c.delete(ctx, tx, &delReq)
			if err != nil {
				tx.Rollback()
				return err
			}

		default:
			tx.Rollback()
			return fmt.Errorf("unsupported operation: %s", o.Operation)
		}
	}

	return tx.Commit()
}

// Query executes a query against store.
func (c *CockroachDBAccess) Query(ctx context.Context, req *state.QueryRequest) (*state.QueryResponse, error) {
	c.logger.Debug("Getting query value from CockroachDB")

	stateQuery := &Query{
		tableName: c.metadata.TableName,
		query:     "",
		params:    []interface{}{},
		limit:     0,
		skip:      ptr.Of[int64](0),
	}
	qbuilder := query.NewQueryBuilder(stateQuery)
	if err := qbuilder.BuildQuery(&req.Query); err != nil {
		return &state.QueryResponse{
			Results:  []state.QueryItem{},
			Token:    "",
			Metadata: map[string]string{},
		}, err
	}

	c.logger.Debug("Query: " + stateQuery.query)

	data, token, err := stateQuery.execute(ctx, c.logger, c.db)
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
func (c *CockroachDBAccess) Ping(ctx context.Context) error {
	retryCount := defaultMaxConnectionAttempts
	if c.metadata.MaxConnectionAttempts != nil && *c.metadata.MaxConnectionAttempts >= 0 {
		retryCount = *c.metadata.MaxConnectionAttempts
	}
	config := retry.DefaultConfig()
	config.Policy = retry.PolicyExponential
	config.MaxInterval = 100 * time.Millisecond
	config.MaxRetries = int64(retryCount)
	backoff := config.NewBackOff()

	return retry.NotifyRecover(func() error {
		err := c.db.PingContext(ctx)
		if errors.Is(err, driver.ErrBadConn) {
			return fmt.Errorf("error when attempting to establish connection with cockroachDB: %v", err)
		}
		return nil
	}, backoff, func(err error, _ time.Duration) {
		c.logger.Debugf("Could not establish connection with cockroachDB. Retrying...: %v", err)
	}, func() {
		c.logger.Debug("Successfully established connection with cockroachDB after it previously failed")
	})
}

// Close implements io.Close.
func (c *CockroachDBAccess) Close() error {
	if c.closed.CompareAndSwap(false, true) {
		close(c.closeCh)
	}
	defer c.wg.Wait()

	if c.db != nil {
		return c.db.Close()
	}

	return nil
}

func (c *CockroachDBAccess) ensureStateTable(ctx context.Context, stateTableName string) error {
	exists, err := tableExists(ctx, c.db, stateTableName)
	if err != nil {
		return err
	}

	if !exists {
		c.logger.Info("Creating CockroachDB state table")
		_, err = c.db.ExecContext(ctx, fmt.Sprintf(`CREATE TABLE %s (
  key text NOT NULL PRIMARY KEY,
  value jsonb NOT NULL,
  isbinary boolean NOT NULL,
  etag INT,
  insertdate TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
  updatedate TIMESTAMP WITH TIME ZONE NULL,
  expiredate TIMESTAMP WITH TIME ZONE NULL,
	INDEX expiredate_idx (expiredate)
);`, stateTableName))
		if err != nil {
			return err
		}
	}

	// If table was created before v1.11.
	_, err = c.db.ExecContext(ctx, fmt.Sprintf(
		`ALTER TABLE %s ADD COLUMN IF NOT EXISTS expiredate TIMESTAMP WITH TIME ZONE NULL;`, stateTableName))
	if err != nil {
		return err
	}
	_, err = c.db.ExecContext(ctx, fmt.Sprintf(
		`CREATE INDEX IF NOT EXISTS expiredate_idx ON %s (expiredate);`, stateTableName))
	if err != nil {
		return err
	}

	return nil
}

func (c *CockroachDBAccess) ensureMetadataTable(ctx context.Context, metaTableName string) error {
	exists, err := tableExists(ctx, c.db, metaTableName)
	if err != nil {
		return err
	}

	if !exists {
		c.logger.Info("Creating CockroachDB metadata table")
		_, err = c.db.ExecContext(ctx, fmt.Sprintf(`CREATE TABLE %s (
			key text NOT NULL PRIMARY KEY,
			value text NOT NULL
);`, metaTableName))
		if err != nil {
			return err
		}
	}

	return nil
}

func tableExists(ctx context.Context, db *sql.DB, tableName string) (bool, error) {
	exists := false
	err := db.QueryRowContext(ctx, "SELECT EXISTS (SELECT * FROM pg_tables where tablename = $1)", tableName).Scan(&exists)

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

// Returns the set requests.
func getSet(req state.TransactionalStateOperation) (state.SetRequest, error) {
	setReq, ok := req.Request.(state.SetRequest)
	if !ok {
		return setReq, fmt.Errorf("expecting set request")
	}

	if setReq.Key == "" {
		return setReq, fmt.Errorf("missing key in upsert operation")
	}

	return setReq, nil
}

// Returns the delete requests.
func getDelete(req state.TransactionalStateOperation) (state.DeleteRequest, error) {
	delReq, ok := req.Request.(state.DeleteRequest)
	if !ok {
		return delReq, fmt.Errorf("expecting delete request")
	}

	if delReq.Key == "" {
		return delReq, fmt.Errorf("missing key in delete operation")
	}

	return delReq, nil
}

func (c *CockroachDBAccess) scheduleCleanup(ctx context.Context) {
	if c.metadata.CleanupInterval == nil || *c.metadata.CleanupInterval <= 0 || c.closed.Load() {
		return
	}

	c.logger.Infof("Schedule expired data clean up every %d seconds", int(c.metadata.CleanupInterval.Seconds()))
	ticker := time.NewTicker(*c.metadata.CleanupInterval)
	defer ticker.Stop()

	var err error
	for {
		select {
		case <-ticker.C:
			err = c.CleanupExpired(ctx)
			if err != nil {
				c.logger.Errorf("Error removing expired data: %v", err)
			}
		case <-ctx.Done():
			c.logger.Debug("Stopped background cleanup of expired data")
			return
		case <-c.closeCh:
			c.logger.Debug("Stopping background because CockroachDBAccess is closing")
			return
		}
	}
}

func (c *CockroachDBAccess) CleanupExpired(ctx context.Context) error {
	// Check if the last iteration was too recent
	// This performs an atomic operation, so allows coordination with other daprd
	// processes too
	canContinue, err := c.updateLastCleanup(ctx, *c.metadata.CleanupInterval)
	if err != nil {
		// Log errors only
		c.logger.Warnf("Failed to read last cleanup time from database: %v", err)
	}
	if !canContinue {
		c.logger.Debug("Last cleanup was performed too recently")
		return nil
	}

	// Note we're not using the transaction here as we don't want this to be
	// rolled back half-way or to lock the table unnecessarily.
	// Need to use fmt.Sprintf because we can't parametrize a table name.
	// Note we are not setting a timeout here as this query can take a "long"
	// time, especially if there's no index on expiredate .
	stmt := fmt.Sprintf(`DELETE FROM %[1]s WHERE expiredate IS NOT NULL AND expiredate < CURRENT_TIMESTAMP`, c.metadata.TableName)
	res, err := c.db.ExecContext(ctx, stmt)
	if err != nil {
		return fmt.Errorf("failed to execute query: %w", err)
	}

	rows, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}
	c.logger.Infof("Removed %d expired rows", rows)

	return nil
}

// updateLastCleanup sets the 'last-cleanup' value only if it's less than
// cleanupInterval.
// Returns true if the row was updated, which means that the cleanup can
// proceed.
func (c *CockroachDBAccess) updateLastCleanup(ctx context.Context, cleanupInterval time.Duration) (bool, error) {
	res, err := c.db.ExecContext(ctx,
		fmt.Sprintf(`INSERT INTO %[1]s (key, value)
      VALUES ('last-cleanup', CURRENT_TIMESTAMP::STRING)
			ON CONFLICT (key)
			DO UPDATE SET value = CURRENT_TIMESTAMP::STRING
				WHERE (EXTRACT('epoch' FROM CURRENT_TIMESTAMP - %[1]s.value::timestamp with time zone) * 1000)::bigint > $1`,
			c.metadata.MetadataTableName),
		cleanupInterval.Milliseconds()-100, // Subtract 100ms for some buffer
	)
	if err != nil {
		return false, fmt.Errorf("failed to execute query: %w", err)
	}

	n, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("failed to get rows affected: %w", err)
	}
	return n > 0, nil
}

// GetCleanupInterval returns the cleanupInterval property.
// This is primarily used for tests.
func (c *CockroachDBAccess) GetCleanupInterval() *time.Duration {
	return c.metadata.CleanupInterval
}
