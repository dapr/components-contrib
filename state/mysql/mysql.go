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

package mysql

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	commonsql "github.com/dapr/components-contrib/common/component/sql"
	sqltransactions "github.com/dapr/components-contrib/common/component/sql/transactions"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/state"
	"github.com/dapr/components-contrib/state/utils"
	"github.com/dapr/kit/logger"
	kitmd "github.com/dapr/kit/metadata"
	"github.com/dapr/kit/ptr"
)

// Optimistic Concurrency is implemented using a string column that stores a UUID.

const (
	// To connect to MySQL running in Azure over SSL you have to download a
	// SSL certificate. If this is provided the driver will connect using
	// SSL. If you have disable SSL you can leave this empty.
	// When the user provides a pem path their connection string must end with
	// &tls=custom
	// The connection string should be in the following format
	// "%s:%s@tcp(%s:3306)/%s?allowNativePasswords=true&tls=custom",'myadmin@mydemoserver', 'yourpassword', 'mydemoserver.mysql.database.azure.com', 'targetdb'.
	keyPemPath = "pemPath"

	// Used if the user does not configure a table name in the metadata.
	defaultTableName = "state"

	// Used if the user does not configure a database name in the metadata.
	defaultSchemaName = "dapr_state_store"

	// Used if the user does not provide a timeoutInSeconds value in the metadata.
	defaultTimeoutInSeconds = 20

	// Standard error message if not connection string is provided.
	errMissingConnectionString = "missing connection string"

	// Key name to configure interval at which entries with TTL are cleaned up.
	// This is parsed as a Go duration.
	cleanupIntervalKey = "cleanupInterval"

	// Used if the user does not configure a metadata table name in the metadata.
	// In terms of TTL, it is required to store value for 'last-cleanup' id.
	defaultMetadataTableName = "dapr_metadata"

	// Used if the user does not configure a cleanup interval in the metadata.
	defaultCleanupInterval = time.Hour
)

// MySQL state store.
type MySQL struct {
	state.BulkStore

	tableName         string
	metadataTableName string
	cleanupInterval   *time.Duration
	schemaName        string
	connectionString  string
	timeout           time.Duration

	// Instance of the database to issue commands to
	db *sql.DB

	// Logger used in a functions
	logger logger.Logger

	factory iMySQLFactory
	gc      commonsql.GarbageCollector
}

type mySQLMetadata struct {
	TableName         string
	SchemaName        string
	ConnectionString  string
	TimeoutInSeconds  int
	PemPath           string
	MetadataTableName string
	CleanupInterval   *time.Duration
}

// NewMySQLStateStore creates a new instance of MySQL state store.
func NewMySQLStateStore(logger logger.Logger) state.Store {
	factory := newMySQLFactory(logger)

	// Store the provided logger and return the object. The rest of the
	// properties will be populated in the Init function
	return newMySQLStateStore(logger, factory)
}

// Hidden implementation for testing.
func newMySQLStateStore(logger logger.Logger, factory iMySQLFactory) *MySQL {
	// Store the provided logger and return the object. The rest of the
	// properties will be populated in the Init function
	s := &MySQL{
		logger:  logger,
		factory: factory,
		timeout: 5 * time.Second,
	}
	s.BulkStore = state.NewDefaultBulkStore(s)
	return s
}

// Init initializes the SQL server state store
// Implements the following interfaces:
// Store
// TransactionalStore
// Populate the rest of the MySQL object by reading the metadata and opening
// a connection to the server.
func (m *MySQL) Init(ctx context.Context, metadata state.Metadata) error {
	m.logger.Debug("Initializing MySql state store")

	err := m.parseMetadata(metadata.Properties)
	if err != nil {
		return err
	}

	db, err := m.factory.Open(m.connectionString)
	if err != nil {
		m.logger.Error(err)
		return err
	}

	// will be nil if everything is good or an err that needs to be returned
	return m.finishInit(ctx, db)
}

func (m *MySQL) parseMetadata(md map[string]string) error {
	meta := mySQLMetadata{
		TableName:         defaultTableName,
		SchemaName:        defaultSchemaName,
		MetadataTableName: defaultMetadataTableName,
		CleanupInterval:   ptr.Of(defaultCleanupInterval),
	}

	err := kitmd.DecodeMetadata(md, &meta)
	if err != nil {
		return err
	}

	if meta.TableName != "" {
		// Sanitize the table name
		if !validIdentifier(meta.TableName) {
			return fmt.Errorf("table name '%s' is not valid", meta.TableName)
		}
	}
	m.tableName = meta.TableName

	if meta.MetadataTableName != "" {
		// Sanitize the metadata table name
		if !validIdentifier(meta.MetadataTableName) {
			return fmt.Errorf("metadata table name '%s' is not valid", meta.MetadataTableName)
		}
	}
	m.metadataTableName = meta.MetadataTableName

	if meta.SchemaName != "" {
		// Sanitize the schema name
		if !validIdentifier(meta.SchemaName) {
			return fmt.Errorf("schema name '%s' is not valid", meta.SchemaName)
		}
	}
	m.schemaName = meta.SchemaName

	if meta.ConnectionString == "" {
		m.logger.Error("Missing MySql connection string")
		return errors.New(errMissingConnectionString)
	}
	m.connectionString = meta.ConnectionString

	// Cleanup interval
	if meta.CleanupInterval != nil {
		// Non-positive value from meta means disable auto cleanup.
		if *meta.CleanupInterval <= 0 {
			if md[cleanupIntervalKey] == "" {
				// unfortunately the mapstructure decoder decodes an empty string to 0, a missing key would be nil however
				meta.CleanupInterval = ptr.Of(defaultCleanupInterval)
			} else {
				meta.CleanupInterval = nil
			}
		}

		m.cleanupInterval = meta.CleanupInterval
	}

	if meta.PemPath != "" {
		err := m.factory.RegisterTLSConfig(meta.PemPath)
		if err != nil {
			m.logger.Error(err)
			return err
		}
	}

	if meta.TimeoutInSeconds > 0 {
		m.timeout = time.Duration(meta.TimeoutInSeconds) * time.Second
	} else {
		m.timeout = time.Duration(defaultTimeoutInSeconds) * time.Second
	}

	return nil
}

// Features returns the features available in this state store.
func (m *MySQL) Features() []state.Feature {
	return []state.Feature{
		state.FeatureETag,
		state.FeatureTransactional,
		state.FeatureTTL,
	}
}

// Ping the database.
func (m *MySQL) Ping(ctx context.Context) error {
	if m.db == nil {
		return sql.ErrConnDone
	}

	return m.PingWithContext(ctx)
}

// PingWithContext is like Ping but accepts a context.
func (m *MySQL) PingWithContext(parentCtx context.Context) error {
	ctx, cancel := context.WithTimeout(parentCtx, m.timeout)
	defer cancel()
	return m.db.PingContext(ctx)
}

// Separated out to make this portion of code testable.
func (m *MySQL) finishInit(ctx context.Context, db *sql.DB) error {
	m.db = db

	err := m.ensureStateSchema(ctx)
	if err != nil {
		m.logger.Error(err)
		return err
	}

	err = m.Ping(ctx)
	if err != nil {
		m.logger.Error(err)
		return err
	}

	// will be nil if everything is good or an err that needs to be returned
	if err = m.ensureStateTable(ctx, m.schemaName, m.tableName); err != nil {
		return err
	}

	if err = m.ensureMetadataTable(ctx, m.schemaName, m.metadataTableName); err != nil {
		return err
	}

	if m.cleanupInterval != nil {
		gc, err := commonsql.ScheduleGarbageCollector(commonsql.GCOptions{
			Logger: m.logger,
			UpdateLastCleanupQuery: func(arg any) (string, any) {
				return fmt.Sprintf(`INSERT INTO %[1]s (id, value)
				VALUES ('last-cleanup', CURRENT_TIMESTAMP)
				ON DUPLICATE KEY UPDATE
				value = IF(CURRENT_TIMESTAMP > DATE_ADD(value, INTERVAL ?*1000 MICROSECOND), CURRENT_TIMESTAMP, value)`,
					m.metadataTableName), arg
			},
			DeleteExpiredValuesQuery: fmt.Sprintf(
				`DELETE FROM %s WHERE expiredate IS NOT NULL AND expiredate <= CURRENT_TIMESTAMP`,
				m.tableName,
			),
			CleanupInterval: *m.cleanupInterval,
			DB:              commonsql.AdaptDatabaseSQLConn(m.db),
		})
		if err != nil {
			return err
		}
		m.gc = gc
	}
	return nil
}

func (m *MySQL) ensureStateSchema(ctx context.Context) error {
	exists, err := schemaExists(ctx, m.db, m.schemaName, m.timeout)
	if err != nil {
		return err
	}

	if !exists {
		m.logger.Infof("Creating MySql schema '%s'", m.schemaName)
		cctx, cancel := context.WithTimeout(ctx, m.timeout)
		defer cancel()
		_, err = m.db.ExecContext(cctx,
			"CREATE DATABASE "+m.schemaName,
		)
		if err != nil {
			return err
		}
	}

	// Build a connection string that contains the new schema name
	// All MySQL connection strings must contain a / so split on it.
	parts := strings.Split(m.connectionString, "/")

	// Even if the connection string ends with a / parts will have two values
	// with the second being an empty string.
	m.connectionString = fmt.Sprintf("%s/%s%s", parts[0], m.schemaName, parts[1])

	// Close the connection we used to confirm and or create the schema
	err = m.db.Close()
	if err != nil {
		return err
	}

	// Open a connection to the new schema
	m.db, err = m.factory.Open(m.connectionString)

	return err
}

func (m *MySQL) ensureStateTable(ctx context.Context, schemaName, stateTableName string) error {
	tableExists, err := tableExists(ctx, m.db, schemaName, stateTableName, m.timeout)
	if err != nil {
		return err
	}

	if !tableExists {
		m.logger.Infof("Creating MySql state table '%s'", stateTableName)

		// updateDate is updated automactically on every UPDATE commands so you
		// never need to pass it in.
		// eTag is a UUID stored as a 36 characters string. It needs to be passed
		// in on inserts and updates and is used for Optimistic Concurrency
		// Note that stateTableName is sanitized
		//nolint:gosec
		createTable := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
			id VARCHAR(255) NOT NULL PRIMARY KEY,
			value JSON NOT NULL,
			isbinary BOOLEAN NOT NULL,
			insertDate TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
			updateDate TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
			eTag VARCHAR(36) NOT NULL,
			expiredate TIMESTAMP NULL,
			INDEX expiredate_idx(expiredate)
			);`, stateTableName)

		_, err = m.db.ExecContext(ctx, createTable)
		if err != nil {
			return err
		}
	}

	// Check if expiredate column exists - to cater cases when table was created before v1.11.
	columnExists, err := columnExists(ctx, m.db, schemaName, stateTableName, "expiredate", m.timeout)
	if err != nil {
		return err
	}

	if !columnExists {
		m.logger.Infof("Adding expiredate column to MySql state table '%s'", stateTableName)
		_, err = m.db.ExecContext(ctx, fmt.Sprintf(
			`ALTER TABLE %s ADD COLUMN IF NOT EXISTS expiredate TIMESTAMP NULL;`, stateTableName))
		if err != nil {
			return err
		}
		_, err = m.db.ExecContext(ctx, fmt.Sprintf(
			`CREATE INDEX IF NOT EXISTS expiredate_idx ON %s (expiredate);`, stateTableName))
		if err != nil {
			return err
		}
	}

	return nil
}

func (m *MySQL) ensureMetadataTable(ctx context.Context, schemaName, metaTableName string) error {
	exists, err := tableExists(ctx, m.db, schemaName, metaTableName, m.timeout)
	if err != nil {
		return err
	}

	if !exists {
		m.logger.Info("Creating MySQL metadata table")
		_, err = m.db.ExecContext(ctx, fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
			id VARCHAR(255) NOT NULL PRIMARY KEY, value TEXT NOT NULL);`, metaTableName))
		if err != nil {
			return err
		}
	}

	return nil
}

func schemaExists(ctx context.Context, db *sql.DB, schemaName string, timeout time.Duration) (bool, error) {
	schemeCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	// Returns 1 or 0 if the table exists or not
	var exists int
	query := `SELECT EXISTS (
		SELECT SCHEMA_NAME FROM information_schema.schemata WHERE SCHEMA_NAME = ?
	) AS 'exists'`
	err := db.QueryRowContext(schemeCtx, query, schemaName).Scan(&exists)
	return exists == 1, err
}

func tableExists(ctx context.Context, db *sql.DB, schemaName, tableName string, timeout time.Duration) (bool, error) {
	tableCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	// Returns 1 or 0 if the table exists or not
	var exists int
	query := `SELECT EXISTS (
		SELECT TABLE_NAME FROM information_schema.tables WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
	) AS 'exists'`
	err := db.QueryRowContext(tableCtx, query, schemaName, tableName).Scan(&exists)
	return exists == 1, err
}

// columnExists returns true if the column exists in the table
func columnExists(ctx context.Context, db *sql.DB, schemaName, tableName, columnName string, timeout time.Duration) (bool, error) {
	columnCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	// Returns 1 or 0 if the column exists or not
	var exists int
	query := `SELECT count(*) AS 'exists' FROM information_schema.columns
	WHERE table_schema = ?
	  AND table_name = ?
	  AND column_name = ?`
	err := db.QueryRowContext(columnCtx, query, schemaName, tableName, columnName).Scan(&exists)
	return exists == 1, err
}

// Delete removes an entity from the store
// Store Interface.
func (m *MySQL) Delete(ctx context.Context, req *state.DeleteRequest) error {
	return m.deleteValue(ctx, m.db, req)
}

// deleteValue is an internal implementation of delete to enable passing the
// logic to state.DeleteWithRetries as a func.
func (m *MySQL) deleteValue(parentCtx context.Context, querier querier, req *state.DeleteRequest) error {
	if req.Key == "" {
		return errors.New("missing key in delete operation")
	}

	var (
		err    error
		result sql.Result
	)

	execCtx, cancel := context.WithTimeout(parentCtx, m.timeout)
	defer cancel()

	if !req.HasETag() {
		result, err = querier.ExecContext(execCtx,
			`DELETE FROM `+m.tableName+` WHERE id = ?`,
			req.Key)
	} else {
		result, err = querier.ExecContext(execCtx,
			`DELETE FROM `+m.tableName+` WHERE id = ? AND eTag = ?`,
			req.Key, *req.ETag)
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

// Get returns an entity from store
// Store Interface.
func (m *MySQL) Get(parentCtx context.Context, req *state.GetRequest) (*state.GetResponse, error) {
	if req.Key == "" {
		return nil, errors.New("missing key in get operation")
	}

	ctx, cancel := context.WithTimeout(parentCtx, m.timeout)
	defer cancel()
	// Concatenation is required for table name because sql.DB does not substitute parameters for table names
	//nolint:gosec
	query := `SELECT id, value, eTag, isbinary, IFNULL(expiredate, "") FROM ` + m.tableName + ` WHERE id = ?
			AND (expiredate IS NULL OR expiredate > CURRENT_TIMESTAMP)`
	row := m.db.QueryRowContext(ctx, query, req.Key)
	_, value, etag, expireTime, err := readRow(row)
	if err != nil {
		// If no rows exist, return an empty response, otherwise return an error.
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

// Set adds/updates an entity on store
// Store Interface.
func (m *MySQL) Set(ctx context.Context, req *state.SetRequest) error {
	return m.setValue(ctx, m.db, req)
}

// setValue is an internal implementation of set to enable passing the logic
// to state.SetWithRetries as a func.
func (m *MySQL) setValue(parentCtx context.Context, querier querier, req *state.SetRequest) error {
	err := state.CheckRequestOptions(req.Options)
	if err != nil {
		return err
	}

	if req.Key == "" {
		return errors.New("missing key in set operation")
	}

	// TTL
	var ttlSeconds int
	ttl, err := utils.ParseTTL(req.Metadata)
	if err != nil {
		return fmt.Errorf("error parsing TTL: %w", err)
	}
	if ttl != nil {
		ttlSeconds = *ttl
	}

	var (
		query    string
		ttlQuery string
		params   []any
		result   sql.Result
	)

	var v any
	isBinary := false
	switch x := req.Value.(type) {
	case []uint8:
		isBinary = true
		v = base64.StdEncoding.EncodeToString(x)
	default:
		v = x
	}

	encB, _ := json.Marshal(v)
	enc := string(encB)

	eTagObj, err := uuid.NewRandom()
	if err != nil {
		return fmt.Errorf("failed to generate etag: %w", err)
	}
	eTag := eTagObj.String()

	if ttlSeconds > 0 {
		ttlQuery = "CURRENT_TIMESTAMP + INTERVAL " + strconv.Itoa(ttlSeconds) + " SECOND"
	} else {
		ttlQuery = "NULL"
	}

	if req.HasETag() {
		// When an eTag is provided do an update - not insert
		query = `UPDATE ` + m.tableName + `
			SET value = ?, eTag = ?, isbinary = ?, expiredate = ` + ttlQuery + `
			WHERE id = ?
				AND eTag = ?
				AND (expiredate IS NULL OR expiredate > CURRENT_TIMESTAMP)`
		params = []any{enc, eTag, isBinary, req.Key, *req.ETag}
	} else if req.Options.Concurrency == state.FirstWrite {
		// If the operation uses first-write concurrency, we need to handle the special case of a row that has expired but hasn't been garbage collected yet
		// In this case, the row should be considered as if it were deleted
		query = `REPLACE INTO ` + m.tableName + `
			WITH a AS (
				SELECT
					? AS id,
					? AS value,
					? AS isbinary,
					CURRENT_TIMESTAMP AS insertDate,
					CURRENT_TIMESTAMP AS updateDate,
					? AS eTag,
					` + ttlQuery + ` AS expiredate
				WHERE NOT EXISTS (
					SELECT 1
					FROM ` + m.tableName + `
					WHERE id = ?
						AND (expiredate IS NULL OR expiredate > CURRENT_TIMESTAMP)
				)
			)
			SELECT * FROM a`
		params = []any{req.Key, enc, isBinary, eTag, req.Key}
	} else {
		query = `REPLACE INTO ` + m.tableName + ` (id, value, eTag, isbinary, expiredate)
			VALUES (?, ?, ?, ?, ` + ttlQuery + `)`
		params = []any{req.Key, enc, eTag, isBinary}
	}

	ctx, cancel := context.WithTimeout(parentCtx, m.timeout)
	defer cancel()
	result, err = querier.ExecContext(ctx, query, params...)
	if err != nil {
		return err
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}

	if rows == 0 && (req.HasETag() || req.Options.Concurrency == state.FirstWrite) {
		err = errors.New("rows affected error: no rows match given key and eTag")
		err = state.NewETagError(state.ETagMismatch, err)
		m.logger.Error(err)
		return err
	}

	return nil
}

func (m *MySQL) BulkGet(parentCtx context.Context, req []state.GetRequest, _ state.BulkGetOpts) ([]state.BulkGetResponse, error) {
	if len(req) == 0 {
		return []state.BulkGetResponse{}, nil
	}

	// MySQL doesn't support passing an array for an IN clause, so we need to build a custom query
	inClause := strings.Repeat("?,", len(req))
	inClause = inClause[:(len(inClause) - 1)]
	params := make([]any, len(req))
	for i, r := range req {
		params[i] = r.Key
	}

	// Concatenation is required for table name because sql.DB does not substitute parameters for table names
	//nolint:gosec
	stmt := `SELECT id, value, eTag, isbinary, IFNULL(expiredate, "") FROM ` + m.tableName + `
		WHERE
			id IN (` + inClause + `)
			AND (expiredate IS NULL OR expiredate > CURRENT_TIMESTAMP)`
	ctx, cancel := context.WithTimeout(parentCtx, m.timeout)
	defer cancel()
	rows, err := m.db.QueryContext(ctx, stmt, params...)
	if err != nil {
		return nil, err
	}

	var n int
	var expireTime *time.Time
	res := make([]state.BulkGetResponse, len(req))
	foundKeys := make(map[string]struct{}, len(req))
	for ; rows.Next(); n++ {
		if n >= len(req) {
			// Sanity check to prevent panics, which should never happen
			return nil, fmt.Errorf("query returned more records than expected (expected %d)", len(req))
		}

		r := state.BulkGetResponse{}
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

func readRow(row interface{ Scan(dest ...any) error }) (key string, value []byte, etagP *string, expireTime *time.Time, err error) {
	var (
		etag     string
		isBinary bool
		expire   string
	)
	err = row.Scan(&key, &value, &etag, &isBinary, &expire)
	if err != nil {
		return key, nil, nil, nil, err
	}

	if len(expire) > 0 {
		var expireT time.Time
		expireT, err = time.Parse(time.DateTime, expire)
		if err != nil {
			return key, nil, nil, nil, fmt.Errorf("failed to parse expiration time: %w", err)
		}
		expireTime = &expireT
	}

	if isBinary {
		var (
			s    string
			data []byte
		)

		err = json.Unmarshal(value, &s)
		if err != nil {
			return key, nil, nil, nil, fmt.Errorf("failed to unmarshal JSON binary data: %w", err)
		}

		data, err = base64.StdEncoding.DecodeString(s)
		if err != nil {
			return key, nil, nil, nil, fmt.Errorf("failed to decode binary data: %w", err)
		}
		return key, data, &etag, expireTime, nil
	}

	return key, value, &etag, expireTime, nil
}

// Multi handles multiple operations in batch.
// Implements the TransactionalStore Interface.
func (m *MySQL) Multi(ctx context.Context, request *state.TransactionalStateRequest) error {
	if request == nil {
		return nil
	}

	// If there's only 1 operation, skip starting a transaction
	switch len(request.Operations) {
	case 0:
		return nil
	case 1:
		return m.execMultiOperation(ctx, request.Operations[0], m.db)
	default:
		_, err := sqltransactions.ExecuteInTransaction(ctx, m.logger, m.db, func(ctx context.Context, tx *sql.Tx) (r struct{}, err error) {
			for _, op := range request.Operations {
				err = m.execMultiOperation(ctx, op, tx)
				if err != nil {
					return r, err
				}
			}
			return r, nil
		})
		return err
	}
}

func (m *MySQL) execMultiOperation(ctx context.Context, op state.TransactionalStateOperation, db querier) error {
	switch req := op.(type) {
	case state.SetRequest:
		return m.setValue(ctx, db, &req)
	case state.DeleteRequest:
		return m.deleteValue(ctx, db, &req)
	default:
		return fmt.Errorf("unsupported operation: %s", op.Operation())
	}
}

// Close implements io.Closer.
func (m *MySQL) Close() error {
	if m.db == nil {
		return nil
	}

	err := m.db.Close()
	m.db = nil
	if m.gc != nil {
		return errors.Join(err, m.gc.Close())
	}

	return err
}

// Validates an identifier, such as table or DB name.
// This is based on the rules for allowed unquoted identifiers (https://dev.mysql.com/doc/refman/8.0/en/identifiers.html), but more restrictive as it doesn't allow non-ASCII characters or the $ sign
func validIdentifier(v string) bool {
	if v == "" {
		return false
	}

	// Loop through the string as byte slice as we only care about ASCII characters
	b := []byte(v)
	for i := range b {
		if (b[i] >= '0' && b[i] <= '9') ||
			(b[i] >= 'a' && b[i] <= 'z') ||
			(b[i] >= 'A' && b[i] <= 'Z') ||
			b[i] == '_' {
			continue
		}
		return false
	}
	return true
}

// Interface for both sql.DB and sql.Tx
type querier interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

func (m *MySQL) GetComponentMetadata() (metadataInfo metadata.MetadataMap) {
	metadataStruct := mySQLMetadata{}
	metadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, metadata.StateStoreType)
	return
}
