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

package sqlserver

import (
	"context"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	commonsql "github.com/dapr/components-contrib/common/component/sql"
	sqltransactions "github.com/dapr/components-contrib/common/component/sql/transactions"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/state"
	"github.com/dapr/components-contrib/state/utils"
	"github.com/dapr/kit/logger"
	"github.com/dapr/kit/ptr"
)

// KeyType defines type of the table identifier.
type KeyType string

// KeyTypeFromString tries to create a KeyType from a string value.
func KeyTypeFromString(k string) (KeyType, error) {
	switch k {
	case string(StringKeyType):
		return StringKeyType, nil
	case string(UUIDKeyType):
		return UUIDKeyType, nil
	case string(IntegerKeyType):
		return IntegerKeyType, nil
	}

	return InvalidKeyType, errors.New("invalid key type")
}

const (
	// StringKeyType defines a key of type string.
	StringKeyType KeyType = "string"

	// UUIDKeyType defines a key of type UUID/GUID.
	UUIDKeyType KeyType = "uuid"

	// IntegerKeyType defines a key of type integer.
	IntegerKeyType KeyType = "integer"

	// InvalidKeyType defines an invalid key type.
	InvalidKeyType KeyType = "invalid"
)

// New creates a new instance of a SQL Server transaction store.
func New(logger logger.Logger) state.Store {
	s := &SQLServer{
		features: []state.Feature{
			state.FeatureETag,
			state.FeatureTransactional,
			state.FeatureTTL,
		},
		logger:          logger,
		migratorFactory: newMigration,
	}
	s.BulkStore = state.NewDefaultBulkStore(s)
	return s
}

// IndexedProperty defines a indexed property.
type IndexedProperty struct {
	ColumnName string `json:"column"`
	Property   string `json:"property"`
	Type       string `json:"type"`
}

// SQLServer defines a MS SQL Server based state store.
type SQLServer struct {
	state.BulkStore

	metadata sqlServerMetadata

	migratorFactory func(*sqlServerMetadata) migrator

	itemRefTableTypeName     string
	upsertCommand            string
	getCommand               string
	deleteWithETagCommand    string
	deleteWithoutETagCommand string

	features []state.Feature
	logger   logger.Logger
	db       *sql.DB
	gc       commonsql.GarbageCollector
}

// Init initializes the SQL server state store.
func (s *SQLServer) Init(ctx context.Context, metadata state.Metadata) error {
	s.metadata = newMetadata()
	metadata.GetProperty()
	err := s.metadata.Parse(metadata.Properties)
	if err != nil {
		return err
	}

	s.metadata.BulkGetChunkSize = normalizeBulkGetChunkSize(s.logger, s.metadata.BulkGetChunkSize)

	migration := s.migratorFactory(&s.metadata)
	mr, err := migration.executeMigrations(ctx)
	if err != nil {
		return err
	}

	s.itemRefTableTypeName = mr.itemRefTableTypeName
	s.upsertCommand = mr.upsertProcFullName
	s.getCommand = mr.getCommand
	s.deleteWithETagCommand = mr.deleteWithETagCommand
	s.deleteWithoutETagCommand = mr.deleteWithoutETagCommand

	conn, _, err := s.metadata.GetConnector(true)
	if err != nil {
		return err
	}
	s.db = sql.OpenDB(conn)

	if s.metadata.CleanupInterval != nil {
		err = s.startGC()
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *SQLServer) startGC() error {
	gc, err := commonsql.ScheduleGarbageCollector(commonsql.GCOptions{
		Logger: s.logger,
		UpdateLastCleanupQuery: func(arg any) (string, any) {
			return fmt.Sprintf(`BEGIN TRANSACTION;
BEGIN TRY
INSERT INTO [%[1]s].[%[2]s] ([Key], [Value]) VALUES ('last-cleanup', CONVERT(nvarchar(MAX), GETDATE(), 21));
END TRY
BEGIN CATCH
UPDATE [%[1]s].[%[2]s] SET [Value] = CONVERT(nvarchar(MAX), GETDATE(), 21) WHERE [Key] = 'last-cleanup' AND Datediff_big(MS, [Value], GETUTCDATE()) > @Interval
END CATCH
COMMIT TRANSACTION;`, s.metadata.SchemaName, s.metadata.MetadataTableName), sql.Named("Interval", arg)
		},
		DeleteExpiredValuesQuery: fmt.Sprintf(
			`DELETE FROM [%s].[%s] WHERE [ExpireDate] IS NOT NULL AND [ExpireDate] < GETDATE()`,
			s.metadata.SchemaName, s.metadata.TableName,
		),
		CleanupInterval: *s.metadata.CleanupInterval,
		DB:              commonsql.AdaptDatabaseSQLConn(s.db),
	})
	if err != nil {
		return err
	}
	s.gc = gc

	return nil
}

// Features returns the features available in this state store.
func (s *SQLServer) Features() []state.Feature {
	return s.features
}

// Multi performs batched updates on a SQL Server store.
func (s *SQLServer) Multi(ctx context.Context, request *state.TransactionalStateRequest) error {
	if request == nil {
		return nil
	}

	// If there's only 1 operation, skip starting a transaction
	switch len(request.Operations) {
	case 0:
		return nil
	case 1:
		return s.execMultiOperation(ctx, request.Operations[0], s.db)
	default:
		_, err := sqltransactions.ExecuteInTransaction(ctx, s.logger, s.db, func(ctx context.Context, tx *sql.Tx) (r struct{}, err error) {
			for _, op := range request.Operations {
				err = s.execMultiOperation(ctx, op, tx)
				if err != nil {
					return r, err
				}
			}
			return r, nil
		})
		return err
	}
}

func (s *SQLServer) execMultiOperation(ctx context.Context, op state.TransactionalStateOperation, db dbExecutor) error {
	switch req := op.(type) {
	case state.SetRequest:
		return s.executeSet(ctx, db, &req)
	case state.DeleteRequest:
		return s.executeDelete(ctx, db, &req)
	default:
		return fmt.Errorf("unsupported operation: %s", op.Operation())
	}
}

// Delete removes an entity from the store.
func (s *SQLServer) Delete(ctx context.Context, req *state.DeleteRequest) error {
	return s.executeDelete(ctx, s.db, req)
}

func (s *SQLServer) executeDelete(ctx context.Context, db dbExecutor, req *state.DeleteRequest) error {
	var err error
	var res sql.Result
	if req.HasETag() {
		var b []byte
		b, err = hex.DecodeString(*req.ETag)
		if err != nil {
			return state.NewETagError(state.ETagInvalid, err)
		}

		res, err = db.ExecContext(ctx, s.deleteWithETagCommand, sql.Named(keyColumnName, req.Key), sql.Named(rowVersionColumnName, b))
	} else {
		res, err = db.ExecContext(ctx, s.deleteWithoutETagCommand, sql.Named(keyColumnName, req.Key))
	}

	// err represents errors thrown by the stored procedure or the database itself
	if err != nil {
		return err
	}

	// if the row with matching key (and ETag if specified) is not found, then the stored procedure returns 0 rows affected
	rows, err := res.RowsAffected()
	if err != nil {
		return err
	}

	// When an ETAG is specified, a row must have been deleted or else we return an ETag mismatch error
	if rows != 1 && req.ETag != nil && *req.ETag != "" {
		return state.NewETagError(state.ETagMismatch, nil)
	}

	// successful deletion, or noop if no ETAG specified
	return nil
}

// Get returns an entity from store.
func (s *SQLServer) Get(ctx context.Context, req *state.GetRequest) (*state.GetResponse, error) {
	rows, err := s.db.QueryContext(ctx, s.getCommand, sql.Named(keyColumnName, req.Key))
	if err != nil {
		return nil, err
	}

	if rows.Err() != nil {
		return nil, rows.Err()
	}

	defer rows.Close()

	if !rows.Next() {
		return &state.GetResponse{}, nil
	}

	var (
		data       sql.NullString
		binaryData []byte
		isBinary   bool
		rowVersion []byte
		expireDate sql.NullTime
	)
	err = rows.Scan(&data, &binaryData, &isBinary, &rowVersion, &expireDate)
	if err != nil {
		return nil, err
	}

	etag := hex.EncodeToString(rowVersion)

	var metadata map[string]string
	if expireDate.Valid {
		metadata = map[string]string{
			state.GetRespMetaKeyTTLExpireTime: expireDate.Time.UTC().Format(time.RFC3339),
		}
	}

	var bytes []byte
	if isBinary {
		bytes = binaryData
	} else {
		if !data.Valid {
			return nil, errors.New("unexpected error: no item was found")
		}
		bytes = []byte(data.String)
	}

	return &state.GetResponse{
		Data:     bytes,
		ETag:     ptr.Of(etag),
		Metadata: metadata,
	}, nil
}

// BulkGet retrieves multiple entities in a single round-trip per chunk using a
// `WHERE [Key] IN (...)` query. When the number of requested keys exceeds the
// configured BulkGetChunkSize, BulkGet issues multiple chunked queries
// sequentially (not in parallel) and merges the results. This avoids SQL
// Server's hard 2100-parameter limit and reduces connection-pool pressure
// versus the default per-key fan-out.
//
// The default chunk size is 1000, so callers with <= 1000 keys see a single
// query.
func (s *SQLServer) BulkGet(ctx context.Context, req []state.GetRequest, _ state.BulkGetOpts) ([]state.BulkGetResponse, error) {
	if len(req) == 0 {
		return []state.BulkGetResponse{}, nil
	}

	// Validate all keys upfront — an empty key is a programmer bug, not a
	// per-key data issue, so we fail fast before issuing any query.
	for _, r := range req {
		if r.Key == "" {
			return nil, errors.New("missing key in bulk get operation")
		}
	}

	chunkSize := s.metadata.BulkGetChunkSize
	if chunkSize <= 0 {
		chunkSize = defaultBulkGetChunkSize
	}

	// Eager ctx.Err() check applies to both fast and chunked paths so
	// cancellation produces a consistent per-key error format regardless
	// of how many request entries the caller passed. Without this, the
	// fast path would let database/sql wrap the context error as "bulk
	// get query failed: ..." while the chunked path would surface
	// ctx.Err() directly, which would silently change format when a
	// caller tunes bulkGetChunkSize.
	if err := ctx.Err(); err != nil {
		res := make([]state.BulkGetResponse, len(req))
		for i, r := range req {
			res[i] = state.BulkGetResponse{
				Key:   r.Key,
				Error: err.Error(),
			}
		}
		return res, nil
	}

	// Fast path: input fits in one chunk.
	if len(req) <= chunkSize {
		return s.bulkGetChunk(ctx, req), nil
	}

	// Chunked path: sequential chunks, results concatenated.
	res := make([]state.BulkGetResponse, 0, len(req))
	for start := 0; start < len(req); start += chunkSize {
		// Re-check context between chunks so a cancellation that fires
		// mid-request stops promptly instead of issuing further round-trips.
		if err := ctx.Err(); err != nil {
			for _, r := range req[start:] {
				res = append(res, state.BulkGetResponse{
					Key:   r.Key,
					Error: err.Error(),
				})
			}
			return res, nil
		}

		end := start + chunkSize
		if end > len(req) {
			end = len(req)
		}

		res = append(res, s.bulkGetChunk(ctx, req[start:end])...)
	}
	return res, nil
}

// bulkGetChunk executes a single IN-clause query for the distinct keys of
// the given requests, then builds the response by iterating the original
// request slice so duplicate request entries each get their own populated
// response (matching the default fan-out BulkStore semantics). Only
// distinct keys hit the wire.
//
// Pre-condition: req is non-empty and all keys are non-empty.
//
// Errors surface as per-key Error entries in the response (consistent with
// other state stores like Redis and Oracle); this helper never returns a
// hard error. Exception: rows.Scan failures lose the row's key and so
// cannot be attributed to a specific request entry. The affected key
// surfaces as a missing-key response (empty Data/ETag/Error) and the
// failure is logged at warn level.
func (s *SQLServer) bulkGetChunk(ctx context.Context, req []state.GetRequest) []state.BulkGetResponse {
	type rowData struct {
		data       []byte
		etag       string
		expireTime time.Time
		hasExpire  bool
		found      bool
		// scanErr is set if reading this row failed in a recoverable way
		// (e.g. NULL data on a non-binary row); surfaced as a per-key
		// error in the response loop.
		scanErr string
	}

	// Build the distinct key list, preserving first-seen order. The map is
	// pre-populated with zero-value entries so we can detect duplicates via
	// the `seen` check without a separate set.
	rowByKey := make(map[string]rowData, len(req))
	distinctKeys := make([]string, 0, len(req))
	for _, r := range req {
		if _, seen := rowByKey[r.Key]; seen {
			continue
		}
		rowByKey[r.Key] = rowData{}
		distinctKeys = append(distinctKeys, r.Key)
	}

	params := make([]any, len(distinctKeys))
	bindVars := make([]string, len(distinctKeys))
	for i, k := range distinctKeys {
		name := "p" + strconv.Itoa(i)
		params[i] = sql.Named(name, k)
		bindVars[i] = "@" + name
	}

	// Schema and table names are validated at Init via IsValidSQLName, so
	// concatenation here is safe.
	//nolint:gosec
	query := fmt.Sprintf(
		"SELECT [Key], [Data], [BinaryData], [isBinary], [RowVersion], [ExpireDate] FROM [%s].[%s] WHERE [Key] IN (%s) AND ([ExpireDate] IS NULL OR [ExpireDate] > GETDATE())",
		s.metadata.SchemaName, s.metadata.TableName, strings.Join(bindVars, ","),
	)

	rows, err := s.db.QueryContext(ctx, query, params...)
	if err != nil {
		// If the query fails, return per-key error entries instead of
		// propagating the error, matching Oracle/Redis convention.
		res := make([]state.BulkGetResponse, len(req))
		errMsg := "bulk get query failed: " + err.Error()
		for i, r := range req {
			res[i] = state.BulkGetResponse{
				Key:   r.Key,
				Error: errMsg,
			}
		}
		return res
	}
	defer rows.Close()

	for rows.Next() {
		var (
			key        string
			data       sql.NullString
			binaryData []byte
			isBinary   bool
			rowVersion []byte
			expireDate sql.NullTime
		)
		if err := rows.Scan(&key, &data, &binaryData, &isBinary, &rowVersion, &expireDate); err != nil {
			// We can't trust `key` after a failed scan, so we can't slot
			// the error here. Log it; the response loop below will treat
			// the corresponding request keys as not-found.
			s.logger.Warnf("SQL Server BulkGet: row scan failed: %v", err)
			continue
		}

		// Defensive: the SQL `IN` clause can only match values we bound,
		// so any returned key must be one we requested (and therefore
		// pre-populated in rowByKey). An unrecognized key would indicate
		// a misbehaving driver or a future schema change leaking through;
		// log and drop rather than insert an orphan entry into the map.
		if _, requested := rowByKey[key]; !requested {
			s.logger.Warnf("SQL Server BulkGet: query returned unrequested key %q, discarding", key)
			continue
		}

		rd := rowData{found: true, etag: hex.EncodeToString(rowVersion)}
		if expireDate.Valid {
			rd.expireTime = expireDate.Time
			rd.hasExpire = true
		}
		if isBinary {
			rd.data = binaryData
		} else if !data.Valid {
			// Row was found and scanned successfully, but a non-binary
			// row has a NULL Data column — shouldn't happen in production
			// (the schema/upsert path always writes Data for isBinary=0).
			// Surface as a per-key error so operators can spot corrupted
			// rows in logs without losing the response slot.
			rd.scanErr = "row has NULL Data column on a non-binary entry (possible data corruption)"
		} else {
			rd.data = []byte(data.String)
		}
		rowByKey[key] = rd
	}

	// rows.Err() reports errors from iteration; we surface it per-key for
	// any request keys that didn't get a row.
	iterErr := rows.Err()

	// Build the response by iterating the original request slice so that
	// duplicate request entries each get their own populated response with
	// the same data, and response order matches request order.
	res := make([]state.BulkGetResponse, len(req))
	anyUnfound := false
	for i, r := range req {
		res[i].Key = r.Key
		rd := rowByKey[r.Key]
		if !rd.found {
			anyUnfound = true
			if iterErr != nil {
				res[i].Error = "rows iteration failed: " + iterErr.Error()
			}
			// else: missing key, empty response — matches per-key Get
			// semantics (`&state.GetResponse{}`).
			continue
		}
		if rd.scanErr != "" {
			res[i].Error = rd.scanErr
			continue
		}
		res[i].Data = rd.data
		res[i].ETag = ptr.Of(rd.etag)
		if rd.hasExpire {
			res[i].Metadata = map[string]string{
				state.GetRespMetaKeyTTLExpireTime: rd.expireTime.UTC().Format(time.RFC3339),
			}
		}
	}

	// rows.Err() that fires after every requested key was already found
	// has no per-key slot to land in (every response carries data).
	// Without a log, the error would vanish entirely — e.g. a network
	// reset after the last row arrived would be invisible. Warn-log it
	// so transient driver/network failures stay diagnosable.
	if iterErr != nil && !anyUnfound {
		s.logger.Warnf("SQL Server BulkGet: rows iteration error after all requested keys were found: %v", iterErr)
	}
	return res
}

// Set adds/updates an entity on store.
func (s *SQLServer) Set(ctx context.Context, req *state.SetRequest) error {
	return s.executeSet(ctx, s.db, req)
}

// dbExecutor implements a common functionality implemented by db or tx.
type dbExecutor interface {
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
}

func (s *SQLServer) executeSet(ctx context.Context, db dbExecutor, req *state.SetRequest) error {
	bytes, isBinary := req.Value.([]byte)
	namedData := sql.Named("Data", nil)
	namedBinaryData := sql.Named("BinaryData", nil)
	if !isBinary {
		bt, err := json.Marshal(req.Value)
		if err != nil {
			return err
		}
		namedData = sql.Named("Data", string(bt))
	} else {
		namedBinaryData = sql.Named("BinaryData", bytes)
	}

	etag := sql.Named(rowVersionColumnName, nil)
	if req.HasETag() {
		b, err := hex.DecodeString(*req.ETag)
		if err != nil {
			return state.NewETagError(state.ETagInvalid, err)
		}
		etag = sql.Named(rowVersionColumnName, b)
	}

	ttl, ttlerr := utils.ParseTTL(req.Metadata)
	if ttlerr != nil {
		return fmt.Errorf("error parsing TTL: %w", ttlerr)
	}

	var res sql.Result
	var err error
	if req.Options.Concurrency == state.FirstWrite {
		res, err = db.ExecContext(ctx, s.upsertCommand,
			sql.Named(keyColumnName, req.Key),
			namedData,
			etag,
			namedBinaryData,
			sql.Named("isBinary", isBinary),
			sql.Named("FirstWrite", 1),
			sql.Named("TTL", ttl))
	} else {
		res, err = db.ExecContext(ctx, s.upsertCommand,
			sql.Named(keyColumnName, req.Key),
			namedData,
			etag,
			namedBinaryData,
			sql.Named("isBinary", isBinary),
			sql.Named("FirstWrite", 0),
			sql.Named("TTL", ttl))
	}

	if err != nil {
		return err
	}

	rows, err := res.RowsAffected()
	if err != nil {
		return err
	}

	if rows != 1 {
		if req.HasETag() {
			return state.NewETagError(state.ETagMismatch, err)
		}
		return errors.New("no item was updated")
	}

	return nil
}

func (s *SQLServer) GetComponentMetadata() (metadataInfo metadata.MetadataMap) {
	settingsStruct := sqlServerMetadata{}
	metadata.GetMetadataInfoFromStructType(reflect.TypeOf(settingsStruct), &metadataInfo, metadata.StateStoreType)
	return
}

// Close implements io.Closer.
func (s *SQLServer) Close() error {
	if s.db != nil {
		s.db.Close()
		s.db = nil
	}

	if s.gc != nil {
		return s.gc.Close()
	}

	return nil
}

// GetCleanupInterval returns the cleanupInterval property.
// This is primarily used for tests.
func (s *SQLServer) GetCleanupInterval() *time.Duration {
	return s.metadata.CleanupInterval
}

func (s *SQLServer) CleanupExpired() error {
	if s.gc != nil {
		return s.gc.CleanupExpired()
	}
	return nil
}
