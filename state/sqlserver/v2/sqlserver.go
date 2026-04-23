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
			state.FeatureDeleteWithPrefix,
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
	metadata.Base.GetProperty()
	err := s.metadata.Parse(metadata.Properties)
	if err != nil {
		return err
	}

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

// sqlServerMaxParams caps IN-clause parameter counts. SQL Server allows up to
// 2100 parameters per query; leave headroom for other bound params the driver
// might add.
const sqlServerMaxParams = 2000

// BulkGet retrieves multiple keys in one SQL Server round trip per chunk by
// building a dynamic IN clause. Chunks are issued serially to respect the
// parameter-count cap; within a chunk a single SELECT returns all matched
// rows. Duplicate keys in the input are supported — every occurrence
// receives the same data, mirroring the default BulkStore fan-out.
func (s *SQLServer) BulkGet(parentCtx context.Context, req []state.GetRequest, _ state.BulkGetOpts) ([]state.BulkGetResponse, error) {
	if len(req) == 0 {
		return []state.BulkGetResponse{}, nil
	}

	// rowByKey caches the row we fetched per distinct key so that multiple
	// request entries for the same key each get populated. We only read
	// distinct keys over the wire.
	type row struct {
		data       []byte
		etag       string
		expireTime time.Time
		hasExpire  bool
		found      bool
	}
	rowByKey := make(map[string]row, len(req))
	distinctKeys := make([]string, 0, len(req))
	for _, r := range req {
		if _, seen := rowByKey[r.Key]; seen {
			continue
		}
		rowByKey[r.Key] = row{}
		distinctKeys = append(distinctKeys, r.Key)
	}

	for start := 0; start < len(distinctKeys); start += sqlServerMaxParams {
		end := start + sqlServerMaxParams
		if end > len(distinctKeys) {
			end = len(distinctKeys)
		}
		placeholders := make([]string, 0, end-start)
		args := make([]any, 0, end-start)
		for i := start; i < end; i++ {
			name := fmt.Sprintf("k%d", i)
			placeholders = append(placeholders, "@"+name)
			args = append(args, sql.Named(name, distinctKeys[i]))
		}
		// Key included in the projection so we can map rows back to the
		// request's positional index without relying on driver ordering.
		// Schema/table names cannot be passed as parameters; they come
		// from the migration layer, not user input.
		//nolint:gosec
		query := fmt.Sprintf(
			"SELECT [Key], [Data], [BinaryData], [isBinary], [RowVersion], [ExpireDate] FROM [%s].[%s] WHERE [Key] IN (%s) AND ([ExpireDate] IS NULL OR [ExpireDate] > GETDATE())",
			s.metadata.SchemaName, s.metadata.TableName, strings.Join(placeholders, ","),
		)
		rows, err := s.db.QueryContext(parentCtx, query, args...)
		if err != nil {
			return nil, fmt.Errorf("sqlserver bulk get: %w", err)
		}
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
				rows.Close()
				return nil, err
			}
			var value []byte
			if isBinary {
				value = binaryData
			} else if data.Valid {
				value = []byte(data.String)
			}
			rowByKey[key] = row{
				data:       value,
				etag:       hex.EncodeToString(rowVersion),
				expireTime: expireDate.Time,
				hasExpire:  expireDate.Valid,
				found:      true,
			}
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return nil, err
		}
		rows.Close()
	}

	// Build the response in the original request order so duplicate keys
	// each get their own populated entry.
	res := make([]state.BulkGetResponse, len(req))
	for i, r := range req {
		res[i].Key = r.Key
		rec := rowByKey[r.Key]
		if !rec.found {
			continue
		}
		res[i].Data = rec.data
		res[i].ETag = ptr.Of(rec.etag)
		if rec.hasExpire {
			res[i].Metadata = map[string]string{
				state.GetRespMetaKeyTTLExpireTime: rec.expireTime.UTC().Format(time.RFC3339),
			}
		}
	}
	return res, nil
}

// DeleteWithPrefix removes direct-children keys of the given prefix in one
// server-side query. Matches in-memory "no-nested-||" semantics.
// escapeSQLServerLikePattern escapes the LIKE metacharacters SQL Server
// recognises (%, _, [, ]) plus the chosen escape character itself, so a
// prefix can be used literally in a LIKE ... ESCAPE '\' expression. The
// escape-char replacement must come first so it does not re-process the
// backslashes we introduce for the other characters.
func escapeSQLServerLikePattern(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, `%`, `\%`)
	s = strings.ReplaceAll(s, `_`, `\_`)
	s = strings.ReplaceAll(s, `[`, `\[`)
	s = strings.ReplaceAll(s, `]`, `\]`)
	return s
}

// DeleteWithPrefix removes direct-children keys of the given prefix in one
// server-side query. Matches in-memory "no-nested-||" semantics.
//
// The prefix is treated literally: LIKE metacharacters (%, _, [, ]) in the
// caller-supplied prefix are escaped so a prefix containing those
// characters cannot widen the delete set.
func (s *SQLServer) DeleteWithPrefix(parentCtx context.Context, req state.DeleteWithPrefixRequest) (state.DeleteWithPrefixResponse, error) {
	if err := req.Validate(); err != nil {
		return state.DeleteWithPrefixResponse{}, err
	}
	escaped := escapeSQLServerLikePattern(req.Prefix)
	prefixLike := escaped + "%"
	nestedLike := escaped + "%||%"
	// Schema/table names cannot be passed as parameters; they come from
	// the migration layer, not user input.
	//nolint:gosec
	query := fmt.Sprintf(
		`DELETE FROM [%s].[%s] WHERE [Key] LIKE @PrefixLike ESCAPE '\' AND [Key] NOT LIKE @NestedPrefixLike ESCAPE '\'`,
		s.metadata.SchemaName, s.metadata.TableName,
	)
	res, err := s.db.ExecContext(
		parentCtx,
		query,
		sql.Named("PrefixLike", prefixLike),
		sql.Named("NestedPrefixLike", nestedLike),
	)
	if err != nil {
		return state.DeleteWithPrefixResponse{}, fmt.Errorf("sqlserver delete with prefix: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return state.DeleteWithPrefixResponse{}, err
	}
	return state.DeleteWithPrefixResponse{Count: n}, nil
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
