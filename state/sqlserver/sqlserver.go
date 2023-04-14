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
	"time"

	mssql "github.com/denisenkom/go-mssqldb"

	internalsql "github.com/dapr/components-contrib/internal/component/sql"
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
		features:        []state.Feature{state.FeatureETag, state.FeatureTransactional},
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

// SQLServer defines a Ms SQL Server based state store.
type SQLServer struct {
	state.BulkStore

	metadata sqlServerMetadata

	migratorFactory func(*sqlServerMetadata) migrator

	bulkDeleteCommand        string
	itemRefTableTypeName     string
	upsertCommand            string
	getCommand               string
	deleteWithETagCommand    string
	deleteWithoutETagCommand string

	features []state.Feature
	logger   logger.Logger
	db       *sql.DB
	gc       internalsql.GarbageCollector
}

// Init initializes the SQL server state store.
func (s *SQLServer) Init(ctx context.Context, metadata state.Metadata) error {
	s.metadata = newMetadata()
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
	s.bulkDeleteCommand = fmt.Sprintf("exec %s @itemsToDelete;", mr.bulkDeleteProcFullName)
	s.upsertCommand = mr.upsertProcFullName
	s.getCommand = mr.getCommand
	s.deleteWithETagCommand = mr.deleteWithETagCommand
	s.deleteWithoutETagCommand = mr.deleteWithoutETagCommand

	s.db, err = sql.Open("sqlserver", s.metadata.ConnectionString)
	if err != nil {
		return err
	}

	if s.metadata.CleanupInterval != nil {
		err = s.startGC()
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *SQLServer) startGC() error {
	gc, err := internalsql.ScheduleGarbageCollector(internalsql.GCOptions{
		Logger: s.logger,
		UpdateLastCleanupQuery: fmt.Sprintf(`BEGIN TRANSACTION;
BEGIN TRY
INSERT INTO [%[1]s].[%[2]s] ([Key], [Value]) VALUES ('last-cleanup', CONVERT(nvarchar(MAX), GETDATE(), 21));
END TRY
BEGIN CATCH
UPDATE [%[1]s].[%[2]s] SET [Value] = CONVERT(nvarchar(MAX), GETDATE(), 21) WHERE [Key] = 'last-cleanup' AND Datediff_big(MS, [Value], GETUTCDATE()) > @Interval
END CATCH
COMMIT TRANSACTION;`, s.metadata.Schema, s.metadata.MetadataTableName),
		UpdateLastCleanupQueryParameterName: "Interval",
		DeleteExpiredValuesQuery: fmt.Sprintf(
			`DELETE FROM [%s].[%s] WHERE [ExpireDate] IS NOT NULL AND [ExpireDate] < GETDATE()`,
			s.metadata.Schema, s.metadata.TableName,
		),
		CleanupInterval: *s.metadata.CleanupInterval,
		DBSql:           s.db,
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

// Multi performs multiple updates on a Sql server store.
func (s *SQLServer) Multi(ctx context.Context, request *state.TransactionalStateRequest) error {
	tx, err := s.db.BeginTx(ctx, nil)
	defer tx.Rollback()
	if err != nil {
		return err
	}

	for _, o := range request.Operations {
		switch req := o.(type) {
		case state.SetRequest:
			err = s.executeSet(ctx, tx, &req)
			if err != nil {
				return err
			}

		case state.DeleteRequest:
			err = s.executeDelete(ctx, tx, &req)
			if err != nil {
				return err
			}

		default:
			return fmt.Errorf("unsupported operation: %s", o.Operation())
		}
	}

	return tx.Commit()
}

// Delete removes an entity from the store.
func (s *SQLServer) Delete(ctx context.Context, req *state.DeleteRequest) error {
	return s.executeDelete(ctx, s.db, req)
}

func (s *SQLServer) executeDelete(ctx context.Context, db dbExecutor, req *state.DeleteRequest) error {
	var err error
	var res sql.Result
	if req.ETag != nil {
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

// TvpDeleteTableStringKey defines a table type with string key.
type TvpDeleteTableStringKey struct {
	ID         string
	RowVersion []byte
}

// BulkDelete removes multiple entries from the store.
func (s *SQLServer) BulkDelete(ctx context.Context, req []state.DeleteRequest) error {
	tx, err := s.db.BeginTx(ctx, nil)
	defer tx.Rollback()
	if err != nil {
		return err
	}

	err = s.executeBulkDelete(ctx, tx, req)
	if err != nil {
		return err
	}

	return tx.Commit()
}

func (s *SQLServer) executeBulkDelete(ctx context.Context, db dbExecutor, req []state.DeleteRequest) error {
	values := make([]TvpDeleteTableStringKey, len(req))
	for i, d := range req {
		var etag []byte
		var err error
		if d.ETag != nil {
			etag, err = hex.DecodeString(*d.ETag)
			if err != nil {
				return state.NewETagError(state.ETagInvalid, err)
			}
		}
		values[i] = TvpDeleteTableStringKey{ID: d.Key, RowVersion: etag}
	}

	itemsToDelete := mssql.TVP{
		TypeName: s.itemRefTableTypeName,
		Value:    values,
	}

	res, err := db.ExecContext(ctx, s.bulkDeleteCommand, sql.Named("itemsToDelete", itemsToDelete))
	if err != nil {
		return err
	}

	rows, err := res.RowsAffected()
	if err != nil {
		return err
	}

	if int(rows) != len(req) {
		return state.NewBulkDeleteRowMismatchError(uint64(rows), uint64(len(req)))
	}

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

	var data string
	var rowVersion []byte
	err = rows.Scan(&data, &rowVersion)
	if err != nil {
		return nil, err
	}

	etag := hex.EncodeToString(rowVersion)

	return &state.GetResponse{
		Data: []byte(data),
		ETag: ptr.Of(etag),
	}, nil
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
	var err error
	var bytes []byte
	bytes, err = utils.Marshal(req.Value, json.Marshal)
	if err != nil {
		return err
	}
	etag := sql.Named(rowVersionColumnName, nil)
	if req.ETag != nil && *req.ETag != "" {
		var b []byte
		b, err = hex.DecodeString(*req.ETag)
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
	if req.Options.Concurrency == state.FirstWrite {
		res, err = db.ExecContext(ctx, s.upsertCommand, sql.Named(keyColumnName, req.Key),
			sql.Named("Data", string(bytes)), etag,
			sql.Named("FirstWrite", 1), sql.Named("TTL", ttl))
	} else {
		res, err = db.ExecContext(ctx, s.upsertCommand, sql.Named(keyColumnName, req.Key),
			sql.Named("Data", string(bytes)), etag,
			sql.Named("FirstWrite", 0), sql.Named("TTL", ttl))
	}

	if err != nil {
		if req.ETag != nil && *req.ETag != "" {
			return state.NewETagError(state.ETagMismatch, err)
		}

		return err
	}

	rows, err := res.RowsAffected()
	if err != nil {
		return err
	}

	if rows != 1 {
		return errors.New("no item was updated")
	}

	return nil
}

// BulkSet adds/updates multiple entities on store.
func (s *SQLServer) BulkSet(ctx context.Context, req []state.SetRequest) error {
	tx, err := s.db.BeginTx(ctx, nil)
	defer tx.Rollback()
	if err != nil {
		return err
	}

	for i := range req {
		err = s.executeSet(ctx, tx, &req[i])
		if err != nil {
			return err
		}
	}

	return tx.Commit()
}

func (s *SQLServer) GetComponentMetadata() map[string]string {
	return map[string]string{}
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
