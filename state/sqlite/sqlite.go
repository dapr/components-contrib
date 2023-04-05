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

package sqlite

import (
	"context"
	"reflect"

	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/state"
	"github.com/dapr/kit/logger"
)

// SQLite Database state store.
type SQLiteStore struct {
	logger   logger.Logger
	dbaccess DBAccess
}

// NewSQLiteStateStore creates a new instance of the SQLite state store.
func NewSQLiteStateStore(logger logger.Logger) state.Store {
	dba := newSqliteDBAccess(logger)

	return newSQLiteStateStore(logger, dba)
}

// newSQLiteStateStore creates a newSQLiteStateStore instance of an Sqlite state store.
// This unexported constructor allows injecting a dbAccess instance for unit testing.
func newSQLiteStateStore(logger logger.Logger, dba DBAccess) *SQLiteStore {
	return &SQLiteStore{
		logger:   logger,
		dbaccess: dba,
	}
}

// Init initializes the Sql server state store.
func (s *SQLiteStore) Init(ctx context.Context, metadata state.Metadata) error {
	return s.dbaccess.Init(ctx, metadata)
}

func (s SQLiteStore) GetComponentMetadata() map[string]string {
	metadataStruct := sqliteMetadataStruct{}
	metadataInfo := map[string]string{}
	metadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, metadata.StateStoreType)
	return metadataInfo
}

// Features returns the features available in this state store.
func (s *SQLiteStore) Features() []state.Feature {
	return []state.Feature{
		state.FeatureETag,
		state.FeatureTransactional,
	}
}

func (s *SQLiteStore) Ping(ctx context.Context) error {
	return s.dbaccess.Ping(ctx)
}

// Delete removes an entity from the store.
func (s *SQLiteStore) Delete(ctx context.Context, req *state.DeleteRequest) error {
	return s.dbaccess.Delete(ctx, req)
}

// BulkDelete removes multiple entries from the store.
func (s *SQLiteStore) BulkDelete(ctx context.Context, req []state.DeleteRequest) error {
	ops := make([]state.TransactionalStateOperation, len(req))
	for i, r := range req {
		ops[i] = state.TransactionalStateOperation{
			Operation: state.Delete,
			Request:   r,
		}
	}
	return s.dbaccess.ExecuteMulti(ctx, ops)
}

// Get returns an entity from store.
func (s *SQLiteStore) Get(ctx context.Context, req *state.GetRequest) (*state.GetResponse, error) {
	return s.dbaccess.Get(ctx, req)
}

// BulkGet performs a bulks get operations.
func (s *SQLiteStore) BulkGet(ctx context.Context, req []state.GetRequest) (bool, []state.BulkGetResponse, error) {
	return s.dbaccess.BulkGet(ctx, req)
}

// Set adds/updates an entity on store.
func (s *SQLiteStore) Set(ctx context.Context, req *state.SetRequest) error {
	return s.dbaccess.Set(ctx, req)
}

// BulkSet adds/updates multiple entities on store.
func (s *SQLiteStore) BulkSet(ctx context.Context, req []state.SetRequest) error {
	ops := make([]state.TransactionalStateOperation, len(req))
	for i, r := range req {
		ops[i] = state.TransactionalStateOperation{
			Operation: state.Upsert,
			Request:   r,
		}
	}
	return s.dbaccess.ExecuteMulti(ctx, ops)
}

// Multi handles multiple transactions. Implements TransactionalStore.
func (s *SQLiteStore) Multi(ctx context.Context, request *state.TransactionalStateRequest) error {
	return s.dbaccess.ExecuteMulti(ctx, request.Operations)
}

// Close implements io.Closer.
func (s *SQLiteStore) Close() error {
	if s.dbaccess != nil {
		return s.dbaccess.Close()
	}

	return nil
}

// Returns the dbaccess property.
// This method is used in tests.
func (s *SQLiteStore) GetDBAccess() *sqliteDBAccess {
	return s.dbaccess.(*sqliteDBAccess)
}
