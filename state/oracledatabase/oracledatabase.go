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

package oracledatabase

import (
	"context"
	"database/sql"
	"reflect"

	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/state"
	"github.com/dapr/kit/logger"
)

// Oracle Database state store.
type OracleDatabase struct {
	state.BulkStore

	features []state.Feature
	logger   logger.Logger
	dbaccess dbAccess
}

// NewOracleDatabaseStateStore creates a new instance of OracleDatabase state store.
func NewOracleDatabaseStateStore(logger logger.Logger) state.Store {
	dba := newOracleDatabaseAccess(logger)
	s := newOracleDatabaseStateStore(logger, dba)
	s.BulkStore = state.NewDefaultBulkStore(s)
	return s
}

// newOracleDatabaseStateStore creates a newOracleDatabaseStateStore instance of an OracleDatabase state store.
// This unexported constructor allows injecting a dbAccess instance for unit testing.
func newOracleDatabaseStateStore(logger logger.Logger, dba dbAccess) *OracleDatabase {
	return &OracleDatabase{
		features: []state.Feature{
			state.FeatureETag,
			state.FeatureTransactional,
			state.FeatureTTL,
		},
		logger:   logger,
		dbaccess: dba,
	}
}

// Init initializes the SQL server state store.
func (o *OracleDatabase) Init(ctx context.Context, metadata state.Metadata) error {
	return o.dbaccess.Init(ctx, metadata)
}

func (o *OracleDatabase) Ping(ctx context.Context) error {
	return o.dbaccess.Ping(ctx)
}

// Features returns the features available in this state store.
func (o *OracleDatabase) Features() []state.Feature {
	return o.features
}

// Delete removes an entity from the store.
func (o *OracleDatabase) Delete(ctx context.Context, req *state.DeleteRequest) error {
	return o.dbaccess.Delete(ctx, req)
}

// Get returns an entity from store.
func (o *OracleDatabase) Get(ctx context.Context, req *state.GetRequest) (*state.GetResponse, error) {
	return o.dbaccess.Get(ctx, req)
}

// Set adds/updates an entity on store.
func (o *OracleDatabase) Set(ctx context.Context, req *state.SetRequest) error {
	return o.dbaccess.Set(ctx, req)
}

// Multi handles multiple transactions. Implements TransactionalStore.
func (o *OracleDatabase) Multi(ctx context.Context, request *state.TransactionalStateRequest) error {
	return o.dbaccess.ExecuteMulti(ctx, request.Operations)
}

// Close implements io.Closer.
func (o *OracleDatabase) Close() error {
	if o.dbaccess != nil {
		return o.dbaccess.Close()
	}

	return nil
}

// Returns the database connection. Used by tests.
func (o *OracleDatabase) getDB() *sql.DB {
	return o.dbaccess.(*oracleDatabaseAccess).db
}

func (o *OracleDatabase) GetComponentMetadata() (metadataInfo metadata.MetadataMap) {
	metadataStruct := oracleDatabaseMetadata{}
	metadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, metadata.StateStoreType)
	return
}
