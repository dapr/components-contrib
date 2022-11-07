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
	"reflect"

	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/state"
	"github.com/dapr/kit/logger"
)

// PostgreSQL state store.
type PostgreSQL struct {
	features []state.Feature
	logger   logger.Logger
	dbaccess dbAccess
}

// NewPostgreSQLStateStore creates a new instance of PostgreSQL state store.
func NewPostgreSQLStateStore(logger logger.Logger) state.Store {
	dba := newPostgresDBAccess(logger)

	return newPostgreSQLStateStore(logger, dba)
}

// newPostgreSQLStateStore creates a newPostgreSQLStateStore instance of a PostgreSQL state store.
// This unexported constructor allows injecting a dbAccess instance for unit testing.
func newPostgreSQLStateStore(logger logger.Logger, dba dbAccess) *PostgreSQL {
	return &PostgreSQL{
		features: []state.Feature{state.FeatureETag, state.FeatureTransactional, state.FeatureQueryAPI},
		logger:   logger,
		dbaccess: dba,
	}
}

// Init initializes the SQL server state store.
func (p *PostgreSQL) Init(metadata state.Metadata) error {
	return p.dbaccess.Init(metadata)
}

// Features returns the features available in this state store.
func (p *PostgreSQL) Features() []state.Feature {
	return p.features
}

// Delete removes an entity from the store.
func (p *PostgreSQL) Delete(req *state.DeleteRequest) error {
	return p.dbaccess.Delete(req)
}

// BulkDelete removes multiple entries from the store.
func (p *PostgreSQL) BulkDelete(req []state.DeleteRequest) error {
	return p.dbaccess.BulkDelete(req)
}

// Get returns an entity from store.
func (p *PostgreSQL) Get(req *state.GetRequest) (*state.GetResponse, error) {
	return p.dbaccess.Get(req)
}

// BulkGet performs a bulks get operations.
func (p *PostgreSQL) BulkGet(req []state.GetRequest) (bool, []state.BulkGetResponse, error) {
	// TODO: replace with ExecuteMulti for performance
	return false, nil, nil
}

// Set adds/updates an entity on store.
func (p *PostgreSQL) Set(req *state.SetRequest) error {
	return p.dbaccess.Set(req)
}

// BulkSet adds/updates multiple entities on store.
func (p *PostgreSQL) BulkSet(req []state.SetRequest) error {
	return p.dbaccess.BulkSet(req)
}

// Multi handles multiple transactions. Implements TransactionalStore.
func (p *PostgreSQL) Multi(request *state.TransactionalStateRequest) error {
	return p.dbaccess.ExecuteMulti(request)
}

// Query executes a query against store.
func (p *PostgreSQL) Query(req *state.QueryRequest) (*state.QueryResponse, error) {
	return p.dbaccess.Query(req)
}

// Close implements io.Closer.
func (p *PostgreSQL) Close() error {
	if p.dbaccess != nil {
		return p.dbaccess.Close()
	}

	return nil
}

func (p *PostgreSQL) GetComponentMetadata() map[string]string {
	metadataStruct := postgresMetadataStruct{}
	metadataInfo := map[string]string{}
	metadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo)
	return metadataInfo
}
