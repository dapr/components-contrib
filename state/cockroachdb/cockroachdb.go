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
	"reflect"

	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/state"
	"github.com/dapr/kit/logger"
)

// CockroachDB state store.
type CockroachDB struct {
	features []state.Feature
	logger   logger.Logger
	dbaccess dbAccess
}

// New creates a new instance of CockroachDB state store.
func New(logger logger.Logger) state.Store {
	dba := newCockroachDBAccess(logger)

	return internalNew(logger, dba)
}

// internalNew creates a new instance of a CockroachDB state store.
// This unexported constructor allows injecting a dbAccess instance for unit testing.
func internalNew(logger logger.Logger, dba dbAccess) *CockroachDB {
	return &CockroachDB{
		features: []state.Feature{state.FeatureETag, state.FeatureTransactional, state.FeatureQueryAPI},
		logger:   logger,
		dbaccess: dba,
	}
}

// Init initializes the CockroachDB state store.
func (c *CockroachDB) Init(metadata state.Metadata) error {
	return c.dbaccess.Init(metadata)
}

// Features returns the features available in this state store.
func (c *CockroachDB) Features() []state.Feature {
	return c.features
}

// Delete removes an entity from the store.
func (c *CockroachDB) Delete(req *state.DeleteRequest) error {
	return c.dbaccess.Delete(req)
}

// Get returns an entity from store.
func (c *CockroachDB) Get(req *state.GetRequest) (*state.GetResponse, error) {
	return c.dbaccess.Get(req)
}

// Set adds/updates an entity on store.
func (c *CockroachDB) Set(req *state.SetRequest) error {
	return c.dbaccess.Set(req)
}

// Ping checks if database is available.
func (c *CockroachDB) Ping() error {
	return c.dbaccess.Ping()
}

// BulkDelete removes multiple entries from the store.
func (c *CockroachDB) BulkDelete(req []state.DeleteRequest) error {
	return c.dbaccess.BulkDelete(req)
}

// BulkGet performs a bulks get operations.
func (c *CockroachDB) BulkGet(req []state.GetRequest) (bool, []state.BulkGetResponse, error) {
	// TODO: replace with ExecuteMulti for performance.
	return false, nil, nil
}

// BulkSet adds/updates multiple entities on store.
func (c *CockroachDB) BulkSet(req []state.SetRequest) error {
	return c.dbaccess.BulkSet(req)
}

// Multi handles multiple transactions. Implements TransactionalStore.
func (c *CockroachDB) Multi(request *state.TransactionalStateRequest) error {
	return c.dbaccess.ExecuteMulti(request)
}

// Query executes a query against store.
func (c *CockroachDB) Query(req *state.QueryRequest) (*state.QueryResponse, error) {
	return c.dbaccess.Query(req)
}

// Close implements io.Closer.
func (c *CockroachDB) Close() error {
	if c.dbaccess != nil {
		return c.dbaccess.Close()
	}

	return nil
}

func (c *CockroachDB) GetComponentMetadata() map[string]string {
	metadataStruct := cockroachDBMetadata{}
	metadataInfo := map[string]string{}
	metadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo)
	return metadataInfo
}
