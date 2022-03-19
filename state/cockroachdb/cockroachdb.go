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
	"fmt"

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
func New(logger logger.Logger) *CockroachDB {
	dba := newCockroachDBAccess(logger)

	return internalNew(logger, dba)
}

// internalNew creates a new instance of a CockroachDB state store.
// This unexported constructor allows injecting a dbAccess instance for unit testing.
func internalNew(logger logger.Logger, dba dbAccess) *CockroachDB {
	return &CockroachDB{
		features: []state.Feature{state.FeatureETag, state.FeatureTransactional},
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
	return c.dbaccess.ExecuteMulti(nil, req)
}

// BulkGet performs a bulks get operations.
func (c *CockroachDB) BulkGet(req []state.GetRequest) (bool, []state.BulkGetResponse, error) {
	// TODO: replace with ExecuteMulti for performance.
	return false, nil, nil
}

// BulkSet adds/updates multiple entities on store.
func (c *CockroachDB) BulkSet(req []state.SetRequest) error {
	return c.dbaccess.ExecuteMulti(req, nil)
}

// Multi handles multiple transactions. Implements TransactionalStore.
func (c *CockroachDB) Multi(request *state.TransactionalStateRequest) error {
	var deletes []state.DeleteRequest
	var sets []state.SetRequest
	for _, req := range request.Operations {
		switch req.Operation {
		case state.Upsert:
			if setReq, ok := req.Request.(state.SetRequest); ok {
				sets = append(sets, setReq)
			} else {
				return fmt.Errorf("expecting set request")
			}

		case state.Delete:
			if delReq, ok := req.Request.(state.DeleteRequest); ok {
				deletes = append(deletes, delReq)
			} else {
				return fmt.Errorf("expecting delete request")
			}

		default:
			return fmt.Errorf("unsupported operation: %s", req.Operation)
		}
	}

	if len(sets) > 0 || len(deletes) > 0 {
		return c.dbaccess.ExecuteMulti(sets, deletes)
	}

	return nil
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
