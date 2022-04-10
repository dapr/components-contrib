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

package inmemory

import (
	"fmt"

	"github.com/dgraph-io/ristretto"

	"github.com/dapr/kit/logger"

	"github.com/dapr/components-contrib/state"
)

// StateStore is an in-memory state store.
// The first version is used for perf test.
type StateStore struct {
	state.DefaultBulkStore
	cache *ristretto.Cache

	features []state.Feature
	logger   logger.Logger
}

// NewInMemoryStateStore returns a new in-memory state store.
func NewInMemoryStateStore(logger logger.Logger) *StateStore {
	s := &StateStore{
		// the first version of in-memory state is used for perf test, no advanced feature provided
		features: []state.Feature{},
		logger:   logger,
	}
	s.DefaultBulkStore = state.NewDefaultBulkStore(s)

	return s
}

func (r *StateStore) Ping() error {
	return nil
}

// Init does metadata and connection parsing.
func (r *StateStore) Init(_ state.Metadata) error {
	c, _ := ristretto.NewCache(&ristretto.Config{
		NumCounters: 1e6,     // number of keys to track frequency of (1M).
		MaxCost:     1 << 27, // maximum cost of cache (128MB).
		BufferItems: 64,      // number of keys per Get buffer.
	})

	r.cache = c
	return nil
}

// Features returns the features available in this state store.
func (r *StateStore) Features() []state.Feature {
	return r.features
}

// Delete performs a delete operation.
func (r *StateStore) Delete(req *state.DeleteRequest) error {
	r.cache.Del(req.GetKey())
	return nil
}

// Get retrieves state from redis with a key.
func (r *StateStore) Get(req *state.GetRequest) (*state.GetResponse, error) {
	value, found := r.cache.Get(req.Key)

	response := &state.GetResponse{Metadata: map[string]string{}}
	if found && value != nil {
		b, _ := value.([]byte)
		response.Data = b
	}
	return response, nil
}

// Set saves state into memory.
func (r *StateStore) Set(req *state.SetRequest) error {
	b, ok := req.Value.([]byte)
	if !ok {
		return fmt.Errorf("in-memory store error: only support byte array")
	}

	ok = r.cache.Set(req.Key, b, int64(len(b)))
	if !ok {
		return fmt.Errorf("in-memory store error: fail to save to in-memory cache, key=%s", req.Key)
	}
	return nil
}

// Multi performs a transactional operation. succeeds only if all operations succeed, and fails if one or more operations fail.
func (r *StateStore) Multi(request *state.TransactionalStateRequest) error {
	return fmt.Errorf("in-memory store error: no support")
}

// Query executes a query against store.
func (r *StateStore) Query(req *state.QueryRequest) (*state.QueryResponse, error) {
	return nil, fmt.Errorf("in-memory store error: no support")
}

func (r *StateStore) Close() error {
	r.cache.Close()
	return nil
}
