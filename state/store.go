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

package state

import (
	"fmt"

	"github.com/dapr/components-contrib/health"
)

// Store is an interface to perform operations on store.
type Store interface {
	BulkStore
	Init(metadata Metadata) error
	Features() []Feature
	Delete(req *DeleteRequest) error
	Get(req *GetRequest) (*GetResponse, error)
	Set(req *SetRequest) error
	GetComponentMetadata() map[string]string
}

func Ping(store Store) error {
	// checks if this store has the ping option then executes
	if storeWithPing, ok := store.(health.Pinger); ok {
		return storeWithPing.Ping()
	} else {
		return fmt.Errorf("ping is not implemented by this state store")
	}
}

// BulkStore is an interface to perform bulk operations on store.
type BulkStore interface {
	BulkDelete(req []DeleteRequest) error
	BulkGet(req []GetRequest) (bool, []BulkGetResponse, error)
	BulkSet(req []SetRequest) error
}

// DefaultBulkStore is a default implementation of BulkStore.
type DefaultBulkStore struct {
	s Store
}

// NewDefaultBulkStore build a default bulk store.
func NewDefaultBulkStore(store Store) DefaultBulkStore {
	defaultBulkStore := DefaultBulkStore{}
	defaultBulkStore.s = store

	return defaultBulkStore
}

// Features returns the features of the encapsulated store.
func (b *DefaultBulkStore) Features() []Feature {
	return b.s.Features()
}

// BulkGet performs a bulks get operations.
func (b *DefaultBulkStore) BulkGet(req []GetRequest) (bool, []BulkGetResponse, error) {
	// by default, the store doesn't support bulk get
	// return false so daprd will fallback to call get() method one by one
	return false, nil, nil
}

// BulkSet performs a bulks save operation.
func (b *DefaultBulkStore) BulkSet(req []SetRequest) error {
	for i := range req {
		err := b.s.Set(&req[i])
		if err != nil {
			return err
		}
	}

	return nil
}

// BulkDelete performs a bulk delete operation.
func (b *DefaultBulkStore) BulkDelete(req []DeleteRequest) error {
	for i := range req {
		err := b.s.Delete(&req[i])
		if err != nil {
			return err
		}
	}

	return nil
}

// Querier is an interface to execute queries.
type Querier interface {
	Query(req *QueryRequest) (*QueryResponse, error)
}
