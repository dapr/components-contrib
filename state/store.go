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

import "context"

// Store is an interface to perform operations on store.
type Store interface {
	BulkStore
	Init(metadata Metadata) error
	Features() []Feature
	Delete(ctx context.Context, req *DeleteRequest) error
	Get(ctx context.Context, req *GetRequest) (*GetResponse, error)
	Set(ctx context.Context, req *SetRequest) error
	Ping(ctx context.Context) error
}

// BulkStore is an interface to perform bulk operations on store.
type BulkStore interface {
	BulkDelete(ctx context.Context, req []DeleteRequest) error
	BulkGet(ctx context.Context, req []GetRequest) (bool, []BulkGetResponse, error)
	BulkSet(ctx context.Context, req []SetRequest) error
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
func (b *DefaultBulkStore) BulkGet(ctx context.Context, req []GetRequest) (bool, []BulkGetResponse, error) {
	// by default, the store doesn't support bulk get
	// return false so daprd will fallback to call get() method one by one
	return false, nil, nil
}

// BulkSet performs a bulks save operation.
func (b *DefaultBulkStore) BulkSet(ctx context.Context, req []SetRequest) error {
	for i := range req {
		err := b.s.Set(ctx, &req[i])
		if err != nil {
			return err
		}
	}

	return nil
}

// BulkDelete performs a bulk delete operation.
func (b *DefaultBulkStore) BulkDelete(ctx context.Context, req []DeleteRequest) error {
	for i := range req {
		err := b.s.Delete(ctx, &req[i])
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
