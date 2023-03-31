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

package state

import (
	"context"
	"encoding/json"
)

// BulkGetOpts contains options for the BulkGet method
type BulkGetOpts struct {
	// Number of requests made in parallel while retrieving values in bulk.
	// When set to <= 0 (the default value), will fetch all requested values in bulk without limit.
	// Note that if the component implements a native BulkGet method, this value may be ignored.
	Parallelism int
}

// BulkStore is an interface to perform bulk operations on store.
type BulkStore interface {
	BulkGet(ctx context.Context, req []GetRequest, opts BulkGetOpts) ([]BulkGetResponse, error)
	BulkDelete(ctx context.Context, req []DeleteRequest) error
	BulkSet(ctx context.Context, req []SetRequest) error
}

// DefaultBulkStore is a default implementation of BulkStore.
type DefaultBulkStore struct {
	base BaseStore
}

// NewDefaultBulkStore build a default bulk store.
func NewDefaultBulkStore(base BaseStore) BulkStore {
	return &DefaultBulkStore{
		base: base,
	}
}

// BulkGet performs a Get operation in bulk.
func (b *DefaultBulkStore) BulkGet(ctx context.Context, req []GetRequest, opts BulkGetOpts) ([]BulkGetResponse, error) {
	// If parallelism isn't set, run all operations in parallel
	if opts.Parallelism <= 0 {
		opts.Parallelism = len(req)
	}
	limitCh := make(chan struct{}, opts.Parallelism)
	res := make([]BulkGetResponse, len(req))
	for i := range req {
		// Limit concurrency
		limitCh <- struct{}{}
		go func(i int) {
			defer func() {
				// Release the token for concurrency
				<-limitCh
			}()

			r := req[i]
			res[i].Key = r.Key
			item, rErr := b.base.Get(ctx, &r)
			if rErr != nil {
				res[i].Error = rErr.Error()
				return
			}

			if item != nil {
				res[i].Data = json.RawMessage(item.Data)
				res[i].ETag = item.ETag
				res[i].Metadata = item.Metadata
			}
		}(i)
	}

	// We can detect that all goroutines are done when limitCh is completely empty
	for i := 0; i < opts.Parallelism; i++ {
		limitCh <- struct{}{}
	}

	return res, nil
}

// BulkSet performs a bulks save operation.
func (b *DefaultBulkStore) BulkSet(ctx context.Context, req []SetRequest) error {
	// Check if the base implementation supports transactions
	if ts, ok := b.base.(TransactionalStore); ok {
		return b.bulkSetTransactional(ctx, req, ts)
	}

	// Fallback to executing all operations in sequence
	for i := range req {
		err := b.base.Set(ctx, &req[i])
		if err != nil {
			return err
		}
	}

	return nil
}

// bulkSetTransactional performs a bulk save operation when the base state store is transactional.
func (b *DefaultBulkStore) bulkSetTransactional(ctx context.Context, req []SetRequest, ts TransactionalStore) error {
	ops := make([]TransactionalStateOperation, len(req))
	for i, r := range req {
		ops[i] = r
	}
	return ts.Multi(ctx, &TransactionalStateRequest{
		Operations: ops,
	})
}

// BulkDelete performs a bulk delete operation.
func (b *DefaultBulkStore) BulkDelete(ctx context.Context, req []DeleteRequest) error {
	// Check if the base implementation supports transactions
	if ts, ok := b.base.(TransactionalStore); ok {
		return b.bulkDeleteTransactional(ctx, req, ts)
	}

	// Fallback to executing all operations in sequence
	for i := range req {
		err := b.base.Delete(ctx, &req[i])
		if err != nil {
			return err
		}
	}

	return nil
}

// bulkDeleteTransactional performs a bulk delete operation when the base state store is transactional.
func (b *DefaultBulkStore) bulkDeleteTransactional(ctx context.Context, req []DeleteRequest, ts TransactionalStore) error {
	ops := make([]TransactionalStateOperation, len(req))
	for i, r := range req {
		ops[i] = r
	}
	return ts.Multi(ctx, &TransactionalStateRequest{
		Operations: ops,
	})
}
