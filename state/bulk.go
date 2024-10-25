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
	"errors"
)

// BulkGetOpts contains options for the BulkGet method.
type BulkGetOpts struct {
	// Number of requests made in parallel while retrieving values in bulk.
	// When set to <= 0 (the default value), will fetch all requested values in bulk without limit.
	// Note that if the component implements a native BulkGet method, this value may be ignored.
	Parallelism int
}

// BulkStoreOpts contains options for the BulkSet and BulkDelete methods.
type BulkStoreOpts struct {
	// Number of requests made in parallel while storing/deleting values in bulk.
	// When set to <= 0 (the default value), will perform all operations in parallel without limit.
	Parallelism int
}

// BulkStore is an interface to perform bulk operations on store.
type BulkStore interface {
	BulkGet(ctx context.Context, req []GetRequest, opts BulkGetOpts) ([]BulkGetResponse, error)
	BulkSet(ctx context.Context, req []SetRequest, opts BulkStoreOpts) error
	BulkDelete(ctx context.Context, req []DeleteRequest, opts BulkStoreOpts) error
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
	return DoBulkGet(ctx, req, opts, b.base.Get)
}

// BulkSet performs a bulk save operation.
func (b *DefaultBulkStore) BulkSet(ctx context.Context, req []SetRequest, opts BulkStoreOpts) error {
	return DoBulkSetDelete(ctx, req, b.base.Set, opts)
}

// BulkDelete performs a bulk delete operation.
func (b *DefaultBulkStore) BulkDelete(ctx context.Context, req []DeleteRequest, opts BulkStoreOpts) error {
	return DoBulkSetDelete(ctx, req, b.base.Delete, opts)
}

// DoBulkGet performs BulkGet.
func DoBulkGet(ctx context.Context, req []GetRequest, opts BulkGetOpts, getFn func(ctx context.Context, req *GetRequest) (*GetResponse, error)) ([]BulkGetResponse, error) {
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
			item, rErr := getFn(ctx, &r)
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
	for range opts.Parallelism {
		limitCh <- struct{}{}
	}

	return res, nil
}

type stateRequestConstraint interface {
	SetRequest | DeleteRequest
	StateRequest
}

// DoBulkSetDelete performs BulkSet and BulkDelete.
func DoBulkSetDelete[T stateRequestConstraint](ctx context.Context, req []T, method func(ctx context.Context, req *T) error, opts BulkStoreOpts) error {
	// If parallelism isn't set, run all operations in parallel
	var limitCh chan struct{}
	if opts.Parallelism > 0 {
		limitCh = make(chan struct{}, opts.Parallelism)
	}
	errCh := make(chan error, len(req))

	// Execute all operations in parallel
	for i := range req {
		// Limit concurrency
		if limitCh != nil {
			limitCh <- struct{}{}
		}

		go func(i int) {
			rErr := method(ctx, &req[i])
			if rErr != nil {
				errCh <- BulkStoreError{
					key: req[i].GetKey(),
					err: rErr,
				}
			} else {
				errCh <- nil
			}

			// Release the token for concurrency
			if limitCh != nil {
				<-limitCh
			}
		}(i)
	}

	errs := make([]error, len(req))
	for i := range req {
		errs[i] = <-errCh
	}

	return errors.Join(errs...)
}
