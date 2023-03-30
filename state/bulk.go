package state

import (
	"context"
	"encoding/json"
)

// BulkStore is an interface to perform bulk operations on store.
type BulkStore interface {
	bulkStoreGet

	// BulkGetWithParallelism performs a Get operation in bulk with a maximum numnber of parallel requests.
	BulkGetWithParallelism(ctx context.Context, req []GetRequest, parallelism int) ([]BulkGetResponse, error)
	BulkDelete(ctx context.Context, req []DeleteRequest) error
	BulkSet(ctx context.Context, req []SetRequest) error
}

type bulkStoreGet interface {
	// BulkGet performs a Get operation in bulk.
	BulkGet(ctx context.Context, req []GetRequest) ([]BulkGetResponse, error)
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
func (b *DefaultBulkStore) BulkGet(ctx context.Context, req []GetRequest) ([]BulkGetResponse, error) {
	return b.BulkGetWithParallelism(ctx, req, 0)
}

// BulkGetWithParallelism performs a Get operation in bulk with a maximum numnber of parallel requests.
// If the base state store implements a native BulkGet, parallelism is ignored.
func (b *DefaultBulkStore) BulkGetWithParallelism(ctx context.Context, req []GetRequest, parallelism int) ([]BulkGetResponse, error) {
	// If the base implementation offers BulkGet, use that
	// Although this doesn't allow controlling parallelism, the component generally can perform all Get operations in a single invocation
	if bs, ok := b.base.(bulkStoreGet); ok {
		return bs.BulkGet(ctx, req)
	}

	// If parallelism isn't set, run all operations in parallel
	if parallelism <= 0 {
		parallelism = len(req)
	}
	limitCh := make(chan struct{}, parallelism)
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
	for i := 0; i < parallelism; i++ {
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
		ops[i] = TransactionalStateOperation{
			Operation: Upsert,
			Request:   r,
		}
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
		ops[i] = TransactionalStateOperation{
			Operation: Delete,
			Request:   r,
		}
	}
	return ts.Multi(ctx, &TransactionalStateRequest{
		Operations: ops,
	})
}
