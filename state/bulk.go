package state

import (
	"context"
	"encoding/json"
)

// BulkStore is an interface to perform bulk operations on store.
type BulkStore interface {
	bulkStoreGet
	bulkStoreDelete
	bulkStoreSet
}

type bulkStoreGet interface {
	// BulkGet performs a Get operation in bulk.
	BulkGet(ctx context.Context, req []GetRequest) ([]BulkGetResponse, error)
	// BulkGetWithParallelism performs a Get operation in bulk with a maximum numnber of parallel requests.
	// If the underlying state store implements a native BulkGet, parallelism is ignored..
	BulkGetWithParallelism(ctx context.Context, req []GetRequest, parallelism int) ([]BulkGetResponse, error)
}

type bulkStoreDelete interface {
	BulkDelete(ctx context.Context, req []DeleteRequest) error
}

type bulkStoreSet interface {
	BulkSet(ctx context.Context, req []SetRequest) error
}

// DefaultBulkStore is a default implementation of BulkStore.
type DefaultBulkStore struct {
	BaseStore
}

// NewDefaultBulkStore build a default bulk store.
func NewDefaultBulkStore(base BaseStore) *DefaultBulkStore {
	return &DefaultBulkStore{
		BaseStore: base,
	}
}

// BulkGet performs a Get operation in bulk.
func (b *DefaultBulkStore) BulkGet(ctx context.Context, req []GetRequest) ([]BulkGetResponse, error) {
	return b.BulkGetWithParallelism(ctx, req, 0)
}

// BulkGetWithParallelism performs a Get operation in bulk with a maximum numnber of parallel requests.
// If the underlying state store implements a native BulkGet, parallelism is ignored..
func (b *DefaultBulkStore) BulkGetWithParallelism(ctx context.Context, req []GetRequest, parallelism int) ([]BulkGetResponse, error) {
	// If the underlying implementation offers BulkGet, use that
	if bs, ok := b.BaseStore.(bulkStoreGet); ok {
		return bs.BulkGet(ctx, req)
	}

	// Fallback to executing all operations in sequence
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
			item, rErr := b.Get(ctx, &r)
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
	// If the underlying implementation offers BulkSet, use that
	if bs, ok := b.BaseStore.(bulkStoreSet); ok {
		return bs.BulkSet(ctx, req)
	}

	// Check if the underlying implementation supports transactions
	if ts, ok := b.BaseStore.(TransactionalStore); ok {
		return b.bulkSetTransactional(ctx, req, ts)
	}

	// Fallback to executing all operations in sequence
	for i := range req {
		err := b.Set(ctx, &req[i])
		if err != nil {
			return err
		}
	}

	return nil
}

// bulkSetTransactional performs a bulk save operation when the underlying state store is transactional.
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
	// If the underlying implementation offers BulkDelete, use that
	if bs, ok := b.BaseStore.(bulkStoreDelete); ok {
		return bs.BulkDelete(ctx, req)
	}

	// Check if the underlying implementation supports transactions
	if ts, ok := b.BaseStore.(TransactionalStore); ok {
		return b.bulkDeleteTransactional(ctx, req, ts)
	}

	// Fallback to executing all operations in sequence
	for i := range req {
		err := b.Delete(ctx, &req[i])
		if err != nil {
			return err
		}
	}

	return nil
}

// bulkDeleteTransactional performs a bulk delete operation when the underlying state store is transactional.
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
