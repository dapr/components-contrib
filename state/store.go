// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package state

// Store is an interface to perform operations on store
type Store interface {
	BulkStore
	Init(metadata Metadata) error
	Delete(req *DeleteRequest) error
	Get(req *GetRequest) (*GetResponse, error)
	Set(req *SetRequest) error
}

// BulkStore is an interface to perform bulk operations on store
type BulkStore interface {
	BulkDelete(req []DeleteRequest) error
	BulkGet(req []GetRequest) (bool, []GetResponse, error)
	BulkSet(req []SetRequest) error
}

// DefaultBulkStore is a default implementation of BulkStore
type DefaultBulkStore struct {
	s Store
}

// NewDefaultBulkStore build a default bulk store
func NewDefaultBulkStore(store Store) DefaultBulkStore  {
	defaultBulkStore := DefaultBulkStore{}
	defaultBulkStore.s = store
	return defaultBulkStore
}

// BulkGet performs a bulks get operations
func (b *DefaultBulkStore) BulkGet(req []GetRequest)  (bool, []GetResponse, error)  {
	// by default, the store doesn't support bulk get
	// return false so daprd will fallback to call get() method one by one
	return false, nil, nil
}

// BulkSet performs a bulks save operation
func (b *DefaultBulkStore) BulkSet(req []SetRequest) error {
	for i := range req {
		err := b.s.Set(&req[i])
		if err != nil {
			return err
		}
	}

	return nil
}

// BulkDelete performs a bulk delete operation
func (b *DefaultBulkStore) BulkDelete(req []DeleteRequest) error {
	for i := range req {
		err := b.s.Delete(&req[i])
		if err != nil {
			return err
		}
	}

	return nil
}