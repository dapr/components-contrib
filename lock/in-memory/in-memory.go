/*
Copyright 2024 The Dapr Authors
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

// duplicate redis/standalone.go using in-memory file instead of redis
// Refer state/in-memory/in-memory.go
import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dapr/components-contrib/lock"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
	"k8s.io/utils/clock"
)

type inMemoryLockStore struct {
	items   map[string]*inMemoryLockItem
	lock    sync.Mutex
	log     logger.Logger
	clock   clock.Clock
	closeCh chan struct{}
	closed  atomic.Bool
	wg      sync.WaitGroup
}

func NewInMemoryLockStore(log logger.Logger) lock.Store {
	return newLockStore(log)
}

func newLockStore(log logger.Logger) *inMemoryLockStore {
	return &inMemoryLockStore{
		items:   map[string]*inMemoryLockItem{},
		log:     log,
		clock:   clock.RealClock{},
		closeCh: make(chan struct{}),
	}
}

func (store *inMemoryLockStore) InitLockStore(ctx context.Context, metadata lock.Metadata) error {
	return nil
}

func (store *inMemoryLockStore) TryLock(ctx context.Context, req *lock.TryLockRequest) (*lock.TryLockResponse, error) {
	store.lock.Lock()
	defer store.lock.Unlock()

	return &lock.TryLockResponse{Success: true}, nil
}

func (store *inMemoryLockStore) Unlock(ctx context.Context, req *lock.UnlockRequest) (*lock.UnlockResponse, error) {
	store.lock.Lock()
	defer store.lock.Unlock()

	return &lock.UnlockResponse{}, nil
}

func (store *inMemoryLockStore) Close() error {
	if store.closed.CompareAndSwap(false, true) {
		close(store.closeCh)
	}

	// release memory reference
	store.lock.Lock()
	defer store.lock.Unlock()
	for k := range store.items {
		delete(store.items, k)
	}

	store.wg.Wait()

	return nil
}

func (store *inMemoryLockStore) GetComponentMetadata() (metadataInfo metadata.MetadataMap) {
	// no metadata, hence no metadata struct to convert here
	return
}

type inMemoryLockItem struct {
	value  string
	expire *time.Time
}
