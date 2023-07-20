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
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"k8s.io/utils/clock"

	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/state"
	"github.com/dapr/components-contrib/state/utils"
	"github.com/dapr/kit/logger"
	"github.com/dapr/kit/ptr"
)

type inMemoryStore struct {
	state.BulkStore

	items   map[string]*inMemStateStoreItem
	lock    sync.RWMutex
	log     logger.Logger
	clock   clock.Clock
	closeCh chan struct{}
	closed  atomic.Bool
	wg      sync.WaitGroup
}

func NewInMemoryStateStore(log logger.Logger) state.Store {
	return newStateStore(log)
}

func newStateStore(log logger.Logger) *inMemoryStore {
	s := &inMemoryStore{
		items:   map[string]*inMemStateStoreItem{},
		log:     log,
		closeCh: make(chan struct{}),
		clock:   clock.RealClock{},
	}
	s.BulkStore = state.NewDefaultBulkStore(s)
	return s
}

func (store *inMemoryStore) Init(ctx context.Context, metadata state.Metadata) error {
	// start a background go routine to clean expired item
	store.wg.Add(1)
	go func() {
		defer store.wg.Done()
		store.startCleanThread()
	}()
	return nil
}

func (store *inMemoryStore) Close() error {
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

func (store *inMemoryStore) Features() []state.Feature {
	return []state.Feature{
		state.FeatureETag,
		state.FeatureTransactional,
		state.FeatureTTL,
	}
}

func (store *inMemoryStore) Delete(ctx context.Context, req *state.DeleteRequest) error {
	// step1: validate parameters
	if err := state.CheckRequestOptions(req.Options); err != nil {
		return err
	}

	// step2 and step3 should be protected by write-lock
	store.lock.Lock()
	defer store.lock.Unlock()

	// step2: validate etag if needed
	if err := store.doValidateEtag(req.Key, req.ETag, req.Options.Concurrency); err != nil {
		return err
	}

	// step3: do really delete
	// this operation won't fail
	store.doDelete(ctx, req.Key)
	return nil
}

func (store *inMemoryStore) doValidateEtag(key string, etag *string, concurrency string) error {
	hasEtag := etag != nil && *etag != ""

	if concurrency == state.FirstWrite && !hasEtag {
		item := store.items[key]
		if item != nil {
			return state.NewETagError(state.ETagMismatch, errors.New("item already exists and no etag was passed"))
		} else {
			return nil
		}
	} else if hasEtag {
		item := store.items[key]
		if item == nil {
			return state.NewETagError(state.ETagMismatch, fmt.Errorf("state not exist or expired for key=%s", key))
		}
		if item.etag == nil {
			return state.NewETagError(state.ETagMismatch, fmt.Errorf(
				"state etag not match for key=%s: current=nil, expect=%s", key, *etag))
		}
		if *item.etag != *etag {
			return state.NewETagError(state.ETagMismatch, fmt.Errorf(
				"state etag not match for key=%s: current=%s, expect=%s", key, *item.etag, *etag))
		}
	}
	return nil
}

func (store *inMemoryStore) doDelete(ctx context.Context, key string) {
	delete(store.items, key)
}

func (store *inMemoryStore) Get(ctx context.Context, req *state.GetRequest) (*state.GetResponse, error) {
	store.lock.RLock()
	item := store.items[req.Key]
	store.lock.RUnlock()
	if item != nil && item.isExpired(store.clock.Now()) {
		store.lock.Lock()
		item = store.getAndExpire(req.Key)
		store.lock.Unlock()
	}

	if item == nil {
		return &state.GetResponse{}, nil
	}

	var metadata map[string]string
	if item.expire != nil {
		metadata = map[string]string{
			state.GetRespMetaKeyTTLExpireTime: item.expire.UTC().Format(time.RFC3339),
		}
	}

	return &state.GetResponse{Data: item.data, ETag: item.etag, Metadata: metadata}, nil
}

func (store *inMemoryStore) BulkGet(ctx context.Context, req []state.GetRequest, _ state.BulkGetOpts) ([]state.BulkGetResponse, error) {
	res := make([]state.BulkGetResponse, len(req))
	if len(req) == 0 {
		return res, nil
	}

	// While working in bulk, we won't delete expired records we may encounter; we'll just let them stay until GC picks them up
	store.lock.RLock()
	defer store.lock.RUnlock()

	for i, r := range req {
		item := store.items[r.Key]
		if item != nil && !item.isExpired(store.clock.Now()) {
			res[i] = state.BulkGetResponse{
				Key:  r.Key,
				Data: item.data,
				ETag: item.etag,
			}

			if item.expire != nil {
				res[i].Metadata = map[string]string{
					state.GetRespMetaKeyTTLExpireTime: item.expire.UTC().Format(time.RFC3339),
				}
			}
		} else {
			res[i] = state.BulkGetResponse{
				Key: r.Key,
			}
		}
	}

	return res, nil
}

func (store *inMemoryStore) getAndExpire(key string) *inMemStateStoreItem {
	// get item and check expired again to avoid if item changed between we got this write-lock
	item := store.items[key]
	if item == nil {
		return nil
	}
	if item.isExpired(store.clock.Now()) {
		delete(store.items, key)
		return nil
	}
	return item
}

func (store *inMemoryStore) marshal(v any) (bt []byte, err error) {
	byteArray, isBinary := v.([]uint8)
	if isBinary {
		bt = byteArray
	} else {
		bt, err = utils.Marshal(v, json.Marshal)
		if err != nil {
			return nil, err
		}
	}
	return bt, nil
}

func (store *inMemoryStore) Set(ctx context.Context, req *state.SetRequest) error {
	// step1: validate parameters
	ttlInSeconds, err := store.doSetValidateParameters(req)
	if err != nil {
		return err
	}

	// step2 and step3 should be protected by write-lock
	store.lock.Lock()
	defer store.lock.Unlock()

	// step2: validate etag if needed
	err = store.doValidateEtag(req.Key, req.ETag, req.Options.Concurrency)
	if err != nil {
		return err
	}

	// step3: do really set
	bt, err := store.marshal(req.Value)
	if err != nil {
		return err
	}

	// this operation won't fail
	store.doSet(ctx, req.Key, bt, ttlInSeconds)
	return nil
}

func (store *inMemoryStore) doSetValidateParameters(req *state.SetRequest) (int, error) {
	err := state.CheckRequestOptions(req.Options)
	if err != nil {
		return 0, err
	}

	ttlInSeconds, err := doParseTTLInSeconds(req.Metadata)
	if err != nil {
		return 0, err
	}

	return ttlInSeconds, nil
}

func doParseTTLInSeconds(metadata map[string]string) (int, error) {
	s := metadata["ttlInSeconds"]
	if s == "" {
		return 0, nil
	}

	i, err := strconv.Atoi(s)
	if err != nil {
		return 0, err
	}

	if i < 0 {
		i = 0
	}

	return i, nil
}

func (store *inMemoryStore) doSet(ctx context.Context, key string, data []byte, ttlInSeconds int) {
	etag := uuid.New().String()
	el := &inMemStateStoreItem{
		data: data,
		etag: &etag,
	}
	if ttlInSeconds > 0 {
		el.expire = ptr.Of(store.clock.Now().Add(time.Duration(ttlInSeconds) * time.Second))
	}

	store.items[key] = el
}

// innerSetRequest is only used to pass ttlInSeconds and data with SetRequest.
type innerSetRequest struct {
	req  state.SetRequest
	ttl  int
	data []byte
}

// Implements state.TransactionalStateOperation
func (innerSetRequest) Operation() state.OperationType {
	return "_internal"
}

// Implements state.StateRequest
func (r innerSetRequest) GetKey() string {
	return r.req.Key
}

func (r innerSetRequest) GetMetadata() map[string]string {
	return r.req.Metadata
}

func (store *inMemoryStore) Multi(ctx context.Context, request *state.TransactionalStateRequest) error {
	if len(request.Operations) == 0 {
		return nil
	}

	// step1: validate parameters
	for i, o := range request.Operations {
		switch req := o.(type) {
		case state.SetRequest:
			ttlInSeconds, err := store.doSetValidateParameters(&req)
			if err != nil {
				return err
			}
			bt, err := store.marshal(req.Value)
			if err != nil {
				return err
			}
			innerSetRequest := &innerSetRequest{
				req:  req,
				ttl:  ttlInSeconds,
				data: bt,
			}
			// replace with innerSetRequest
			request.Operations[i] = innerSetRequest
		case state.DeleteRequest:
			err := state.CheckRequestOptions(&req)
			if err != nil {
				return err
			}
		}
	}

	// step2 and step3 should be protected by write-lock
	store.lock.Lock()
	defer store.lock.Unlock()

	// step2: validate etag if needed
	for _, o := range request.Operations {
		switch req := o.(type) {
		case *innerSetRequest:
			err := store.doValidateEtag(req.req.Key, req.req.ETag, req.req.Options.Concurrency)
			if err != nil {
				return err
			}
		case state.DeleteRequest:
			err := store.doValidateEtag(req.Key, req.ETag, req.Options.Concurrency)
			if err != nil {
				return err
			}
		}
	}

	// step3: do really set
	// these operations won't fail
	for _, o := range request.Operations {
		switch req := o.(type) {
		case *innerSetRequest:
			store.doSet(ctx, req.req.Key, req.data, req.ttl)
		case state.DeleteRequest:
			store.doDelete(ctx, req.Key)
		}
	}
	return nil
}

func (store *inMemoryStore) startCleanThread() {
	for {
		select {
		case <-time.After(time.Second):
			store.doCleanExpiredItems()
		case <-store.closeCh:
			return
		}
	}
}

func (store *inMemoryStore) doCleanExpiredItems() {
	store.lock.Lock()
	defer store.lock.Unlock()

	for key, item := range store.items {
		if item.expire != nil && item.isExpired(store.clock.Now()) {
			store.doDelete(context.Background(), key)
		}
	}
}

func (store *inMemoryStore) GetComponentMetadata() (metadataInfo metadata.MetadataMap) {
	// no metadata, hence no metadata struct to convert here
	return
}

type inMemStateStoreItem struct {
	data   []byte
	etag   *string
	expire *time.Time
}

func (item *inMemStateStoreItem) isExpired(now time.Time) bool {
	if item == nil || item.expire == nil {
		return false
	}
	return now.After(*item.expire)
}
