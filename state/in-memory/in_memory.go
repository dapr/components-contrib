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
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	jsoniter "github.com/json-iterator/go"

	"github.com/dapr/kit/logger"
	"github.com/dapr/kit/ptr"

	"github.com/dapr/components-contrib/state"
	"github.com/dapr/components-contrib/state/utils"
)

type inMemStateStoreItem struct {
	data     []byte
	etag     *string
	expire   *int64
	isBinary bool
}

type inMemoryStore struct {
	items   map[string]*inMemStateStoreItem
	lock    *sync.RWMutex
	log     logger.Logger
	closeCh chan struct{}
	closed  atomic.Bool
	wg      sync.WaitGroup
}

func NewInMemoryStateStore(logger logger.Logger) state.Store {
	return &inMemoryStore{
		items:   map[string]*inMemStateStoreItem{},
		lock:    &sync.RWMutex{},
		log:     logger,
		closeCh: make(chan struct{}),
	}
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
	defer store.wg.Wait()
	if store.closed.CompareAndSwap(false, true) {
		close(store.closeCh)
	}

	// release memory reference
	store.lock.Lock()
	defer store.lock.Unlock()
	for k := range store.items {
		delete(store.items, k)
	}

	return nil
}

func (store *inMemoryStore) Features(ctx context.Context) []state.Feature {
	return []state.Feature{state.FeatureETag, state.FeatureTransactional}
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

func (store *inMemoryStore) BulkDelete(ctx context.Context, req []state.DeleteRequest) error {
	if len(req) == 0 {
		return nil
	}

	// step1: validate parameters
	for i := 0; i < len(req); i++ {
		if err := state.CheckRequestOptions(&req[i].Options); err != nil {
			return err
		}
	}

	// step2 and step3 should be protected by write-lock
	store.lock.Lock()
	defer store.lock.Unlock()

	// step2: validate etag if needed
	for _, dr := range req {
		err := store.doValidateEtag(dr.Key, dr.ETag, dr.Options.Concurrency)
		if err != nil {
			return err
		}
	}

	// step3: do really delete
	for _, dr := range req {
		store.doDelete(ctx, dr.Key)
	}
	return nil
}

func (store *inMemoryStore) Get(ctx context.Context, req *state.GetRequest) (*state.GetResponse, error) {
	item := store.doGetWithReadLock(ctx, req.Key)
	if item != nil && isExpired(item) {
		item = store.doGetWithWriteLock(ctx, req.Key)
	}

	if item == nil {
		return &state.GetResponse{Data: nil, ETag: nil}, nil
	}

	data := item.data
	if item.isBinary {
		var (
			s   string
			err error
		)

		if err = jsoniter.Unmarshal(data, &s); err != nil {
			return nil, err
		}

		data, err = base64.StdEncoding.DecodeString(s)
		if err != nil {
			return nil, err
		}
	}

	return &state.GetResponse{Data: data, ETag: item.etag}, nil
}

func (store *inMemoryStore) doGetWithReadLock(ctx context.Context, key string) *inMemStateStoreItem {
	store.lock.RLock()
	defer store.lock.RUnlock()

	return store.items[key]
}

func (store *inMemoryStore) doGetWithWriteLock(ctx context.Context, key string) *inMemStateStoreItem {
	store.lock.Lock()
	defer store.lock.Unlock()
	// get item and check expired again to avoid if item changed between we got this write-lock
	item := store.items[key]
	if item == nil {
		return nil
	}
	if isExpired(item) {
		store.doDelete(ctx, key)
		return nil
	}
	return item
}

func isExpired(item *inMemStateStoreItem) bool {
	if item == nil || item.expire == nil {
		return false
	}
	return time.Now().UnixMilli() > *item.expire
}

func (store *inMemoryStore) BulkGet(ctx context.Context, req []state.GetRequest) (bool, []state.BulkGetResponse, error) {
	return false, nil, nil
}

func (store *inMemoryStore) marshal(v any) (bt []byte, isBinary bool, err error) {
	byteArray, isBinary := v.([]uint8)
	if isBinary {
		v = base64.StdEncoding.EncodeToString(byteArray)
	}
	bt, err = utils.Marshal(v, json.Marshal)
	if err != nil {
		return nil, false, err
	}
	return bt, isBinary, nil
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
	bt, isBinary, err := store.marshal(req.Value)
	if err != nil {
		return err
	}

	// this operation won't fail
	store.doSet(ctx, req.Key, bt, ttlInSeconds, isBinary)
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

func (store *inMemoryStore) doSet(ctx context.Context, key string, data []byte, ttlInSeconds int, isBinary bool) {
	etag := uuid.New().String()
	el := &inMemStateStoreItem{
		data:     data,
		etag:     &etag,
		isBinary: isBinary,
	}
	if ttlInSeconds > 0 {
		el.expire = ptr.Of(time.Now().UnixMilli() + int64(ttlInSeconds)*1000)
	}

	store.items[key] = el
}

// innerSetRequest is only used to pass ttlInSeconds and data with SetRequest.
type innerSetRequest struct {
	req      state.SetRequest
	ttl      int
	data     []byte
	isBinary bool
}

func (store *inMemoryStore) BulkSet(ctx context.Context, req []state.SetRequest) error {
	if len(req) == 0 {
		return nil
	}

	// step1: validate parameters
	innerSetRequestList := make([]*innerSetRequest, 0, len(req))
	for i := 0; i < len(req); i++ {
		ttlInSeconds, err := store.doSetValidateParameters(&req[i])
		if err != nil {
			return err
		}

		bt, isBinary, err := store.marshal(req[i].Value)
		if err != nil {
			return err
		}
		innerSetRequest := &innerSetRequest{
			req:      req[i],
			ttl:      ttlInSeconds,
			data:     bt,
			isBinary: isBinary,
		}
		innerSetRequestList = append(innerSetRequestList, innerSetRequest)
	}

	// step2 and step3 should be protected by write-lock
	store.lock.Lock()
	defer store.lock.Unlock()

	// step2: validate etag if needed
	for _, dr := range req {
		err := store.doValidateEtag(dr.Key, dr.ETag, dr.Options.Concurrency)
		if err != nil {
			return err
		}
	}

	// step3: do really set
	// these operations won't fail
	for _, innerSetRequest := range innerSetRequestList {
		store.doSet(ctx, innerSetRequest.req.Key, innerSetRequest.data, innerSetRequest.ttl, innerSetRequest.isBinary)
	}
	return nil
}

func (store *inMemoryStore) Multi(ctx context.Context, request *state.TransactionalStateRequest) error {
	if len(request.Operations) == 0 {
		return nil
	}

	// step1: validate parameters
	for i, o := range request.Operations {
		if o.Operation == state.Upsert {
			s := o.Request.(state.SetRequest)
			ttlInSeconds, err := store.doSetValidateParameters(&s)
			if err != nil {
				return err
			}
			bt, isBinary, err := store.marshal(s.Value)
			if err != nil {
				return err
			}
			innerSetRequest := &innerSetRequest{
				req:      s,
				ttl:      ttlInSeconds,
				data:     bt,
				isBinary: isBinary,
			}
			// replace with innerSetRequest
			request.Operations[i].Request = innerSetRequest
		} else if o.Operation == state.Delete {
			d := o.Request.(state.DeleteRequest)
			err := state.CheckRequestOptions(&d)
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
		if o.Operation == state.Upsert {
			s := o.Request.(*innerSetRequest)
			err := store.doValidateEtag(s.req.Key, s.req.ETag, s.req.Options.Concurrency)
			if err != nil {
				return err
			}
		} else if o.Operation == state.Delete {
			d := o.Request.(state.DeleteRequest)
			err := store.doValidateEtag(d.Key, d.ETag, d.Options.Concurrency)
			if err != nil {
				return err
			}
		}
	}

	// step3: do really set
	// these operations won't fail
	for _, o := range request.Operations {
		if o.Operation == state.Upsert {
			s := o.Request.(*innerSetRequest)
			store.doSet(ctx, s.req.Key, s.data, s.ttl, s.isBinary)
		} else if o.Operation == state.Delete {
			d := o.Request.(state.DeleteRequest)
			store.doDelete(ctx, d.Key)
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
		if item.expire != nil && isExpired(item) {
			store.doDelete(context.Background(), key)
		}
	}
}

func (store *inMemoryStore) GetComponentMetadata() map[string]string {
	// no metadata, hence no metadata struct to convert here
	return map[string]string{}
}
