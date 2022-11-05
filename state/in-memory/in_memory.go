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
	"fmt"
	"strconv"
	"sync"
	"time"

	jsoniter "github.com/json-iterator/go"

	"github.com/dapr/kit/logger"

	"github.com/dapr/components-contrib/state"
)

type inMemStateStoreItem struct {
	data   []byte
	etag   *string
	expire int64
}

type inMemoryStore struct {
	items map[string]*inMemStateStoreItem
	lock  *sync.RWMutex
	log   logger.Logger

	ctx    context.Context
	cancel context.CancelFunc
}

func NewInMemoryStateStore(logger logger.Logger) state.Store {
	return &inMemoryStore{
		items: map[string]*inMemStateStoreItem{},
		lock:  &sync.RWMutex{},
		log:   logger,
	}
}

func (store *inMemoryStore) Init(metadata state.Metadata) error {
	store.ctx, store.cancel = context.WithCancel(context.Background())
	// start a background go routine to clean expired item
	go store.startCleanThread()
	return nil
}

func (store *inMemoryStore) Close() error {
	if store.cancel != nil {
		store.cancel()
	}
	// release memory reference
	store.lock.Lock()
	defer store.lock.Unlock()
	store.items = map[string]*inMemStateStoreItem{}

	return nil
}

func (store *inMemoryStore) Features() []state.Feature {
	return []state.Feature{state.FeatureETag, state.FeatureTransactional}
}

func (store *inMemoryStore) Delete(req *state.DeleteRequest) error {
	// step1: validate parameters
	if err := store.doDeleteValidateParameters(req); err != nil {
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
	store.doDelete(req.Key)
	return nil
}

func (store *inMemoryStore) doDeleteValidateParameters(req *state.DeleteRequest) error {
	return state.CheckRequestOptions(req.Options)
}

func (store *inMemoryStore) doValidateEtag(key string, etag *string, concurrency string) error {
	if etag != nil && *etag != "" && concurrency == state.FirstWrite {
		// For FirstWrite, we need to validate etag before delete
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

func (store *inMemoryStore) doDelete(key string) {
	delete(store.items, key)
}

func (store *inMemoryStore) BulkDelete(req []state.DeleteRequest) error {
	if len(req) == 0 {
		return nil
	}

	// step1: validate parameters

	for i := 0; i < len(req); i++ {
		if err := store.doDeleteValidateParameters(&req[i]); err != nil {
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
		store.doDelete(dr.Key)
	}
	return nil
}

func (store *inMemoryStore) Get(req *state.GetRequest) (*state.GetResponse, error) {
	item := store.doGetWithReadLock(req.Key)
	if item != nil && isExpired(item.expire) {
		item = store.doGetWithWriteLock(req.Key)
	}

	if item == nil {
		return &state.GetResponse{Data: nil, ETag: nil}, nil
	}
	return &state.GetResponse{Data: unmarshal(item.data), ETag: item.etag}, nil
}

func (store *inMemoryStore) doGetWithReadLock(key string) *inMemStateStoreItem {
	store.lock.RLock()
	defer store.lock.RUnlock()

	return store.items[key]
}

func (store *inMemoryStore) doGetWithWriteLock(key string) *inMemStateStoreItem {
	store.lock.Lock()
	defer store.lock.Unlock()
	// get item and check expired again to avoid if item changed between we got this write-lock
	item := store.items[key]
	if item == nil {
		return nil
	}
	if isExpired(item.expire) {
		store.doDelete(key)
		return nil
	}
	return item
}

func isExpired(expire int64) bool {
	if expire <= 0 {
		return false
	}
	return time.Now().UnixMilli() > expire
}

func (store *inMemoryStore) BulkGet(req []state.GetRequest) (bool, []state.BulkGetResponse, error) {
	return false, nil, nil
}

func (store *inMemoryStore) Set(req *state.SetRequest) error {
	// step1: validate parameters
	ttlInSeconds, err := store.doSetValidateParameters(req)
	if err != nil {
		return err
	}

	b, _ := marshal(req.Value)
	// step2 and step3 should be protected by write-lock
	store.lock.Lock()
	defer store.lock.Unlock()

	// step2: validate etag if needed
	if err := store.doValidateEtag(req.Key, req.ETag, req.Options.Concurrency); err != nil {
		return err
	}

	// step3: do really set
	// this operation won't fail
	store.doSet(req.Key, b, req.ETag, ttlInSeconds)
	return nil
}

func (store *inMemoryStore) doSetValidateParameters(req *state.SetRequest) (int, error) {
	err := state.CheckRequestOptions(req.Options)
	if err != nil {
		return -1, err
	}

	ttlInSeconds, err := doParseTTLInSeconds(req.Metadata)
	if err != nil {
		return -1, err
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

func (store *inMemoryStore) doSet(key string, data []byte, etag *string, ttlInSeconds int) {
	store.items[key] = &inMemStateStoreItem{
		data:   data,
		etag:   etag,
		expire: time.Now().UnixMilli() + int64(ttlInSeconds)*1000,
	}
}

// innerSetRequest is only used to pass ttlInSeconds and data with SetRequest.
type innerSetRequest struct {
	req          state.SetRequest
	ttlInSeconds int
	data         []byte
}

func (store *inMemoryStore) BulkSet(req []state.SetRequest) error {
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

		b, _ := marshal(req[i].Value)
		innerSetRequest := &innerSetRequest{
			req:          req[i],
			ttlInSeconds: ttlInSeconds,
			data:         b,
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
		store.doSet(innerSetRequest.req.Key, innerSetRequest.data, innerSetRequest.req.ETag, innerSetRequest.ttlInSeconds)
	}
	return nil
}

func (store *inMemoryStore) Multi(request *state.TransactionalStateRequest) error {
	if len(request.Operations) == 0 {
		return nil
	}

	// step1: validate parameters
	for _, o := range request.Operations {
		if o.Operation == state.Upsert {
			s := o.Request.(state.SetRequest)
			ttlInSeconds, err := store.doSetValidateParameters(&s)
			if err != nil {
				return err
			}
			b, _ := marshal(s.Value)
			innerSetRequest := &innerSetRequest{
				req:          s,
				ttlInSeconds: ttlInSeconds,
				data:         b,
			}
			// replace with innerSetRequest
			o.Request = innerSetRequest
		} else if o.Operation == state.Delete {
			d := o.Request.(state.DeleteRequest)
			err := store.doDeleteValidateParameters(&d)
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
			s := o.Request.(innerSetRequest)
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
			s := o.Request.(innerSetRequest)
			store.doSet(s.req.Key, s.data, s.req.ETag, s.ttlInSeconds)
		} else if o.Operation == state.Delete {
			d := o.Request.(state.DeleteRequest)
			store.doDelete(d.Key)
		}
	}
	return nil
}

func marshal(value interface{}) ([]byte, error) {
	v, _ := jsoniter.MarshalToString(value)

	return []byte(v), nil
}

func unmarshal(val interface{}) []byte {
	var output string

	jsoniter.UnmarshalFromString(string(val.([]byte)), &output)

	return []byte(output)
}

func (store *inMemoryStore) startCleanThread() {
	for {
		select {
		case <-time.After(time.Second):
			store.doCleanExpiredItems()
		case <-store.ctx.Done():
			return
		}
	}
}

func (store *inMemoryStore) doCleanExpiredItems() {
	store.lock.Lock()
	defer store.lock.Unlock()

	for key, item := range store.items {
		if isExpired(item.expire) {
			store.doDelete(key)
		}
	}
}

func (store *inMemoryStore) GetComponentMetadata() map[string]string {
	// no metadata, hence no metadata struct to convert here
	return map[string]string{}
}
