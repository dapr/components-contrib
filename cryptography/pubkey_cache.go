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

package crypto

import (
	"context"
	"sync"

	"github.com/chebyrash/promise"
	"github.com/lestrrat-go/jwx/v2/jwk"

	kitctx "github.com/dapr/kit/context"
)

// GetKeyFn is the type of the getKeyFn function used by the PubKeyCache.
type GetKeyFn = func(ctx context.Context, key string) func(resolve func(jwk.Key), reject func(error))

// PubKeyCache implements GetKey with a local cache. We use promises for cache
// entries so that multiple callers getting the same key at the same time
// (where the key is not in the cache yet), will result in only a single key
// fetch.
// Each cache item uses a context pool so that a key fetch call will only be
// cancelled once all callers have cancelled their context.
type PubKeyCache struct {
	getKeyFn GetKeyFn

	pubKeys map[string]pubKeyCacheEntry
	lock    sync.Mutex
}

type pubKeyCacheEntry struct {
	promise *promise.Promise[jwk.Key]
	ctx     *kitctx.Pool
}

// NewPubKeyCache returns a new PubKeyCache object
func NewPubKeyCache(getKeyFn GetKeyFn) *PubKeyCache {
	return &PubKeyCache{
		getKeyFn: getKeyFn,
		pubKeys:  make(map[string]pubKeyCacheEntry),
	}
}

// GetKey returns a public key from the cache, or uses getKeyFn to request it.
func (kc *PubKeyCache) GetKey(ctx context.Context, key string) (jwk.Key, error) {
	// Check if the key is in the cache already
	kc.lock.Lock()
	p, ok := kc.pubKeys[key]
	if ok {
		// Add the context to the context pool and return the promise (which may
		// already be resolved).
		kc.pubKeys[key].ctx.Add(ctx)
		kc.lock.Unlock()
		jwkKey, err := p.promise.Await(ctx)
		if err != nil || jwkKey == nil {
			return nil, err
		}
		return *jwkKey, nil
	}

	// Key is not in the cache, create the promise in the cache and return
	// result. Create a new context pool for the promise. Cancel the pool on
	// return so that the context pool doesn't expand indefinitely on cache
	// reads.
	p.ctx = kitctx.NewPool(ctx)
	p.promise = promise.Catch(
		promise.New(kc.getKeyFn(p.ctx, key)),
		p.ctx,
		func(err error) error {
			kc.lock.Lock()
			p.ctx.Cancel()
			delete(kc.pubKeys, key)
			kc.lock.Unlock()
			return err
		},
	)
	kc.pubKeys[key] = p
	kc.lock.Unlock()

	jwkKey, err := p.promise.Await(ctx)
	if err != nil || jwkKey == nil {
		return nil, err
	}
	p.ctx.Cancel()
	return *jwkKey, nil
}
