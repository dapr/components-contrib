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
)

// GetKeyFn is the type of the getKeyFn function used by the PubKeyCache.
type GetKeyFn = func(ctx context.Context, key string) func(resolve func(jwk.Key), reject func(error))

// PubKeyCache implements GetKey with a local cache. We use promises for cache
// entries so that multiple callers getting the same key at the same time
// (where the key is not in the cache yet), will result in only a single key
// fetch.
type PubKeyCache struct {
	getKeyFn GetKeyFn

	pubKeys     map[string]*promise.Promise[jwk.Key]
	pubKeysLock sync.Mutex
}

// NewPubKeyCache returns a new PubKeyCache object
func NewPubKeyCache(getKeyFn GetKeyFn) *PubKeyCache {
	return &PubKeyCache{
		getKeyFn: getKeyFn,
		pubKeys:  map[string]*promise.Promise[jwk.Key]{},
	}
}

// GetKey returns a public key from the cache, or uses getKeyFn to request it.
func (kc *PubKeyCache) GetKey(ctx context.Context, key string) (jwk.Key, error) {
	timeoutPromise := promise.New(func(_ func(jwk.Key), reject func(error)) {
		<-ctx.Done()
		reject(ctx.Err())
	})

	// Check if the key is in the cache already
	kc.pubKeysLock.Lock()
	p, ok := kc.pubKeys[key]
	if ok {
		kc.pubKeysLock.Unlock()
		return promise.Race(p, timeoutPromise).Await()
	}

	// Key is not in the cache, create the promise in the cache and return
	// result.
	p = promise.New(kc.getKeyFn(ctx, key))
	p = promise.Catch(p, func(err error) error {
		kc.pubKeysLock.Lock()
		delete(kc.pubKeys, key)
		kc.pubKeysLock.Unlock()
		return err
	})
	kc.pubKeys[key] = p
	kc.pubKeysLock.Unlock()

	return promise.Race(p, timeoutPromise).Await()
}
