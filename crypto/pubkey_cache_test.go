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
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/chebyrash/promise"
	"github.com/lestrrat-go/jwx/v2/jwk"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	kitctx "github.com/dapr/kit/context"
)

func TestPubKeyCacheGetKey(t *testing.T) {
	pk, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	testKey, err := jwk.FromRaw(pk.PublicKey)
	require.NoError(t, err)

	pk2, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	testKey2, err := jwk.FromRaw(pk2.PublicKey)
	require.NoError(t, err)

	t.Run("existing key should return key", func(t *testing.T) {
		t.Parallel()
		cache := NewPubKeyCache(func(context.Context, string) func(resolve func(jwk.Key), reject func(error)) {
			return func(resolve func(jwk.Key), reject func(error)) { assert.Fail(t, "should not be called") }
		})
		cache.pubKeys["key"] = pubKeyCacheEntry{
			promise: promise.New(func(resolve func(jwk.Key), reject func(error)) {
				resolve(testKey)
			}),
			ctx: kitctx.NewPool(),
		}
		result, err := cache.GetKey(context.Background(), "key")
		require.NoError(t, err)
		assert.Equal(t, testKey, result)
	})

	t.Run("two different keys should be returned", func(t *testing.T) {
		t.Parallel()
		cache := NewPubKeyCache(func(context.Context, string) func(resolve func(jwk.Key), reject func(error)) {
			return func(resolve func(jwk.Key), reject func(error)) { assert.Fail(t, "should not be called") }
		})
		cache.pubKeys["key"] = pubKeyCacheEntry{
			promise: promise.New(func(resolve func(jwk.Key), reject func(error)) {
				resolve(testKey)
			}),
			ctx: kitctx.NewPool(),
		}
		cache.pubKeys["another-key"] = pubKeyCacheEntry{
			promise: promise.New(func(resolve func(jwk.Key), reject func(error)) {
				resolve(testKey2)
			}),
			ctx: kitctx.NewPool(),
		}

		result, err := cache.GetKey(context.Background(), "key")
		require.NoError(t, err)
		assert.Equal(t, testKey, result)

		result, err = cache.GetKey(context.Background(), "another-key")
		require.NoError(t, err)
		assert.Equal(t, testKey2, result)
	})

	t.Run("cold cache should fetch key", func(t *testing.T) {
		t.Parallel()
		var called int
		cache := NewPubKeyCache(func(context.Context, string) func(resolve func(jwk.Key), reject func(error)) {
			return func(resolve func(jwk.Key), reject func(error)) {
				called++
				resolve(testKey)
			}
		})

		result, err := cache.GetKey(context.Background(), "key")
		require.NoError(t, err)
		assert.Equal(t, testKey, result)
		assert.Equal(t, 1, called, "should be called once")
	})

	t.Run("cold cache should fetch different keys", func(t *testing.T) {
		t.Parallel()
		var called int
		cache := NewPubKeyCache(func(ctx context.Context, i string) func(resolve func(jwk.Key), reject func(error)) {
			return func(resolve func(jwk.Key), reject func(error)) {
				called++
				switch i {
				case "key":
					resolve(testKey)
				default:
					resolve(testKey2)
				}
			}
		})

		result, err := cache.GetKey(context.Background(), "key")
		require.NoError(t, err)
		assert.Equal(t, testKey, result)
		result, err = cache.GetKey(context.Background(), "another-key")
		require.NoError(t, err)
		assert.Equal(t, testKey2, result)

		assert.Equal(t, 2, called, "should be called once")
	})

	t.Run("fetch key which errors, should error getKey", func(t *testing.T) {
		t.Parallel()
		var called int
		cache := NewPubKeyCache(func(ctx context.Context, i string) func(resolve func(jwk.Key), reject func(error)) {
			return func(resolve func(jwk.Key), reject func(error)) {
				assert.Equal(t, "key", i)
				called++
				reject(assert.AnError)
			}
		})

		result, err := cache.GetKey(context.Background(), "key")
		require.Error(t, err)
		assert.Nil(t, result)
		assert.Equal(t, 1, called, "should be called once")
	})

	t.Run("multiple fetch key at the same time should only all getKey once", func(t *testing.T) {
		t.Parallel()
		var called int
		cache := NewPubKeyCache(func(ctx context.Context, i string) func(resolve func(jwk.Key), reject func(error)) {
			return func(resolve func(jwk.Key), reject func(error)) {
				assert.Equal(t, "key", i)
				called++
				resolve(testKey)
			}
		})

		var wg sync.WaitGroup
		wg.Add(10)
		for range 10 {
			go func() {
				defer wg.Done()
				result, err := cache.GetKey(context.Background(), "key")
				require.NoError(t, err)
				assert.Equal(t, testKey, result)
			}()
		}

		wg.Wait()
		assert.Equal(t, 1, called, "should be called once")
	})

	t.Run("calling get key and context is cancelled should return context error", func(t *testing.T) {
		t.Parallel()
		ctx, cancel := context.WithCancelCause(context.Background())
		getKeyReturned := make(chan struct{})

		cache := NewPubKeyCache(func(ctx context.Context, i string) func(resolve func(jwk.Key), reject func(error)) {
			return func(resolve func(jwk.Key), reject func(error)) {
				t.Cleanup(func() {
					select {
					case <-getKeyReturned:
					case <-time.After(1 * time.Second):
						assert.Fail(t, "expected GetKey to return from cancelled context in time")
					}
				})
				assert.Equal(t, "key", i)
				cancel(assert.AnError)
				resolve(testKey)
			}
		})

		result, err := cache.GetKey(ctx, "key")
		assert.Equal(t, context.Canceled, err)
		assert.Nil(t, result)
		close(getKeyReturned)
	})

	t.Run("only callers with cancelled contexts should return context error", func(t *testing.T) {
		t.Parallel()
		ctx1, cancel1 := context.WithCancelCause(context.Background())
		ctx2 := context.Background()

		getKeyReturned := make(chan struct{})

		var cache *PubKeyCache
		cache = NewPubKeyCache(func(ctx context.Context, i string) func(resolve func(jwk.Key), reject func(error)) {
			return func(resolve func(jwk.Key), reject func(error)) {
				require.Eventually(t, func() bool {
					cache.lock.Lock()
					defer cache.lock.Unlock()
					return cache.pubKeys["key"].ctx.Size() == 2
				}, time.Second*5, time.Millisecond)

				t.Cleanup(func() {
					select {
					case <-getKeyReturned:
					case <-time.After(1 * time.Second):
						assert.Fail(t, "expected GetKey to return from cancelled context in time")
					}
				})

				assert.Equal(t, "key", i)
				cancel1(assert.AnError)
				select {
				case <-ctx.Done():
					assert.Fail(t, "GetKey context should not be cancelled")
				default:
				}
				resolve(testKey)
			}
		})

		go func() {
			result, err := cache.GetKey(ctx2, "key")
			require.NoError(t, err)
			assert.Equal(t, testKey, result)
			close(getKeyReturned)
		}()

		result, err := cache.GetKey(ctx1, "key")
		assert.Equal(t, context.Canceled, err)
		assert.Nil(t, result)
	})

	t.Run("if all callers give cancelled contexts, the underlying context should also be cancelled", func(t *testing.T) {
		t.Parallel()
		ctx1, cancel1 := context.WithCancelCause(context.Background())
		ctx2, cancel2 := context.WithCancelCause(context.Background())

		getKeyReturned := make(chan struct{})

		var cache *PubKeyCache
		cache = NewPubKeyCache(func(ctx context.Context, i string) func(resolve func(jwk.Key), reject func(error)) {
			return func(resolve func(jwk.Key), reject func(error)) {
				require.Eventually(t, func() bool {
					cache.lock.Lock()
					defer cache.lock.Unlock()
					pk, ok := cache.pubKeys["key"]
					return ok && pk.ctx.Size() == 2
				}, time.Second*5, time.Millisecond)

				select {
				case <-ctx.Done():
				case <-time.After(1 * time.Second):
					assert.Fail(t, "expected GetKey to get cancelled context in time")
				}

				t.Cleanup(func() {
					select {
					case <-getKeyReturned:
					case <-time.After(1 * time.Second):
						assert.Fail(t, "expected GetKey to return from cancelled context in time")
					}
				})

				reject(errors.New("error which is not surfaced"))
			}
		})

		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			result, err := cache.GetKey(ctx1, "key")
			assert.Equal(t, context.Canceled, err)
			assert.Nil(t, result)
		}()

		go func() {
			defer wg.Done()
			result, err := cache.GetKey(ctx2, "key")
			assert.Equal(t, context.Canceled, err)
			assert.Nil(t, result)
		}()

		cancel1(assert.AnError)
		cancel2(assert.AnError)
		wg.Wait()
		close(getKeyReturned)
	})

	t.Run("if first caller cancels their context, other callers should still await", func(t *testing.T) {
		t.Parallel()

		var cache *PubKeyCache
		assertSize := func(size int) {
			require.Eventually(t, func() bool {
				cache.lock.Lock()
				defer cache.lock.Unlock()
				pk, ok := cache.pubKeys["key"]
				return ok && pk.ctx.Size() == size
			}, time.Second*5, time.Millisecond)
		}

		ctx, cancel := context.WithCancelCause(context.Background())

		getKeyReturned := make(chan struct{})

		cache = NewPubKeyCache(func(ctx context.Context, i string) func(resolve func(jwk.Key), reject func(error)) {
			return func(resolve func(jwk.Key), reject func(error)) {
				assertSize(3)

				// cancel the first caller
				cancel(assert.AnError)

				t.Cleanup(func() {
					select {
					case <-getKeyReturned:
					case <-time.After(1 * time.Second):
						assert.Fail(t, "expected GetKey to return from cancelled context in time")
					}
				})

				resolve(testKey)
			}
		})

		var wg sync.WaitGroup
		wg.Add(3)
		go func() {
			defer wg.Done()
			result, err := cache.GetKey(ctx, "key")
			assert.Equal(t, context.Canceled, err)
			assert.Nil(t, result)
		}()

		go func() {
			defer wg.Done()
			assertSize(1)
			result, err := cache.GetKey(context.Background(), "key")
			require.NoError(t, err)
			assert.Equal(t, testKey, result)
		}()
		go func() {
			defer wg.Done()
			assertSize(2)
			result, err := cache.GetKey(context.Background(), "key")
			require.NoError(t, err)
			assert.Equal(t, testKey, result)
		}()

		wg.Wait()
		close(getKeyReturned)
	})
}
