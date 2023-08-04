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

package state

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/lock"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/tests/conformance/utils"
	"github.com/dapr/kit/config"
)

type TestConfig struct {
	utils.CommonConfig
}

func NewTestConfig(component string, operations []string, configMap map[string]interface{}) (TestConfig, error) {
	testConfig := TestConfig{
		CommonConfig: utils.CommonConfig{
			ComponentType: "lock",
			ComponentName: component,
			Operations:    utils.NewStringSet(operations...),
		},
	}

	err := config.Decode(configMap, &testConfig)
	if err != nil {
		return testConfig, err
	}

	return testConfig, nil
}

// ConformanceTests runs conf tests for lock stores.
func ConformanceTests(t *testing.T, props map[string]string, lockstore lock.Store, config TestConfig) {
	// Test vars
	key := strings.ReplaceAll(uuid.New().String(), "-", "")
	t.Logf("Base key for test: %s", key)

	t.Run("init", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		err := lockstore.Init(ctx, lock.Metadata{Base: metadata.Base{
			Properties: props,
		}})
		require.NoError(t, err)
	})

	// Don't run more tests if init failed
	if t.Failed() {
		t.Fatal("Init failed, stopping further tests")
	}

	const lockOwner = "conftest"
	var lockKeys = [2]string{
		key + "-1",
		key + "-2",
	}

	var expirationChs [2]*time.Timer

	t.Run("TryLock", func(t *testing.T) {
		// Acquire a lock
		t.Run("acquire lock1", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			res, err := lockstore.TryLock(ctx, &lock.LockRequest{
				ResourceID:      lockKeys[0],
				LockOwner:       lockOwner,
				ExpiryInSeconds: 15,
			})
			require.NoError(t, err)
			require.NotNil(t, res)
			assert.True(t, res.Success)
		})

		// Acquire a second lock (with a shorter expiration)
		t.Run("acquire lock2", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			res, err := lockstore.TryLock(ctx, &lock.LockRequest{
				ResourceID:      lockKeys[1],
				LockOwner:       lockOwner,
				ExpiryInSeconds: 8,
			})
			require.NoError(t, err)
			require.NotNil(t, res)
			assert.True(t, res.Success)

			// Set expirationChs[0] to when lock2 expires
			expirationChs[0] = time.NewTimer(3 * time.Second)
		})

		// Acquiring the same lock again should fail
		t.Run("fails to acquire existing lock", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			res, err := lockstore.TryLock(ctx, &lock.LockRequest{
				ResourceID:      lockKeys[0],
				LockOwner:       lockOwner,
				ExpiryInSeconds: 15,
			})
			require.NoError(t, err)
			require.NotNil(t, res)
			assert.False(t, res.Success)
		})
	})

	t.Run("Unlock", func(t *testing.T) {
		t.Run("fails to unlock with nonexistent resource ID", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			res, err := lockstore.Unlock(ctx, &lock.UnlockRequest{
				ResourceID: "nonexistent",
				LockOwner:  lockOwner,
			})
			require.NoError(t, err)
			require.NotNil(t, res)
			assert.Equal(t, lock.LockStatusNotExist, res.Status)
		})

		t.Run("fails to unlock with wrong owner", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			res, err := lockstore.Unlock(ctx, &lock.UnlockRequest{
				ResourceID: lockKeys[0],
				LockOwner:  "nonowner",
			})
			require.NoError(t, err)
			require.NotNil(t, res)
			assert.Equal(t, lock.LockStatusOwnerMismatch, res.Status)
		})

		t.Run("unlocks successfully", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			res, err := lockstore.Unlock(ctx, &lock.UnlockRequest{
				ResourceID: lockKeys[0],
				LockOwner:  lockOwner,
			})
			require.NoError(t, err)
			require.NotNil(t, res)
			assert.Equal(t, lock.LockStatusSuccess, res.Status)
		})
	})

	t.Run("Renew lock", func(t *testing.T) {
		// Sleep for 1s to allow some time to advance
		time.Sleep(1 * time.Second)

		t.Run("renews locks successfully", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			res, err := lockstore.RenewLock(ctx, &lock.RenewLockRequest{
				ResourceID:      lockKeys[1],
				LockOwner:       lockOwner,
				ExpiryInSeconds: 5,
			})
			require.NoError(t, err)
			require.NotNil(t, res)
			assert.Equal(t, lock.LockStatusSuccess, res.Status)

			// Set expirationChs[1] to when lock2 expires (after being updated)
			expirationChs[1] = time.NewTimer(3 * time.Second)
		})

		t.Run("fails to renew locks with nonexistent resource ID", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			res, err := lockstore.RenewLock(ctx, &lock.RenewLockRequest{
				ResourceID:      "nonexistent",
				LockOwner:       lockOwner,
				ExpiryInSeconds: 15,
			})
			require.NoError(t, err)
			require.NotNil(t, res)
			assert.Equal(t, lock.LockStatusNotExist, res.Status)
		})

		t.Run("fails to renew locks with wrong owner", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			res, err := lockstore.RenewLock(ctx, &lock.RenewLockRequest{
				ResourceID:      lockKeys[1],
				LockOwner:       "nonowner",
				ExpiryInSeconds: 15,
			})
			require.NoError(t, err)
			require.NotNil(t, res)
			assert.Equal(t, lock.LockStatusOwnerMismatch, res.Status)
		})
	})

	t.Run("Lock expires", func(t *testing.T) {
		// Wait until the lock was originally supposed to expire at
		<-expirationChs[0].C

		assertLockReleasedFn := func(resourceID string) func() bool {
			return func() bool {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				res, err := lockstore.TryLock(ctx, &lock.LockRequest{
					ResourceID:      resourceID,
					LockOwner:       lockOwner,
					ExpiryInSeconds: 3,
				})
				return err == nil && res != nil && res.Success
			}
		}

		// Assert that the lock is still active - we can't re-acquire it
		assert.Never(t, assertLockReleasedFn(lockKeys[1]), time.Second, 100*time.Millisecond, "Lock 2 was released before its expected expiration")

		// Wait for when the lock is now supposed to expire at
		<-expirationChs[1].C

		// Assert that the lock doesn't exist anymore - we should be able to re-acquire it
		assert.Eventually(t, assertLockReleasedFn(lockKeys[1]), 5*time.Second, 100*time.Millisecond, "Lock 2 was not released in time after its expected expiration")
	})

	t.Run("Lock", func(t *testing.T) {
		var exp time.Time
		t.Run("acquire available lock right away", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			res, err := lockstore.Lock(ctx, &lock.LockRequest{
				ResourceID:      lockKeys[0],
				LockOwner:       lockOwner,
				ExpiryInSeconds: 8,
			})
			require.NoError(t, err)
			require.NotNil(t, res)
			assert.True(t, res.Success)

			// Subtract 500ms to give some buffer
			exp = time.Now().Add(8*time.Second - 500*time.Millisecond)
		})

		t.Run("times out trying to acquire lock that already exists", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			res, err := lockstore.Lock(ctx, &lock.LockRequest{
				ResourceID:      lockKeys[0],
				LockOwner:       lockOwner,
				ExpiryInSeconds: 10,
			})
			require.Error(t, err)
			require.ErrorIs(t, err, context.DeadlineExceeded)
			require.Nil(t, res)
		})

		t.Run("acquires the lock after it expires", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			res, err := lockstore.Lock(ctx, &lock.LockRequest{
				ResourceID:      lockKeys[0],
				LockOwner:       lockOwner,
				ExpiryInSeconds: 10,
			})
			require.NoError(t, err)
			require.NotNil(t, res)
			assert.True(t, res.Success)

			assert.Greater(t, time.Now().UnixMilli(), exp.UnixMilli())
		})
	})
}
