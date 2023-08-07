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
		err := lockstore.InitLockStore(ctx, lock.Metadata{Base: metadata.Base{
			Properties: props,
		}})
		require.NoError(t, err)
	})

	// Don't run more tests if init failed
	if t.Failed() {
		t.Fatal("Init failed, stopping further tests")
	}

	const lockOwner = "conftest"
	lockKey1 := key + "-1"
	lockKey2 := key + "-2"

	var expirationCh *time.Timer

	t.Run("TryLock", func(t *testing.T) {
		// Acquire a lock
		t.Run("acquire lock1", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			res, err := lockstore.TryLock(ctx, &lock.TryLockRequest{
				ResourceID:      lockKey1,
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
			res, err := lockstore.TryLock(ctx, &lock.TryLockRequest{
				ResourceID:      lockKey2,
				LockOwner:       lockOwner,
				ExpiryInSeconds: 3,
			})
			require.NoError(t, err)
			require.NotNil(t, res)
			assert.True(t, res.Success)

			// Set expirationCh to when lock2 expires
			expirationCh = time.NewTimer(3 * time.Second)
		})

		// Acquiring the same lock again should fail
		t.Run("fails to acquire existing lock", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			res, err := lockstore.TryLock(ctx, &lock.TryLockRequest{
				ResourceID:      lockKey1,
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
			assert.Equal(t, lock.LockDoesNotExist, res.Status)
		})

		t.Run("fails to unlock with wrong owner", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			res, err := lockstore.Unlock(ctx, &lock.UnlockRequest{
				ResourceID: lockKey1,
				LockOwner:  "nonowner",
			})
			require.NoError(t, err)
			require.NotNil(t, res)
			assert.Equal(t, lock.LockBelongsToOthers, res.Status)
		})

		t.Run("unlocks successfully", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			res, err := lockstore.Unlock(ctx, &lock.UnlockRequest{
				ResourceID: lockKey1,
				LockOwner:  lockOwner,
			})
			require.NoError(t, err)
			require.NotNil(t, res)
			assert.Equal(t, lock.Success, res.Status)
		})
	})

	t.Run("lock expires", func(t *testing.T) {
		// Wait until the lock is supposed to expire
		<-expirationCh.C

		// Assert that the lock doesn't exist anymore - we should be able to re-acquire it
		assert.Eventually(t, func() bool {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			res, err := lockstore.TryLock(ctx, &lock.TryLockRequest{
				ResourceID:      lockKey2,
				LockOwner:       lockOwner,
				ExpiryInSeconds: 3,
			})
			return err == nil && res != nil && res.Success
		}, 5*time.Second, 100*time.Millisecond, "Lock 2 was not released in time after its scheduled expiration")
	})
}
