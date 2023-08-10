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

package redis

import (
	"context"
	"testing"

	miniredis "github.com/alicebob/miniredis/v2"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/lock"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
)

const resourceID = "resource_xxx"

func TestStandaloneRedisLock_InitError(t *testing.T) {
	t.Run("error when connection fail", func(t *testing.T) {
		// construct component
		comp := NewStandaloneRedisLock(logger.NewLogger("test")).(*StandaloneRedisLock)
		defer comp.Close()

		cfg := lock.Metadata{Base: metadata.Base{
			Properties: make(map[string]string),
		}}
		cfg.Properties["redisHost"] = "127.0.0.1"
		cfg.Properties["redisPassword"] = ""

		// init
		err := comp.InitLockStore(context.Background(), cfg)
		assert.Error(t, err)
	})

	t.Run("error when no host", func(t *testing.T) {
		// construct component
		comp := NewStandaloneRedisLock(logger.NewLogger("test")).(*StandaloneRedisLock)
		defer comp.Close()

		cfg := lock.Metadata{Base: metadata.Base{
			Properties: make(map[string]string),
		}}
		cfg.Properties["redisHost"] = ""
		cfg.Properties["redisPassword"] = ""

		// init
		err := comp.InitLockStore(context.Background(), cfg)
		assert.Error(t, err)
	})

	t.Run("error when wrong MaxRetries", func(t *testing.T) {
		// construct component
		comp := NewStandaloneRedisLock(logger.NewLogger("test")).(*StandaloneRedisLock)
		defer comp.Close()

		cfg := lock.Metadata{Base: metadata.Base{
			Properties: make(map[string]string),
		}}
		cfg.Properties["redisHost"] = "127.0.0.1"
		cfg.Properties["redisPassword"] = ""
		cfg.Properties["maxRetries"] = "1 "

		// init
		err := comp.InitLockStore(context.Background(), cfg)
		assert.Error(t, err)
	})
}

func TestStandaloneRedisLock_TryLock(t *testing.T) {
	// 0. prepare
	// start redis
	s, err := miniredis.Run()
	require.NoError(t, err)
	defer s.Close()

	// Construct component
	comp := NewStandaloneRedisLock(logger.NewLogger("test")).(*StandaloneRedisLock)
	defer comp.Close()

	cfg := lock.Metadata{Base: metadata.Base{
		Properties: make(map[string]string),
	}}
	cfg.Properties["redisHost"] = s.Addr()
	cfg.Properties["redisPassword"] = ""

	// Init
	err = comp.InitLockStore(context.Background(), cfg)
	require.NoError(t, err)

	// 1. client1 trylock
	ownerID1 := uuid.New().String()
	resp, err := comp.TryLock(context.Background(), &lock.TryLockRequest{
		ResourceID:      resourceID,
		LockOwner:       ownerID1,
		ExpiryInSeconds: 10,
	})
	require.NoError(t, err)
	assert.True(t, resp.Success)

	//	2. Client2 tryLock fail
	owner2 := uuid.New().String()
	resp, err = comp.TryLock(context.Background(), &lock.TryLockRequest{
		ResourceID:      resourceID,
		LockOwner:       owner2,
		ExpiryInSeconds: 10,
	})
	require.NoError(t, err)
	assert.False(t, resp.Success)

	// 3. Client 1 unlock
	unlockResp, err := comp.Unlock(context.Background(), &lock.UnlockRequest{
		ResourceID: resourceID,
		LockOwner:  ownerID1,
	})
	require.NoError(t, err)
	assert.True(t, unlockResp.Status == 0, "client1 failed to unlock!")

	// 4. Client 2 get lock
	owner2 = uuid.New().String()
	resp2, err2 := comp.TryLock(context.Background(), &lock.TryLockRequest{
		ResourceID:      resourceID,
		LockOwner:       owner2,
		ExpiryInSeconds: 10,
	})
	require.NoError(t, err2)
	assert.True(t, resp2.Success, "client2 failed to get lock?!")

	// 5. client2 unlock
	unlockResp, err = comp.Unlock(context.Background(), &lock.UnlockRequest{
		ResourceID: resourceID,
		LockOwner:  owner2,
	})
	require.NoError(t, err)
	assert.True(t, unlockResp.Status == 0, "client2 failed to unlock!")
}
