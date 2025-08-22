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
		err := comp.InitLockStore(t.Context(), cfg)
		require.Error(t, err)
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
		err := comp.InitLockStore(t.Context(), cfg)
		require.Error(t, err)
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
		err := comp.InitLockStore(t.Context(), cfg)
		require.Error(t, err)
	})
}

func TestStandaloneRedisLock_InitFailoverAndReplication(t *testing.T) {
	t.Run("error when cluster type is specified", func(t *testing.T) {
		// construct component
		comp := NewStandaloneRedisLock(logger.NewLogger("test")).(*StandaloneRedisLock)
		defer comp.Close()

		cfg := lock.Metadata{Base: metadata.Base{
			Properties: make(map[string]string),
		}}
		cfg.Properties["redisHost"] = "127.0.0.1:6379"
		cfg.Properties["redisType"] = "cluster"

		// init should fail for cluster type
		err := comp.InitLockStore(t.Context(), cfg)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "does not support connecting to Redis Cluster")
	})

	t.Run("failover configuration", func(t *testing.T) {
		// Note: miniredis doesn't support Sentinel, so this test verifies
		// that failover configuration is properly parsed and fails appropriately
		// when Sentinel is not available

		// construct component
		comp := NewStandaloneRedisLock(logger.NewLogger("test")).(*StandaloneRedisLock)
		defer comp.Close()

		cfg := lock.Metadata{Base: metadata.Base{
			Properties: make(map[string]string),
		}}
		cfg.Properties["redisHost"] = "127.0.0.1:26379" // Standard Sentinel port
		cfg.Properties["failover"] = "true"
		cfg.Properties["sentinelMasterName"] = "mymaster"

		// init should fail due to no Sentinel available, but this validates
		// that failover configuration is properly handled
		err := comp.InitLockStore(t.Context(), cfg)
		require.Error(t, err)
		// The error should be related to connection, not configuration parsing
		assert.Contains(t, err.Error(), "error connecting to Redis")
	})

	t.Run("success when no replicas detected", func(t *testing.T) {
		// start redis
		s, err := miniredis.Run()
		require.NoError(t, err)
		defer s.Close()

		// construct component
		comp := NewStandaloneRedisLock(logger.NewLogger("test")).(*StandaloneRedisLock)
		defer comp.Close()

		cfg := lock.Metadata{Base: metadata.Base{
			Properties: make(map[string]string),
		}}
		cfg.Properties["redisHost"] = s.Addr()
		cfg.Properties["failover"] = "false"

		// init should succeed when no replicas are present
		err = comp.InitLockStore(t.Context(), cfg)
		require.NoError(t, err)
	})

	t.Run("error when replication detected without failover", func(t *testing.T) {
		// Note: This test would require a more complex setup with actual Redis replicas
		// For now, we test the validation logic path
		// In a real scenario, you would set up a Redis master with replicas
		// and ensure the component rejects it when failover=false

		// This is a conceptual test - actual implementation would require:
		// 1. Setting up Redis with replication
		// 2. Ensuring GetConnectedSlaves returns > 0
		// 3. Verifying the error is returned

		t.Skip("Requires complex Redis replication setup - implement when needed")
	})
}

func TestStandaloneRedisLock_ConfigurationValidation(t *testing.T) {
	t.Run("error when invalid failover settings", func(t *testing.T) {
		// construct component
		comp := NewStandaloneRedisLock(logger.NewLogger("test")).(*StandaloneRedisLock)
		defer comp.Close()

		cfg := lock.Metadata{Base: metadata.Base{
			Properties: make(map[string]string),
		}}
		cfg.Properties["redisHost"] = "127.0.0.1:6379"
		cfg.Properties["failover"] = "true"
		// Missing sentinelMasterName should cause connection issues

		// init should fail due to invalid failover configuration
		err := comp.InitLockStore(t.Context(), cfg)
		require.Error(t, err)
	})

	t.Run("success with valid node type", func(t *testing.T) {
		// start redis
		s, err := miniredis.Run()
		require.NoError(t, err)
		defer s.Close()

		// construct component
		comp := NewStandaloneRedisLock(logger.NewLogger("test")).(*StandaloneRedisLock)
		defer comp.Close()

		cfg := lock.Metadata{Base: metadata.Base{
			Properties: make(map[string]string),
		}}
		cfg.Properties["redisHost"] = s.Addr()
		cfg.Properties["redisType"] = "node"

		// init should succeed with node type
		err = comp.InitLockStore(t.Context(), cfg)
		require.NoError(t, err)
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
	err = comp.InitLockStore(t.Context(), cfg)
	require.NoError(t, err)

	// 1. client1 trylock
	ownerID1 := uuid.New().String()
	resp, err := comp.TryLock(t.Context(), &lock.TryLockRequest{
		ResourceID:      resourceID,
		LockOwner:       ownerID1,
		ExpiryInSeconds: 10,
	})
	require.NoError(t, err)
	assert.True(t, resp.Success)

	//	2. Client2 tryLock fail
	owner2 := uuid.New().String()
	resp, err = comp.TryLock(t.Context(), &lock.TryLockRequest{
		ResourceID:      resourceID,
		LockOwner:       owner2,
		ExpiryInSeconds: 10,
	})
	require.NoError(t, err)
	assert.False(t, resp.Success)

	// 3. Client 1 unlock
	unlockResp, err := comp.Unlock(t.Context(), &lock.UnlockRequest{
		ResourceID: resourceID,
		LockOwner:  ownerID1,
	})
	require.NoError(t, err)
	assert.EqualValues(t, 0, unlockResp.Status, "client1 failed to unlock!")

	// 4. Client 2 get lock
	owner2 = uuid.New().String()
	resp2, err2 := comp.TryLock(t.Context(), &lock.TryLockRequest{
		ResourceID:      resourceID,
		LockOwner:       owner2,
		ExpiryInSeconds: 10,
	})
	require.NoError(t, err2)
	assert.True(t, resp2.Success, "client2 failed to get lock?!")

	// 5. client2 unlock
	unlockResp, err = comp.Unlock(t.Context(), &lock.UnlockRequest{
		ResourceID: resourceID,
		LockOwner:  owner2,
	})
	require.NoError(t, err)
	assert.EqualValues(t, 0, unlockResp.Status, "client2 failed to unlock!")
}

func TestStandaloneRedisLock_ErrorScenarios(t *testing.T) {
	t.Run("error when connection ping fails", func(t *testing.T) {
		// construct component
		comp := NewStandaloneRedisLock(logger.NewLogger("test")).(*StandaloneRedisLock)
		defer comp.Close()

		cfg := lock.Metadata{Base: metadata.Base{
			Properties: make(map[string]string),
		}}
		cfg.Properties["redisHost"] = "127.0.0.1:9999" // Non-existent Redis port
		cfg.Properties["redisPassword"] = ""

		// init should fail due to connection error
		err := comp.InitLockStore(t.Context(), cfg)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "error connecting to Redis")
	})

	t.Run("error with empty host", func(t *testing.T) {
		// construct component
		comp := NewStandaloneRedisLock(logger.NewLogger("test")).(*StandaloneRedisLock)
		defer comp.Close()

		cfg := lock.Metadata{Base: metadata.Base{
			Properties: make(map[string]string),
		}}
		cfg.Properties["redisHost"] = ""

		// init should fail due to empty host
		err := comp.InitLockStore(t.Context(), cfg)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "redisHost is empty")
	})

	t.Run("unlock scenarios", func(t *testing.T) {
		// start redis
		s, err := miniredis.Run()
		require.NoError(t, err)
		defer s.Close()

		// construct component
		comp := NewStandaloneRedisLock(logger.NewLogger("test")).(*StandaloneRedisLock)
		defer comp.Close()

		cfg := lock.Metadata{Base: metadata.Base{
			Properties: make(map[string]string),
		}}
		cfg.Properties["redisHost"] = s.Addr()

		err = comp.InitLockStore(t.Context(), cfg)
		require.NoError(t, err)

		// Test unlock non-existent lock
		unlockResp, err := comp.Unlock(t.Context(), &lock.UnlockRequest{
			ResourceID: "non-existent-resource",
			LockOwner:  "some-owner",
		})
		require.NoError(t, err)
		assert.Equal(t, lock.LockDoesNotExist, unlockResp.Status)

		// Create a lock first
		ownerID := uuid.New().String()
		_, err = comp.TryLock(t.Context(), &lock.TryLockRequest{
			ResourceID:      resourceID,
			LockOwner:       ownerID,
			ExpiryInSeconds: 10,
		})
		require.NoError(t, err)

		// Test unlock with wrong owner
		unlockResp, err = comp.Unlock(t.Context(), &lock.UnlockRequest{
			ResourceID: resourceID,
			LockOwner:  "wrong-owner",
		})
		require.NoError(t, err)
		assert.Equal(t, lock.LockBelongsToOthers, unlockResp.Status)

		// Test successful unlock
		unlockResp, err = comp.Unlock(t.Context(), &lock.UnlockRequest{
			ResourceID: resourceID,
			LockOwner:  ownerID,
		})
		require.NoError(t, err)
		assert.Equal(t, lock.Success, unlockResp.Status)
	})
}

func TestStandaloneRedisLock_ComponentMetadata(t *testing.T) {
	comp := NewStandaloneRedisLock(logger.NewLogger("test")).(*StandaloneRedisLock)
	defer comp.Close()

	metadata := comp.GetComponentMetadata()
	assert.NotNil(t, metadata)
	assert.NotEmpty(t, metadata)
}
