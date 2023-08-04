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
	"errors"
	"fmt"
	"reflect"
	"sync/atomic"
	"time"

	"github.com/cenkalti/backoff/v4"
	rediscomponent "github.com/dapr/components-contrib/internal/component/redis"
	"github.com/dapr/components-contrib/lock"
	contribMetadata "github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
)

const (
	unlockScript    = `local v = redis.call("get",KEYS[1]); if v==false then return -1 end; if v~=ARGV[1] then return -2 else return redis.call("del",KEYS[1]) end`
	renewLockScript = `local v = redis.call("get",KEYS[1]); if v==false then return -1 end; if v~=ARGV[1] then return -2 else return redis.call("expire",KEYS[1],ARGV[2]) end`
)

var ErrComponentClosed = errors.New("component is closed")

// Standalone Redis lock store.
// Any fail-over related features are not supported, such as Sentinel and Redis Cluster.
type StandaloneRedisLock struct {
	client         rediscomponent.RedisClient
	clientSettings *rediscomponent.Settings

	closed    atomic.Bool
	runnincCh chan struct{}
	logger    logger.Logger
}

// NewStandaloneRedisLock returns a new standalone redis lock.
// Do not use this lock with a Redis cluster, which might lead to unexpected lock loss.
func NewStandaloneRedisLock(logger logger.Logger) lock.Store {
	s := &StandaloneRedisLock{
		logger:    logger,
		runnincCh: make(chan struct{}),
	}

	return s
}

// Init the lock store.
func (r *StandaloneRedisLock) Init(ctx context.Context, metadata lock.Metadata) (err error) {
	// Create the client
	r.client, r.clientSettings, err = rediscomponent.ParseClientFromProperties(metadata.Properties, contribMetadata.LockStoreType)
	if err != nil {
		return err
	}

	// Ensure we have a host
	if r.clientSettings.Host == "" {
		return errors.New("metadata property redisHost is empty")
	}

	// We do not support failover or having replicas
	if r.clientSettings.Failover {
		return errors.New("this component does not support connecting to Redis with failover")
	}

	// Ping Redis to ensure the connection is uo
	if _, err = r.client.PingResult(ctx); err != nil {
		return fmt.Errorf("error connecting to Redis: %v", err)
	}

	// Ensure there are no replicas
	// Pass the validation if error occurs, since some Redis versions such as miniredis do not recognize the `INFO` command.
	replicas, err := rediscomponent.GetConnectedSlaves(ctx, r.client)
	if err == nil && replicas > 0 {
		return errors.New("replication is not supported")
	}
	return nil
}

// Features returns the list of supported features.
func (r *StandaloneRedisLock) Features() []lock.Feature {
	return nil
}

// Lock tries to acquire a lock.
// If the lock is owned by someone else, this method blocks until the lock can be acquired or the context is canceled.
func (r *StandaloneRedisLock) Lock(ctx context.Context, req *lock.LockRequest) (res *lock.LockResponse, err error) {
	if r.closed.Load() {
		return nil, ErrComponentClosed
	}

	// We try to acquire a lock through periodic polling
	// A potentially more efficient way would be to use keyspace notifications to subscribe to changes in the key we subscribe to
	// However, keyspace notifications:
	// 1. Are not enabled by default in Redis, and require an explicit configuration change, which adds quite a bit of complexity for the user: https://redis.io/docs/manual/keyspace-notifications/
	// 2. When a connection to Redis calls SUBSCRIBE to watch for notifications, it cannot be used for anything else (unless we switch the protocol to RESP3, which must be explicitly chosen and only works with Redis 6+: https://redis.io/commands/hello/)
	// So, periodic polling it is

	// We use an exponential backoff here because it supports a randomization factor
	bo := backoff.NewExponentialBackOff()
	bo.MaxElapsedTime = 0
	bo.InitialInterval = 50 * time.Millisecond
	bo.MaxInterval = 500 * time.Millisecond
	bo.RandomizationFactor = 0.5

	// Repat until we get the lock, or context is canceled
	for {
		// Try to acquire the lock
		res, err = r.TryLock(ctx, req)
		if err != nil {
			// If we got an error, return right away
			return nil, err
		}

		// Let's see if we got the lock
		if res.Success {
			return res, nil
		}

		// Sleep till the next tick and try again
		// Stop when context is done or component is closed
		t := time.NewTimer(bo.NextBackOff())
		select {
		case <-t.C:
			// Nop, retry
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-r.runnincCh:
			return nil, ErrComponentClosed
		}
	}
}

// TryLock tries to acquire a lock.
// If the lock cannot be acquired, it returns immediately.
func (r *StandaloneRedisLock) TryLock(ctx context.Context, req *lock.LockRequest) (*lock.LockResponse, error) {
	if r.closed.Load() {
		return nil, ErrComponentClosed
	}

	// Set a key if doesn't exist, with an expiration time
	nxval, err := r.client.SetNX(ctx, req.ResourceID, req.LockOwner, time.Second*time.Duration(req.ExpiryInSeconds))
	if nxval == nil {
		return &lock.LockResponse{}, fmt.Errorf("setNX returned a nil response")
	}
	if err != nil {
		return &lock.LockResponse{}, err
	}

	return &lock.LockResponse{
		Success: *nxval,
	}, nil
}

// RenewLock attempts to renew a lock if the lock is still valid.
func (r *StandaloneRedisLock) RenewLock(ctx context.Context, req *lock.RenewLockRequest) (*lock.RenewLockResponse, error) {
	if r.closed.Load() {
		return nil, ErrComponentClosed
	}

	// Delegate to client.eval lua script
	evalInt, parseErr, err := r.client.EvalInt(ctx, renewLockScript, []string{req.ResourceID}, req.LockOwner, req.ExpiryInSeconds)
	if evalInt == nil {
		res := &lock.RenewLockResponse{
			Status: lock.LockStatusInternalError,
		}
		return res, errors.New("eval renew lock script returned a nil response")
	}

	// Parse result
	if parseErr != nil {
		return &lock.RenewLockResponse{
			Status: lock.LockStatusInternalError,
		}, err
	}
	var status lock.LockStatus
	switch {
	case *evalInt >= 0:
		status = lock.LockStatusSuccess
	case *evalInt == -1:
		status = lock.LockStatusNotExist
	case *evalInt == -2:
		status = lock.LockStatusOwnerMismatch
	default:
		status = lock.LockStatusInternalError
	}

	return &lock.RenewLockResponse{
		Status: status,
	}, nil
}

// Unlock tries to release a lock if the lock is still valid.
func (r *StandaloneRedisLock) Unlock(ctx context.Context, req *lock.UnlockRequest) (*lock.UnlockResponse, error) {
	if r.closed.Load() {
		return nil, ErrComponentClosed
	}

	// Delegate to client.eval lua script
	evalInt, parseErr, err := r.client.EvalInt(ctx, unlockScript, []string{req.ResourceID}, req.LockOwner)
	if evalInt == nil {
		res := &lock.UnlockResponse{
			Status: lock.LockStatusInternalError,
		}
		return res, errors.New("eval unlock script returned a nil response")
	}

	// Parse result
	if parseErr != nil {
		return &lock.UnlockResponse{
			Status: lock.LockStatusInternalError,
		}, err
	}
	var status lock.LockStatus
	switch {
	case *evalInt >= 0:
		status = lock.LockStatusSuccess
	case *evalInt == -1:
		status = lock.LockStatusNotExist
	case *evalInt == -2:
		status = lock.LockStatusOwnerMismatch
	default:
		status = lock.LockStatusInternalError
	}

	return &lock.UnlockResponse{
		Status: status,
	}, nil
}

// Close shuts down the client's redis connections.
func (r *StandaloneRedisLock) Close() error {
	if !r.closed.CompareAndSwap(false, true) {
		return nil
	}

	close(r.runnincCh)

	if r.client == nil {
		return nil
	}

	return r.client.Close()
}

// GetComponentMetadata returns the metadata of the component.
func (r *StandaloneRedisLock) GetComponentMetadata() (metadataInfo contribMetadata.MetadataMap) {
	metadataStruct := rediscomponent.Settings{}
	contribMetadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, contribMetadata.LockStoreType)
	return
}
