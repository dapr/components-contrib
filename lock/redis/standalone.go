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
	"time"

	rediscomponent "github.com/dapr/components-contrib/common/component/redis"
	"github.com/dapr/components-contrib/lock"
	contribMetadata "github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
)

const unlockScript = `local v = redis.call("get",KEYS[1]); if v==false then return -1 end; if v~=ARGV[1] then return -2 else return redis.call("del",KEYS[1]) end`

// Standalone Redis lock store.
// Any fail-over related features are not supported, such as Sentinel and Redis Cluster.
type StandaloneRedisLock struct {
	client         rediscomponent.RedisClient
	clientSettings *rediscomponent.Settings

	logger logger.Logger
}

// NewStandaloneRedisLock returns a new standalone redis lock.
// Do not use this lock with a redis cluster, which might lead to unexpected lock loss.
func NewStandaloneRedisLock(logger logger.Logger) lock.Store {
	s := &StandaloneRedisLock{
		logger: logger,
	}

	return s
}

// Init StandaloneRedisLock.
func (r *StandaloneRedisLock) InitLockStore(ctx context.Context, metadata lock.Metadata) (err error) {
	// Create the client
	r.client, r.clientSettings, err = rediscomponent.ParseClientFromProperties(metadata.Properties, contribMetadata.LockStoreType, ctx, &r.logger)
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

// TryLock tries to acquire a lock.
// If the lock cannot be acquired, it returns immediately.
func (r *StandaloneRedisLock) TryLock(ctx context.Context, req *lock.TryLockRequest) (*lock.TryLockResponse, error) {
	// Set a key if doesn't exist with an expiration time
	nxval, err := r.client.SetNX(ctx, req.ResourceID, req.LockOwner, time.Second*time.Duration(req.ExpiryInSeconds))
	if nxval == nil {
		return &lock.TryLockResponse{}, errors.New("setNX returned a nil response")
	}

	if err != nil {
		return &lock.TryLockResponse{}, err
	}

	return &lock.TryLockResponse{
		Success: *nxval,
	}, nil
}

// Unlock tries to release a lock if the lock is still valid.
func (r *StandaloneRedisLock) Unlock(ctx context.Context, req *lock.UnlockRequest) (*lock.UnlockResponse, error) {
	// Delegate to client.eval lua script
	evalInt, parseErr, err := r.client.EvalInt(ctx, unlockScript, []string{req.ResourceID}, req.LockOwner)
	if evalInt == nil {
		res := &lock.UnlockResponse{
			Status: lock.InternalError,
		}
		return res, errors.New("eval unlock script returned a nil response")
	}

	// Parse result
	if parseErr != nil {
		return &lock.UnlockResponse{
			Status: lock.InternalError,
		}, err
	}
	var status lock.Status
	switch {
	case *evalInt >= 0:
		status = lock.Success
	case *evalInt == -1:
		status = lock.LockDoesNotExist
	case *evalInt == -2:
		status = lock.LockBelongsToOthers
	default:
		status = lock.InternalError
	}

	return &lock.UnlockResponse{
		Status: status,
	}, nil
}

// Close shuts down the client's redis connections.
func (r *StandaloneRedisLock) Close() error {
	if r.client != nil {
		err := r.client.Close()
		r.client = nil
		return err
	}
	return nil
}

// GetComponentMetadata returns the metadata of the component.
func (r *StandaloneRedisLock) GetComponentMetadata() (metadataInfo contribMetadata.MetadataMap) {
	metadataStruct := rediscomponent.Settings{}
	contribMetadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, contribMetadata.LockStoreType)
	return
}
