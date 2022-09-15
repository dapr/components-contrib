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
	"fmt"
	"strconv"
	"strings"
	"time"

	v8 "github.com/go-redis/redis/v8"
	v9 "github.com/go-redis/redis/v9"

	rediscomponent "github.com/dapr/components-contrib/internal/component/redis"
	"github.com/dapr/components-contrib/lock"
	"github.com/dapr/kit/logger"
)

const (
	unlockScript             = "local v = redis.call(\"get\",KEYS[1]); if v==false then return -1 end; if v~=ARGV[1] then return -2 else return redis.call(\"del\",KEYS[1]) end"
	connectedSlavesReplicas  = "connected_slaves:"
	infoReplicationDelimiter = "\r\n"
)

// Standalone Redis lock store.Any fail-over related features are not supported,such as Sentinel and Redis Cluster.
type StandaloneRedisLock struct {
	clientv8       v8.UniversalClient
	clientv9       v9.UniversalClient
	clientSettings *rediscomponent.Settings
	metadata       rediscomponent.Metadata
	legacyRedis    bool

	logger logger.Logger

	ctx    context.Context
	cancel context.CancelFunc
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
func (r *StandaloneRedisLock) InitLockStore(metadata lock.Metadata) error {
	// 1. parse config
	m, err := rediscomponent.ParseRedisMetadata(metadata.Properties)
	if err != nil {
		return err
	}
	r.metadata = m
	if rediscomponent.IsLegacyRedisVersion(metadata.Properties) {
		r.legacyRedis = true
	} else {
		r.legacyRedis = false
		r.logger.Warnf("Redis version 7 and above uses a Beta SDK at this time. Please use caution.")
	}
	// must have `redisHost`
	if metadata.Properties["redisHost"] == "" {
		return fmt.Errorf("[standaloneRedisLock]: InitLockStore error. redisHost is empty")
	}
	// no failover
	if needFailover(metadata.Properties) {
		return fmt.Errorf("[standaloneRedisLock]: InitLockStore error. Failover is not supported")
	}
	// 2. construct client
	defaultSettings := rediscomponent.Settings{RedisMaxRetries: m.MaxRetries, RedisMaxRetryInterval: rediscomponent.Duration(m.MaxRetryBackoff)}
	if r.legacyRedis {
		r.clientv8, r.clientSettings, err = rediscomponent.ParseClientv8FromProperties(metadata.Properties, &defaultSettings)
	} else {
		r.clientv9, r.clientSettings, err = rediscomponent.ParseClientv9FromProperties(metadata.Properties, &defaultSettings)
	}
	if err != nil {
		return err
	}
	r.ctx, r.cancel = context.WithCancel(context.Background())
	// 3. connect to redis
	if r.legacyRedis {
		if _, err = r.clientv8.Ping(r.ctx).Result(); err != nil {
			return fmt.Errorf("[standaloneRedisLock]: error connecting to redis at %s: %s", r.clientSettings.Host, err)
		}
	} else {
		if _, err = r.clientv9.Ping(r.ctx).Result(); err != nil {
			return fmt.Errorf("[standaloneRedisLock]: error connecting to redis at %s: %s", r.clientSettings.Host, err)
		}
	}
	// no replica
	replicas, err := r.getConnectedSlaves()
	// pass the validation if error occurs,
	// since some redis versions such as miniredis do not recognize the `INFO` command.
	if err == nil && replicas > 0 {
		return fmt.Errorf("[standaloneRedisLock]: InitLockStore error. Replication is not supported")
	}
	return nil
}

func needFailover(properties map[string]string) bool {
	if val, ok := properties["failover"]; ok && val != "" {
		parsedVal, err := strconv.ParseBool(val)
		if err != nil {
			return false
		}
		return parsedVal
	}
	return false
}

func (r *StandaloneRedisLock) getConnectedSlaves() (int, error) {
	var res interface{}
	var err error
	if r.legacyRedis {
		res, err = r.clientv8.Do(r.ctx, "INFO", "replication").Result()
	} else {
		res, err = r.clientv9.Do(r.ctx, "INFO", "replication").Result()
	}
	if err != nil {
		return 0, err
	}

	// Response example: https://redis.io/commands/info#return-value
	// # Replication\r\nrole:master\r\nconnected_slaves:1\r\n
	s, _ := strconv.Unquote(fmt.Sprintf("%q", res))
	if len(s) == 0 {
		return 0, nil
	}

	return r.parseConnectedSlaves(s), nil
}

func (r *StandaloneRedisLock) parseConnectedSlaves(res string) int {
	infos := strings.Split(res, infoReplicationDelimiter)
	for _, info := range infos {
		if strings.Contains(info, connectedSlavesReplicas) {
			parsedReplicas, _ := strconv.ParseUint(info[len(connectedSlavesReplicas):], 10, 32)

			return int(parsedReplicas)
		}
	}

	return 0
}

// Try to acquire a redis lock.
func (r *StandaloneRedisLock) TryLock(req *lock.TryLockRequest) (*lock.TryLockResponse, error) {
	if r.legacyRedis {
		// 1.Setting redis expiration time
		nx := r.clientv8.SetNX(r.ctx, req.ResourceID, req.LockOwner, time.Second*time.Duration(req.ExpiryInSeconds))
		if nx == nil {
			return &lock.TryLockResponse{}, fmt.Errorf("[standaloneRedisLock]: SetNX returned nil.ResourceID: %s", req.ResourceID)
		}
		// 2. check error
		err := nx.Err()
		if err != nil {
			return &lock.TryLockResponse{}, err
		}

		return &lock.TryLockResponse{
			Success: nx.Val(),
		}, nil
	} else {
		// 1.Setting redis expiration time
		nx := r.clientv9.SetNX(r.ctx, req.ResourceID, req.LockOwner, time.Second*time.Duration(req.ExpiryInSeconds))
		if nx == nil {
			return &lock.TryLockResponse{}, fmt.Errorf("[standaloneRedisLock]: SetNX returned nil.ResourceID: %s", req.ResourceID)
		}
		// 2. check error
		err := nx.Err()
		if err != nil {
			return &lock.TryLockResponse{}, err
		}

		return &lock.TryLockResponse{
			Success: nx.Val(),
		}, nil
	}
}

// Try to release a redis lock.
func (r *StandaloneRedisLock) Unlock(req *lock.UnlockRequest) (*lock.UnlockResponse, error) {
	var i int
	var err error
	var status lock.Status
	if r.legacyRedis {
		// 1. delegate to client.eval lua script
		eval := r.clientv8.Eval(r.ctx, unlockScript, []string{req.ResourceID}, req.LockOwner)
		// 2. check error
		if eval == nil {
			return newInternalErrorUnlockResponse(), fmt.Errorf("[standaloneRedisLock]: Eval unlock script returned nil.ResourceID: %s", req.ResourceID)
		}
		err = eval.Err()
		if err != nil {
			return newInternalErrorUnlockResponse(), err
		}
		// 3. parse result
		i, err = eval.Int()
		status = lock.InternalError
		if err != nil {
			return &lock.UnlockResponse{
				Status: status,
			}, err
		}
	} else {
		// 1. delegate to client.eval lua script
		eval := r.clientv9.Eval(r.ctx, unlockScript, []string{req.ResourceID}, req.LockOwner)
		// 2. check error
		if eval == nil {
			return newInternalErrorUnlockResponse(), fmt.Errorf("[standaloneRedisLock]: Eval unlock script returned nil.ResourceID: %s", req.ResourceID)
		}
		err = eval.Err()
		if err != nil {
			return newInternalErrorUnlockResponse(), err
		}
		// 3. parse result
		i, err = eval.Int()
		status = lock.InternalError
		if err != nil {
			return &lock.UnlockResponse{
				Status: status,
			}, err
		}
	}

	if i >= 0 {
		status = lock.Success
	} else if i == -1 {
		status = lock.LockDoesNotExist
	} else if i == -2 {
		status = lock.LockBelongsToOthers
	}
	return &lock.UnlockResponse{
		Status: status,
	}, nil
}

func newInternalErrorUnlockResponse() *lock.UnlockResponse {
	return &lock.UnlockResponse{
		Status: lock.InternalError,
	}
}

// Close shuts down the client's redis connections.
func (r *StandaloneRedisLock) Close() error {
	if r.cancel != nil {
		r.cancel()
	}
	if r.legacyRedis {
		if r.clientv8 != nil {
			return r.clientv8.Close()
		}
	} else {
		if r.clientv9 != nil {
			return r.clientv9.Close()
		}
	}
	return nil
}
