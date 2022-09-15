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

	v8 "github.com/go-redis/redis/v8"
	v9 "github.com/go-redis/redis/v9"

	"github.com/dapr/components-contrib/bindings"
	rediscomponent "github.com/dapr/components-contrib/internal/component/redis"
	"github.com/dapr/kit/logger"
)

// Redis is a redis output binding.
type Redis struct {
	clientv8       v8.UniversalClient
	clientv9       v9.UniversalClient
	clientSettings *rediscomponent.Settings
	logger         logger.Logger
	legacyRedis    bool

	ctx    context.Context
	cancel context.CancelFunc
}

// NewRedis returns a new redis bindings instance.
func NewRedis(logger logger.Logger) bindings.OutputBinding {
	return &Redis{logger: logger}
}

// Init performs metadata parsing and connection creation.
func (r *Redis) Init(meta bindings.Metadata) (err error) {
	if rediscomponent.IsLegacyRedisVersion(meta.Properties) {
		r.legacyRedis = true
		r.clientv8, r.clientSettings, err = rediscomponent.ParseClientv8FromProperties(meta.Properties, nil)
	} else {
		r.legacyRedis = false
		r.clientv9, r.clientSettings, err = rediscomponent.ParseClientv9FromProperties(meta.Properties, nil)
	}
	if err != nil {
		return err
	}

	r.ctx, r.cancel = context.WithCancel(context.Background())

	if r.legacyRedis {
		_, err = r.clientv8.Ping(r.ctx).Result()
	} else {
		_, err = r.clientv9.Ping(r.ctx).Result()
	}
	if err != nil {
		return fmt.Errorf("redis binding: error connecting to redis at %s: %s", r.clientSettings.Host, err)
	}

	return err
}

func (r *Redis) Ping() error {
	if r.legacyRedis {
		if _, err := r.clientv8.Ping(r.ctx).Result(); err != nil {
			return fmt.Errorf("redis binding: error connecting to redis at %s: %s", r.clientSettings.Host, err)
		}
	} else {
		if _, err := r.clientv9.Ping(r.ctx).Result(); err != nil {
			return fmt.Errorf("redis binding: error connecting to redis at %s: %s", r.clientSettings.Host, err)
		}
	}

	return nil
}

func (r *Redis) Operations() []bindings.OperationKind {
	return []bindings.OperationKind{bindings.CreateOperation}
}

func (r *Redis) Invoke(ctx context.Context, req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	if val, ok := req.Metadata["key"]; ok && val != "" {
		key := val
		var err error
		if r.legacyRedis {
			_, err = r.clientv8.Do(ctx, "SET", key, req.Data).Result()
		} else {
			_, err = r.clientv9.Do(ctx, "SET", key, req.Data).Result()
		}
		if err != nil {
			return nil, err
		}

		return nil, nil
	}

	return nil, errors.New("redis binding: missing key on write request metadata")
}

func (r *Redis) Close() error {
	r.cancel()

	if r.legacyRedis {
		return r.clientv8.Close()
	} else {
		return r.clientv9.Close()
	}
}
