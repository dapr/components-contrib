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

	"github.com/dapr/components-contrib/bindings"
	rediscomponent "github.com/dapr/components-contrib/common/component/redis"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
)

// Redis is a redis output binding.
type Redis struct {
	client         rediscomponent.RedisClient
	clientSettings *rediscomponent.Settings
	logger         logger.Logger
}

const (
	// IncrementOperation is the operation to increment a key.
	IncrementOperation bindings.OperationKind = "increment"
)

// NewRedis returns a new redis bindings instance.
func NewRedis(logger logger.Logger) bindings.OutputBinding {
	return &Redis{logger: logger}
}

// Init performs metadata parsing and connection creation.
func (r *Redis) Init(ctx context.Context, meta bindings.Metadata) (err error) {
	r.client, r.clientSettings, err = rediscomponent.ParseClientFromProperties(meta.Properties, metadata.BindingType, ctx, &r.logger)
	if err != nil {
		return err
	}

	_, err = r.client.PingResult(ctx)
	if err != nil {
		return fmt.Errorf("redis binding: error connecting to redis at %s: %s", r.clientSettings.Host, err)
	}

	return err
}

func (r *Redis) Ping(ctx context.Context) error {
	if _, err := r.client.PingResult(ctx); err != nil {
		return fmt.Errorf("redis binding: error connecting to redis at %s: %s", r.clientSettings.Host, err)
	}

	return nil
}

func (r *Redis) Operations() []bindings.OperationKind {
	return []bindings.OperationKind{
		bindings.CreateOperation,
		bindings.DeleteOperation,
		bindings.GetOperation,
		IncrementOperation,
	}
}

func (r *Redis) expireKeyIfRequested(ctx context.Context, requestMetadata map[string]string, key string) error {
	// get ttl from request metadata
	ttl, ok, err := metadata.TryGetTTL(requestMetadata)
	if err != nil {
		return err
	}
	if ok {
		errExpire := r.client.DoWrite(ctx, "EXPIRE", key, int(ttl.Seconds()))
		if errExpire != nil {
			return errExpire
		}
	}
	return nil
}

func (r *Redis) Invoke(ctx context.Context, req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	if key, ok := req.Metadata["key"]; ok && key != "" {
		switch req.Operation {
		case bindings.DeleteOperation:
			err := r.client.Del(ctx, key)
			if err != nil {
				return nil, err
			}
		case bindings.GetOperation:
			var data string
			var err error
			if req.Metadata["delete"] == "true" {
				data, err = r.client.GetDel(ctx, key)
			} else {
				data, err = r.client.Get(ctx, key)
			}
			if err != nil {
				if err.Error() == "redis: nil" {
					return &bindings.InvokeResponse{}, nil
				}
				return nil, err
			}
			rep := &bindings.InvokeResponse{}
			rep.Data = []byte(data)
			return rep, nil
		case bindings.CreateOperation:
			err := r.client.DoWrite(ctx, "SET", key, req.Data)
			if err != nil {
				return nil, err
			}
			err = r.expireKeyIfRequested(ctx, req.Metadata, key)
			if err != nil {
				return nil, err
			}
		case IncrementOperation:
			err := r.client.DoWrite(ctx, "INCR", key)
			if err != nil {
				return nil, err
			}
			err = r.expireKeyIfRequested(ctx, req.Metadata, key)
			if err != nil {
				return nil, err
			}
		default:
			return nil, fmt.Errorf("invalid operation type: %s", req.Operation)
		}
		return nil, nil
	}
	return nil, errors.New("redis binding: missing key in request metadata")
}

func (r *Redis) Close() error {
	return r.client.Close()
}

// GetComponentMetadata returns the metadata of the component.
func (r *Redis) GetComponentMetadata() (metadataInfo metadata.MetadataMap) {
	metadataStruct := rediscomponent.Settings{}
	metadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, metadata.BindingType)
	return
}
