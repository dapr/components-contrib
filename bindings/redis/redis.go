// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package redis

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/dapr/components-contrib/bindings"
	rediscomponent "github.com/dapr/components-contrib/internal/component/redis"
	"github.com/dapr/kit/logger"
)

const (
	maxRetries             = "maxRetries"
	maxRetryBackoff        = "maxRetryBackoff"
	defaultBase            = 10
	defaultBitSize         = 0
	defaultMaxRetries      = 3
	defaultMaxRetryBackoff = time.Second * 2
)

// Redis is a redis output binding
type Redis struct {
	*rediscomponent.ComponentClient
	logger logger.Logger

	ctx    context.Context
	cancel context.CancelFunc
}

// NewRedis returns a new redis bindings instance
func NewRedis(logger logger.Logger) *Redis {
	return &Redis{logger: logger, ComponentClient: &rediscomponent.ComponentClient{}}
}

// Init performs metadata parsing and connection creation
func (r *Redis) Init(meta bindings.Metadata) error {
	_, err := r.parseMetadata(meta)
	if err != nil {
		return err
	}

	err = r.ComponentClient.Init(meta.Properties)
	if err != nil {
		return err
	}

	r.ctx, r.cancel = context.WithCancel(context.Background())

	_, err = r.Client.Ping(r.ctx).Result()
	if err != nil {
		return fmt.Errorf("redis binding: error connecting to redis at %s: %s", r.ClientMetadata.Host, err)
	}

	return err
}

func (r *Redis) parseMetadata(meta bindings.Metadata) (metadata, error) {
	m := metadata{}

	m.maxRetries = defaultMaxRetries
	if val, ok := meta.Properties[maxRetries]; ok && val != "" {
		parsedVal, err := strconv.ParseInt(val, defaultBase, defaultBitSize)
		if err != nil {
			return m, fmt.Errorf("redis binding error: can't parse maxRetries field: %s", err)
		}
		m.maxRetries = int(parsedVal)
	}

	m.maxRetryBackoff = defaultMaxRetryBackoff
	if val, ok := meta.Properties[maxRetryBackoff]; ok && val != "" {
		parsedVal, err := strconv.ParseInt(val, defaultBase, defaultBitSize)
		if err != nil {
			return m, fmt.Errorf("redis binding error: can't parse maxRetries field: %s", err)
		}
		m.maxRetryBackoff = time.Duration(parsedVal)
	}

	return m, nil
}

func (r *Redis) Operations() []bindings.OperationKind {
	return []bindings.OperationKind{bindings.CreateOperation}
}

func (r *Redis) Invoke(req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	if val, ok := req.Metadata["key"]; ok && val != "" {
		key := val
		_, err := r.Client.Do(r.ctx, "SET", key, req.Data).Result()
		if err != nil {
			return nil, err
		}

		return nil, nil
	}

	return nil, errors.New("redis binding: missing key on write request metadata")
}

func (r *Redis) Close() error {
	r.cancel()

	return r.Client.Close()
}
