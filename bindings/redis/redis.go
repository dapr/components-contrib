// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package redis

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/dapr/pkg/logger"
	redis "github.com/go-redis/redis/v7"
)

const (
	host                   = "redisHost"
	password               = "redisPassword"
	enableTLS              = "enableTLS"
	maxRetries             = "maxRetries"
	maxRetryBackoff        = "maxRetryBackoff"
	defaultBase            = 10
	defaultBitSize         = 0
	defaultDB              = 0
	defaultMaxRetries      = 3
	defaultMaxRetryBackoff = time.Second * 2
	defaultEnableTLS       = false
)

// Redis is a redis output binding
type Redis struct {
	client *redis.Client
	logger logger.Logger
}

// NewRedis returns a new redis bindings instance
func NewRedis(logger logger.Logger) *Redis {
	return &Redis{logger: logger}
}

// Init performs metadata parsing and connection creation
func (r *Redis) Init(meta bindings.Metadata) error {
	m, err := r.parseMetadata(meta)
	if err != nil {
		return err
	}

	opts := &redis.Options{
		Addr:            m.host,
		Password:        m.password,
		DB:              defaultDB,
		MaxRetries:      m.maxRetries,
		MaxRetryBackoff: m.maxRetryBackoff,
	}

	/* #nosec */
	if m.enableTLS {
		opts.TLSConfig = &tls.Config{
			InsecureSkipVerify: m.enableTLS,
		}
	}

	r.client = redis.NewClient(opts)
	_, err = r.client.Ping().Result()
	if err != nil {
		return fmt.Errorf("redis binding: error connecting to redis at %s: %s", m.host, err)
	}

	return err
}

func (r *Redis) parseMetadata(meta bindings.Metadata) (metadata, error) {
	m := metadata{}

	if val, ok := meta.Properties[host]; ok && val != "" {
		m.host = val
	} else {
		return m, errors.New("redis binding error: missing host address")
	}

	if val, ok := meta.Properties[password]; ok && val != "" {
		m.password = val
	}

	m.enableTLS = defaultEnableTLS
	if val, ok := meta.Properties[enableTLS]; ok && val != "" {
		tls, err := strconv.ParseBool(val)
		if err != nil {
			return m, fmt.Errorf("redis binding error: can't parse enableTLS field: %s", err)
		}
		m.enableTLS = tls
	}

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
		_, err := r.client.DoContext(context.Background(), "SET", key, req.Data).Result()
		if err != nil {
			return nil, err
		}

		return nil, nil
	}

	return nil, errors.New("redis binding: missing key on write request metadata")
}
