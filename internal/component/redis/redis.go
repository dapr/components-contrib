// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package redis

import (
	"crypto/tls"
	"fmt"
	"strings"

	"github.com/go-redis/redis/v8"
)

const (
	ClusterType = "cluster"
	NodeType    = "node"
)

func ParseClientFromProperties(properties map[string]string, defaultSettings *Settings) (client redis.UniversalClient, settings *Settings, err error) {
	if defaultSettings == nil {
		settings = &Settings{}
	} else {
		settings = defaultSettings
	}
	err = settings.Decode(properties)
	if err != nil {
		return nil, nil, fmt.Errorf("redis client configuration error: %w", err)
	}
	if settings.Failover {
		return newFailoverClient(settings), settings, nil
	}

	return newClient(settings), settings, nil
}

func newFailoverClient(s *Settings) redis.UniversalClient {
	if s == nil {
		return nil
	}
	opts := &redis.FailoverOptions{
		DB:                 s.DB,
		MasterName:         s.SentinelMasterName,
		SentinelAddrs:      []string{s.Host},
		Password:           s.Password,
		MaxRetries:         s.RedisMaxRetries,
		MaxRetryBackoff:    s.RedisMaxRetryInterval,
		MinRetryBackoff:    s.RedisMinRetryInterval,
		DialTimeout:        s.DialTimeout,
		ReadTimeout:        s.ReadTimeout,
		WriteTimeout:       s.WriteTimeout,
		PoolSize:           s.PoolSize,
		MaxConnAge:         s.MaxConnAge,
		MinIdleConns:       s.MinIdleConns,
		PoolTimeout:        s.PoolTimeout,
		IdleCheckFrequency: s.IdleCheckFrequency,
		IdleTimeout:        s.IdleTimeout,
	}

	/* #nosec */
	if s.EnableTLS {
		opts.TLSConfig = &tls.Config{
			InsecureSkipVerify: s.EnableTLS,
		}
	}

	if s.RedisType == ClusterType {
		opts.SentinelAddrs = strings.Split(s.Host, ",")

		return redis.NewFailoverClusterClient(opts)
	}

	return redis.NewFailoverClient(opts)
}

func newClient(s *Settings) redis.UniversalClient {
	if s == nil {
		return nil
	}
	if s.RedisType == ClusterType {
		options := &redis.ClusterOptions{
			Addrs:              strings.Split(s.Host, ","),
			Password:           s.Password,
			MaxRetries:         s.RedisMaxRetries,
			MaxRetryBackoff:    s.RedisMaxRetryInterval,
			MinRetryBackoff:    s.RedisMinRetryInterval,
			DialTimeout:        s.DialTimeout,
			ReadTimeout:        s.ReadTimeout,
			WriteTimeout:       s.WriteTimeout,
			PoolSize:           s.PoolSize,
			MaxConnAge:         s.MaxConnAge,
			MinIdleConns:       s.MinIdleConns,
			PoolTimeout:        s.PoolTimeout,
			IdleCheckFrequency: s.IdleCheckFrequency,
			IdleTimeout:        s.IdleTimeout,
		}
		/* #nosec */
		if s.EnableTLS {
			options.TLSConfig = &tls.Config{
				InsecureSkipVerify: s.EnableTLS,
			}
		}

		return redis.NewClusterClient(options)
	}

	options := &redis.Options{
		Addr:               s.Host,
		Password:           s.Password,
		DB:                 s.DB,
		MaxRetries:         s.RedisMaxRetries,
		MaxRetryBackoff:    s.RedisMaxRetryInterval,
		MinRetryBackoff:    s.RedisMinRetryInterval,
		DialTimeout:        s.DialTimeout,
		ReadTimeout:        s.ReadTimeout,
		WriteTimeout:       s.WriteTimeout,
		PoolSize:           s.PoolSize,
		MaxConnAge:         s.MaxConnAge,
		MinIdleConns:       s.MinIdleConns,
		PoolTimeout:        s.PoolTimeout,
		IdleCheckFrequency: s.IdleCheckFrequency,
		IdleTimeout:        s.IdleTimeout,
	}

	/* #nosec */
	if s.EnableTLS {
		options.TLSConfig = &tls.Config{
			InsecureSkipVerify: s.EnableTLS,
		}
	}

	return redis.NewClient(options)
}
