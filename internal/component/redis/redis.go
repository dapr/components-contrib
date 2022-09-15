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
	"crypto/tls"
	"fmt"
	"strings"
	"time"

	v8 "github.com/go-redis/redis/v8"
	v9 "github.com/go-redis/redis/v9"
)

const (
	ClusterType     = "cluster"
	NodeType        = "node"
	RedisVersionKey = "redisVersion"
)

func ParseClientv8FromProperties(properties map[string]string, defaultSettings *Settings) (client v8.UniversalClient, settings *Settings, err error) {
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
		return newFailoverClientv8(settings), settings, nil
	}
	return newClientv8(settings), settings, nil
}

func ParseClientv9FromProperties(properties map[string]string, defaultSettings *Settings) (client v9.UniversalClient, settings *Settings, err error) {
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
		return newFailoverClientv9(settings), settings, nil
	}
	return newClientv9(settings), settings, nil
}

func newFailoverClientv8(s *Settings) v8.UniversalClient {
	if s == nil {
		return nil
	}
	opts := &v8.FailoverOptions{
		DB:                 s.DB,
		MasterName:         s.SentinelMasterName,
		SentinelAddrs:      []string{s.Host},
		Password:           s.Password,
		Username:           s.Username,
		MaxRetries:         s.RedisMaxRetries,
		MaxRetryBackoff:    time.Duration(s.RedisMaxRetryInterval),
		MinRetryBackoff:    time.Duration(s.RedisMinRetryInterval),
		DialTimeout:        time.Duration(s.DialTimeout),
		ReadTimeout:        time.Duration(s.ReadTimeout),
		WriteTimeout:       time.Duration(s.WriteTimeout),
		PoolSize:           s.PoolSize,
		MaxConnAge:         time.Duration(s.MaxConnAge),
		MinIdleConns:       s.MinIdleConns,
		PoolTimeout:        time.Duration(s.PoolTimeout),
		IdleCheckFrequency: time.Duration(s.IdleCheckFrequency),
		IdleTimeout:        time.Duration(s.IdleTimeout),
	}

	/* #nosec */
	if s.EnableTLS {
		opts.TLSConfig = &tls.Config{
			InsecureSkipVerify: s.EnableTLS,
		}
	}

	if s.RedisType == ClusterType {
		opts.SentinelAddrs = strings.Split(s.Host, ",")

		return v8.NewFailoverClusterClient(opts)
	}

	return v8.NewFailoverClient(opts)
}

func newFailoverClientv9(s *Settings) v9.UniversalClient {
	if s == nil {
		return nil
	}
	opts := &v9.FailoverOptions{
		DB:              s.DB,
		MasterName:      s.SentinelMasterName,
		SentinelAddrs:   []string{s.Host},
		Password:        s.Password,
		Username:        s.Username,
		MaxRetries:      s.RedisMaxRetries,
		MaxRetryBackoff: time.Duration(s.RedisMaxRetryInterval),
		MinRetryBackoff: time.Duration(s.RedisMinRetryInterval),
		DialTimeout:     time.Duration(s.DialTimeout),
		ReadTimeout:     time.Duration(s.ReadTimeout),
		WriteTimeout:    time.Duration(s.WriteTimeout),
		PoolSize:        s.PoolSize,
		ConnMaxLifetime: time.Duration(s.MaxConnAge),
		ConnMaxIdleTime: time.Duration(s.IdleTimeout),
		MinIdleConns:    s.MinIdleConns,
		PoolTimeout:     time.Duration(s.PoolTimeout),
	}

	/* #nosec */
	if s.EnableTLS {
		opts.TLSConfig = &tls.Config{
			InsecureSkipVerify: s.EnableTLS,
		}
	}

	if s.RedisType == ClusterType {
		opts.SentinelAddrs = strings.Split(s.Host, ",")

		return v9.NewFailoverClusterClient(opts)
	}

	return v9.NewFailoverClient(opts)
}

func newClientv8(s *Settings) v8.UniversalClient {
	if s == nil {
		return nil
	}
	if s.RedisType == ClusterType {
		options := &v8.ClusterOptions{
			Addrs:              strings.Split(s.Host, ","),
			Password:           s.Password,
			Username:           s.Username,
			MaxRetries:         s.RedisMaxRetries,
			MaxRetryBackoff:    time.Duration(s.RedisMaxRetryInterval),
			MinRetryBackoff:    time.Duration(s.RedisMinRetryInterval),
			DialTimeout:        time.Duration(s.DialTimeout),
			ReadTimeout:        time.Duration(s.ReadTimeout),
			WriteTimeout:       time.Duration(s.WriteTimeout),
			PoolSize:           s.PoolSize,
			MaxConnAge:         time.Duration(s.MaxConnAge),
			MinIdleConns:       s.MinIdleConns,
			PoolTimeout:        time.Duration(s.PoolTimeout),
			IdleCheckFrequency: time.Duration(s.IdleCheckFrequency),
			IdleTimeout:        time.Duration(s.IdleTimeout),
		}
		/* #nosec */
		if s.EnableTLS {
			options.TLSConfig = &tls.Config{
				InsecureSkipVerify: s.EnableTLS,
			}
		}

		return v8.NewClusterClient(options)
	}

	options := &v8.Options{
		Addr:               s.Host,
		Password:           s.Password,
		Username:           s.Username,
		DB:                 s.DB,
		MaxRetries:         s.RedisMaxRetries,
		MaxRetryBackoff:    time.Duration(s.RedisMaxRetryInterval),
		MinRetryBackoff:    time.Duration(s.RedisMinRetryInterval),
		DialTimeout:        time.Duration(s.DialTimeout),
		ReadTimeout:        time.Duration(s.ReadTimeout),
		WriteTimeout:       time.Duration(s.WriteTimeout),
		PoolSize:           s.PoolSize,
		MaxConnAge:         time.Duration(s.MaxConnAge),
		MinIdleConns:       s.MinIdleConns,
		PoolTimeout:        time.Duration(s.PoolTimeout),
		IdleCheckFrequency: time.Duration(s.IdleCheckFrequency),
		IdleTimeout:        time.Duration(s.IdleTimeout),
	}

	/* #nosec */
	if s.EnableTLS {
		options.TLSConfig = &tls.Config{
			InsecureSkipVerify: s.EnableTLS,
		}
	}

	return v8.NewClient(options)
}

func newClientv9(s *Settings) v9.UniversalClient {
	if s == nil {
		return nil
	}
	if s.RedisType == ClusterType {
		options := &v9.ClusterOptions{
			Addrs:           strings.Split(s.Host, ","),
			Password:        s.Password,
			Username:        s.Username,
			MaxRetries:      s.RedisMaxRetries,
			MaxRetryBackoff: time.Duration(s.RedisMaxRetryInterval),
			MinRetryBackoff: time.Duration(s.RedisMinRetryInterval),
			DialTimeout:     time.Duration(s.DialTimeout),
			ReadTimeout:     time.Duration(s.ReadTimeout),
			WriteTimeout:    time.Duration(s.WriteTimeout),
			PoolSize:        s.PoolSize,
			ConnMaxLifetime: time.Duration(s.MaxConnAge),
			MinIdleConns:    s.MinIdleConns,
			PoolTimeout:     time.Duration(s.PoolTimeout),
			ConnMaxIdleTime: time.Duration(s.IdleTimeout),
		}
		/* #nosec */
		if s.EnableTLS {
			options.TLSConfig = &tls.Config{
				InsecureSkipVerify: s.EnableTLS,
			}
		}

		return v9.NewClusterClient(options)
	}

	options := &v9.Options{
		Addr:            s.Host,
		Password:        s.Password,
		Username:        s.Username,
		DB:              s.DB,
		MaxRetries:      s.RedisMaxRetries,
		MaxRetryBackoff: time.Duration(s.RedisMaxRetryInterval),
		MinRetryBackoff: time.Duration(s.RedisMinRetryInterval),
		DialTimeout:     time.Duration(s.DialTimeout),
		ReadTimeout:     time.Duration(s.ReadTimeout),
		WriteTimeout:    time.Duration(s.WriteTimeout),
		PoolSize:        s.PoolSize,
		ConnMaxLifetime: time.Duration(s.MaxConnAge),
		MinIdleConns:    s.MinIdleConns,
		PoolTimeout:     time.Duration(s.PoolTimeout),
		ConnMaxIdleTime: time.Duration(s.IdleTimeout),
	}

	/* #nosec */
	if s.EnableTLS {
		options.TLSConfig = &tls.Config{
			InsecureSkipVerify: s.EnableTLS,
		}
	}

	return v9.NewClient(options)
}

func IsLegacyRedisVersion(props map[string]string) bool {
	if val, ok := props[RedisVersionKey]; !ok {
		return true
	} else {
		if val == "" || (strings.Trim(val, "vV ") < "7") {
			return true
		}
		return false
	}
}
