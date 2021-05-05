// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package redis

import (
	"crypto/tls"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/go-redis/redis/v8"
)

const (
	host                  = "redisHost"
	password              = "redisPassword"
	db                    = "redisDB"
	redisType             = "redisType"
	redisMaxRetries       = "redisMaxRetries"
	redisMinRetryInterval = "redisMinRetryInterval"
	redisMaxRetryInterval = "redisMaxRetryInterval"
	dialTimeout           = "dialTimeout"
	readTimeout           = "readTimeout"
	writeTimeout          = "writeTimeout"
	poolSize              = "poolSize"
	minIdleConns          = "minIdleConns"
	poolTimeout           = "poolTimeout"
	idleTimeout           = "idleTimeout"
	idleCheckFrequency    = "idleCheckFrequency"
	maxConnAge            = "maxConnAge"
	enableTLS             = "enableTLS"
	failover              = "failover"
	sentinelMasterName    = "sentinelMasterName"
)

const (
	ClusterType = "cluster"
	NodeType    = "node"
)

// redisStreams handles consuming from a Redis stream using
// `XREADGROUP` for reading new messages and `XPENDING` and
// `XCLAIM` for redelivering messages that previously failed.
//
// See https://redis.io/topics/streams-intro for more information
// on the mechanics of Redis Streams.
type ComponentClient struct {
	ClientMetadata Metadata
	Client         redis.UniversalClient
}

func parseRedisMetadata(properties map[string]string) (Metadata, error) {
	// Default values
	m := Metadata{}
	if val, ok := properties[host]; ok && val != "" {
		m.Host = val
	} else {
		return m, errors.New("redis streams error: missing host address")
	}

	if val, ok := properties[password]; ok && val != "" {
		m.password = val
	}

	if val, ok := properties[redisType]; ok && val != "" {
		if val != NodeType && val != ClusterType {
			return m, fmt.Errorf("redis type error: unknown redis type: %s", val)
		}
		m.redisType = val
	}

	if val, ok := properties[db]; ok && val != "" {
		db, err := strconv.Atoi(val)
		if err != nil {
			return m, fmt.Errorf("redis streams error: can't parse db field: %s", err)
		}
		m.db = db
	}

	if val, ok := properties[enableTLS]; ok && val != "" {
		tls, err := strconv.ParseBool(val)
		if err != nil {
			return m, fmt.Errorf("redis streams error: can't parse enableTLS field: %s", err)
		}
		m.enableTLS = tls
	}

	if val, ok := properties[redisMaxRetries]; ok && val != "" {
		redisMaxRetries, err := strconv.Atoi(val)
		if err != nil {
			return m, fmt.Errorf("redis streams error: can't parse redisMaxRetries field: %s", err)
		}
		m.redisMaxRetries = redisMaxRetries
	}

	if val, ok := properties[redisMinRetryInterval]; ok && val != "" {
		if val == "-1" {
			m.redisMinRetryInterval = -1
		} else if redisMinRetryIntervalMs, err := strconv.ParseUint(val, 10, 64); err == nil {
			m.redisMinRetryInterval = time.Duration(redisMinRetryIntervalMs) * time.Millisecond
		} else if d, err := time.ParseDuration(val); err == nil {
			m.redisMinRetryInterval = d
		} else {
			return m, fmt.Errorf("redis streams error: invalid redisMinRetryInterval %s, %s", val, err)
		}
	}

	if val, ok := properties[redisMaxRetryInterval]; ok && val != "" {
		if val == "-1" {
			m.redisMaxRetryInterval = -1
		} else if redisMaxRetryIntervalMs, err := strconv.ParseUint(val, 10, 64); err == nil {
			m.redisMaxRetryInterval = time.Duration(redisMaxRetryIntervalMs) * time.Millisecond
		} else if d, err := time.ParseDuration(val); err == nil {
			m.redisMaxRetryInterval = d
		} else {
			return m, fmt.Errorf("redis streams error: invalid redisMaxRetryInterval %s, %s", val, err)
		}
	}

	if val, ok := properties[dialTimeout]; ok && val != "" {
		if dialTimeoutMs, err := strconv.ParseUint(val, 10, 64); err == nil {
			m.dialTimeout = time.Duration(dialTimeoutMs) * time.Millisecond
		} else if d, err := time.ParseDuration(val); err == nil {
			m.dialTimeout = d
		} else {
			return m, fmt.Errorf("redis streams error: invalid dialTimeout %s, %s", val, err)
		}
	}

	if val, ok := properties[readTimeout]; ok && val != "" {
		if val == "-1" {
			m.ReadTimeout = -1
		} else if readTimeoutMs, err := strconv.ParseUint(val, 10, 64); err == nil {
			m.ReadTimeout = time.Duration(readTimeoutMs) * time.Millisecond
		} else if d, err := time.ParseDuration(val); err == nil {
			m.ReadTimeout = d
		} else {
			return m, fmt.Errorf("redis streams error: invalid readTimeout %s, %s", val, err)
		}
	}

	if val, ok := properties[writeTimeout]; ok && val != "" {
		if writeTimeoutMs, err := strconv.ParseUint(val, 10, 64); err == nil {
			m.writeTimeout = time.Duration(writeTimeoutMs) * time.Millisecond
		} else if d, err := time.ParseDuration(val); err == nil {
			m.writeTimeout = d
		} else {
			return m, fmt.Errorf("redis streams error: invalid writeTimeout %s, %s", val, err)
		}
	}

	if val, ok := properties[poolSize]; ok && val != "" {
		var err error
		m.poolSize, err = strconv.Atoi(val)
		if err != nil {
			return m, fmt.Errorf("redis streams error: invalid poolSize %s, %s", val, err)
		}
	}

	if val, ok := properties[maxConnAge]; ok && val != "" {
		if maxConnAgeMs, err := strconv.ParseUint(val, 10, 64); err == nil {
			m.maxConnAge = time.Duration(maxConnAgeMs) * time.Millisecond
		} else if d, err := time.ParseDuration(val); err == nil {
			m.maxConnAge = d
		} else {
			return m, fmt.Errorf("redis streams error: invalid maxConnAge %s, %s", val, err)
		}
	}

	if val, ok := properties[minIdleConns]; ok && val != "" {
		minIdleConns, err := strconv.Atoi(val)
		if err != nil {
			return m, fmt.Errorf("redis streams error: can't parse minIdleConns field: %s", err)
		}
		m.minIdleConns = minIdleConns
	}

	if val, ok := properties[poolTimeout]; ok && val != "" {
		if poolTimeoutMs, err := strconv.ParseUint(val, 10, 64); err == nil {
			m.poolTimeout = time.Duration(poolTimeoutMs) * time.Millisecond
		} else if d, err := time.ParseDuration(val); err == nil {
			m.poolTimeout = d
		} else {
			return m, fmt.Errorf("redis streams error: invalid poolTimeout %s, %s", val, err)
		}
	}

	if val, ok := properties[idleTimeout]; ok && val != "" {
		if val == "-1" {
			m.idleTimeout = -1
		} else if idleTimeoutMs, err := strconv.ParseUint(val, 10, 64); err == nil {
			m.idleTimeout = time.Duration(idleTimeoutMs) * time.Millisecond
		} else if d, err := time.ParseDuration(val); err == nil {
			m.idleTimeout = d
		} else {
			return m, fmt.Errorf("redis streams error: invalid idleTimeout %s, %s", val, err)
		}
	}

	if val, ok := properties[idleCheckFrequency]; ok && val != "" {
		if val == "-1" {
			m.idleCheckFrequency = -1
		} else if idleCheckFrequencyMs, err := strconv.ParseUint(val, 10, 64); err == nil {
			m.idleCheckFrequency = time.Duration(idleCheckFrequencyMs) * time.Millisecond
		} else if d, err := time.ParseDuration(val); err == nil {
			m.idleCheckFrequency = d
		} else {
			return m, fmt.Errorf("redis streams error: invalid idleCheckFrequency %s, %s", val, err)
		}
	}

	if val, ok := properties[failover]; ok && val != "" {
		failover, err := strconv.ParseBool(val)
		if err != nil {
			return m, fmt.Errorf("redis store error: can't parse failover field: %s", err)
		}
		m.failover = failover
	}

	// set the sentinelMasterName only with failover == true.
	if m.failover {
		if val, ok := properties[sentinelMasterName]; ok && val != "" {
			m.sentinelMasterName = val
		} else {
			return m, errors.New("redis store error: missing sentinelMasterName")
		}
	}

	return m, nil
}

func (r *ComponentClient) Init(properties map[string]string) error {
	m, err := parseRedisMetadata(properties)
	if err != nil {
		return err
	}
	r.ClientMetadata = m
	if r.ClientMetadata.failover {
		r.Client = r.newFailoverClient(m)
	} else {
		r.Client = r.newClient(m)
	}
	return nil
}

func (r *ComponentClient) newFailoverClient(m Metadata) redis.UniversalClient {
	opts := &redis.FailoverOptions{
		MasterName:         m.sentinelMasterName,
		SentinelAddrs:      []string{m.Host},
		Password:           m.password,
		DB:                 m.db,
		MaxRetries:         m.redisMaxRetries,
		MaxRetryBackoff:    m.redisMaxRetryInterval,
		MinRetryBackoff:    m.redisMinRetryInterval,
		DialTimeout:        m.dialTimeout,
		ReadTimeout:        m.ReadTimeout,
		WriteTimeout:       m.writeTimeout,
		PoolSize:           m.poolSize,
		MaxConnAge:         m.maxConnAge,
		MinIdleConns:       m.minIdleConns,
		PoolTimeout:        m.poolTimeout,
		IdleCheckFrequency: m.idleCheckFrequency,
		IdleTimeout:        m.idleTimeout,
	}

	/* #nosec */
	if m.enableTLS {
		opts.TLSConfig = &tls.Config{
			InsecureSkipVerify: m.enableTLS,
		}
	}

	if m.redisType == ClusterType {
		opts.SentinelAddrs = strings.Split(m.Host, ",")
		return redis.NewFailoverClusterClient(opts)
	}
	return redis.NewFailoverClient(opts)
}

func (r *ComponentClient) newClient(m Metadata) redis.UniversalClient {
	if m.redisType == ClusterType {
		options := &redis.ClusterOptions{
			Addrs:              strings.Split(m.Host, ","),
			Password:           m.password,
			MaxRetries:         m.redisMaxRetries,
			MaxRetryBackoff:    m.redisMaxRetryInterval,
			MinRetryBackoff:    m.redisMinRetryInterval,
			DialTimeout:        m.dialTimeout,
			ReadTimeout:        m.ReadTimeout,
			WriteTimeout:       m.writeTimeout,
			PoolSize:           m.poolSize,
			MaxConnAge:         m.maxConnAge,
			MinIdleConns:       m.minIdleConns,
			PoolTimeout:        m.poolTimeout,
			IdleCheckFrequency: m.idleCheckFrequency,
			IdleTimeout:        m.idleTimeout,
		}
		/* #nosec */
		if r.ClientMetadata.enableTLS {
			options.TLSConfig = &tls.Config{
				InsecureSkipVerify: r.ClientMetadata.enableTLS,
			}
		}

		return redis.NewClusterClient(options)
	}

	options := &redis.Options{
		Addr:               m.Host,
		Password:           m.password,
		DB:                 m.db,
		MaxRetries:         m.redisMaxRetries,
		MaxRetryBackoff:    m.redisMaxRetryInterval,
		MinRetryBackoff:    m.redisMinRetryInterval,
		DialTimeout:        m.dialTimeout,
		ReadTimeout:        m.ReadTimeout,
		WriteTimeout:       m.writeTimeout,
		PoolSize:           m.poolSize,
		MaxConnAge:         m.maxConnAge,
		MinIdleConns:       m.minIdleConns,
		PoolTimeout:        m.poolTimeout,
		IdleCheckFrequency: m.idleCheckFrequency,
		IdleTimeout:        m.idleTimeout,
	}

	/* #nosec */
	if r.ClientMetadata.enableTLS {
		options.TLSConfig = &tls.Config{
			InsecureSkipVerify: r.ClientMetadata.enableTLS,
		}
	}

	return redis.NewClient(options)
}
