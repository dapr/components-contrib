// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package redis

import (
	"fmt"
	"time"

	"github.com/dapr/components-contrib/internal/config"
)

type Settings struct {
	// The Redis host
	Host string `mapstructure:"redisHost"`
	// The Redis password
	Password string `mapstructure:"redisPassword"`
	// Database to be selected after connecting to the server.
	DB int `mapstructure:"redisDB"`
	// The redis type node or cluster
	RedisType string `mapstructure:"redisType"`
	// Maximum number of retries before giving up.
	// A value of -1 (not 0) disables retries
	// Default is 3 retries
	RedisMaxRetries int `mapstructure:"redisMaxRetries"`
	// Minimum backoff between each retry.
	// Default is 8 milliseconds; -1 disables backoff.
	RedisMinRetryInterval time.Duration `mapstructure:"redisMinRetryInterval"`
	// Maximum backoff between each retry.
	// Default is 512 milliseconds; -1 disables backoff.
	RedisMaxRetryInterval time.Duration `mapstructure:"redisMaxRetryInterval"`
	// Dial timeout for establishing new connections.
	DialTimeout time.Duration `mapstructure:"dialTimeout"`
	// Timeout for socket reads. If reached, commands will fail
	// with a timeout instead of blocking. Use value -1 for no timeout and 0 for default.
	ReadTimeout time.Duration `mapstructure:"readTimeout"`
	// Timeout for socket writes. If reached, commands will fail
	WriteTimeout time.Duration `mapstructure:"writeTimeout"`
	// Maximum number of socket connections.
	PoolSize int `mapstructure:"poolSize"`
	// Minimum number of idle connections which is useful when establishing
	// new connection is slow.
	MinIdleConns int `mapstructure:"minIdleConns"`
	// Connection age at which client retires (closes) the connection.
	// Default is to not close aged connections.
	MaxConnAge time.Duration `mapstructure:"maxConnAge"`
	// Amount of time client waits for connection if all connections
	// are busy before returning an error.
	// Default is ReadTimeout + 1 second.
	PoolTimeout time.Duration `mapstructure:"poolTimeout"`
	// Amount of time after which client closes idle connections.
	// Should be less than server's timeout.
	// Default is 5 minutes. -1 disables idle timeout check.
	IdleTimeout time.Duration `mapstructure:"idleTimeout"`
	// Frequency of idle checks made by idle connections reaper.
	// Default is 1 minute. -1 disables idle connections reaper,
	// but idle connections are still discarded by the client
	// if IdleTimeout is set.
	IdleCheckFrequency time.Duration `mapstructure:"idleCheckFrequency"`
	// The master name
	SentinelMasterName string `mapstructure:"sentinelMasterName"`
	// Use Redis Sentinel for automatic failover.
	Failover bool `mapstructure:"failover"`

	// A flag to enables TLS by setting InsecureSkipVerify to true
	EnableTLS bool `mapstructure:"enableTLS"`
}

func (s *Settings) Decode(in interface{}) error {
	if err := config.Decode(in, s); err != nil {
		return fmt.Errorf("decode failed. %w", err)
	}

	return nil
}
