// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package redis

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func getFakeProperties() map[string]string {
	return map[string]string{
		host:                  "fake.redis.com",
		password:              "fakePassword",
		redisType:             "node",
		enableTLS:             "true",
		dialTimeout:           "5s",
		readTimeout:           "5s",
		writeTimeout:          "50000",
		poolSize:              "20",
		maxConnAge:            "200s",
		db:                    "1",
		redisMaxRetries:       "1",
		redisMinRetryInterval: "8ms",
		redisMaxRetryInterval: "1s",
		minIdleConns:          "1",
		poolTimeout:           "1s",
		idleTimeout:           "1s",
		idleCheckFrequency:    "1s",
	}
}

func TestParseRedisMetadata(t *testing.T) {
	t.Run("ClientMetadata is correct", func(t *testing.T) {
		fakeProperties := getFakeProperties()

		// act
		m, err := parseRedisMetadata(fakeProperties)

		// assert
		assert.NoError(t, err)
		assert.Equal(t, fakeProperties[host], m.Host)
		assert.Equal(t, fakeProperties[password], m.password)
		assert.Equal(t, fakeProperties[redisType], m.redisType)
		assert.Equal(t, true, m.enableTLS)
		assert.Equal(t, 5*time.Second, m.dialTimeout)
		assert.Equal(t, 5*time.Second, m.ReadTimeout)
		assert.Equal(t, 50000*time.Millisecond, m.writeTimeout)
		assert.Equal(t, 20, m.poolSize)
		assert.Equal(t, 200*time.Second, m.maxConnAge)
		assert.Equal(t, 1, m.db)
		assert.Equal(t, 1, m.redisMaxRetries)
		assert.Equal(t, 8*time.Millisecond, m.redisMinRetryInterval)
		assert.Equal(t, 1*time.Second, m.redisMaxRetryInterval)
		assert.Equal(t, 1, m.minIdleConns)
		assert.Equal(t, 1*time.Second, m.poolTimeout)
		assert.Equal(t, 1*time.Second, m.idleTimeout)
		assert.Equal(t, 1*time.Second, m.idleCheckFrequency)
	})

	t.Run("host is not given", func(t *testing.T) {
		fakeProperties := getFakeProperties()

		fakeProperties[host] = ""

		// act
		m, err := parseRedisMetadata(fakeProperties)

		// assert
		assert.Error(t, errors.New("redis streams error: missing host address"), err)
		assert.Empty(t, m.Host)
		assert.Empty(t, m.password)
	})

	t.Run("check values can be set as -1", func(t *testing.T) {
		fakeProperties := getFakeProperties()

		fakeProperties[readTimeout] = "-1"
		fakeProperties[idleTimeout] = "-1"
		fakeProperties[idleCheckFrequency] = "-1"
		fakeProperties[redisMaxRetryInterval] = "-1"
		fakeProperties[redisMinRetryInterval] = "-1"

		// act
		m, err := parseRedisMetadata(fakeProperties)
		// assert
		assert.NoError(t, err)
		assert.True(t, m.ReadTimeout == -1)
		assert.True(t, m.idleTimeout == -1)
		assert.True(t, m.idleCheckFrequency == -1)
		assert.True(t, m.redisMaxRetryInterval == -1)
		assert.True(t, m.redisMinRetryInterval == -1)
	})
}
