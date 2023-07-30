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
	"strconv"
	"testing"
	"time"

	miniredis "github.com/alicebob/miniredis/v2"
	redis "github.com/go-redis/redis/v8"
	jsoniter "github.com/json-iterator/go"
	"github.com/stretchr/testify/assert"

	rediscomponent "github.com/dapr/components-contrib/internal/component/redis"
	"github.com/dapr/components-contrib/state"
	"github.com/dapr/kit/logger"
	"github.com/dapr/kit/ptr"
)

func TestGetKeyVersion(t *testing.T) {
	store := newStateStore(logger.NewLogger("test"))
	t.Run("With all required fields", func(t *testing.T) {
		key, ver, err := store.getKeyVersion([]interface{}{"data", "TEST_KEY", "version", "TEST_VER"})
		assert.NoError(t, err, "failed to read all fields")
		assert.Equal(t, "TEST_KEY", key, "failed to read key")
		assert.Equal(t, ptr.Of("TEST_VER"), ver, "failed to read version")
	})
	t.Run("With missing data", func(t *testing.T) {
		_, _, err := store.getKeyVersion([]interface{}{"version", "TEST_VER"})
		assert.NotNil(t, err, "failed to respond to missing data field")
	})
	t.Run("With missing version", func(t *testing.T) {
		_, _, err := store.getKeyVersion([]interface{}{"data", "TEST_KEY"})
		assert.NotNil(t, err, "failed to respond to missing version field")
	})
	t.Run("With all required fields - out of order", func(t *testing.T) {
		key, ver, err := store.getKeyVersion([]interface{}{"version", "TEST_VER", "dragon", "TEST_DRAGON", "data", "TEST_KEY"})
		assert.NoError(t, err, "failed to read all fields")
		assert.Equal(t, "TEST_KEY", key, "failed to read key")
		assert.Equal(t, ptr.Of("TEST_VER"), ver, "failed to read version")
	})
	t.Run("With no fields", func(t *testing.T) {
		_, _, err := store.getKeyVersion([]interface{}{})
		assert.NotNil(t, err, "failed to respond to missing fields")
	})
	t.Run("With wrong fields", func(t *testing.T) {
		_, _, err := store.getKeyVersion([]interface{}{"dragon", "TEST_DRAGON"})
		assert.NotNil(t, err, "failed to respond to missing fields")
	})
}

func TestParseEtag(t *testing.T) {
	store := newStateStore(logger.NewLogger("test"))
	t.Run("Empty ETag", func(t *testing.T) {
		etag := ""
		ver, err := store.parseETag(&state.SetRequest{
			ETag: &etag,
		})
		assert.NoError(t, err, "failed to parse ETag")
		assert.Equal(t, 0, ver, "default version should be 0")
	})
	t.Run("Number ETag", func(t *testing.T) {
		etag := "354"
		ver, err := store.parseETag(&state.SetRequest{
			ETag: &etag,
		})
		assert.NoError(t, err, "failed to parse ETag")
		assert.Equal(t, 354, ver, "version should be 254")
	})
	t.Run("String ETag", func(t *testing.T) {
		etag := "dragon"
		_, err := store.parseETag(&state.SetRequest{
			ETag: &etag,
		})
		assert.NotNil(t, err, "shouldn't recognize string ETag")
	})
	t.Run("Concurrency=LastWrite", func(t *testing.T) {
		etag := "dragon"
		ver, err := store.parseETag(&state.SetRequest{
			Options: state.SetStateOption{
				Concurrency: state.LastWrite,
			},
			ETag: &etag,
		})
		assert.NoError(t, err, "failed to parse ETag")
		assert.Equal(t, 0, ver, "version should be 0")
	})
	t.Run("Concurrency=FirstWrite", func(t *testing.T) {
		ver, err := store.parseETag(&state.SetRequest{
			Options: state.SetStateOption{
				Concurrency: state.FirstWrite,
			},
		})
		assert.NoError(t, err, "failed to parse Concurrency")
		assert.Equal(t, 0, ver, "version should be 0")

		// ETag is nil
		req := &state.SetRequest{
			Options: state.SetStateOption{},
		}
		ver, err = store.parseETag(req)
		assert.NoError(t, err, "failed to parse Concurrency")
		assert.Equal(t, 0, ver, "version should be 0")

		// ETag is empty
		emptyString := ""
		req = &state.SetRequest{
			ETag: &emptyString,
		}
		ver, err = store.parseETag(req)
		assert.NoError(t, err, "failed to parse Concurrency")
		assert.Equal(t, 0, ver, "version should be 0")
	})
}

func TestParseTTL(t *testing.T) {
	store := newStateStore(logger.NewLogger("test"))
	t.Run("TTL Not an integer", func(t *testing.T) {
		ttlInSeconds := "not an integer"
		ttl, err := store.parseTTL(&state.SetRequest{
			Metadata: map[string]string{
				"ttlInSeconds": ttlInSeconds,
			},
		})
		assert.Error(t, err)
		assert.Nil(t, ttl)
	})
	t.Run("TTL specified with wrong key", func(t *testing.T) {
		ttlInSeconds := 12345
		ttl, err := store.parseTTL(&state.SetRequest{
			Metadata: map[string]string{
				"expirationTime": strconv.Itoa(ttlInSeconds),
			},
		})
		assert.NoError(t, err)
		assert.Nil(t, ttl)
	})
	t.Run("TTL is a number", func(t *testing.T) {
		ttlInSeconds := 12345
		ttl, err := store.parseTTL(&state.SetRequest{
			Metadata: map[string]string{
				"ttlInSeconds": strconv.Itoa(ttlInSeconds),
			},
		})
		assert.NoError(t, err)
		assert.Equal(t, *ttl, ttlInSeconds)
	})

	t.Run("TTL never expires", func(t *testing.T) {
		ttlInSeconds := -1
		ttl, err := store.parseTTL(&state.SetRequest{
			Metadata: map[string]string{
				"ttlInSeconds": strconv.Itoa(ttlInSeconds),
			},
		})
		assert.NoError(t, err)
		assert.Equal(t, *ttl, ttlInSeconds)
	})
}

func TestParseConnectedSlavs(t *testing.T) {
	store := newStateStore(logger.NewLogger("test"))

	t.Run("Empty info", func(t *testing.T) {
		slaves := store.parseConnectedSlaves("")
		assert.Equal(t, 0, slaves, "connected slaves must be 0")
	})

	t.Run("connectedSlaves property is not included", func(t *testing.T) {
		slaves := store.parseConnectedSlaves("# Replication\r\nrole:master\r\n")
		assert.Equal(t, 0, slaves, "connected slaves must be 0")
	})

	t.Run("connectedSlaves is 2", func(t *testing.T) {
		slaves := store.parseConnectedSlaves("# Replication\r\nrole:master\r\nconnected_slaves:2\r\n")
		assert.Equal(t, 2, slaves, "connected slaves must be 2")
	})

	t.Run("connectedSlaves is 1", func(t *testing.T) {
		slaves := store.parseConnectedSlaves("# Replication\r\nrole:master\r\nconnected_slaves:1")
		assert.Equal(t, 1, slaves, "connected slaves must be 1")
	})
}

func TestTransactionalUpsert(t *testing.T) {
	s, c := setupMiniredis()
	defer s.Close()

	ss := &StateStore{
		client:         c,
		clientSettings: &rediscomponent.Settings{},
		json:           jsoniter.ConfigFastest,
		logger:         logger.NewLogger("test"),
	}

	err := ss.Multi(context.Background(), &state.TransactionalStateRequest{
		Operations: []state.TransactionalStateOperation{
			state.SetRequest{
				Key:   "weapon",
				Value: "deathstar",
			},
			state.SetRequest{
				Key:   "weapon2",
				Value: "deathstar2",
				Metadata: map[string]string{
					"ttlInSeconds": "123",
				},
			},
			state.SetRequest{
				Key:   "weapon3",
				Value: "deathstar3",
				Metadata: map[string]string{
					"ttlInSeconds": "-1",
				},
			},
		},
	})
	assert.NoError(t, err)

	res, err := c.DoRead(context.Background(), "HGETALL", "weapon")
	assert.NoError(t, err)

	vals := res.([]interface{})
	data, version, err := ss.getKeyVersion(vals)
	assert.NoError(t, err)
	assert.Equal(t, ptr.Of("1"), version)
	assert.Equal(t, `"deathstar"`, data)

	res, err = c.DoRead(context.Background(), "TTL", "weapon")
	assert.NoError(t, err)
	assert.Equal(t, int64(-1), res)

	res, err = c.DoRead(context.Background(), "TTL", "weapon2")
	assert.NoError(t, err)
	assert.Equal(t, int64(123), res)

	res, err = c.DoRead(context.Background(), "TTL", "weapon3")
	assert.NoError(t, err)
	assert.Equal(t, int64(-1), res)
}

func TestTransactionalDelete(t *testing.T) {
	s, c := setupMiniredis()
	defer s.Close()

	ss := &StateStore{
		client:         c,
		clientSettings: &rediscomponent.Settings{},
		json:           jsoniter.ConfigFastest,
		logger:         logger.NewLogger("test"),
	}

	// Insert a record first.
	ss.Set(context.Background(), &state.SetRequest{
		Key:   "weapon",
		Value: "deathstar",
	})

	etag := "1"
	err := ss.Multi(context.Background(), &state.TransactionalStateRequest{
		Operations: []state.TransactionalStateOperation{
			state.DeleteRequest{
				Key:  "weapon",
				ETag: &etag,
			},
		},
	})
	assert.NoError(t, err)

	res, err := c.DoRead(context.Background(), "HGETALL", "weapon")
	assert.NoError(t, err)

	vals := res.([]interface{})
	assert.Equal(t, 0, len(vals))
}

func TestPing(t *testing.T) {
	s, c := setupMiniredis()

	ss := &StateStore{
		client:         c,
		json:           jsoniter.ConfigFastest,
		logger:         logger.NewLogger("test"),
		clientSettings: &rediscomponent.Settings{},
	}

	err := ss.Ping(context.Background())
	assert.NoError(t, err)

	s.Close()

	err = ss.Ping(context.Background())
	assert.Error(t, err)
}

func TestRequestsWithGlobalTTL(t *testing.T) {
	s, c := setupMiniredis()
	defer s.Close()

	globalTTLInSeconds := 100

	ss := &StateStore{
		client:         c,
		json:           jsoniter.ConfigFastest,
		logger:         logger.NewLogger("test"),
		clientSettings: &rediscomponent.Settings{TTLInSeconds: &globalTTLInSeconds},
	}

	t.Run("TTL: Only global specified", func(t *testing.T) {
		ss.Set(context.Background(), &state.SetRequest{
			Key:   "weapon100",
			Value: "deathstar100",
		})
		ttl, _ := ss.client.TTLResult(context.Background(), "weapon100")

		assert.Equal(t, time.Duration(globalTTLInSeconds)*time.Second, ttl)
	})

	t.Run("TTL: Global and Request specified", func(t *testing.T) {
		requestTTL := 200
		ss.Set(context.Background(), &state.SetRequest{
			Key:   "weapon100",
			Value: "deathstar100",
			Metadata: map[string]string{
				"ttlInSeconds": strconv.Itoa(requestTTL),
			},
		})
		ttl, _ := ss.client.TTLResult(context.Background(), "weapon100")

		assert.Equal(t, time.Duration(requestTTL)*time.Second, ttl)
	})

	t.Run("TTL: Global and Request specified", func(t *testing.T) {
		err := ss.Multi(context.Background(), &state.TransactionalStateRequest{
			Operations: []state.TransactionalStateOperation{
				state.SetRequest{
					Key:   "weapon",
					Value: "deathstar",
				},
				state.SetRequest{
					Key:   "weapon2",
					Value: "deathstar2",
					Metadata: map[string]string{
						"ttlInSeconds": "123",
					},
				},
				state.SetRequest{
					Key:   "weapon3",
					Value: "deathstar3",
					Metadata: map[string]string{
						"ttlInSeconds": "-1",
					},
				},
			},
		})
		assert.NoError(t, err)

		res, err := c.DoRead(context.Background(), "HGETALL", "weapon")
		assert.NoError(t, err)

		vals := res.([]interface{})
		data, version, err := ss.getKeyVersion(vals)
		assert.NoError(t, err)
		assert.Equal(t, ptr.Of("1"), version)
		assert.Equal(t, `"deathstar"`, data)

		res, err = c.DoRead(context.Background(), "TTL", "weapon")
		assert.NoError(t, err)
		assert.Equal(t, int64(globalTTLInSeconds), res)

		res, err = c.DoRead(context.Background(), "TTL", "weapon2")
		assert.NoError(t, err)
		assert.Equal(t, int64(123), res)

		res, err = c.DoRead(context.Background(), "TTL", "weapon3")
		assert.NoError(t, err)
		assert.Equal(t, int64(-1), res)
	})
}

func TestSetRequestWithTTL(t *testing.T) {
	s, c := setupMiniredis()
	defer s.Close()

	ss := &StateStore{
		client:         c,
		clientSettings: &rediscomponent.Settings{},
		json:           jsoniter.ConfigFastest,
		logger:         logger.NewLogger("test"),
	}

	t.Run("TTL specified", func(t *testing.T) {
		ttlInSeconds := 100
		ss.Set(context.Background(), &state.SetRequest{
			Key:   "weapon100",
			Value: "deathstar100",
			Metadata: map[string]string{
				"ttlInSeconds": strconv.Itoa(ttlInSeconds),
			},
		})

		ttl, _ := ss.client.TTLResult(context.Background(), "weapon100")

		assert.Equal(t, time.Duration(ttlInSeconds)*time.Second, ttl)
	})

	t.Run("TTL not specified", func(t *testing.T) {
		ss.Set(context.Background(), &state.SetRequest{
			Key:   "weapon200",
			Value: "deathstar200",
		})

		ttl, _ := ss.client.TTLResult(context.Background(), "weapon200")

		assert.Equal(t, time.Duration(-1), ttl)
	})

	t.Run("TTL Changed for Existing Key", func(t *testing.T) {
		ss.Set(context.Background(), &state.SetRequest{
			Key:   "weapon300",
			Value: "deathstar300",
		})
		ttl, _ := ss.client.TTLResult(context.Background(), "weapon300")
		assert.Equal(t, time.Duration(-1), ttl)

		// make the key no longer persistent
		ttlInSeconds := 123
		ss.Set(context.Background(), &state.SetRequest{
			Key:   "weapon300",
			Value: "deathstar300",
			Metadata: map[string]string{
				"ttlInSeconds": strconv.Itoa(ttlInSeconds),
			},
		})
		ttl, _ = ss.client.TTLResult(context.Background(), "weapon300")
		assert.Equal(t, time.Duration(ttlInSeconds)*time.Second, ttl)

		// make the key persistent again
		ss.Set(context.Background(), &state.SetRequest{
			Key:   "weapon300",
			Value: "deathstar301",
			Metadata: map[string]string{
				"ttlInSeconds": strconv.Itoa(-1),
			},
		})
		ttl, _ = ss.client.TTLResult(context.Background(), "weapon300")
		assert.Equal(t, time.Duration(-1), ttl)
	})
}

func TestTransactionalDeleteNoEtag(t *testing.T) {
	s, c := setupMiniredis()
	defer s.Close()

	ss := &StateStore{
		client:         c,
		clientSettings: &rediscomponent.Settings{},
		json:           jsoniter.ConfigFastest,
		logger:         logger.NewLogger("test"),
	}

	// Insert a record first.
	ss.Set(context.Background(), &state.SetRequest{
		Key:   "weapon100",
		Value: "deathstar100",
	})

	err := ss.Multi(context.Background(), &state.TransactionalStateRequest{
		Operations: []state.TransactionalStateOperation{
			state.DeleteRequest{
				Key: "weapon100",
			},
		},
	})
	assert.NoError(t, err)

	res, err := c.DoRead(context.Background(), "HGETALL", "weapon100")
	assert.NoError(t, err)

	vals := res.([]interface{})
	assert.Equal(t, 0, len(vals))
}

func TestGetMetadata(t *testing.T) {
	s, c := setupMiniredis()
	defer s.Close()

	ss := &StateStore{
		client: c,
		json:   jsoniter.ConfigFastest,
		logger: logger.NewLogger("test"),
	}

	metadataInfo := ss.GetComponentMetadata()
	assert.Contains(t, metadataInfo, "redisHost")
	assert.Contains(t, metadataInfo, "idleCheckFrequency")
}

func setupMiniredis() (*miniredis.Miniredis, rediscomponent.RedisClient) {
	s, err := miniredis.Run()
	if err != nil {
		panic(err)
	}
	opts := &redis.Options{
		Addr: s.Addr(),
		DB:   defaultDB,
	}

	return s, rediscomponent.ClientFromV8Client(redis.NewClient(opts))
}
