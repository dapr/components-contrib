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
	"strconv"
	"testing"
	"time"

	miniredis "github.com/alicebob/miniredis/v2"
	redis "github.com/go-redis/redis/v8"
	jsoniter "github.com/json-iterator/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	rediscomponent "github.com/dapr/components-contrib/common/component/redis"
	"github.com/dapr/components-contrib/state"
	"github.com/dapr/kit/logger"
	"github.com/dapr/kit/ptr"
)

func TestGetKeyVersion(t *testing.T) {
	store := newStateStore(logger.NewLogger("test"))
	t.Run("With all required fields", func(t *testing.T) {
		key, ver, err := store.getKeyVersion([]interface{}{"data", "TEST_KEY", "version", "TEST_VER"})
		require.NoError(t, err, "failed to read all fields")
		assert.Equal(t, "TEST_KEY", key, "failed to read key")
		assert.Equal(t, ptr.Of("TEST_VER"), ver, "failed to read version")
	})
	t.Run("With missing data", func(t *testing.T) {
		_, _, err := store.getKeyVersion([]interface{}{"version", "TEST_VER"})
		require.Error(t, err, "failed to respond to missing data field")
	})
	t.Run("With missing version", func(t *testing.T) {
		_, _, err := store.getKeyVersion([]interface{}{"data", "TEST_KEY"})
		require.Error(t, err, "failed to respond to missing version field")
	})
	t.Run("With all required fields - out of order", func(t *testing.T) {
		key, ver, err := store.getKeyVersion([]interface{}{"version", "TEST_VER", "dragon", "TEST_DRAGON", "data", "TEST_KEY"})
		require.NoError(t, err, "failed to read all fields")
		assert.Equal(t, "TEST_KEY", key, "failed to read key")
		assert.Equal(t, ptr.Of("TEST_VER"), ver, "failed to read version")
	})
	t.Run("With no fields", func(t *testing.T) {
		_, _, err := store.getKeyVersion([]interface{}{})
		require.Error(t, err, "failed to respond to missing fields")
	})
	t.Run("With wrong fields", func(t *testing.T) {
		_, _, err := store.getKeyVersion([]interface{}{"dragon", "TEST_DRAGON"})
		require.Error(t, err, "failed to respond to missing fields")
	})
}

func TestParseEtag(t *testing.T) {
	store := newStateStore(logger.NewLogger("test"))
	t.Run("Empty ETag", func(t *testing.T) {
		etag := ""
		ver, err := store.parseETag(&state.SetRequest{
			ETag: &etag,
		})
		require.NoError(t, err, "failed to parse ETag")
		assert.Equal(t, 0, ver, "default version should be 0")
	})
	t.Run("Number ETag", func(t *testing.T) {
		etag := "354"
		ver, err := store.parseETag(&state.SetRequest{
			ETag: &etag,
		})
		require.NoError(t, err, "failed to parse ETag")
		assert.Equal(t, 354, ver, "version should be 254")
	})
	t.Run("String ETag", func(t *testing.T) {
		etag := "dragon"
		_, err := store.parseETag(&state.SetRequest{
			ETag: &etag,
		})
		require.Error(t, err, "shouldn't recognize string ETag")
	})
	t.Run("Concurrency=LastWrite", func(t *testing.T) {
		etag := "dragon"
		ver, err := store.parseETag(&state.SetRequest{
			Options: state.SetStateOption{
				Concurrency: state.LastWrite,
			},
			ETag: &etag,
		})
		require.NoError(t, err, "failed to parse ETag")
		assert.Equal(t, 0, ver, "version should be 0")
	})
	t.Run("Concurrency=FirstWrite", func(t *testing.T) {
		ver, err := store.parseETag(&state.SetRequest{
			Options: state.SetStateOption{
				Concurrency: state.FirstWrite,
			},
		})
		require.NoError(t, err, "failed to parse Concurrency")
		assert.Equal(t, 0, ver, "version should be 0")

		// ETag is nil
		req := &state.SetRequest{
			Options: state.SetStateOption{},
		}
		ver, err = store.parseETag(req)
		require.NoError(t, err, "failed to parse Concurrency")
		assert.Equal(t, 0, ver, "version should be 0")

		// ETag is empty
		emptyString := ""
		req = &state.SetRequest{
			ETag: &emptyString,
		}
		ver, err = store.parseETag(req)
		require.NoError(t, err, "failed to parse Concurrency")
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
		require.Error(t, err)
		assert.Nil(t, ttl)
	})
	t.Run("TTL specified with wrong key", func(t *testing.T) {
		ttlInSeconds := 12345
		ttl, err := store.parseTTL(&state.SetRequest{
			Metadata: map[string]string{
				"expirationTime": strconv.Itoa(ttlInSeconds),
			},
		})
		require.NoError(t, err)
		assert.Nil(t, ttl)
	})
	t.Run("TTL is a number", func(t *testing.T) {
		ttlInSeconds := 12345
		ttl, err := store.parseTTL(&state.SetRequest{
			Metadata: map[string]string{
				"ttlInSeconds": strconv.Itoa(ttlInSeconds),
			},
		})
		require.NoError(t, err)
		assert.Equal(t, *ttl, ttlInSeconds)
	})

	t.Run("TTL never expires", func(t *testing.T) {
		ttlInSeconds := -1
		ttl, err := store.parseTTL(&state.SetRequest{
			Metadata: map[string]string{
				"ttlInSeconds": strconv.Itoa(ttlInSeconds),
			},
		})
		require.NoError(t, err)
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

	err := ss.Multi(t.Context(), &state.TransactionalStateRequest{
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
	require.NoError(t, err)

	res, err := c.DoRead(t.Context(), "HGETALL", "weapon")
	require.NoError(t, err)

	vals := res.([]interface{})
	data, version, err := ss.getKeyVersion(vals)
	require.NoError(t, err)
	assert.Equal(t, ptr.Of("1"), version)
	assert.Equal(t, `"deathstar"`, data)

	res, err = c.DoRead(t.Context(), "TTL", "weapon")
	require.NoError(t, err)
	assert.Equal(t, int64(-1), res)

	res, err = c.DoRead(t.Context(), "TTL", "weapon2")
	require.NoError(t, err)
	assert.Equal(t, int64(123), res)

	res, err = c.DoRead(t.Context(), "TTL", "weapon3")
	require.NoError(t, err)
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
	ss.Set(t.Context(), &state.SetRequest{
		Key:   "weapon",
		Value: "deathstar",
	})

	etag := "1"
	err := ss.Multi(t.Context(), &state.TransactionalStateRequest{
		Operations: []state.TransactionalStateOperation{
			state.DeleteRequest{
				Key:  "weapon",
				ETag: &etag,
			},
		},
	})
	require.NoError(t, err)

	res, err := c.DoRead(t.Context(), "HGETALL", "weapon")
	require.NoError(t, err)

	vals := res.([]interface{})
	assert.Empty(t, vals)
}

func TestPing(t *testing.T) {
	s, c := setupMiniredis()

	ss := &StateStore{
		client:         c,
		json:           jsoniter.ConfigFastest,
		logger:         logger.NewLogger("test"),
		clientSettings: &rediscomponent.Settings{},
	}

	err := ss.Ping(t.Context())
	require.NoError(t, err)

	s.Close()

	err = ss.Ping(t.Context())
	require.Error(t, err)
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
		ss.Set(t.Context(), &state.SetRequest{
			Key:   "weapon100",
			Value: "deathstar100",
		})
		ttl, _ := ss.client.TTLResult(t.Context(), "weapon100")

		assert.Equal(t, time.Duration(globalTTLInSeconds)*time.Second, ttl)
	})

	t.Run("TTL: Global and Request specified", func(t *testing.T) {
		requestTTL := 200
		ss.Set(t.Context(), &state.SetRequest{
			Key:   "weapon100",
			Value: "deathstar100",
			Metadata: map[string]string{
				"ttlInSeconds": strconv.Itoa(requestTTL),
			},
		})
		ttl, _ := ss.client.TTLResult(t.Context(), "weapon100")

		assert.Equal(t, time.Duration(requestTTL)*time.Second, ttl)
	})

	t.Run("TTL: Global and Request specified", func(t *testing.T) {
		err := ss.Multi(t.Context(), &state.TransactionalStateRequest{
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
		require.NoError(t, err)

		res, err := c.DoRead(t.Context(), "HGETALL", "weapon")
		require.NoError(t, err)

		vals := res.([]interface{})
		data, version, err := ss.getKeyVersion(vals)
		require.NoError(t, err)
		assert.Equal(t, ptr.Of("1"), version)
		assert.Equal(t, `"deathstar"`, data)

		res, err = c.DoRead(t.Context(), "TTL", "weapon")
		require.NoError(t, err)
		assert.Equal(t, int64(globalTTLInSeconds), res)

		res, err = c.DoRead(t.Context(), "TTL", "weapon2")
		require.NoError(t, err)
		assert.Equal(t, int64(123), res)

		res, err = c.DoRead(t.Context(), "TTL", "weapon3")
		require.NoError(t, err)
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
		ss.Set(t.Context(), &state.SetRequest{
			Key:   "weapon100",
			Value: "deathstar100",
			Metadata: map[string]string{
				"ttlInSeconds": strconv.Itoa(ttlInSeconds),
			},
		})

		ttl, _ := ss.client.TTLResult(t.Context(), "weapon100")

		assert.Equal(t, time.Duration(ttlInSeconds)*time.Second, ttl)
	})

	t.Run("TTL not specified", func(t *testing.T) {
		ss.Set(t.Context(), &state.SetRequest{
			Key:   "weapon200",
			Value: "deathstar200",
		})

		ttl, _ := ss.client.TTLResult(t.Context(), "weapon200")

		assert.Equal(t, time.Duration(-1), ttl)
	})

	t.Run("TTL Changed for Existing Key", func(t *testing.T) {
		ss.Set(t.Context(), &state.SetRequest{
			Key:   "weapon300",
			Value: "deathstar300",
		})
		ttl, _ := ss.client.TTLResult(t.Context(), "weapon300")
		assert.Equal(t, time.Duration(-1), ttl)

		// make the key no longer persistent
		ttlInSeconds := 123
		ss.Set(t.Context(), &state.SetRequest{
			Key:   "weapon300",
			Value: "deathstar300",
			Metadata: map[string]string{
				"ttlInSeconds": strconv.Itoa(ttlInSeconds),
			},
		})
		ttl, _ = ss.client.TTLResult(t.Context(), "weapon300")
		assert.Equal(t, time.Duration(ttlInSeconds)*time.Second, ttl)

		// make the key persistent again
		ss.Set(t.Context(), &state.SetRequest{
			Key:   "weapon300",
			Value: "deathstar301",
			Metadata: map[string]string{
				"ttlInSeconds": strconv.Itoa(-1),
			},
		})
		ttl, _ = ss.client.TTLResult(t.Context(), "weapon300")
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
	ss.Set(t.Context(), &state.SetRequest{
		Key:   "weapon100",
		Value: "deathstar100",
	})

	err := ss.Multi(t.Context(), &state.TransactionalStateRequest{
		Operations: []state.TransactionalStateOperation{
			state.DeleteRequest{
				Key: "weapon100",
			},
		},
	})
	require.NoError(t, err)

	res, err := c.DoRead(t.Context(), "HGETALL", "weapon100")
	require.NoError(t, err)

	vals := res.([]interface{})
	assert.Empty(t, vals)
}

// TestGetMissingKeyReturnsEmptyNotError ensures that when a key does not exist
// we don't propagate 'redis: nil' as an error to users
func TestGetMissingKeyReturnsEmptyNotError(t *testing.T) {
	s, c := setupMiniredis()
	defer s.Close()

	ss := &StateStore{
		client: c,
		json:   jsoniter.ConfigFastest,
		logger: logger.NewLogger("test"),
	}

	resp, err := ss.Get(t.Context(), &state.GetRequest{Key: "nonexistent-key"})
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Empty(t, resp.Data, "missing key should return empty data")
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

func TestToString(t *testing.T) {
	// happy paths
	if s, ok := toString("abc"); assert.True(t, ok) {
		assert.Equal(t, "abc", s)
	}
	if s, ok := toString([]byte("def")); assert.True(t, ok) {
		assert.Equal(t, "def", s)
	}
	// unsupported
	_, ok := toString(123)
	assert.False(t, ok)
}

func BenchmarkGetKeyVersion(b *testing.B) {
	/*
		On a Mac M1 Pro:
		BenchmarkGetKeyVersion-10    	    13651144	        83.84 ns/op	      64 B/op	       6 allocs/op

		// old getkeyversion method
		BenchmarkGetKeyVersionOld-10    	 1631097	       729.1 ns/op	      96 B/op	      10 allocs/op

		// ~8x speed - ~1/2 allocations

		// unsafe comparison
		BenchmarkGetKeyVersion-10    	    28636363	        41.53 ns/op	      32 B/op	       2 allocs/op
	*/
	store := newStateStore(logger.NewLogger("bench"))
	input1 := []any{[]byte("data"), []byte("payload"), []byte("version"), []byte("42")}
	input2 := []any{[]byte("data"), []byte("payload2"), []byte("version"), []byte("43")}
	b.ReportAllocs()
	for range b.N {
		if _, _, err := store.getKeyVersion(input1); err != nil {
			b.Fatal(err)
		}
		if _, _, err := store.getKeyVersion(input2); err != nil {
			b.Fatal(err)
		}
	}
}

func Test_KeyList(t *testing.T) {
	s := NewRedisStateStore(logger.NewLogger("test"))
	_, ok := s.(state.KeysLiker)
	require.True(t, ok)
}

func TestMultiETags(t *testing.T) {
	newStore := func(t *testing.T) *StateStore {
		s, c := setupMiniredis()
		t.Cleanup(s.Close)
		return &StateStore{
			client:         c,
			clientSettings: &rediscomponent.Settings{},
			json:           jsoniter.ConfigFastest,
			logger:         logger.NewLogger("test"),
		}
	}
	get := func(t *testing.T, ss *StateStore, key string) *state.GetResponse {
		t.Helper()
		res, err := ss.Get(t.Context(), &state.GetRequest{Key: key})
		require.NoError(t, err)
		return res
	}
	requireKind := func(t *testing.T, err error, kind state.ETagErrorKind) {
		t.Helper()
		var etagErr *state.ETagError
		require.ErrorAs(t, err, &etagErr)
		assert.Equal(t, kind, etagErr.Kind())
	}
	multi := func(ss *StateStore, ops ...state.TransactionalStateOperation) error {
		return ss.Multi(t.Context(), &state.TransactionalStateRequest{Operations: ops})
	}
	firstWrite := state.SetStateOption{Concurrency: state.FirstWrite}

	t.Run("stale etag applies nothing", func(t *testing.T) {
		ss := newStore(t)
		require.NoError(t, ss.Set(t.Context(), &state.SetRequest{Key: "a", Value: "a1"}))
		require.NoError(t, ss.Set(t.Context(), &state.SetRequest{Key: "b", Value: "b1"}))
		err := multi(ss,
			state.SetRequest{Key: "a", Value: "a2"},
			state.DeleteRequest{Key: "b"},
			state.SetRequest{Key: "c", Value: "c1"},
			state.SetRequest{Key: "b", Value: "b2", ETag: ptr.Of("7"), Options: firstWrite},
		)
		requireKind(t, err, state.ETagMismatch)
		assert.JSONEq(t, `"a1"`, string(get(t, ss, "a").Data))
		assert.Equal(t, "1", *get(t, ss, "a").ETag)
		assert.JSONEq(t, `"b1"`, string(get(t, ss, "b").Data))
		assert.Nil(t, get(t, ss, "c").Data)
	})

	t.Run("stale delete applies nothing", func(t *testing.T) {
		ss := newStore(t)
		require.NoError(t, ss.Set(t.Context(), &state.SetRequest{Key: "a", Value: "a1"}))
		require.NoError(t, ss.Set(t.Context(), &state.SetRequest{Key: "a", Value: "a2"}))
		err := multi(ss,
			state.SetRequest{Key: "b", Value: "b1"},
			state.DeleteRequest{Key: "a", ETag: ptr.Of("1")},
		)
		requireKind(t, err, state.ETagMismatch)
		assert.JSONEq(t, `"a2"`, string(get(t, ss, "a").Data))
		assert.Nil(t, get(t, ss, "b").Data)
	})

	t.Run("etag on missing key mismatches", func(t *testing.T) {
		ss := newStore(t)
		requireKind(t, multi(ss, state.SetRequest{Key: "a", Value: "a1", ETag: ptr.Of("1"), Options: firstWrite}), state.ETagMismatch)
		assert.Nil(t, get(t, ss, "a").Data)
		requireKind(t, multi(ss, state.DeleteRequest{Key: "a", ETag: ptr.Of("1")}), state.ETagMismatch)

		require.NoError(t, ss.Set(t.Context(), &state.SetRequest{Key: "a", Value: "a1"}))
		requireKind(t, multi(ss, state.SetRequest{Key: "a", Value: "a2", ETag: ptr.Of("0"), Options: firstWrite}), state.ETagMismatch)
		assert.JSONEq(t, `"a1"`, string(get(t, ss, "a").Data))
	})

	t.Run("invalid etag", func(t *testing.T) {
		ss := newStore(t)
		requireKind(t, multi(ss, state.DeleteRequest{Key: "a", ETag: ptr.Of("abc")}), state.ETagInvalid)
		requireKind(t, multi(ss, state.SetRequest{Key: "a", Value: "a", ETag: ptr.Of("abc"), Options: firstWrite}), state.ETagInvalid)
	})

	t.Run("empty etag is last write wins", func(t *testing.T) {
		ss := newStore(t)
		require.NoError(t, ss.Set(t.Context(), &state.SetRequest{Key: "a", Value: "a1"}))
		require.NoError(t, ss.Set(t.Context(), &state.SetRequest{Key: "b", Value: "b1"}))
		require.NoError(t, multi(ss,
			state.SetRequest{Key: "a", Value: "a2"},
			state.SetRequest{Key: "c", Value: "c1"},
			state.DeleteRequest{Key: "b"},
			state.DeleteRequest{Key: "missing"},
			state.SetRequest{Key: "a", Value: "a3", ETag: ptr.Of("9"), Options: state.SetStateOption{Concurrency: state.LastWrite}},
		))
		assert.JSONEq(t, `"a3"`, string(get(t, ss, "a").Data))
		assert.Equal(t, "3", *get(t, ss, "a").ETag)
		assert.JSONEq(t, `"c1"`, string(get(t, ss, "c").Data))
		assert.Nil(t, get(t, ss, "b").Data)
	})

	t.Run("matching etags apply", func(t *testing.T) {
		ss := newStore(t)
		require.NoError(t, ss.Set(t.Context(), &state.SetRequest{Key: "a", Value: "a1"}))
		require.NoError(t, ss.Set(t.Context(), &state.SetRequest{Key: "b", Value: "b1"}))
		require.NoError(t, multi(ss,
			state.SetRequest{Key: "a", Value: "a2", ETag: ptr.Of("1"), Options: firstWrite},
			state.SetRequest{Key: "a", Value: "a3", ETag: ptr.Of("2"), Options: firstWrite},
			state.DeleteRequest{Key: "b", ETag: ptr.Of("1")},
		))
		assert.JSONEq(t, `"a3"`, string(get(t, ss, "a").Data))
		assert.Equal(t, "3", *get(t, ss, "a").ETag)
		assert.Nil(t, get(t, ss, "b").Data)
	})

	t.Run("first-write field still blocks unguarded writes", func(t *testing.T) {
		ss := newStore(t)
		require.NoError(t, ss.Set(t.Context(), &state.SetRequest{Key: "a", Value: "a1", Options: firstWrite}))
		requireKind(t, multi(ss, state.SetRequest{Key: "a", Value: "a2"}), state.ETagMismatch)
		require.NoError(t, multi(ss, state.SetRequest{Key: "a", Value: "a2", ETag: ptr.Of("1")}))
	})

	t.Run("non integer version applies nothing", func(t *testing.T) {
		ss := newStore(t)
		require.NoError(t, ss.client.DoWrite(t.Context(), "HSET", "b", "data", `"b1"`, "version", "5.5"))
		require.Error(t, multi(ss,
			state.SetRequest{Key: "a", Value: "a1"},
			state.SetRequest{Key: "b", Value: "b2"},
		))
		assert.Nil(t, get(t, ss, "a").Data)
	})

	t.Run("json content type without RedisJSON applies nothing", func(t *testing.T) {
		ss := newStore(t)
		jsonMD := map[string]string{"contentType": "application/json"}
		require.ErrorIs(t, multi(ss,
			state.SetRequest{Key: "a", Value: "a1"},
			state.SetRequest{Key: "b", Value: "b1", Metadata: jsonMD},
		), errJSONUnsupported)
		require.ErrorIs(t, multi(ss, state.DeleteRequest{Key: "a", Metadata: jsonMD}), errJSONUnsupported)
		require.ErrorIs(t, ss.Multi(t.Context(), &state.TransactionalStateRequest{
			Operations: []state.TransactionalStateOperation{state.SetRequest{Key: "a", Value: "a1"}},
			Metadata:   jsonMD,
		}), errJSONUnsupported)
		assert.Nil(t, get(t, ss, "a").Data)
	})

	t.Run("empty stored version counts as no version", func(t *testing.T) {
		ss := newStore(t)
		require.NoError(t, ss.client.DoWrite(t.Context(), "HSET", "bad", "data", `"x"`, "version", ""))
		requireKind(t, multi(ss,
			state.SetRequest{Key: "other", Value: "o1"},
			state.SetRequest{Key: "bad", Value: "b2", ETag: ptr.Of("0")},
		), state.ETagMismatch)
		assert.Nil(t, get(t, ss, "other").Data)
		assert.JSONEq(t, `"x"`, string(get(t, ss, "bad").Data))

		require.NoError(t, multi(ss,
			state.SetRequest{Key: "other", Value: "o1"},
			state.SetRequest{Key: "bad", Value: "b2"},
			state.SetRequest{Key: "bad", Value: "b3", ETag: ptr.Of("1")},
		))
		assert.JSONEq(t, `"o1"`, string(get(t, ss, "other").Data))
		res := get(t, ss, "bad")
		assert.JSONEq(t, `"b3"`, string(res.Data))
		assert.Equal(t, "2", *res.ETag)
	})

	t.Run("first-write in multi blocks later unguarded writes", func(t *testing.T) {
		ss := newStore(t)
		require.NoError(t, multi(ss, state.SetRequest{Key: "a", Value: "a1", Options: firstWrite}))
		requireKind(t, multi(ss, state.SetRequest{Key: "a", Value: "a2"}), state.ETagMismatch)
		require.Error(t, ss.Set(t.Context(), &state.SetRequest{Key: "a", Value: "a2"}))
		assert.JSONEq(t, `"a1"`, string(get(t, ss, "a").Data))

		require.NoError(t, multi(ss, state.SetRequest{Key: "a", Value: "a3", ETag: ptr.Of("1")}))
		assert.JSONEq(t, `"a3"`, string(get(t, ss, "a").Data))
	})

	t.Run("first-write then unguarded write on the same key applies nothing", func(t *testing.T) {
		ss := newStore(t)
		err := multi(ss,
			state.SetRequest{Key: "a", Value: "a1", Options: firstWrite},
			state.SetRequest{Key: "b", Value: "b1"},
			state.SetRequest{Key: "a", Value: "a2"},
		)
		requireKind(t, err, state.ETagMismatch)
		assert.Nil(t, get(t, ss, "a").Data)
		assert.Nil(t, get(t, ss, "b").Data)
	})

	t.Run("marshal error applies nothing", func(t *testing.T) {
		ss := newStore(t)
		err := multi(ss,
			state.SetRequest{Key: "a", Value: "a1"},
			state.SetRequest{Key: "b", Value: make(chan int)},
		)
		require.ErrorContains(t, err, "failed to marshal value for key b")
		assert.Nil(t, get(t, ss, "a").Data)
	})

	t.Run("ttl too large applies nothing", func(t *testing.T) {
		if strconv.IntSize < 64 {
			t.Skip("an int ttl can't exceed maxTTLSeconds on 32-bit")
		}
		ss := newStore(t)
		require.NoError(t, ss.Set(t.Context(), &state.SetRequest{Key: "a", Value: "a1"}))
		huge := map[string]string{"ttlInSeconds": strconv.FormatInt(maxTTLSeconds+1, 10)}
		err := multi(ss,
			state.SetRequest{Key: "a", Value: "a2"},
			state.SetRequest{Key: "b", Value: "b1", Metadata: huge},
		)
		require.ErrorContains(t, err, "larger than the maximum")
		assert.JSONEq(t, `"a1"`, string(get(t, ss, "a").Data))
		assert.Nil(t, get(t, ss, "b").Data)
	})

	t.Run("cluster cross slot falls back to per key scripts", func(t *testing.T) {
		ss := newStore(t)
		ss.clientSettings.RedisType = rediscomponent.ClusterType
		require.NoError(t, ss.Set(t.Context(), &state.SetRequest{Key: "b", Value: "b1"}))
		require.NotEqual(t, keySlot("a"), keySlot("b"))
		err := multi(ss,
			state.SetRequest{Key: "a", Value: "a1"},
			state.SetRequest{Key: "b", Value: "b2", ETag: ptr.Of("5")},
		)
		requireKind(t, err, state.ETagMismatch)
		// Documents the non-atomic cross slot fallback on purpose.
		assert.JSONEq(t, `"a1"`, string(get(t, ss, "a").Data), "cross slot fallback isn't atomic")

		err = multi(ss,
			state.SetRequest{Key: "{x}a", Value: "a1"},
			state.SetRequest{Key: "{x}b", Value: "b2", ETag: ptr.Of("5")},
		)
		requireKind(t, err, state.ETagMismatch)
		assert.Nil(t, get(t, ss, "{x}a").Data, "same slot runs as one script")
	})

	t.Run("cluster cross slot groups ops by slot", func(t *testing.T) {
		ss := newStore(t)
		ss.clientSettings.RedisType = rediscomponent.ClusterType
		require.Equal(t, keySlot("a"), keySlot("{a}x"))
		require.NotEqual(t, keySlot("a"), keySlot("b"))
		err := multi(ss,
			state.SetRequest{Key: "a", Value: "a1"},
			state.SetRequest{Key: "b", Value: "b1"},
			state.SetRequest{Key: "{a}x", Value: "x1", ETag: ptr.Of("5")},
		)
		requireKind(t, err, state.ETagMismatch)
		assert.Nil(t, get(t, ss, "a").Data)
		assert.Nil(t, get(t, ss, "b").Data)

		require.NoError(t, multi(ss,
			state.SetRequest{Key: "a", Value: "a1"},
			state.SetRequest{Key: "b", Value: "b1"},
			state.SetRequest{Key: "a", Value: "a2", ETag: ptr.Of("1")},
		))
		res := get(t, ss, "a")
		assert.JSONEq(t, `"a2"`, string(res.Data))
		assert.Equal(t, "2", *res.ETag)
	})

	t.Run("version too large applies nothing", func(t *testing.T) {
		for _, v := range []string{"99999999999999", "9223372036854775807"} {
			ss := newStore(t)
			require.NoError(t, ss.client.DoWrite(t.Context(), "HSET", "b", "data", `"b1"`, "version", v))
			require.Error(t, multi(ss,
				state.SetRequest{Key: "a", Value: "a1"},
				state.SetRequest{Key: "b", Value: "b2"},
			))
			require.Error(t, multi(ss,
				state.SetRequest{Key: "a", Value: "a1"},
				state.SetRequest{Key: "b", Value: "b2", ETag: ptr.Of(v)},
			))
			assert.Nil(t, get(t, ss, "a").Data)
		}
	})
}

func TestGroupBySlot(t *testing.T) {
	ops := []multiOp{{key: "a"}, {key: "b"}, {key: "{a}x"}, {key: "a"}}
	assert.Equal(t, [][]multiOp{{{key: "a"}, {key: "{a}x"}, {key: "a"}}, {{key: "b"}}}, groupBySlot(ops))
}

func TestKeySlot(t *testing.T) {
	assert.Equal(t, uint16(12182), keySlot("foo"))
	assert.Equal(t, uint16(5061), keySlot("bar"))
	assert.Equal(t, uint16(12739), keySlot("123456789"))
	assert.Equal(t, keySlot("user1000"), keySlot("{user1000}.following"))
	assert.NotEqual(t, keySlot("x"), keySlot("{}x"))
	assert.Equal(t, keySlot("bar"), keySlot("foo{bar}{zap}"))
}

func TestSingleOpETags(t *testing.T) {
	newStore := func(t *testing.T) (*StateStore, *miniredis.Miniredis) {
		s, c := setupMiniredis()
		t.Cleanup(s.Close)
		ss := &StateStore{
			client:         c,
			clientSettings: &rediscomponent.Settings{},
			json:           jsoniter.ConfigFastest,
			logger:         logger.NewLogger("test"),
		}
		ss.BulkStore = state.NewDefaultBulkStore(ss)
		return ss, s
	}
	requireKind := func(t *testing.T, err error, kind state.ETagErrorKind) {
		t.Helper()
		var etagErr *state.ETagError
		require.ErrorAs(t, err, &etagErr)
		assert.Equal(t, kind, etagErr.Kind())
	}
	set := func(ss *StateStore, req state.SetRequest) error {
		return ss.Set(t.Context(), &req)
	}
	del := func(ss *StateStore, req state.DeleteRequest) error {
		return ss.Delete(t.Context(), &req)
	}
	bulkSet := func(ss *StateStore, req state.SetRequest) error {
		return ss.BulkSet(t.Context(), []state.SetRequest{req}, state.BulkStoreOpts{})
	}
	bulkDel := func(ss *StateStore, req state.DeleteRequest) error {
		return ss.BulkDelete(t.Context(), []state.DeleteRequest{req}, state.BulkStoreOpts{})
	}

	for name, setFn := range map[string]func(*StateStore, state.SetRequest) error{"set": set, "bulk set": bulkSet} {
		t.Run(name, func(t *testing.T) {
			t.Run("etag on a missing key mismatches", func(t *testing.T) {
				ss, s := newStore(t)
				requireKind(t, setFn(ss, state.SetRequest{Key: "a", Value: "v", ETag: ptr.Of("5")}), state.ETagMismatch)
				assert.False(t, s.Exists("a"))
			})
			t.Run("etag 0 on an existing key mismatches", func(t *testing.T) {
				ss, s := newStore(t)
				require.NoError(t, set(ss, state.SetRequest{Key: "a", Value: "v"}))
				requireKind(t, setFn(ss, state.SetRequest{Key: "a", Value: "w", ETag: ptr.Of("0")}), state.ETagMismatch)
				assert.JSONEq(t, `"v"`, s.HGet("a", "data"))
			})
			t.Run("etag on a hash with no version mismatches", func(t *testing.T) {
				ss, s := newStore(t)
				s.HSet("a", "data", `"v"`)
				requireKind(t, setFn(ss, state.SetRequest{Key: "a", Value: "w", ETag: ptr.Of("1")}), state.ETagMismatch)
				assert.JSONEq(t, `"v"`, s.HGet("a", "data"))
			})
			t.Run("invalid etag", func(t *testing.T) {
				ss, s := newStore(t)
				requireKind(t, setFn(ss, state.SetRequest{Key: "a", Value: "v", ETag: ptr.Of("x")}), state.ETagInvalid)
				assert.False(t, s.Exists("a"))
			})
			t.Run("matching etag applies", func(t *testing.T) {
				ss, s := newStore(t)
				require.NoError(t, set(ss, state.SetRequest{Key: "a", Value: "v"}))
				require.NoError(t, setFn(ss, state.SetRequest{Key: "a", Value: "w", ETag: ptr.Of("1")}))
				assert.JSONEq(t, `"w"`, s.HGet("a", "data"))
				assert.Equal(t, "2", s.HGet("a", "version"))
			})
			t.Run("first-write blocks a later unguarded write", func(t *testing.T) {
				ss, s := newStore(t)
				require.NoError(t, setFn(ss, state.SetRequest{Key: "a", Value: "v", Options: state.SetStateOption{Concurrency: state.FirstWrite}}))
				requireKind(t, setFn(ss, state.SetRequest{Key: "a", Value: "w"}), state.ETagMismatch)
				assert.JSONEq(t, `"v"`, s.HGet("a", "data"))
			})
		})
	}

	for name, delFn := range map[string]func(*StateStore, state.DeleteRequest) error{"delete": del, "bulk delete": bulkDel} {
		t.Run(name, func(t *testing.T) {
			t.Run("etag on a missing key mismatches", func(t *testing.T) {
				ss, _ := newStore(t)
				requireKind(t, delFn(ss, state.DeleteRequest{Key: "a", ETag: ptr.Of("5")}), state.ETagMismatch)
			})
			t.Run("etag 0 on an existing key mismatches", func(t *testing.T) {
				ss, s := newStore(t)
				require.NoError(t, set(ss, state.SetRequest{Key: "a", Value: "v"}))
				requireKind(t, delFn(ss, state.DeleteRequest{Key: "a", ETag: ptr.Of("0")}), state.ETagMismatch)
				assert.True(t, s.Exists("a"))
			})
			t.Run("etag on a hash with no version mismatches", func(t *testing.T) {
				ss, s := newStore(t)
				s.HSet("a", "data", `"v"`)
				requireKind(t, delFn(ss, state.DeleteRequest{Key: "a", ETag: ptr.Of("1")}), state.ETagMismatch)
				assert.True(t, s.Exists("a"))
			})
			t.Run("invalid etag", func(t *testing.T) {
				ss, s := newStore(t)
				require.NoError(t, set(ss, state.SetRequest{Key: "a", Value: "v"}))
				requireKind(t, delFn(ss, state.DeleteRequest{Key: "a", ETag: ptr.Of("x")}), state.ETagInvalid)
				assert.True(t, s.Exists("a"))
			})
			t.Run("matching etag deletes", func(t *testing.T) {
				ss, s := newStore(t)
				require.NoError(t, set(ss, state.SetRequest{Key: "a", Value: "v"}))
				require.NoError(t, delFn(ss, state.DeleteRequest{Key: "a", ETag: ptr.Of("1")}))
				assert.False(t, s.Exists("a"))
			})
			t.Run("no etag deletes unconditionally", func(t *testing.T) {
				ss, s := newStore(t)
				require.NoError(t, set(ss, state.SetRequest{Key: "a", Value: "v", Options: state.SetStateOption{Concurrency: state.FirstWrite}}))
				s.HSet("b", "data", `"v"`)
				require.NoError(t, delFn(ss, state.DeleteRequest{Key: "a"}))
				require.NoError(t, delFn(ss, state.DeleteRequest{Key: "b"}))
				require.NoError(t, delFn(ss, state.DeleteRequest{Key: "missing"}))
				assert.False(t, s.Exists("a"))
				assert.False(t, s.Exists("b"))
			})
		})
	}
}
