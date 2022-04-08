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

package dcs

import (
	"context"
	"fmt"
	"testing"
	"time"
	"unsafe"

	"github.com/alicebob/miniredis/v2"
	jsoniter "github.com/json-iterator/go"

	rediscomponent "github.com/dapr/components-contrib/internal/component/redis"
	"github.com/dapr/components-contrib/state"
	redis "github.com/dapr/components-contrib/state/redis"
	"github.com/dapr/kit/logger"

	redisext "github.com/go-redis/redis/v8"

	"github.com/stretchr/testify/assert"
)

const (
	defaultDB              = 0
	defaultMaxRetries      = 3
	defaultMaxRetryBackoff = time.Second * 2
)

type metadata struct {
	maxRetries      int
	maxRetryBackoff time.Duration
	ttlInSeconds    *int
	queryIndexes    string
}

type querySchemaElem struct {
	schema []interface{}
	keys   map[string]string
}

type querySchemas map[string]*querySchemaElem

// This is a copy (mock) for the actual redis.StateStore
// make sure to update this when necessary, to keep the test cases passed.
type MockRedisStateStore struct {
	state.DefaultBulkStore
	client         redisext.UniversalClient
	clientSettings *rediscomponent.Settings
	json           jsoniter.API
	metadata       metadata
	replicas       int
	querySchemas   querySchemas

	features []state.Feature
	logger   logger.Logger

	ctx    context.Context
	cancel context.CancelFunc
}

func TestInit(t *testing.T) {
	// TODO: Modify to use the Mock redis store. currently, it uses the local redis instance

	l := logger.NewLogger("test")
	m := state.Metadata{}
	d := NewDCSStateStore(l)

	// TODO: Enable this test case once mock is ready
	// t.Run("Init with valid metadata", func(t *testing.T) {
	// 	m.Properties = map[string]string{
	// 		"instanceName": "my-dcs",
	// 		"instanceId":   "1234",
	// 		"projectId":    "abcd",
	// 		"region":       "cn-north-4",
	// 		"vpcId":        "vpc-test",
	// 		"dcsHost":      "127.0.0.1:6379",
	// 		"dcsPassword":  "",
	// 	}
	// 	err := d.Init(m)
	// 	assert.Nil(t, err)
	// })

	t.Run("Init with missing instance id", func(t *testing.T) {
		m.Properties = map[string]string{
			"projectId": "abcd",
		}
		err := d.Init(m)
		assert.NotNil(t, err)
		assert.Equal(t, err, fmt.Errorf("missing dcs instance id"))
	})

	t.Run("Init with missing project id", func(t *testing.T) {
		m.Properties = map[string]string{
			"instanceId": "1234",
		}
		err := d.Init(m)
		assert.NotNil(t, err)
		assert.Equal(t, err, fmt.Errorf("missing dcs project id"))
	})

	t.Run("Init with missing DCS host", func(t *testing.T) {
		m.Properties = map[string]string{
			"instanceId": "1234",
			"projectId":  "abcd",
		}
		err := d.Init(m)
		assert.NotNil(t, err)
		assert.Equal(t, err, fmt.Errorf("missing dcs host"))
	})

	t.Run("Init with missing DCS password", func(t *testing.T) {
		m.Properties = map[string]string{
			"instanceId": "1234",
			"projectId":  "abcd",
			"dcsHost":    "127.0.0.1:6379",
		}
		err := d.Init(m)
		assert.NotNil(t, err)
		assert.Equal(t, err, fmt.Errorf("missing dcs password"))
	})

	// TODO: Enable this test case once mock is ready
	// t.Run("Init only with mandatory fields", func(t *testing.T) {
	// 	m.Properties = map[string]string{
	// 		"instanceId":  "1234",
	// 		"projectId":   "abcd",
	// 		"dcsHost":     "127.0.0.1:6379",
	// 		"dcsPassword": "",
	// 	}
	// 	err := d.Init(m)
	// 	assert.Nil(t, err)
	// })
}

func TestPing(t *testing.T) {
	l := logger.NewLogger("test")
	rs := setupMockRedisStateStore(l)

	d := &StateStore{
		r:      rs,
		logger: l,
	}

	err := d.Ping()
	assert.NoError(t, err)

	rs.Close()

	err = d.Ping()
	assert.Error(t, err)
}

func TestSet(t *testing.T) {
	l := logger.NewLogger("test")
	rs := setupMockRedisStateStore(l)
	defer rs.Close()

	type value struct {
		Value string
	}

	t.Run("Successfully set item", func(t *testing.T) {
		d := StateStore{
			r:      rs,
			logger: l,
		}
		req := &state.SetRequest{
			Key: "key",
			Value: value{
				Value: "value",
			},
		}
		err := d.Set(req)
		assert.Nil(t, err)
	})
}

func TestGet(t *testing.T) {
	l := logger.NewLogger("test")
	rs := setupMockRedisStateStore(l)
	defer rs.Close()

	t.Run("Successfully with no required key", func(t *testing.T) {
		d := StateStore{
			r:      rs,
			logger: l,
		}
		req := &state.GetRequest{
			Key:      "key",
			Metadata: nil,
			Options: state.GetStateOption{
				Consistency: "strong",
			},
		}
		out, err := d.Get(req)
		assert.Nil(t, err)
		assert.Nil(t, out.Data)
	})
}

func TestDelete(t *testing.T) {
	l := logger.NewLogger("test")
	rs := setupMockRedisStateStore(l)
	defer rs.Close()

	t.Run("Successfully with non existing item", func(t *testing.T) {
		req := &state.DeleteRequest{
			Key: "key",
		}

		d := StateStore{
			r:      rs,
			logger: l,
		}
		err := d.Delete(req)
		assert.Nil(t, err)
	})
}

func TestBulkSet(t *testing.T) {
	l := logger.NewLogger("test")
	rs := setupMockRedisStateStore(l)
	defer rs.Close()

	type value struct {
		Value string
	}

	t.Run("Successfully set items", func(t *testing.T) {
		d := StateStore{
			r:      rs,
			logger: l,
		}
		req := []state.SetRequest{
			{
				Key: "key1",
				Value: value{
					Value: "value1",
				},
			},
			{
				Key: "key2",
				Value: value{
					Value: "value2",
				},
			},
		}
		err := d.BulkSet(req)
		assert.Nil(t, err)
	})
}

func TestBulkDelete(t *testing.T) {
	l := logger.NewLogger("test")
	rs := setupMockRedisStateStore(l)
	defer rs.Close()

	t.Run("Successfully with non existing items", func(t *testing.T) {
		d := StateStore{
			r:      rs,
			logger: l,
		}
		req := []state.DeleteRequest{
			{
				Key: "key1",
			},
			{
				Key: "key2",
			},
		}
		err := d.BulkDelete(req)
		assert.Nil(t, err)
	})
}

func setupMockRedisStateStore(logger logger.Logger) *redis.StateStore {
	_, c := setupMiniredis()

	globalTTLInSeconds := 100

	qs := make(map[string]*querySchemaElem)
	qs["test"] = &querySchemaElem{
		keys:   make(map[string]string),
		schema: []interface{}{},
	}
	ss := MockRedisStateStore{
		DefaultBulkStore: state.DefaultBulkStore{},
		client:           c,
		clientSettings:   &rediscomponent.Settings{},
		json:             jsoniter.ConfigFastest,
		metadata:         metadata{maxRetries: defaultMaxRetries, maxRetryBackoff: defaultMaxRetryBackoff, ttlInSeconds: &globalTTLInSeconds, queryIndexes: ""},
		replicas:         1,
		querySchemas:     qs,
		features:         []state.Feature{},
		logger:           logger,
		ctx:              nil,
		cancel: func() {
		},
	}
	ss.ctx, ss.cancel = context.WithCancel(context.Background())
	rs := (*redis.StateStore)(unsafe.Pointer(&ss))
	ss.DefaultBulkStore = state.NewDefaultBulkStore(rs)

	return rs
}

func setupMiniredis() (*miniredis.Miniredis, *redisext.Client) {
	s, err := miniredis.Run()
	if err != nil {
		panic(err)
	}
	opts := &redisext.Options{
		Addr: s.Addr(),
		DB:   defaultDB,
	}

	return s, redisext.NewClient(opts)
}
