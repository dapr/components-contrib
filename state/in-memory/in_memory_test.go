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

package inmemory

import (
	"context"
	"sort"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	clocktesting "k8s.io/utils/clock/testing"

	"github.com/dapr/components-contrib/state"
	"github.com/dapr/kit/logger"
)

func TestReadAndWrite(t *testing.T) {
	ctl := gomock.NewController(t)

	defer ctl.Finish()

	store := NewInMemoryStateStore(logger.NewLogger("test")).(*inMemoryStore)
	fakeClock := clocktesting.NewFakeClock(time.Now())
	store.clock = fakeClock
	store.Init(context.Background(), state.Metadata{})

	keyA := "theFirstKey"
	valueA := "value of key"
	t.Run("set kv with etag and then get", func(t *testing.T) {
		// set
		setReq := &state.SetRequest{
			Key:   keyA,
			Value: valueA,
		}
		err := store.Set(context.Background(), setReq)
		assert.Nil(t, err)
		// get after set
		getReq := &state.GetRequest{
			Key: keyA,
		}
		resp, err := store.Get(context.Background(), getReq)
		assert.NoError(t, err)
		assert.NotNil(t, resp)
		assert.Equal(t, `"`+valueA+`"`, string(resp.Data))
		_ = assert.NotNil(t, resp.ETag) &&
			assert.NotEmpty(t, *resp.ETag)
	})

	t.Run("get nothing when expired", func(t *testing.T) {
		// set with LWW
		setReq := &state.SetRequest{
			Key:      keyA,
			Value:    valueA,
			Metadata: map[string]string{"ttlInSeconds": "1"},
		}
		err := store.Set(context.Background(), setReq)
		assert.NoError(t, err)
		// simulate expiration
		fakeClock.Step(2 * time.Second)
		// get
		getReq := &state.GetRequest{
			Key: keyA,
		}
		resp, err := store.Get(context.Background(), getReq)
		assert.NoError(t, err)
		assert.NotNil(t, resp)
		assert.Nil(t, resp.Data)
		assert.Nil(t, resp.ETag)
	})

	t.Run("return expire time when ttlInSeconds set with Get", func(t *testing.T) {
		now := fakeClock.Now()

		// set with LWW
		setReq := &state.SetRequest{
			Key:      keyA,
			Value:    valueA,
			Metadata: map[string]string{"ttlInSeconds": "1000"},
		}

		err := store.Set(context.Background(), setReq)
		assert.NoError(t, err)

		// get
		getReq := &state.GetRequest{
			Key: keyA,
		}
		resp, err := store.Get(context.Background(), getReq)
		assert.NoError(t, err)
		assert.NotNil(t, resp)
		assert.Equal(t, `"value of key"`, string(resp.Data))
		assert.Len(t, resp.Metadata, 1)
		require.Contains(t, resp.Metadata, "ttlExpireTime")
		assert.Equal(t, now.Add(time.Second*1000).UTC().Format(time.RFC3339), resp.Metadata["ttlExpireTime"])
	})

	t.Run("return expire time when ttlInSeconds set with GetBulk", func(t *testing.T) {
		assert.NoError(t, store.Set(context.Background(), &state.SetRequest{
			Key:      "a",
			Value:    "123",
			Metadata: map[string]string{"ttlInSeconds": "1000"},
		}))
		assert.NoError(t, store.Set(context.Background(), &state.SetRequest{
			Key:      "b",
			Value:    "456",
			Metadata: map[string]string{"ttlInSeconds": "2001"},
		}))

		resp, err := store.BulkGet(context.Background(), []state.GetRequest{
			{Key: "a"},
			{Key: "b"},
		}, state.BulkGetOpts{})
		assert.NoError(t, err)
		assert.NotNil(t, resp)
		require.Len(t, resp, 2)
		sort.Slice(resp, func(i, j int) bool {
			return resp[i].Key < resp[j].Key
		})
		assert.Equal(t, `"123"`, string(resp[0].Data))
		assert.Equal(t, `"456"`, string(resp[1].Data))
		assert.Len(t, resp[0].Metadata, 1)
		require.Contains(t, resp[0].Metadata, "ttlExpireTime")
		assert.Len(t, resp[1].Metadata, 1)
		require.Contains(t, resp[1].Metadata, "ttlExpireTime")
		assert.Equal(t, fakeClock.Now().Add(time.Second*1000).UTC().Format(time.RFC3339), resp[0].Metadata["ttlExpireTime"])
		assert.Equal(t, fakeClock.Now().Add(time.Second*2001).UTC().Format(time.RFC3339), resp[1].Metadata["ttlExpireTime"])
	})

	t.Run("set and get the second key successfully", func(t *testing.T) {
		// set
		setReq := &state.SetRequest{
			Key:   "theSecondKey",
			Value: 1234,
		}
		err := store.Set(context.Background(), setReq)
		assert.NoError(t, err)
		// get
		getReq := &state.GetRequest{
			Key: "theSecondKey",
		}
		resp, err := store.Get(context.Background(), getReq)
		assert.NoError(t, err)
		assert.NotNil(t, resp)
		assert.Equal(t, `1234`, string(resp.Data))
	})

	t.Run("BulkSet two keys", func(t *testing.T) {
		err := store.BulkSet(context.Background(), []state.SetRequest{{
			Key:   "theFirstKey",
			Value: "42",
		}, {
			Key:   "theSecondKey",
			Value: "84",
		}}, state.BulkStoreOpts{})

		assert.NoError(t, err)
	})

	t.Run("delete theFirstKey", func(t *testing.T) {
		req := &state.DeleteRequest{
			Key: "theFirstKey",
		}
		err := store.Delete(context.Background(), req)
		assert.NoError(t, err)
	})
}
