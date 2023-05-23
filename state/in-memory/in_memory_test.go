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
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"

	"github.com/dapr/components-contrib/state"
	"github.com/dapr/kit/logger"
)

func TestReadAndWrite(t *testing.T) {
	ctl := gomock.NewController(t)

	defer ctl.Finish()

	store := NewInMemoryStateStore(logger.NewLogger("test"))
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
		time.Sleep(2 * time.Second)
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
