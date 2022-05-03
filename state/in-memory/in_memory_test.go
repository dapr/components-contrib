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
	"testing"

	"github.com/agrea/ptr"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"

	"github.com/dapr/kit/logger"

	"github.com/dapr/components-contrib/state"
)

func TestReadAndWrite(t *testing.T) {
	ctl := gomock.NewController(t)

	defer ctl.Finish()

	store := NewInMemoryStateStore(logger.NewLogger("test"))
	store.Init(state.Metadata{})

	t.Run("set kv with etag successfully", func(t *testing.T) {
		setReq := &state.SetRequest{
			Key:   "theFirstKey",
			Value: "value of key",
			ETag:  ptr.String("the etag"),
		}
		err := store.Set(setReq)
		assert.Nil(t, err)
	})

	t.Run("get the previous value after set", func(t *testing.T) {
		getReq := &state.GetRequest{
			Key: "theFirstKey",
		}
		resp, err := store.Get(getReq)
		assert.Nil(t, err)
		assert.NotNil(t, resp)
		assert.Equal(t, "value of key", string(resp.Data))
	})

	t.Run("set the second key successfully", func(t *testing.T) {
		setReq := &state.SetRequest{
			Key:   "theSecondKey",
			Value: "1234",
			ETag:  ptr.String("the etag"),
		}
		err := store.Set(setReq)
		assert.Nil(t, err)
	})

	t.Run("get value of the second key successfuly", func(t *testing.T) {
		getReq := &state.GetRequest{
			Key: "theSecondKey",
		}
		resp, err := store.Get(getReq)
		assert.Nil(t, err)
		assert.NotNil(t, resp)
		assert.Equal(t, "1234", string(resp.Data))
	})

	t.Run("BulkSet two keys", func(t *testing.T) {
		err := store.BulkSet([]state.SetRequest{{
			Key:   "theFirstKey",
			Value: "666",
		}, {
			Key:   "theSecondKey",
			Value: "777",
		}})

		assert.Nil(t, err)
	})

	t.Run("BulkGet fails when not supported", func(t *testing.T) {
		supportBulk, _, err := store.BulkGet([]state.GetRequest{{
			Key: "theFirstKey",
		}, {
			Key: "theSecondKey",
		}})

		assert.Nil(t, err)
		assert.Equal(t, false, supportBulk)
	})

	t.Run("delete theFirstKey", func(t *testing.T) {
		req := &state.DeleteRequest{
			Key: "theFirstKey",
		}
		err := store.Delete(req)
		assert.Nil(t, err)
	})

	t.Run("BulkGet fails when not supported", func(t *testing.T) {
		supportBulk, _, err := store.BulkGet([]state.GetRequest{{
			Key: "theFirstKey",
		}, {
			Key: "theSecondKey",
		}})

		assert.Nil(t, err)
		assert.Equal(t, false, supportBulk)
	})
}
