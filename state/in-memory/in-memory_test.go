// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

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

	t.Run("test set 1", func(t *testing.T) {
		setReq := &state.SetRequest{
			Key:   "theFirstKey",
			Value: "value of key",
			ETag:  ptr.String("the etag"),
		}
		err := store.Set(setReq)
		assert.Nil(t, err)
	})

	t.Run("test get 1", func(t *testing.T) {
		getReq := &state.GetRequest{
			Key: "theFirstKey",
		}
		resp, err := store.Get(getReq)
		assert.Nil(t, err)
		assert.NotNil(t, resp)
		assert.Equal(t, "value of key", string(resp.Data))
	})

	t.Run("test set 2", func(t *testing.T) {
		setReq := &state.SetRequest{
			Key:   "theSecondKey",
			Value: "1234",
			ETag:  ptr.String("the etag"),
		}
		err := store.Set(setReq)
		assert.Nil(t, err)
	})

	t.Run("test get 2", func(t *testing.T) {
		getReq := &state.GetRequest{
			Key: "theSecondKey",
		}
		resp, err := store.Get(getReq)
		assert.Nil(t, err)
		assert.NotNil(t, resp)
		assert.Equal(t, "1234", string(resp.Data))
	})

	t.Run("test BulkSet", func(t *testing.T) {
		err := store.BulkSet([]state.SetRequest{{
			Key:   "theFirstKey",
			Value: "666",
		}, {
			Key:   "theSecondKey",
			Value: "777",
		}})

		assert.Nil(t, err)
	})

	t.Run("test BulkGet", func(t *testing.T) {
		_, resp, err := store.BulkGet([]state.GetRequest{{
			Key: "theFirstKey",
		}, {
			Key: "theSecondKey",
		}})

		assert.Nil(t, err)
		assert.Equal(t, 2, len(resp))
		assert.Equal(t, "666", string(resp[0].Data))
		assert.Equal(t, "777", string(resp[1].Data))
	})

	t.Run("test delete", func(t *testing.T) {
		req := &state.DeleteRequest{
			Key: "theFirstKey",
		}
		err := store.Delete(req)
		assert.Nil(t, err)
	})

	t.Run("test BulkGet2", func(t *testing.T) {
		_, resp, err := store.BulkGet([]state.GetRequest{{
			Key: "theFirstKey",
		}, {
			Key: "theSecondKey",
		}})

		assert.Nil(t, err)
		assert.Equal(t, 2, len(resp))
		for _, r := range resp {
			if r.Key == "theSecondKey" {
				assert.Equal(t, "777", string(r.Data))
			}
		}
	})
}
