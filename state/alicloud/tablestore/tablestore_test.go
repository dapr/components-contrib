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

package tablestore

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/state"
	"github.com/dapr/kit/logger"
	"github.com/dapr/kit/ptr"
)

func TestTableStoreMetadata(t *testing.T) {
	m := state.Metadata{}
	m.Properties = map[string]string{
		"accessKeyID":  "ACCESSKEYID",
		"accessKey":    "ACCESSKEY",
		"instanceName": "INSTANCENAME",
		"tableName":    "TABLENAME",
		"endpoint":     "ENDPOINT",
	}
	aliCloudTableStore := AliCloudTableStore{}

	meta, err := aliCloudTableStore.parse(m)

	require.NoError(t, err)
	assert.Equal(t, "ACCESSKEYID", meta.AccessKeyID)
	assert.Equal(t, "ACCESSKEY", meta.AccessKey)
	assert.Equal(t, "INSTANCENAME", meta.InstanceName)
	assert.Equal(t, "TABLENAME", meta.TableName)
	assert.Equal(t, "ENDPOINT", meta.Endpoint)
}

func TestReadAndWrite(t *testing.T) {
	ctl := gomock.NewController(t)

	defer ctl.Finish()

	store := &AliCloudTableStore{
		logger: logger.NewLogger("test"),
	}
	store.BulkStore = state.NewDefaultBulkStore(store)
	store.Init(context.Background(), state.Metadata{})

	store.client = &mockClient{
		data: make(map[string][]byte),
	}

	t.Run("test set 1", func(t *testing.T) {
		setReq := &state.SetRequest{
			Key:   "theFirstKey",
			Value: "value of key",
			ETag:  ptr.Of("the etag"),
		}
		err := store.Set(context.Background(), setReq)
		require.NoError(t, err)
	})

	t.Run("test get 1", func(t *testing.T) {
		getReq := &state.GetRequest{
			Key: "theFirstKey",
		}
		resp, err := store.Get(context.Background(), getReq)
		require.NoError(t, err)
		assert.NotNil(t, resp)
		assert.Equal(t, "value of key", string(resp.Data))
	})

	t.Run("test set 2", func(t *testing.T) {
		setReq := &state.SetRequest{
			Key:   "theSecondKey",
			Value: "1234",
			ETag:  ptr.Of("the etag"),
		}
		err := store.Set(context.Background(), setReq)
		require.NoError(t, err)
	})

	t.Run("test get 2", func(t *testing.T) {
		getReq := &state.GetRequest{
			Key: "theSecondKey",
		}
		resp, err := store.Get(context.Background(), getReq)
		require.NoError(t, err)
		assert.NotNil(t, resp)
		assert.Equal(t, "1234", string(resp.Data))
	})

	t.Run("test BulkSet", func(t *testing.T) {
		err := store.BulkSet(context.Background(), []state.SetRequest{{
			Key:   "theFirstKey",
			Value: "666",
		}, {
			Key:   "theSecondKey",
			Value: "777",
		}}, state.BulkStoreOpts{})

		require.NoError(t, err)
	})

	t.Run("test BulkGet", func(t *testing.T) {
		resp, err := store.BulkGet(context.Background(), []state.GetRequest{{
			Key: "theFirstKey",
		}, {
			Key: "theSecondKey",
		}}, state.BulkGetOpts{})

		require.NoError(t, err)
		assert.Equal(t, 2, len(resp))
		assert.Equal(t, "666", string(resp[0].Data))
		assert.Equal(t, "777", string(resp[1].Data))
	})

	t.Run("test delete", func(t *testing.T) {
		req := &state.DeleteRequest{
			Key: "theFirstKey",
		}
		err := store.Delete(context.Background(), req)
		require.NoError(t, err)
	})

	t.Run("test BulkGet2", func(t *testing.T) {
		resp, err := store.BulkGet(context.Background(), []state.GetRequest{{
			Key: "theFirstKey",
		}, {
			Key: "theSecondKey",
		}}, state.BulkGetOpts{})

		require.NoError(t, err)
		assert.Equal(t, 1, len(resp))
		assert.Equal(t, "777", string(resp[0].Data))
	})
}
