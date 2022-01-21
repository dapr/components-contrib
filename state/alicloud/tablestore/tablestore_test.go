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
	"testing"

	"github.com/agrea/ptr"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"

	"github.com/dapr/components-contrib/state"
	"github.com/dapr/kit/logger"
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

	assert.Nil(t, err)
	assert.Equal(t, "ACCESSKEYID", meta.AccessKeyID)
	assert.Equal(t, "ACCESSKEY", meta.AccessKey)
	assert.Equal(t, "INSTANCENAME", meta.InstanceName)
	assert.Equal(t, "TABLENAME", meta.TableName)
	assert.Equal(t, "ENDPOINT", meta.Endpoint)
}

func TestReadAndWrite(t *testing.T) {
	ctl := gomock.NewController(t)

	defer ctl.Finish()

	store := NewAliCloudTableStore(logger.NewLogger("test"))
	store.Init(state.Metadata{})

	store.client = &mockClient{
		data: make(map[string][]byte),
	}

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
		assert.Equal(t, 1, len(resp))
		assert.Equal(t, "777", string(resp[0].Data))
	})
}
