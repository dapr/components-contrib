// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package tablestore

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/agrea/ptr"
	"github.com/dapr/components-contrib/state"
	"github.com/dapr/components-contrib/state/utils"
	"github.com/dapr/kit/logger"
	"github.com/stretchr/testify/assert"
)

func TestTableStoreMetadata(t *testing.T) {
	m := state.Metadata{}
	m.Properties = map[string]string{"accessKeyID": "ACCESSKEYID", "accessKey": "ACCESSKEY", "instanceName": "INSTANCENAME", "tableName": "TABLENAME", "endpoint": "ENDPOINT"}
	aliCloudTableStore := AliCloudTableStore{}

	meta, err := aliCloudTableStore.parse(m)

	assert.Nil(t, err)
	assert.Equal(t, "ACCESSKEYID", meta.AccessKeyID)
	assert.Equal(t, "ACCESSKEY", meta.AccessKey)
	assert.Equal(t, "INSTANCENAME", meta.InstanceName)
	assert.Equal(t, "TABLENAME", meta.TableName)
	assert.Equal(t, "ENDPOINT", meta.Endpoint)
}

func TestDataEncodeAndDecode(t *testing.T) {
	if os.Getenv("ACCESS_KEY_ID") == "" || os.Getenv("ACCESS_KEY") == "" {
		t.Skip()
	}

	aliCloudTableStore := NewAliCloudTableStore(logger.NewLogger("test"))

	metadata := state.Metadata{
		Properties: map[string]string{
			"accessKeyID":  os.Getenv("ACCESS_KEY_ID"),
			"accessKey":    os.Getenv("ACCESS_KEY"),
			"instanceName": "dapr-test",
			"tableName":    "dapr_test_table1",
			"endpoint":     "https://dapr-test.cn-hangzhou.ots.aliyuncs.com",
		},
	}
	aliCloudTableStore.Init(metadata)

	setReq := &state.SetRequest{
		Key:   "theFirstKey",
		Value: "value of key",
		ETag:  ptr.String("the etag"),
	}
	err := aliCloudTableStore.Set(setReq)

	assert.Nil(t, err)

	getReq := &state.GetRequest{
		Key: "theFirstKey",
	}
	resp, err := aliCloudTableStore.Get(getReq)

	assert.Nil(t, err)
	assert.NotNil(t, resp)

	value, _ := utils.Marshal(setReq.Value, json.Marshal)

	assert.Equal(t, resp.Data, value)
}
