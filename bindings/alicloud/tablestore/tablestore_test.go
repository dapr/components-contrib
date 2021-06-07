// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package tablestore

import (
	"encoding/json"
	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/kit/logger"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestTableStoreMetadata(t *testing.T) {
	m := bindings.Metadata{}
	m.Properties = map[string]string{"accessKeyID": "ACCESSKEYID", "accessKey": "ACCESSKEY", "instanceName": "INSTANCENAME", "tableName": "TABLENAME", "endpoint": "ENDPOINT"}
	aliCloudTableStore := AliCloudTableStore{}

	meta, err := aliCloudTableStore.parseMetadata(m)

	assert.Nil(t, err)
	assert.Equal(t, "ACCESSKEYID", meta.AccessKeyID)
	assert.Equal(t, "ACCESSKEY", meta.AccessKey)
	assert.Equal(t, "INSTANCENAME", meta.InstanceName)
	assert.Equal(t, "TABLENAME", meta.TableName)
	assert.Equal(t, "ENDPOINT", meta.Endpoint)
}

func TestDataEncodeAndDecode(t *testing.T) {

	if !isLiveTest() {
		return
	}

	aliCloudTableStore := NewAliCloudTableStore(logger.NewLogger("test"))

	metadata := bindings.Metadata{
		Properties: getTestProperties(),
	}
	aliCloudTableStore.Init(metadata)

	// test create
	putData := map[string]interface{}{
		"pk1":     "data1",
		"column1": "the string value of column1",
		"column2": int64(2),
	}
	data, err := json.Marshal(putData)
	putRowReq := &bindings.InvokeRequest{
		Operation: bindings.CreateOperation,
		Metadata: map[string]string{
			tableName:   "dapr_test_table2",
			primaryKeys: "pk1",
		},
		Data: data,
	}

	putInvokeResp, err := aliCloudTableStore.Invoke(putRowReq)

	assert.Nil(t, err)
	assert.NotNil(t, putInvokeResp)

	putRowReq.Data, _ = json.Marshal(map[string]interface{}{
		"pk1":     "data2",
		"column1": "the string value of column1",
		"column2": int64(2),
	})

	putInvokeResp, err = aliCloudTableStore.Invoke(putRowReq)

	assert.Nil(t, err)
	assert.NotNil(t, putInvokeResp)

	// test get
	getData, err := json.Marshal(map[string]interface{}{
		"pk1": "data1",
	})
	getInvokeReq := &bindings.InvokeRequest{
		Operation: bindings.GetOperation,
		Metadata: map[string]string{
			tableName:   "dapr_test_table2",
			primaryKeys: "pk1",
			columnToGet: "column1,column2,column3",
		},
		Data: getData,
	}

	getInvokeResp, err := aliCloudTableStore.Invoke(getInvokeReq)

	assert.Nil(t, err)
	assert.NotNil(t, getInvokeResp)

	var respData = make(map[string]interface{})
	err = json.Unmarshal(getInvokeResp.Data, &respData)

	assert.Nil(t, err)

	assert.Equal(t, putData["column1"], respData["column1"])
	assert.Equal(t, putData["column2"], int64(respData["column2"].(float64)))

	// test list
	listData, err := json.Marshal([]map[string]interface{}{
		{
			"pk1": "data1",
		},
		{
			"pk1": "data2",
		},
	})

	listReq := &bindings.InvokeRequest{
		Operation: bindings.ListOperation,
		Metadata: map[string]string{
			tableName:   "dapr_test_table2",
			primaryKeys: "pk1",
			columnToGet: "column1,column2,column3",
		},
		Data: listData,
	}

	listResp, err := aliCloudTableStore.Invoke(listReq)
	assert.Nil(t, err)
	assert.NotNil(t, listResp)

	listRespData := make([]map[string]interface{}, len(listData))
	err = json.Unmarshal(listResp.Data, &listRespData)

	assert.Nil(t, err)
	assert.Len(t, listRespData, 2)

	assert.Equal(t, listRespData[0]["column1"], putData["column1"])
	assert.Equal(t, listRespData[1]["pk1"], "data2")

	// test delete
	deleteData, err := json.Marshal(map[string]interface{}{
		"pk1": "data1",
	})

	deleteReq := &bindings.InvokeRequest{
		Operation: bindings.DeleteOperation,
		Metadata: map[string]string{
			tableName:   "dapr_test_table2",
			primaryKeys: "pk1",
		},
		Data: deleteData,
	}

	deleteResp, err := aliCloudTableStore.Invoke(deleteReq)

	assert.Nil(t, err)
	assert.NotNil(t, deleteResp)

	getInvokeResp, err = aliCloudTableStore.Invoke(getInvokeReq)

	assert.Nil(t, err)
	assert.Nil(t, getInvokeResp.Data)
}

func getTestProperties() map[string]string {
	return map[string]string{
		"accessKeyID":  "****",
		"accessKey":    "****",
		"instanceName": "dapr-test",
		"tableName":    "dapr_test_table2",
		"endpoint":     "https://dapr-test.cn-hangzhou.ots.aliyuncs.com"}
}

func isLiveTest() bool {
	return os.Getenv("RUN_LIVE_ROCKETMQ_TEST") == "true"
}
