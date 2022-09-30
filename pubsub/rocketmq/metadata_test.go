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

package rocketmq

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	mdata "github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/pubsub"
)

func TestMetaDataDecode(t *testing.T) {
	props := map[string]string{
		"instanceName":               "dapr-rocketmq-test",
		"producerGroup":              "dapr-rocketmq-test-g-p",
		"consumerGroup":              "dapr-rocketmq-test-g-c",
		"groupName":                  "dapr-rocketmq-test-g-c",
		"nameSpace":                  "dapr-test",
		"nameServerDomain":           "www.baidu.com",
		"nameServer":                 "test.nameserver",
		"accessKey":                  "accessKey",
		"secretKey":                  "secretKey",
		"securityToken":              "securityToken",
		"retries":                    "5",
		"consumerModel":              "Clustering",
		"fromWhere":                  "ConsumeFromLastOffset",
		"consumeTimestamp":           "20220817101902",
		"consumeOrderly":             "true",
		"consumeMessageBatchMaxSize": "10",
		"consumeConcurrentlyMaxSpan": "10",
		"maxReconsumeTimes":          "10000",
		"autoCommit":                 "true",
		"consumeTimeout":             "10",
		"consumerPullTimeout":        "10",
		"pullInterval":               "10",
		"consumerBatchSize":          "10",
		"pullBatchSize":              "10",
		"pullThresholdForQueue":      "100",
		"pullThresholdForTopic":      "100",
		"pullThresholdSizeForQueue":  "10",
		"pullThresholdSizeForTopic":  "10",
		"content-type":               "json",
		"sendTimeOutSec":             "10",
		"logLevel":                   "ERROR",
		"mspProperties":              "UNIQ_KEY",
	}
	pubsubMeta := pubsub.Metadata{Base: mdata.Base{Properties: props}}
	metaData, err := parseRocketMQMetaData(pubsubMeta)
	require.NoError(t, err)
	assert.Equal(t, "dapr-rocketmq-test", metaData.InstanceName)
	assert.Equal(t, "dapr-rocketmq-test-g-p", metaData.ProducerGroup)
	assert.Equal(t, "dapr-rocketmq-test-g-c", metaData.ConsumerGroup)
	assert.Equal(t, "dapr-rocketmq-test-g-c", metaData.GroupName)
	assert.Equal(t, "dapr-test", metaData.NameSpace)
	assert.Equal(t, "www.baidu.com", metaData.NameServerDomain)
	assert.Equal(t, "test.nameserver", metaData.NameServer)
	assert.Equal(t, "accessKey", metaData.AccessKey)
	assert.Equal(t, "secretKey", metaData.SecretKey)
	assert.Equal(t, "securityToken", metaData.SecurityToken)
	assert.Equal(t, 5, metaData.Retries)
	assert.Equal(t, "Clustering", metaData.ConsumerModel)
	assert.Equal(t, "ConsumeFromLastOffset", metaData.FromWhere)
	assert.Equal(t, "20220817101902", metaData.ConsumeTimestamp)
	assert.Equal(t, "true", metaData.ConsumeOrderly)
	assert.Equal(t, 10, metaData.ConsumeMessageBatchMaxSize)
	assert.Equal(t, 10, metaData.ConsumeConcurrentlyMaxSpan)
	assert.Equal(t, int32(10000), metaData.MaxReconsumeTimes)
	assert.Equal(t, "true", metaData.AutoCommit)
	assert.Equal(t, 10, metaData.ConsumeTimeout)
	assert.Equal(t, 10, metaData.ConsumerPullTimeout)
	assert.Equal(t, 10, metaData.PullInterval)
	assert.Equal(t, int32(10), metaData.PullBatchSize)
	assert.Equal(t, int(10), metaData.ConsumerBatchSize)
	assert.Equal(t, int64(100), metaData.PullThresholdForQueue)
	assert.Equal(t, int64(100), metaData.PullThresholdForTopic)
	assert.Equal(t, 10, metaData.PullThresholdSizeForQueue)
	assert.Equal(t, 10, metaData.PullThresholdSizeForTopic)
	assert.Equal(t, "json", metaData.ContentType)
	assert.Equal(t, 10, metaData.SendTimeOutSec)
	assert.Equal(t, "ERROR", metaData.LogLevel)
	assert.Equal(t, "UNIQ_KEY", metaData.MsgProperties)
}
