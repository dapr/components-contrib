// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package rocketmq

import (
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/dapr/pkg/logger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseMetadata(t *testing.T) { //nolint:paralleltest
	m := bindings.Metadata{Name: "test",
		Properties: map[string]string{
			"accessProto":   "http",
			"accessKey":     "**",
			"secretKey":     "***",
			"endpoint":      "http://test.endpoint",
			"nameServer":    "http://test.nameserver",
			"consumerGroup": "defaultGroup",
			"instanceId":    "defaultNamespace",
			"topics":        "defaultTopic",
		}}
	b, err := parseMetadata(m)
	assert.Nil(t, err)

	assert.Equal(t, "http", b.AccessProto)
	assert.Equal(t, "**", b.AccessKey)
	assert.Equal(t, "***", b.SecretKey)
	assert.Equal(t, "http://test.endpoint", b.Endpoint)
	assert.Equal(t, "defaultGroup", b.ConsumerGroup)
	assert.Equal(t, "defaultNamespace", b.InstanceID)
	assert.Equal(t, "defaultTopic", b.Topics)
}

func TestInputBindingRead(t *testing.T) { //nolint:paralleltest
	if !isLiveTest() {
		return
	}
	var m = bindings.Metadata{} //nolint:exhaustivestruct
	m.Properties = getTestMetadata()
	r := NewAliCloudRocketMQ(logger.NewLogger("test"))
	err := r.Init(m)
	require.NoError(t, err)

	var count int32
	handler := func(in *bindings.ReadResponse) ([]byte, error) {
		require.Equal(t, "hello", string(in.Data))
		atomic.AddInt32(&count, 1)

		return nil, nil
	}
	go func() {
		err = r.Read(handler)
		require.NoError(t, err)
	}()

	time.Sleep(5 * time.Second)
	atomic.StoreInt32(&count, 0)
	var req = &bindings.InvokeRequest{Data: []byte("hello"), Operation: bindings.CreateOperation, Metadata: map[string]string{}}
	_, err = r.Invoke(req)
	require.NoError(t, err)

	time.Sleep(10 * time.Second)
	for i := 0; i < 30; i++ {
		if atomic.LoadInt32(&count) > 0 {
			break
		}
		time.Sleep(time.Second)
	}
	assert.Equal(t, int32(1), atomic.LoadInt32(&count))
}

func isLiveTest() bool {
	return os.Getenv("RUN_LIVE_ROCKETMQ_TEST") == "true"
}

func getTestMetadata() map[string]string {
	return map[string]string{
		"accessProto":        "tcp",
		"nameServer":         "http://xx.mq-internet-access.mq-internet.aliyuncs.com:80",
		"consumerGroup":      "GID_DAPR-MQ-TCP",
		"topics":             "TOPIC_TEST",
		"accessKey":          "xx",
		"secretKey":          "xx",
		"instanceId":         "MQ_INST_xx",
		"consumerBatchSize":  "1",
		"consumerThreadNums": "5",
	}
}
