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
	"context"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/kit/logger"
)

func TestInputBindingRead(t *testing.T) { //nolint:paralleltest
	if !isLiveTest() {
		t.Skip()
		return
	}
	m := bindings.Metadata{} //nolint:exhaustivestruct
	m.Properties = getTestMetadata()
	r := NewRocketMQ(logger.NewLogger("test"))
	err := r.Init(context.Background(), m)
	require.NoError(t, err)

	var count int32
	handler := func(ctx context.Context, in *bindings.ReadResponse) ([]byte, error) {
		require.Equal(t, "hello", string(in.Data))
		atomic.AddInt32(&count, 1)

		return nil, nil
	}
	err = r.Read(context.Background(), handler)
	require.NoError(t, err)

	time.Sleep(5 * time.Second)
	atomic.StoreInt32(&count, 0)
	req := &bindings.InvokeRequest{Data: []byte("hello"), Operation: bindings.CreateOperation, Metadata: map[string]string{}}
	_, err = r.Invoke(context.Background(), req)
	require.NoError(t, err)

	time.Sleep(10 * time.Second)
	for range 30 {
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
