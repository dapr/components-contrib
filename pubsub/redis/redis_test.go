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

package redis

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	commonredis "github.com/dapr/components-contrib/common/component/redis"
	mdata "github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/kit/logger"
	kitmd "github.com/dapr/kit/metadata"
)

func getFakeProperties() map[string]string {
	return map[string]string{
		consumerID:   "fakeConsumer",
		enableTLS:    "true",
		maxLenApprox: "1000",
	}
}

func TestParseRedisMetadata(t *testing.T) {
	t.Run("metadata is correct", func(t *testing.T) {
		fakeProperties := getFakeProperties()

		fakeMetaData := pubsub.Metadata{
			Base: mdata.Base{Properties: fakeProperties},
		}

		// act
		m := commonredis.Settings{}
		err := kitmd.DecodeMetadata(fakeMetaData, &m)

		// assert
		require.NoError(t, err)
		assert.Equal(t, fakeProperties[consumerID], m.ConsumerID)
		assert.Equal(t, int64(1000), m.MaxLenApprox)
	})

	// TODO: fix the code to return the error for the missing property to make this test work
	// t.Run("consumerID is not given", func(t *testing.T) {
	// 	fakeProperties := getFakeProperties()

	// 	fakeMetaData := pubsub.Metadata{
	// 		Base: mdata.Base{Properties: fakeProperties},
	// 	}
	// 	fakeMetaData.Properties[consumerID] = ""

	// 	// act
	// 	m := commonredis.Settings{}
	// 	err := kitmd.DecodeMetadata(fakeMetaData, &m)
	// 	// assert
	// 	require.ErrorIs(t, err, errors.New("redis streams error: missing consumerID"))
	// 	assert.Empty(t, m.ConsumerID)
	// })
}

func TestProcessStreams(t *testing.T) {
	fakeConsumerID := "fakeConsumer"
	messageCount := 0
	expectedData := "testData"
	expectedMetadata := "testMetadata"

	var wg sync.WaitGroup
	wg.Add(3)

	fakeHandler := func(ctx context.Context, msg *pubsub.NewMessage) error {
		defer wg.Done()

		messageCount++

		// assert
		assert.Equal(t, expectedData, string(msg.Data))
		assert.Equal(t, expectedMetadata, msg.Metadata["mymetadata"])

		// return fake error to skip executing redis client command
		return errors.New("fake error")
	}

	// act
	testRedisStream := &redisStreams{
		logger:         logger.NewLogger("test"),
		clientSettings: &commonredis.Settings{},
	}
	testRedisStream.queue = make(chan redisMessageWrapper, 10)
	go testRedisStream.worker()
	testRedisStream.enqueueMessages(context.Background(), fakeConsumerID, fakeHandler, generateRedisStreamTestData(3, expectedData, expectedMetadata))

	// Wait for the handler to finish processing
	wg.Wait()

	// assert
	assert.Equal(t, 3, messageCount)
}

func TestProcessStreamsWithoutEventMetadata(t *testing.T) {
	fakeConsumerID := "fakeConsumer"
	messageCount := 0
	expectedData := "testData"

	var wg sync.WaitGroup
	wg.Add(3)

	fakeHandler := func(ctx context.Context, msg *pubsub.NewMessage) error {
		defer wg.Done()

		messageCount++

		// assert
		assert.Equal(t, expectedData, string(msg.Data))

		assert.Nil(t, msg.Metadata)

		// return fake error to skip executing redis client command
		return errors.New("fake error")
	}

	// act
	testRedisStream := &redisStreams{
		logger:         logger.NewLogger("test"),
		clientSettings: &commonredis.Settings{},
	}
	testRedisStream.queue = make(chan redisMessageWrapper, 10)
	go testRedisStream.worker()
	testRedisStream.enqueueMessages(context.Background(), fakeConsumerID, fakeHandler, generateRedisStreamTestData(3, expectedData, ""))

	// Wait for the handler to finish processing
	wg.Wait()

	// assert
	assert.Equal(t, 3, messageCount)
}

func generateRedisStreamTestData(messageCount int, data string, metadata string) []commonredis.RedisXMessage {
	generateXMessage := func(id int) commonredis.RedisXMessage {
		values := map[string]interface{}{
			"data": data,
		}

		if metadata != "" {
			values["metadata"] = "{\"mymetadata\": \"" + metadata + "\"}"
		}

		return commonredis.RedisXMessage{
			ID:     strconv.Itoa(id),
			Values: values,
		}
	}

	xmessageArray := make([]commonredis.RedisXMessage, messageCount)
	for i := range xmessageArray {
		xmessageArray[i] = generateXMessage(i)
	}

	return xmessageArray
}
