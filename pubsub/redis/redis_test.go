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
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"

	mdata "github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/kit/logger"

	internalredis "github.com/dapr/components-contrib/internal/component/redis"
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
		m := internalredis.Settings{}
		err := mdata.DecodeMetadata(fakeMetaData, &m)

		// assert
		assert.NoError(t, err)
		assert.Equal(t, fakeProperties[consumerID], m.ConsumerID)
		assert.Equal(t, int64(1000), m.MaxLenApprox)
	})

	t.Run("consumerID is not given", func(t *testing.T) {
		fakeProperties := getFakeProperties()

		fakeMetaData := pubsub.Metadata{
			Base: mdata.Base{Properties: fakeProperties},
		}
		fakeMetaData.Properties[consumerID] = ""

		// act
		m := internalredis.Settings{}
		err := mdata.DecodeMetadata(fakeMetaData, &m)
		// assert
		assert.Error(t, errors.New("redis streams error: missing consumerID"), err)
		assert.Empty(t, m.ConsumerID)
	})
}

func TestProcessStreams(t *testing.T) {
	fakeConsumerID := "fakeConsumer"
	topicCount := 0
	messageCount := 0
	expectedData := "testData"

	var wg sync.WaitGroup
	wg.Add(3)

	fakeHandler := func(ctx context.Context, msg *pubsub.NewMessage) error {
		defer wg.Done()

		messageCount++
		if topicCount == 0 {
			topicCount = 1
		}

		// assert
		assert.Equal(t, expectedData, string(msg.Data))

		// return fake error to skip executing redis client command
		return errors.New("fake error")
	}

	// act
	testRedisStream := &redisStreams{
		logger:         logger.NewLogger("test"),
		clientSettings: &internalredis.Settings{},
	}
	testRedisStream.queue = make(chan redisMessageWrapper, 10)
	go testRedisStream.worker()
	testRedisStream.enqueueMessages(context.Background(), fakeConsumerID, fakeHandler, generateRedisStreamTestData(2, 3, expectedData))

	// Wait for the handler to finish processing
	wg.Wait()

	// assert
	assert.Equal(t, 1, topicCount)
	assert.Equal(t, 3, messageCount)
}

func generateRedisStreamTestData(topicCount, messageCount int, data string) []internalredis.RedisXMessage {
	generateXMessage := func(id int) internalredis.RedisXMessage {
		return internalredis.RedisXMessage{
			ID: fmt.Sprintf("%d", id),
			Values: map[string]interface{}{
				"data": data,
			},
		}
	}

	xmessageArray := make([]internalredis.RedisXMessage, messageCount)
	for i := range xmessageArray {
		xmessageArray[i] = generateXMessage(i)
	}

	return xmessageArray
}
