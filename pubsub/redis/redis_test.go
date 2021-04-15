// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package redis

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/go-redis/redis/v7"
	"github.com/stretchr/testify/assert"

	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/dapr/pkg/logger"
)

func getFakeProperties() map[string]string {
	return map[string]string{
		consumerID:            "fakeConsumer",
		host:                  "fake.redis.com",
		password:              "fakePassword",
		redisType:             "node",
		enableTLS:             "true",
		dialTimeout:           "5s",
		readTimeout:           "5s",
		writeTimeout:          "50000",
		poolSize:              "20",
		maxConnAge:            "200s",
		db:                    "1",
		redisMaxRetries:       "1",
		redisMinRetryInterval: "8ms",
		redisMaxRetryInterval: "1s",
		minIdleConns:          "1",
		poolTimeout:           "1s",
		idleTimeout:           "1s",
		idleCheckFrequency:    "1s",
	}
}

func TestParseRedisMetadata(t *testing.T) {
	t.Run("metadata is correct", func(t *testing.T) {
		fakeProperties := getFakeProperties()

		fakeMetaData := pubsub.Metadata{
			Properties: fakeProperties,
		}

		// act
		m, err := parseRedisMetadata(fakeMetaData)

		// assert
		assert.NoError(t, err)
		assert.Equal(t, fakeProperties[host], m.host)
		assert.Equal(t, fakeProperties[password], m.password)
		assert.Equal(t, fakeProperties[redisType], m.redisType)
		assert.Equal(t, fakeProperties[consumerID], m.consumerID)
		assert.Equal(t, true, m.enableTLS)
		assert.Equal(t, 5*time.Second, m.dialTimeout)
		assert.Equal(t, 5*time.Second, m.readTimeout)
		assert.Equal(t, 50000*time.Millisecond, m.writeTimeout)
		assert.Equal(t, 20, m.poolSize)
		assert.Equal(t, 200*time.Second, m.maxConnAge)
		assert.Equal(t, 1, m.db)
		assert.Equal(t, 1, m.redisMaxRetries)
		assert.Equal(t, 8*time.Millisecond, m.redisMinRetryInterval)
		assert.Equal(t, 1*time.Second, m.redisMaxRetryInterval)
		assert.Equal(t, 1, m.minIdleConns)
		assert.Equal(t, 1*time.Second, m.poolTimeout)
		assert.Equal(t, 1*time.Second, m.idleTimeout)
		assert.Equal(t, 1*time.Second, m.idleCheckFrequency)
	})

	t.Run("host is not given", func(t *testing.T) {
		fakeProperties := getFakeProperties()

		fakeMetaData := pubsub.Metadata{
			Properties: fakeProperties,
		}
		fakeMetaData.Properties[host] = ""

		// act
		m, err := parseRedisMetadata(fakeMetaData)

		// assert
		assert.Error(t, errors.New("redis streams error: missing host address"), err)
		assert.Empty(t, m.host)
		assert.Empty(t, m.password)
		assert.Empty(t, m.consumerID)
	})

	t.Run("consumerID is not given", func(t *testing.T) {
		fakeProperties := getFakeProperties()

		fakeMetaData := pubsub.Metadata{
			Properties: fakeProperties,
		}
		fakeMetaData.Properties[consumerID] = ""

		// act
		m, err := parseRedisMetadata(fakeMetaData)
		// assert
		assert.Error(t, errors.New("redis streams error: missing consumerID"), err)
		assert.Equal(t, fakeProperties[host], m.host)
		assert.Equal(t, fakeProperties[password], m.password)
		assert.Empty(t, m.consumerID)
	})

	t.Run("check values can be set as -1", func(t *testing.T) {
		fakeProperties := getFakeProperties()

		fakeMetaData := pubsub.Metadata{
			Properties: fakeProperties,
		}
		fakeMetaData.Properties[readTimeout] = "-1"
		fakeMetaData.Properties[idleTimeout] = "-1"
		fakeMetaData.Properties[idleCheckFrequency] = "-1"
		fakeMetaData.Properties[redisMaxRetryInterval] = "-1"
		fakeMetaData.Properties[redisMinRetryInterval] = "-1"

		// act
		m, err := parseRedisMetadata(fakeMetaData)
		// assert
		assert.NoError(t, err)
		assert.True(t, m.readTimeout == -1)
		assert.True(t, m.idleTimeout == -1)
		assert.True(t, m.idleCheckFrequency == -1)
		assert.True(t, m.redisMaxRetryInterval == -1)
		assert.True(t, m.redisMinRetryInterval == -1)
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
	testRedisStream := &redisStreams{logger: logger.NewLogger("test")}
	testRedisStream.ctx, testRedisStream.cancel = context.WithCancel(context.Background())
	testRedisStream.queue = make(chan redisMessageWrapper, 10)
	go testRedisStream.worker()
	testRedisStream.enqueueMessages(fakeConsumerID, fakeHandler, generateRedisStreamTestData(2, 3, expectedData))

	// Wait for the handler to finish processing
	wg.Wait()

	// assert
	assert.Equal(t, 1, topicCount)
	assert.Equal(t, 3, messageCount)
}

func generateRedisStreamTestData(topicCount, messageCount int, data string) []redis.XMessage {
	generateXMessage := func(id int) redis.XMessage {
		return redis.XMessage{
			ID: fmt.Sprintf("%d", id),
			Values: map[string]interface{}{
				"data": data,
			},
		}
	}

	xmessageArray := make([]redis.XMessage, messageCount)
	for i := range xmessageArray {
		xmessageArray[i] = generateXMessage(i)
	}

	return xmessageArray
}
