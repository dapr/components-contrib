// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package azureservicebus

import (
	"errors"
	"fmt"
	"testing"

	"github.com/Azure/azure-service-bus-go"
	"github.com/stretchr/testify/assert"

	"github.com/dapr/components-contrib/pubsub"
)

func getFakeProperties() map[string]string {
	return map[string]string{
		connString: "fakeConnectionString",
	}
}

func TestParseServiceBusMetadata(t *testing.T) {
	t.Run("metadata is correct", func(t *testing.T) {
		fakeProperties := getFakeProperties()

		fakeMetaData := pubsub.Metadata{
			Properties: fakeProperties,
		}

		// act
		m, err := parseServiceBusMetadata(fakeMetaData)

		// assert
		assert.NoError(t, err)
		assert.Equal(t, fakeProperties[connString], m.connectionString)
	})

	t.Run("connectionstring is not given", func(t *testing.T) {
		fakeProperties := getFakeProperties()

		fakeMetaData := pubsub.Metadata{
			Properties: fakeProperties,
		}
		fakeMetaData.Properties[connString] = ""

		// act
		m, err := parseServiceBusMetadata(fakeMetaData)

		// assert
		assert.Error(t, errors.New(""), err)
		assert.Empty(t, m.connectionString)
	})
}

func TestProcessTopics(t *testing.T) {
	fakeConsumerID := "fakeConsumer"
	topicCount := 0
	messageCount := 0

	fakeHandler := func(msg *pubsub.NewMessage) error {
		expectedTopic := fmt.Sprintf("Topic%d", topicCount)
		expectedData := fmt.Sprintf("testData%d", messageCount)

		messageCount++
		if topicCount == 0 && messageCount >= 3 {
			topicCount = 1
			messageCount = 0
		}

		// assert
		assert.Equal(t, expectedTopic, msg.Topic)
		assert.Equal(t, expectedData, string(msg.Data))

		// return fake error to skip executing redis client command
		return errors.New("fake error")
	}

	// act
	testRedisStream := &serviceBus{}
	testRedisStream.processStreams(fakeConsumerID, generateRedisStreamTestData(2, 3), fakeHandler)

	// assert
	assert.Equal(t, 1, topicCount)
	assert.Equal(t, 3, messageCount)
}

func generateRedisStreamTestData(topicCount, messageCount int) []redis.XStream {
	generateXMessage := func(id int) redis.XMessage {
		return redis.XMessage{
			ID: fmt.Sprintf("%d", id),
			Values: map[string]interface{}{
				"data": fmt.Sprintf("testData%d", id),
			},
		}
	}

	xmessageArray := make([]redis.XMessage, messageCount)
	for i := range xmessageArray {
		xmessageArray[i] = generateXMessage(i)
	}

	redisStreams := make([]redis.XStream, topicCount)
	for i := range redisStreams {
		redisStreams[i] = redis.XStream{
			Stream:   fmt.Sprintf("Topic%d", i),
			Messages: xmessageArray,
		}
	}
	return redisStreams
}
