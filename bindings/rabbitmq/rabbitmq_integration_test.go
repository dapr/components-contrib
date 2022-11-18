//go:build integration_test
// +build integration_test

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

package rabbitmq

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"

	"github.com/dapr/components-contrib/bindings"
	contribMetadata "github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
)

const (
	// Environment variable containing the host name for RabbitMQ integration tests
	// To run using docker: docker run -d --hostname -rabbit --name test-rabbit -p 15672:15672 -p 5672:5672 rabbitmq:3-management
	// In that case the connection string will be: amqp://guest:guest@localhost:5672/
	testRabbitMQHostEnvKey = "DAPR_TEST_RABBITMQ_HOST"
)

func getTestRabbitMQHost() string {
	return os.Getenv(testRabbitMQHostEnvKey)
}

func getMessageWithRetries(ch *amqp.Channel, queueName string, maxDuration time.Duration) (msg amqp.Delivery, ok bool, err error) {
	start := time.Now()
	for time.Since(start) < maxDuration {
		msg, ok, err := ch.Get(queueName, true)
		if err != nil || ok {
			return msg, ok, err
		}

		time.Sleep(100 * time.Millisecond)
	}

	return amqp.Delivery{}, false, nil
}

func TestQueuesWithTTL(t *testing.T) {
	rabbitmqHost := getTestRabbitMQHost()
	assert.NotEmpty(t, rabbitmqHost, fmt.Sprintf("RabbitMQ host configuration must be set in environment variable '%s' (example 'amqp://guest:guest@localhost:5672/')", testRabbitMQHostEnvKey))

	queueName := uuid.New().String()
	durable := true
	exclusive := false
	const ttlInSeconds = 1
	const maxGetDuration = ttlInSeconds * time.Second

	metadata := bindings.Metadata{
		Base: contribMetadata.Base{
			Name: "testQueue",
			Properties: map[string]string{
				"queueName":                    queueName,
				"host":                         rabbitmqHost,
				"deleteWhenUnused":             strconv.FormatBool(exclusive),
				"durable":                      strconv.FormatBool(durable),
				contribMetadata.TTLMetadataKey: strconv.FormatInt(ttlInSeconds, 10),
			},
		},
	}

	logger := logger.NewLogger("test")

	r := NewRabbitMQ(logger).(*RabbitMQ)
	err := r.Init(metadata)
	assert.Nil(t, err)

	// Assert that if waited too long, we won't see any message
	conn, err := amqp.Dial(rabbitmqHost)
	assert.Nil(t, err)
	defer conn.Close()

	ch, err := conn.Channel()
	assert.Nil(t, err)
	defer ch.Close()

	const tooLateMsgContent = "too_late_msg"
	_, err = r.Invoke(context.Background(), &bindings.InvokeRequest{Data: []byte(tooLateMsgContent)})
	assert.Nil(t, err)

	time.Sleep(time.Second + (ttlInSeconds * time.Second))

	_, ok, err := getMessageWithRetries(ch, queueName, maxGetDuration)
	assert.Nil(t, err)
	assert.False(t, ok)

	// Getting before it is expired, should return it
	const testMsgContent = "test_msg"
	_, err = r.Invoke(context.Background(), &bindings.InvokeRequest{Data: []byte(testMsgContent)})
	assert.Nil(t, err)

	msg, ok, err := getMessageWithRetries(ch, queueName, maxGetDuration)
	assert.Nil(t, err)
	assert.True(t, ok)
	msgBody := string(msg.Body)
	assert.Equal(t, testMsgContent, msgBody)
}

func TestPublishingWithTTL(t *testing.T) {
	rabbitmqHost := getTestRabbitMQHost()
	assert.NotEmpty(t, rabbitmqHost, fmt.Sprintf("RabbitMQ host configuration must be set in environment variable '%s' (example 'amqp://guest:guest@localhost:5672/')", testRabbitMQHostEnvKey))

	queueName := uuid.New().String()
	durable := true
	exclusive := false
	const ttlInSeconds = 1
	const maxGetDuration = ttlInSeconds * time.Second

	metadata := bindings.Metadata{
		Base: contribMetadata.Base{
			Name: "testQueue",
			Properties: map[string]string{
				"queueName":        queueName,
				"host":             rabbitmqHost,
				"deleteWhenUnused": strconv.FormatBool(exclusive),
				"durable":          strconv.FormatBool(durable),
			},
		},
	}

	logger := logger.NewLogger("test")

	rabbitMQBinding1 := NewRabbitMQ(logger).(*RabbitMQ)
	err := rabbitMQBinding1.Init(metadata)
	assert.Nil(t, err)

	// Assert that if waited too long, we won't see any message
	conn, err := amqp.Dial(rabbitmqHost)
	assert.Nil(t, err)
	defer conn.Close()

	ch, err := conn.Channel()
	assert.Nil(t, err)
	defer ch.Close()

	const tooLateMsgContent = "too_late_msg"
	writeRequest := bindings.InvokeRequest{
		Data: []byte(tooLateMsgContent),
		Metadata: map[string]string{
			contribMetadata.TTLMetadataKey: strconv.Itoa(ttlInSeconds),
		},
	}

	_, err = rabbitMQBinding1.Invoke(context.Background(), &writeRequest)
	assert.Nil(t, err)

	time.Sleep(time.Second + (ttlInSeconds * time.Second))

	_, ok, err := getMessageWithRetries(ch, queueName, maxGetDuration)
	assert.Nil(t, err)
	assert.False(t, ok)

	// Getting before it is expired, should return it
	rabbitMQBinding2 := NewRabbitMQ(logger).(*RabbitMQ)
	err = rabbitMQBinding2.Init(metadata)
	assert.Nil(t, err)

	const testMsgContent = "test_msg"
	writeRequest = bindings.InvokeRequest{
		Data: []byte(testMsgContent),
		Metadata: map[string]string{
			contribMetadata.TTLMetadataKey: strconv.Itoa(ttlInSeconds * 1000),
		},
	}
	_, err = rabbitMQBinding2.Invoke(context.Background(), &writeRequest)
	assert.Nil(t, err)

	msg, ok, err := getMessageWithRetries(ch, queueName, maxGetDuration)
	assert.Nil(t, err)
	assert.True(t, ok)
	msgBody := string(msg.Body)
	assert.Equal(t, testMsgContent, msgBody)
}

func TestExclusiveQueue(t *testing.T) {
	rabbitmqHost := getTestRabbitMQHost()
	assert.NotEmpty(t, rabbitmqHost, fmt.Sprintf("RabbitMQ host configuration must be set in environment variable '%s' (example 'amqp://guest:guest@localhost:5672/')", testRabbitMQHostEnvKey))

	queueName := uuid.New().String()
	durable := true
	exclusive := true
	const ttlInSeconds = 1
	const maxGetDuration = ttlInSeconds * time.Second

	metadata := bindings.Metadata{
		Base: contribMetadata.Base{
			Name: "testQueue",
			Properties: map[string]string{
				"queueName":                    queueName,
				"host":                         rabbitmqHost,
				"deleteWhenUnused":             strconv.FormatBool(exclusive),
				"durable":                      strconv.FormatBool(durable),
				"exclusive":                    strconv.FormatBool(exclusive),
				contribMetadata.TTLMetadataKey: strconv.FormatInt(ttlInSeconds, 10),
			},
		},
	}

	logger := logger.NewLogger("test")

	r := NewRabbitMQ(logger).(*RabbitMQ)
	err := r.Init(metadata)
	assert.Nil(t, err)

	// Assert that if waited too long, we won't see any message
	conn, err := amqp.Dial(rabbitmqHost)
	assert.Nil(t, err)
	defer conn.Close()

	ch, err := conn.Channel()
	assert.Nil(t, err)

	if _, err = ch.QueueDeclarePassive(queueName, durable, false, false, false, amqp.Table{}); err != nil {
		// Assert that queue actually exists if an error is thrown
		assert.Equal(t, strings.Contains(err.Error(), "404"), false)
	}

	ch.Close()
	r.connection.Close()

	ch, err = conn.Channel()
	assert.Nil(t, err)
	defer ch.Close()

	if _, err = ch.QueueDeclarePassive(queueName, durable, false, false, false, amqp.Table{}); err != nil {
		// Assert that queue actually no longer exists if an error is thrown
		assert.Equal(t, strings.Contains(err.Error(), "404"), true)
	}
}

func TestPublishWithPriority(t *testing.T) {
	rabbitmqHost := getTestRabbitMQHost()
	assert.NotEmpty(t, rabbitmqHost, fmt.Sprintf("RabbitMQ host configuration must be set in environment variable '%s' (example 'amqp://guest:guest@localhost:5672/')", testRabbitMQHostEnvKey))

	queueName := uuid.New().String()
	durable := true
	exclusive := false
	const maxPriority = 10

	metadata := bindings.Metadata{
		Base: contribMetadata.Base{
			Name: "testQueue",
			Properties: map[string]string{
				"queueName":        queueName,
				"host":             rabbitmqHost,
				"deleteWhenUnused": strconv.FormatBool(exclusive),
				"durable":          strconv.FormatBool(durable),
				"maxPriority":      strconv.FormatInt(maxPriority, 10),
			},
		},
	}

	logger := logger.NewLogger("test")

	r := NewRabbitMQ(logger).(*RabbitMQ)
	err := r.Init(metadata)
	assert.Nil(t, err)

	// Assert that if waited too long, we won't see any message
	conn, err := amqp.Dial(rabbitmqHost)
	assert.Nil(t, err)
	defer conn.Close()

	ch, err := conn.Channel()
	assert.Nil(t, err)
	defer ch.Close()

	const middlePriorityMsgContent = "middle"
	_, err = r.Invoke(context.Background(), &bindings.InvokeRequest{
		Metadata: map[string]string{
			contribMetadata.PriorityMetadataKey: "5",
		},
		Data: []byte(middlePriorityMsgContent),
	})
	assert.Nil(t, err)

	const lowPriorityMsgContent = "low"
	_, err = r.Invoke(context.Background(), &bindings.InvokeRequest{
		Metadata: map[string]string{
			contribMetadata.PriorityMetadataKey: "1",
		},
		Data: []byte(lowPriorityMsgContent),
	})
	assert.Nil(t, err)

	const highPriorityMsgContent = "high"
	_, err = r.Invoke(context.Background(), &bindings.InvokeRequest{
		Metadata: map[string]string{
			contribMetadata.PriorityMetadataKey: "10",
		},
		Data: []byte(highPriorityMsgContent),
	})
	assert.Nil(t, err)

	time.Sleep(100 * time.Millisecond)

	msg, ok, err := getMessageWithRetries(ch, queueName, 1*time.Second)
	assert.Nil(t, err)
	assert.True(t, ok)
	assert.Equal(t, highPriorityMsgContent, string(msg.Body))

	msg, ok, err = getMessageWithRetries(ch, queueName, 1*time.Second)
	assert.Nil(t, err)
	assert.True(t, ok)
	assert.Equal(t, middlePriorityMsgContent, string(msg.Body))

	msg, ok, err = getMessageWithRetries(ch, queueName, 1*time.Second)
	assert.Nil(t, err)
	assert.True(t, ok)
	assert.Equal(t, lowPriorityMsgContent, string(msg.Body))
}
