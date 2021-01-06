// +build integration_test

// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package rabbitmq

import (
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/dapr/pkg/logger"
	"github.com/google/uuid"
	"github.com/streadway/amqp"
	"github.com/stretchr/testify/assert"
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
		Name: "testQueue",
		Properties: map[string]string{
			"queueName":             queueName,
			"host":                  rabbitmqHost,
			"deleteWhenUnused":      strconv.FormatBool(exclusive),
			"durable":               strconv.FormatBool(durable),
			metadata.TTLMetadataKey: strconv.FormatInt(ttlInSeconds, 10),
		},
	}

	logger := logger.NewLogger("test")

	r := NewRabbitMQ(logger)
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
	err = r.Write(&bindings.InvokeRequest{Data: []byte(tooLateMsgContent)})
	assert.Nil(t, err)

	time.Sleep(time.Second + (ttlInSeconds * time.Second))

	_, ok, err := getMessageWithRetries(ch, queueName, maxGetDuration)
	assert.Nil(t, err)
	assert.False(t, ok)

	// Getting before it is expired, should return it
	const testMsgContent = "test_msg"
	err = r.Write(&bindings.InvokeRequest{Data: []byte(testMsgContent)})
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
		Name: "testQueue",
		Properties: map[string]string{
			"queueName":        queueName,
			"host":             rabbitmqHost,
			"deleteWhenUnused": strconv.FormatBool(exclusive),
			"durable":          strconv.FormatBool(durable),
		},
	}

	logger := logger.NewLogger("test")

	rabbitMQBinding1 := NewRabbitMQ(logger)
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
			metadata.TTLMetadataKey: strconv.Itoa(ttlInSeconds),
		},
	}

	err = rabbitMQBinding1.Write(&writeRequest)
	assert.Nil(t, err)

	time.Sleep(time.Second + (ttlInSeconds * time.Second))

	_, ok, err := getMessageWithRetries(ch, queueName, maxGetDuration)
	assert.Nil(t, err)
	assert.False(t, ok)

	// Getting before it is expired, should return it
	rabbitMQBinding2 := NewRabbitMQ(logger)
	err = rabbitMQBinding2.Init(metadata)
	assert.Nil(t, err)

	const testMsgContent = "test_msg"
	writeRequest = bindings.InvokeRequest{
		Data: []byte(testMsgContent),
		Metadata: map[string]string{
			metadata.TTLMetadataKey: strconv.Itoa(ttlInSeconds * 1000),
		},
	}
	err = rabbitMQBinding2.Write(&writeRequest)
	assert.Nil(t, err)

	msg, ok, err := getMessageWithRetries(ch, queueName, maxGetDuration)
	assert.Nil(t, err)
	assert.True(t, ok)
	msgBody := string(msg.Body)
	assert.Equal(t, testMsgContent, msgBody)
}
