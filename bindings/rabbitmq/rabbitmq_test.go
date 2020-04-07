// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package rabbitmq

import (
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/dapr/pkg/logger"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	"github.com/streadway/amqp"
)

const (
	// Environment variable containing the host name for RabbitMQ integration tests
	// To run using docker: docker run -d --hostname -rabbit --name test-rabbit -p 15672:15672 -p 5672:5672 rabbitmq:3-management
	// In that case the connection string will be: amqp://guest:guest@localhost:5672/
	testRabbitMQHostEnvKey = "DAPR_TEST_RABBITMQ_HOST"

	ttlBindingMetadataKey = "ttl"
)

func getTestRabbitMQHost() string {
	return os.Getenv(testRabbitMQHostEnvKey)
}

func TestParseMetadata(t *testing.T) {
	const queueName = "test-queue"
	const host = "test-host"
	var oneSecondTTL time.Duration = time.Second

	testCases := []struct {
		name                     string
		properties               map[string]string
		expectedDeleteWhenUnused bool
		expectedDurable          bool
		expectedTTL              *time.Duration
	}{
		{
			name:                     "Delete / Durable",
			properties:               map[string]string{"QueueName": queueName, "Host": host, "DeleteWhenUnused": "true", "Durable": "true"},
			expectedDeleteWhenUnused: true,
			expectedDurable:          true,
		},
		{
			name:                     "Not Delete / Not Durable",
			properties:               map[string]string{"QueueName": queueName, "Host": host, "DeleteWhenUnused": "false", "Durable": "false"},
			expectedDeleteWhenUnused: false,
			expectedDurable:          false,
		},
		{
			name:                     "TTL",
			properties:               map[string]string{"QueueName": queueName, "Host": host, "DeleteWhenUnused": "false", "Durable": "false", "ttl": "1"},
			expectedDeleteWhenUnused: false,
			expectedDurable:          false,
			expectedTTL:              &oneSecondTTL,
		},
		{
			name:                     "Empty TTL",
			properties:               map[string]string{"QueueName": queueName, "Host": host, "DeleteWhenUnused": "false", "Durable": "false", "ttl": ""},
			expectedDeleteWhenUnused: false,
			expectedDurable:          false,
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			m := bindings.Metadata{}
			m.Properties = tt.properties
			r := RabbitMQ{logger: logger.NewLogger("test")}
			err := r.parseMetadata(m)
			assert.Nil(t, err)
			assert.Equal(t, queueName, r.metadata.QueueName)
			assert.Equal(t, host, r.metadata.Host)
			assert.Equal(t, tt.expectedDeleteWhenUnused, r.metadata.DeleteWhenUnused)
			assert.Equal(t, tt.expectedDurable, r.metadata.Durable)
			assert.Equal(t, tt.expectedTTL, r.defaultQueueTTL)
		})
	}
}

func TestParseMetadataWithInvalidTTL(t *testing.T) {
	const queueName = "test-queue"
	const host = "test-host"

	testCases := []struct {
		name       string
		properties map[string]string
	}{
		{
			name:       "Whitespaces TTL",
			properties: map[string]string{"QueueName": queueName, "Host": host, "ttl": "  "},
		},
		{
			name:       "Negative ttl",
			properties: map[string]string{"QueueName": queueName, "Host": host, "ttl": "-1"},
		},
		{
			name:       "Non-numeric ttl",
			properties: map[string]string{"QueueName": queueName, "Host": host, "ttl": "abc"},
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			m := bindings.Metadata{}
			m.Properties = tt.properties
			r := RabbitMQ{logger: logger.NewLogger("test")}
			err := r.parseMetadata(m)
			assert.NotNil(t, err)
		})
	}
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
	if rabbitmqHost == "" {
		t.Skipf("RabbitMQ binding integration tests skipped. To enable define the connection string using environment variable '%s' (example 'amqp://guest:guest@localhost:5672/')", testRabbitMQHostEnvKey)
	}

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
			"ttl":              strconv.FormatInt(ttlInSeconds, 10),
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
	err = r.Write(&bindings.WriteRequest{Data: []byte(tooLateMsgContent)})
	assert.Nil(t, err)

	time.Sleep(time.Second + (ttlInSeconds * time.Second))

	_, ok, err := getMessageWithRetries(ch, queueName, maxGetDuration)
	assert.Nil(t, err)
	assert.False(t, ok)

	// Getting before it is expired, should return it
	const testMsgContent = "test_msg"
	err = r.Write(&bindings.WriteRequest{Data: []byte(testMsgContent)})
	assert.Nil(t, err)

	msg, ok, err := getMessageWithRetries(ch, queueName, maxGetDuration)
	assert.Nil(t, err)
	assert.True(t, ok)
	msgBody := string(msg.Body)
	assert.Equal(t, testMsgContent, msgBody)
}

func TestPublishingWithTTL(t *testing.T) {
	rabbitmqHost := getTestRabbitMQHost()
	if rabbitmqHost == "" {
		t.Skipf("RabbitMQ binding integration tests skipped. To enable define the connection string using environment variable '%s' (example 'amqp://guest:guest@localhost:5672/')", testRabbitMQHostEnvKey)
	}

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
	writeRequest := bindings.WriteRequest{
		Data: []byte(tooLateMsgContent),
		Metadata: map[string]string{
			ttlBindingMetadataKey: strconv.Itoa(ttlInSeconds),
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
	writeRequest = bindings.WriteRequest{
		Data: []byte(testMsgContent),
		Metadata: map[string]string{
			ttlBindingMetadataKey: strconv.Itoa(ttlInSeconds * 1000),
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
