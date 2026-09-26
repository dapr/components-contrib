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
	"github.com/stretchr/testify/require"

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

func TestQueueTTLDeclaration(t *testing.T) {
	rabbitmqHost := getTestRabbitMQHost()
	require.NotEmpty(t, rabbitmqHost)

	testCases := []struct {
		name       string
		properties map[string]string
		args       amqp.Table
	}{
		{
			name: "unset",
		},
		{
			name:       "legacy one second",
			properties: map[string]string{"ttlInSeconds": "1"},
			args:       amqp.Table{rabbitMQQueueMessageTTLKey: int32(1000)},
		},
		{
			name:       "below signed 32-bit milliseconds",
			properties: map[string]string{"ttl": "2147483646ms"},
			args:       amqp.Table{rabbitMQQueueMessageTTLKey: int64(2147483646)},
		},
		{
			name:       "at signed 32-bit milliseconds",
			properties: map[string]string{"ttl": "2147483647ms"},
			args:       amqp.Table{rabbitMQQueueMessageTTLKey: int64(2147483647)},
		},
		{
			name:       "above signed 32-bit milliseconds",
			properties: map[string]string{"ttl": "2147483648ms"},
			args:       amqp.Table{rabbitMQQueueMessageTTLKey: int64(2147483648)},
		},
		{
			name:       "thirty days alias with priority",
			properties: map[string]string{"ttlInSeconds": "2592000", "maxPriority": "10"},
			args:       amqp.Table{rabbitMQQueueMessageTTLKey: int64(2592000000), rabbitMQMaxPriorityKey: uint8(10)},
		},
		{
			name:       "thirty days duration",
			properties: map[string]string{"ttl": "720h"},
			args:       amqp.Table{rabbitMQQueueMessageTTLKey: int64(2592000000)},
		},
	}
	for _, tt := range testCases {
		for _, predeclare := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/predeclare=%t", tt.name, predeclare), func(t *testing.T) {
				conn, err := amqp.Dial(rabbitmqHost)
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, conn.Close()) })
				ch, err := conn.Channel()
				require.NoError(t, err)

				queueName := uuid.New().String()
				created := false
				t.Cleanup(func() {
					if created {
						cleanupCh, cleanupErr := conn.Channel()
						require.NoError(t, cleanupErr)
						_, cleanupErr = cleanupCh.QueueDelete(queueName, false, false, false)
						require.NoError(t, cleanupErr)
					}
				})
				if predeclare {
					_, err = ch.QueueDeclare(queueName, true, false, false, false, tt.args)
					require.NoError(t, err)
					created = true
				}

				properties := map[string]string{"queueName": queueName, "host": rabbitmqHost, "durable": "true"}
				for key, value := range tt.properties {
					properties[key] = value
				}
				r := NewRabbitMQ(logger.NewLogger("test"))
				t.Cleanup(func() { require.NoError(t, r.Close()) })
				require.NoError(t, r.Init(t.Context(), bindings.Metadata{
					Base: contribMetadata.Base{Properties: properties},
				}))
				created = true

				// RabbitMQ rejects a redeclaration if the stored TTL differs from these exact arguments.
				_, err = ch.QueueDeclare(queueName, true, false, false, false, tt.args)
				require.NoError(t, err)
				_, err = r.Invoke(t.Context(), &bindings.InvokeRequest{Data: []byte("test-message")})
				require.NoError(t, err)
				msg, ok, err := getMessageWithRetries(ch, queueName, time.Second)
				require.NoError(t, err)
				require.True(t, ok)
				assert.Equal(t, "test-message", string(msg.Body))
			})
		}
	}
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

	conn, err := amqp.Dial(rabbitmqHost)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })

	ch, err := conn.Channel()
	require.NoError(t, err)

	// Queues created by older bindings used a 32-bit TTL argument.
	_, err = ch.QueueDeclare(queueName, durable, false, exclusive, false, amqp.Table{
		rabbitMQQueueMessageTTLKey: int32(ttlInSeconds * 1000),
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		_, cleanupErr := ch.QueueDelete(queueName, false, false, false)
		require.NoError(t, cleanupErr)
	})

	r := NewRabbitMQ(logger.NewLogger("test"))
	t.Cleanup(func() { require.NoError(t, r.Close()) })
	require.NoError(t, r.Init(t.Context(), metadata))

	// Assert that if waited too long, we won't see any message
	const tooLateMsgContent = "too_late_msg"
	_, err = r.Invoke(t.Context(), &bindings.InvokeRequest{Data: []byte(tooLateMsgContent)})
	require.NoError(t, err)

	time.Sleep(time.Second + (ttlInSeconds * time.Second))

	_, ok, err := getMessageWithRetries(ch, queueName, maxGetDuration)
	require.NoError(t, err)
	assert.False(t, ok)

	// Getting before it is expired, should return it
	const testMsgContent = "test_msg"
	_, err = r.Invoke(t.Context(), &bindings.InvokeRequest{Data: []byte(testMsgContent)})
	require.NoError(t, err)

	msg, ok, err := getMessageWithRetries(ch, queueName, maxGetDuration)
	require.NoError(t, err)
	assert.True(t, ok)
	msgBody := string(msg.Body)
	assert.Equal(t, testMsgContent, msgBody)
}

func TestQueuesReconnect(t *testing.T) {
	rabbitmqHost := getTestRabbitMQHost()
	assert.NotEmpty(t, rabbitmqHost, fmt.Sprintf("RabbitMQ host configuration must be set in environment variable '%s' (example 'amqp://guest:guest@localhost:5672/')", testRabbitMQHostEnvKey))

	queueName := uuid.New().String()
	durable := true
	exclusive := false

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

	var messageReceivedCount int
	var handler bindings.Handler = func(ctx context.Context, in *bindings.ReadResponse) ([]byte, error) {
		messageReceivedCount++
		return nil, nil
	}

	logger := logger.NewLogger("test")

	r := NewRabbitMQ(logger).(*RabbitMQ)
	err := r.Init(t.Context(), metadata)
	require.NoError(t, err)

	err = r.Read(t.Context(), handler)
	require.NoError(t, err)

	const tooLateMsgContent = "success_msg1"
	_, err = r.Invoke(t.Context(), &bindings.InvokeRequest{Data: []byte(tooLateMsgContent)})
	require.NoError(t, err)

	// perform a close connection with the rabbitmq server
	r.channel.Close()
	time.Sleep(3 * defaultReconnectWait)

	const testMsgContent = "reconnect_msg"
	_, err = r.Invoke(t.Context(), &bindings.InvokeRequest{Data: []byte(testMsgContent)})
	require.NoError(t, err)

	time.Sleep(defaultReconnectWait)
	// sending 2 messages, one before the reconnect and one after
	assert.Equal(t, 2, messageReceivedCount)
	require.NoError(t, r.Close())
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
	err := rabbitMQBinding1.Init(t.Context(), metadata)
	require.NoError(t, err)

	// Assert that if waited too long, we won't see any message
	conn, err := amqp.Dial(rabbitmqHost)
	require.NoError(t, err)
	defer conn.Close()

	ch, err := conn.Channel()
	require.NoError(t, err)
	defer ch.Close()

	const tooLateMsgContent = "too_late_msg"
	writeRequest := bindings.InvokeRequest{
		Data: []byte(tooLateMsgContent),
		Metadata: map[string]string{
			contribMetadata.TTLMetadataKey: strconv.Itoa(ttlInSeconds),
		},
	}

	_, err = rabbitMQBinding1.Invoke(t.Context(), &writeRequest)
	require.NoError(t, err)

	time.Sleep(time.Second + (ttlInSeconds * time.Second))

	_, ok, err := getMessageWithRetries(ch, queueName, maxGetDuration)
	require.NoError(t, err)
	assert.False(t, ok)

	// Getting before it is expired, should return it
	rabbitMQBinding2 := NewRabbitMQ(logger).(*RabbitMQ)
	err = rabbitMQBinding2.Init(t.Context(), metadata)
	require.NoError(t, err)

	const testMsgContent = "test_msg"
	writeRequest = bindings.InvokeRequest{
		Data: []byte(testMsgContent),
		Metadata: map[string]string{
			contribMetadata.TTLMetadataKey: strconv.Itoa(ttlInSeconds * 1000),
		},
	}
	_, err = rabbitMQBinding2.Invoke(t.Context(), &writeRequest)
	require.NoError(t, err)

	msg, ok, err := getMessageWithRetries(ch, queueName, maxGetDuration)
	require.NoError(t, err)
	assert.True(t, ok)
	msgBody := string(msg.Body)
	assert.Equal(t, testMsgContent, msgBody)

	require.NoError(t, rabbitMQBinding1.Close())
	require.NoError(t, rabbitMQBinding2.Close())
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
	err := r.Init(t.Context(), metadata)
	require.NoError(t, err)

	// Assert that if waited too long, we won't see any message
	conn, err := amqp.Dial(rabbitmqHost)
	require.NoError(t, err)
	defer conn.Close()

	ch, err := conn.Channel()
	require.NoError(t, err)

	if _, err = ch.QueueDeclarePassive(queueName, durable, false, false, false, amqp.Table{}); err != nil {
		// Assert that queue actually exists if an error is thrown
		assert.Equal(t, strings.Contains(err.Error(), "404"), false)
	}

	ch.Close()
	r.connection.Close()

	ch, err = conn.Channel()
	require.NoError(t, err)
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
	err := r.Init(t.Context(), metadata)
	require.NoError(t, err)

	// Assert that if waited too long, we won't see any message
	conn, err := amqp.Dial(rabbitmqHost)
	require.NoError(t, err)
	defer conn.Close()

	ch, err := conn.Channel()
	require.NoError(t, err)
	defer ch.Close()

	const middlePriorityMsgContent = "middle"
	_, err = r.Invoke(t.Context(), &bindings.InvokeRequest{
		Metadata: map[string]string{
			contribMetadata.PriorityMetadataKey: "5",
		},
		Data: []byte(middlePriorityMsgContent),
	})
	require.NoError(t, err)

	const lowPriorityMsgContent = "low"
	_, err = r.Invoke(t.Context(), &bindings.InvokeRequest{
		Metadata: map[string]string{
			contribMetadata.PriorityMetadataKey: "1",
		},
		Data: []byte(lowPriorityMsgContent),
	})
	require.NoError(t, err)

	const highPriorityMsgContent = "high"
	_, err = r.Invoke(t.Context(), &bindings.InvokeRequest{
		Metadata: map[string]string{
			contribMetadata.PriorityMetadataKey: "10",
		},
		Data: []byte(highPriorityMsgContent),
	})
	require.NoError(t, err)

	time.Sleep(100 * time.Millisecond)

	msg, ok, err := getMessageWithRetries(ch, queueName, 1*time.Second)
	require.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, highPriorityMsgContent, string(msg.Body))

	msg, ok, err = getMessageWithRetries(ch, queueName, 1*time.Second)
	require.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, middlePriorityMsgContent, string(msg.Body))

	msg, ok, err = getMessageWithRetries(ch, queueName, 1*time.Second)
	require.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, lowPriorityMsgContent, string(msg.Body))
}

func TestPublishWithHeaders(t *testing.T) {
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
	err := r.Init(t.Context(), metadata)
	require.NoError(t, err)

	// Assert that if waited too long, we won't see any message
	conn, err := amqp.Dial(rabbitmqHost)
	require.NoError(t, err)
	defer conn.Close()

	ch, err := conn.Channel()
	require.NoError(t, err)
	defer ch.Close()

	const msgContent = "some content"
	_, err = r.Invoke(t.Context(), &bindings.InvokeRequest{
		Metadata: map[string]string{
			"custom_header1": "some value",
			"custom_header2": "some other value",
		},
		Data: []byte(msgContent),
	})
	require.NoError(t, err)

	time.Sleep(100 * time.Millisecond)

	msg, ok, err := getMessageWithRetries(ch, queueName, 1*time.Second)
	require.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, msgContent, string(msg.Body))
	// assert.Contains(t, msg.Header, "custom_header1")
	// assert.Contains(t, msg.Header, "custom_header2")
}

func TestPublishMetadataProperties(t *testing.T) {
	rabbitmqHost := getTestRabbitMQHost()
	require.NotEmpty(t, rabbitmqHost, fmt.Sprintf("RabbitMQ host configuration must be set in environment variable '%s'", testRabbitMQHostEnvKey))

	queueName := uuid.New().String()
	durable := true
	exclusive := false

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
	r := NewRabbitMQ(logger).(*RabbitMQ)
	err := r.Init(t.Context(), metadata)
	require.NoError(t, err)

	conn, err := amqp.Dial(rabbitmqHost)
	require.NoError(t, err)
	defer conn.Close()

	ch, err := conn.Channel()
	require.NoError(t, err)
	defer ch.Close()

	const messageData = "test message"
	const msgID = "msg-123"
	const corrID = "corr-456"
	const msgType = "testType"
	const contentType = "application/json"

	writeRequest := bindings.InvokeRequest{
		Data: []byte(messageData),
		Metadata: map[string]string{
			"messageID":     msgID,
			"correlationID": corrID,
			"type":          msgType,
			"contentType":   contentType,
		},
	}
	_, err = r.Invoke(t.Context(), &writeRequest)
	require.NoError(t, err)

	// Retrieve the message.
	msg, ok, err := getMessageWithRetries(ch, queueName, 2*time.Second)
	require.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, messageData, string(msg.Body))
	assert.Equal(t, msgID, msg.MessageId)
	assert.Equal(t, corrID, msg.CorrelationId)
	assert.Equal(t, msgType, msg.Type)
	assert.Equal(t, contentType, msg.ContentType)

	require.NoError(t, r.Close())
}
