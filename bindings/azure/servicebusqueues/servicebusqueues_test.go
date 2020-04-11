// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package servicebusqueues

import (
	"context"
	"os"
	"testing"
	"time"

	servicebus "github.com/Azure/azure-service-bus-go"
	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/dapr/pkg/logger"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

const (
	// Environment variable containing the connection string to Azure Service Bus
	testServiceBusEnvKey = "DAPR_TEST_AZURE_SERVICEBUS"
)

func getTestServiceBusConnectionString() string {
	return os.Getenv(testServiceBusEnvKey)
}

func TestParseMetadata(t *testing.T) {
	var oneSecondDuration time.Duration = time.Second

	testCases := []struct {
		name                     string
		properties               map[string]string
		expectedConnectionString string
		expectedQueueName        string
		expectedTTL              time.Duration
	}{
		{
			name:                     "ConnectionString and queue name",
			properties:               map[string]string{"connectionString": "connString", "queueName": "queue1"},
			expectedConnectionString: "connString",
			expectedQueueName:        "queue1",
			expectedTTL:              AzureServiceBusDefaultMessageTimeToLive,
		},
		{
			name:                     "Empty TTL",
			properties:               map[string]string{"connectionString": "connString", "queueName": "queue1", bindings.TTLMetadataKey: ""},
			expectedConnectionString: "connString",
			expectedQueueName:        "queue1",
			expectedTTL:              AzureServiceBusDefaultMessageTimeToLive,
		},
		{
			name:                     "With TTL",
			properties:               map[string]string{"connectionString": "connString", "queueName": "queue1", bindings.TTLMetadataKey: "1"},
			expectedConnectionString: "connString",
			expectedQueueName:        "queue1",
			expectedTTL:              oneSecondDuration,
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			m := bindings.Metadata{}
			m.Properties = tt.properties
			a := NewAzureServiceBusQueues(logger.NewLogger("test"))
			meta, err := a.parseMetadata(m)
			assert.Nil(t, err)
			assert.Equal(t, tt.expectedConnectionString, meta.ConnectionString)
			assert.Equal(t, tt.expectedQueueName, meta.QueueName)
			assert.Equal(t, tt.expectedTTL, meta.ttl)
		})
	}
}

func TestParseMetadataWithInvalidTTL(t *testing.T) {
	testCases := []struct {
		name       string
		properties map[string]string
	}{
		{
			name:       "Whitespaces TTL",
			properties: map[string]string{"connectionString": "connString", "queueName": "queue1", bindings.TTLMetadataKey: "  "},
		},
		{
			name:       "Negative ttl",
			properties: map[string]string{"connectionString": "connString", "queueName": "queue1", bindings.TTLMetadataKey: "-1"},
		},
		{
			name:       "Non-numeric ttl",
			properties: map[string]string{"connectionString": "connString", "queueName": "queue1", bindings.TTLMetadataKey: "abc"},
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			m := bindings.Metadata{}
			m.Properties = tt.properties

			a := NewAzureServiceBusQueues(logger.NewLogger("test"))
			_, err := a.parseMetadata(m)
			assert.NotNil(t, err)
		})
	}
}

type testQueueHandler struct {
	callback func(*servicebus.Message)
}

func (h testQueueHandler) Handle(ctx context.Context, message *servicebus.Message) error {
	h.callback(message)
	return message.Complete(ctx)
}

func getMessageWithRetries(queue *servicebus.Queue, maxDuration time.Duration) (*servicebus.Message, bool, error) {
	var receivedMessage *servicebus.Message

	queueHandler := testQueueHandler{
		callback: func(msg *servicebus.Message) {
			receivedMessage = msg
		},
	}
	ctx, cancel := context.WithTimeout(context.Background(), maxDuration)
	defer cancel()
	err := queue.ReceiveOne(ctx, queueHandler)
	if err != nil && err != context.DeadlineExceeded {
		return nil, false, err
	}

	return receivedMessage, receivedMessage != nil, nil
}

func TestQueueWithTTL(t *testing.T) {
	serviceBusConnectionString := getTestServiceBusConnectionString()
	if serviceBusConnectionString == "" {
		t.Skipf("Azure ServiceBus binding integration tests skipped. To enable define the connection string using environment variable '%s'", testServiceBusEnvKey)
	}

	queueName := uuid.New().String()
	a := NewAzureServiceBusQueues(logger.NewLogger("test"))
	m := bindings.Metadata{}
	m.Properties = map[string]string{"connectionString": serviceBusConnectionString, "queueName": queueName, bindings.TTLMetadataKey: "1"}
	err := a.Init(m)
	assert.Nil(t, err)

	// Assert thet queue was created with an time to live value
	ns, err := servicebus.NewNamespace(servicebus.NamespaceWithConnectionString(serviceBusConnectionString))
	assert.Nil(t, err)
	queue, err := ns.NewQueue(queueName)
	assert.Nil(t, err)

	qmr := ns.NewQueueManager()
	defer qmr.Delete(context.Background(), queueName)

	queueEntity, err := qmr.Get(context.Background(), queueName)
	assert.Nil(t, err)
	assert.Equal(t, "PT1S", *queueEntity.DefaultMessageTimeToLive)

	// Assert that if waited too long, we won't see any message
	const tooLateMsgContent = "too_late_msg"
	err = a.Write(&bindings.WriteRequest{Data: []byte(tooLateMsgContent)})
	assert.Nil(t, err)

	time.Sleep(time.Second * 2)

	const ttlInSeconds = 1
	const maxGetDuration = ttlInSeconds * time.Second

	_, ok, err := getMessageWithRetries(queue, maxGetDuration)
	assert.Nil(t, err)
	assert.False(t, ok)

	// Getting before it is expired, should return it
	const testMsgContent = "test_msg"
	err = a.Write(&bindings.WriteRequest{Data: []byte(testMsgContent)})
	assert.Nil(t, err)

	msg, ok, err := getMessageWithRetries(queue, maxGetDuration)
	assert.Nil(t, err)
	assert.True(t, ok)
	msgBody := string(msg.Data)
	assert.Equal(t, testMsgContent, msgBody)
	assert.NotNil(t, msg.TTL)
	assert.Equal(t, time.Second, *msg.TTL)
}

func TestPublishingWithTTL(t *testing.T) {
	serviceBusConnectionString := getTestServiceBusConnectionString()
	if serviceBusConnectionString == "" {
		t.Skipf("Azure ServiceBus binding integration tests skipped. To enable define the connection string using environment variable '%s'", testServiceBusEnvKey)
	}

	queueName := uuid.New().String()
	queueBinding1 := NewAzureServiceBusQueues(logger.NewLogger("test"))
	bindingMetadata := bindings.Metadata{}
	bindingMetadata.Properties = map[string]string{"connectionString": serviceBusConnectionString, "queueName": queueName}
	err := queueBinding1.Init(bindingMetadata)
	assert.Nil(t, err)

	// Assert thet queue was created with Azure default time to live value
	ns, err := servicebus.NewNamespace(servicebus.NamespaceWithConnectionString(serviceBusConnectionString))
	assert.Nil(t, err)

	queue, err := ns.NewQueue(queueName)
	assert.Nil(t, err)

	qmr := ns.NewQueueManager()
	defer qmr.Delete(context.Background(), queueName)

	queueEntity, err := qmr.Get(context.Background(), queueName)
	assert.Nil(t, err)
	const defaultAzureServiceBusMessageTimeToLive = "P14D"
	assert.Equal(t, defaultAzureServiceBusMessageTimeToLive, *queueEntity.DefaultMessageTimeToLive)

	const tooLateMsgContent = "too_late_msg"
	writeRequest := bindings.WriteRequest{
		Data: []byte(tooLateMsgContent),
		Metadata: map[string]string{
			bindings.TTLMetadataKey: "1",
		},
	}
	err = queueBinding1.Write(&writeRequest)
	assert.Nil(t, err)

	time.Sleep(time.Second * 5)

	const ttlInSeconds = 1
	const maxGetDuration = ttlInSeconds * time.Second

	_, ok, err := getMessageWithRetries(queue, maxGetDuration)
	assert.Nil(t, err)
	assert.False(t, ok)

	// Getting before it is expired, should return it
	queueBinding2 := NewAzureServiceBusQueues(logger.NewLogger("test"))
	err = queueBinding2.Init(bindingMetadata)
	assert.Nil(t, err)

	const testMsgContent = "test_msg"
	writeRequest = bindings.WriteRequest{
		Data: []byte(testMsgContent),
		Metadata: map[string]string{
			bindings.TTLMetadataKey: "1",
		},
	}
	err = queueBinding2.Write(&writeRequest)
	assert.Nil(t, err)

	msg, ok, err := getMessageWithRetries(queue, maxGetDuration)
	assert.Nil(t, err)
	assert.True(t, ok)
	msgBody := string(msg.Data)
	assert.Equal(t, testMsgContent, msgBody)
	assert.NotNil(t, msg.TTL)

	assert.Equal(t, time.Second, *msg.TTL)
}
