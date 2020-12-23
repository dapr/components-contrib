// +build integration_test

// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package servicebusqueues

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	servicebus "github.com/Azure/azure-service-bus-go"
	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/components-contrib/metadata"
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
	assert.NotEmpty(serviceBusConnectionString, fmt.Sprintf("Azure ServiceBus connection string must set in environment variable '%s'", testServiceBusEnvKey))

	queueName := uuid.New().String()
	a := NewAzureServiceBusQueues(logger.NewLogger("test"))
	m := bindings.Metadata{}
	m.Properties = map[string]string{"connectionString": serviceBusConnectionString, "queueName": queueName, metadata.TTLMetadataKey: "1"}
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
	err = a.Write(&bindings.InvokeRequest{Data: []byte(tooLateMsgContent)})
	assert.Nil(t, err)

	time.Sleep(time.Second * 2)

	const ttlInSeconds = 1
	const maxGetDuration = ttlInSeconds * time.Second

	_, ok, err := getMessageWithRetries(queue, maxGetDuration)
	assert.Nil(t, err)
	assert.False(t, ok)

	// Getting before it is expired, should return it
	const testMsgContent = "test_msg"
	err = a.Write(&bindings.InvokeRequest{Data: []byte(testMsgContent)})
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
	assert.NotEmpty(serviceBusConnectionString, fmt.Sprintf("Azure ServiceBus connection string must set in environment variable '%s'", testServiceBusEnvKey))

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
	writeRequest := bindings.InvokeRequest{
		Data: []byte(tooLateMsgContent),
		Metadata: map[string]string{
			metadata.TTLMetadataKey: "1",
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
	writeRequest = bindings.InvokeRequest{
		Data: []byte(testMsgContent),
		Metadata: map[string]string{
			metadata.TTLMetadataKey: "1",
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
