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
	"github.com/dapr/kit/logger"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

const (
	// Environment variable containing the connection string to Azure Service Bus
	testServiceBusEnvKey = "DAPR_TEST_AZURE_SERVICEBUS"
	ttlInSeconds         = 5
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
	assert.NotEmpty(t, serviceBusConnectionString, fmt.Sprintf("Azure ServiceBus connection string must set in environment variable '%s'", testServiceBusEnvKey))

	queueName := uuid.New().String()
	a := NewAzureServiceBusQueues(logger.NewLogger("test"))
	m := bindings.Metadata{}
	m.Properties = map[string]string{"connectionString": serviceBusConnectionString, "queueName": queueName, metadata.TTLMetadataKey: fmt.Sprintf("%d", ttlInSeconds)}
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
	assert.Equal(t, fmt.Sprintf("PT%dS", ttlInSeconds), *queueEntity.DefaultMessageTimeToLive)

	// Assert that if waited too long, we won't see any message
	const tooLateMsgContent = "too_late_msg"
	_, err = a.Invoke(&bindings.InvokeRequest{Data: []byte(tooLateMsgContent)})
	assert.Nil(t, err)

	time.Sleep(time.Second * (ttlInSeconds + 2))

	const maxGetDuration = ttlInSeconds * time.Second

	_, ok, err := getMessageWithRetries(queue, maxGetDuration)
	assert.Nil(t, err)
	assert.False(t, ok)

	// Getting before it is expired, should return it
	const testMsgContent = "test_msg"
	_, err = a.Invoke(&bindings.InvokeRequest{Data: []byte(testMsgContent)})
	assert.Nil(t, err)

	msg, ok, err := getMessageWithRetries(queue, maxGetDuration)
	assert.Nil(t, err)
	assert.True(t, ok)
	msgBody := string(msg.Data)
	assert.Equal(t, testMsgContent, msgBody)
	assert.NotNil(t, msg.TTL)
	assert.Equal(t, ttlInSeconds*time.Second, *msg.TTL)
}

func TestPublishingWithTTL(t *testing.T) {
	serviceBusConnectionString := getTestServiceBusConnectionString()
	assert.NotEmpty(t, serviceBusConnectionString, fmt.Sprintf("Azure ServiceBus connection string must set in environment variable '%s'", testServiceBusEnvKey))

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
			metadata.TTLMetadataKey: fmt.Sprintf("%d", ttlInSeconds),
		},
	}
	_, err = queueBinding1.Invoke(&writeRequest)
	assert.Nil(t, err)

	time.Sleep(time.Second * (ttlInSeconds + 2))

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
			metadata.TTLMetadataKey: fmt.Sprintf("%d", ttlInSeconds),
		},
	}
	_, err = queueBinding2.Invoke(&writeRequest)
	assert.Nil(t, err)

	msg, ok, err := getMessageWithRetries(queue, maxGetDuration)
	assert.Nil(t, err)
	assert.True(t, ok)
	msgBody := string(msg.Data)
	assert.Equal(t, testMsgContent, msgBody)
	assert.NotNil(t, msg.TTL)

	assert.Equal(t, ttlInSeconds*time.Second, *msg.TTL)
}
