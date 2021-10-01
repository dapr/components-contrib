//go:build integration_test
// +build integration_test

// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------
package eventhubs

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/kit/logger"
)

const (
	// Note: Reuse the environment variables names from the conformance tests where possible to support reuse of setup workflows

	// iotHubConnectionStringEnvKey defines the key containing the integration test connection string
	// For the default EventHub endpoint for an Azure IoT Hub, it will resemble:
	// Endpoint=sb://<iotHubGeneratedNamespace>.servicebus.windows.net/;SharedAccessKeyName=service;SharedAccessKey=<key>;EntityPath=<iotHubGeneratedPath>
	iotHubConnectionStringEnvKey = "AzureIotHubEventHubConnectionString"
	iotHubConsumerGroupEnvKey    = "AzureIotHubPubsubConsumerGroup"
	iotHubNameEnvKey             = "AzureIotHubName"
	storageAccountNameEnvKey     = "AzureBlobStorageAccount"
	storageAccountKeyEnvKey      = "AzureBlobStorageAccessKey"
	azureCredentialsEnvKey       = "AZURE_CREDENTIALS"

	testStorageContainerName = "iothub-pubsub-integration-test"
	testTopic                = "integration-test-topic"
)

func createIotHubPubsubMetadata() pubsub.Metadata {
	metadata := pubsub.Metadata{
		Properties: map[string]string{
			connectionString:     os.Getenv(iotHubConnectionStringEnvKey),
			consumerID:           os.Getenv(iotHubConsumerGroupEnvKey),
			storageAccountName:   os.Getenv(storageAccountNameEnvKey),
			storageAccountKey:    os.Getenv(storageAccountKeyEnvKey),
			storageContainerName: testStorageContainerName,
		},
	}

	return metadata
}

func testReadIotHubEvents(t *testing.T) {
	logger := logger.NewLogger("pubsub.azure.eventhubs.integration.test")
	eh := NewAzureEventHubs(logger)
	err := eh.Init(createIotHubPubsubMetadata())
	assert.Nil(t, err)

	// Invoke az CLI via bash script to send test IoT device events
	// Requires the AZURE_CREDENTIALS environment variable to be already set (output of `az ad sp create-for-rbac`)
	cmd := exec.Command("/bin/bash", "../../../tests/scripts/send-iot-device-events.sh")
	cmd.Env = append(os.Environ(), fmt.Sprintf("IOT_HUB_NAME=%s", os.Getenv(iotHubNameEnvKey)))
	out, err := cmd.CombinedOutput()
	assert.Nil(t, err, "Error in send-iot-device-events.sh:\n%s", out)

	// Setup subscription to capture messages in a closure so that test asserts can be
	// performed on the main thread, including the case where the handler is never invoked.
	var messages []pubsub.NewMessage
	handler := func(ctx context.Context, msg *pubsub.NewMessage) error {
		messages = append(messages, *msg)
		return nil
	}

	req := pubsub.SubscribeRequest{
		Topic:    testTopic, // TODO: Handle Topic configuration after EventHubs pubsub rewrite #951
		Metadata: map[string]string{},
	}
	err = eh.Subscribe(req, handler)
	assert.Nil(t, err)

	// Note: azure-event-hubs-go SDK defaultLeasePersistenceInterval is 5s
	// Sleep long enough so that the azure event hubs SDK has time to persist updated checkpoints
	// before the test process exits.
	time.Sleep(10 * time.Second)

	assert.Greater(t, len(messages), 0, "Failed to receive any IotHub events")
	logger.Infof("Received %d messages", len(messages))
	for _, r := range messages {
		assert.Equal(t, r.Topic, testTopic, "Message topic doesn't match subscription")
		assert.Contains(t, string(r.Data), "Integration test message")

		// Verify expected IoT Hub device event metadata exists
		// TODO: add device messages than can populate the sysPropPartitionKey and sysPropIotHubConnectionModuleID metadata
		assert.Contains(t, r.Metadata, sysPropSequenceNumber, "IoT device event missing: %s", sysPropSequenceNumber)
		assert.Contains(t, r.Metadata, sysPropEnqueuedTime, "IoT device event missing: %s", sysPropEnqueuedTime)
		assert.Contains(t, r.Metadata, sysPropOffset, "IoT device event missing: %s", sysPropOffset)
		assert.Contains(t, r.Metadata, sysPropIotHubDeviceConnectionID, "IoT device event missing: %s", sysPropIotHubDeviceConnectionID)
		assert.Contains(t, r.Metadata, sysPropIotHubAuthGenerationID, "IoT device event missing: %s", sysPropIotHubAuthGenerationID)
		assert.Contains(t, r.Metadata, sysPropIotHubConnectionAuthMethod, "IoT device event missing: %s", sysPropIotHubConnectionAuthMethod)
		assert.Contains(t, r.Metadata, sysPropIotHubEnqueuedTime, "IoT device event missing: %s", sysPropIotHubEnqueuedTime)
	}

	eh.Close()
}

func TestIntegrationCases(t *testing.T) {
	connectionString := os.Getenv(iotHubConnectionStringEnvKey)
	if connectionString == "" {
		t.Skipf("EventHubs pubsub integration to IoT Hub tests skipped. To enable them, define the endpoint connection string using environment variable '%s')", iotHubConnectionStringEnvKey)
	}

	t.Run("Read IoT Hub events", testReadIotHubEvents)
}
