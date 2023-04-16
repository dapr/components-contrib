//go:build integration_test
// +build integration_test

/*
Copyright 2023 The Dapr Authors
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

package eventhubs

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/pubsub"
	kitLogger "github.com/dapr/kit/logger"
)

const (
	// Note: Reuse the environment variables names from the conformance tests where possible to support reuse of setup workflows

	// iotHubConnectionStringEnvKey defines the key containing the integration test connection string
	// For the default EventHub endpoint for an Azure IoT Hub, it will resemble:
	// Endpoint=sb://<iotHubGeneratedNamespace>.servicebus.windows.net/;SharedAccessKeyName=service;SharedAccessKey=<key>;EntityPath=integration-test-topic
	iotHubConnectionStringEnvKey = "AzureIotHubEventHubConnectionString"
	iotHubConsumerGroupEnvKey    = "AzureIotHubPubsubConsumerGroup"
	iotHubNameEnvKey             = "AzureIotHubName"
	storageAccountNameEnvKey     = "AzureBlobStorageAccount"
	storageAccountKeyEnvKey      = "AzureBlobStorageAccessKey"
	azureCredentialsEnvKey       = "AZURE_CREDENTIALS"

	testStorageContainerName = "iothub-pubsub-integration-test"
	testTopic                = "integration-test-topic"
	applicationProperty      = "applicationProperty"
)

func createIotHubPubsubMetadata() pubsub.Metadata {
	metadata := pubsub.Metadata{
		Base: metadata.Base{
			Properties: map[string]string{
				"connectionString":     os.Getenv(iotHubConnectionStringEnvKey),
				"consumerID":           os.Getenv(iotHubConsumerGroupEnvKey),
				"storageAccountName":   os.Getenv(storageAccountNameEnvKey),
				"storageAccountKey":    os.Getenv(storageAccountKeyEnvKey),
				"storageContainerName": testStorageContainerName,
			},
		},
	}

	return metadata
}

func testReadIotHubEvents(t *testing.T) {
	logger := kitLogger.NewLogger("pubsub.azure.eventhubs.integration.test")
	logger.SetOutputLevel(kitLogger.DebugLevel)
	eh := NewAzureEventHubs(logger).(*AzureEventHubs)
	err := eh.Init(context.Background(), createIotHubPubsubMetadata())
	assert.NoError(t, err)

	// Invoke az CLI via bash script to send test IoT device events
	// Requires the AZURE_CREDENTIALS environment variable to be already set (output of `az ad sp create-for-rbac`)
	cmd := exec.Command("/bin/bash", "../../../tests/scripts/send-iot-device-events.sh")
	cmd.Env = append(os.Environ(), fmt.Sprintf("IOT_HUB_NAME=%s", os.Getenv(iotHubNameEnvKey)))
	out, err := cmd.CombinedOutput()
	assert.NoError(t, err, "Error in send-iot-device-events.sh:\n%s", string(out))

	// Setup subscription to capture messages in a closure so that test asserts can be
	// performed on the main thread, including the case where the handler is never invoked.
	var messages []pubsub.NewMessage
	handler := func(_ context.Context, msg *pubsub.NewMessage) error {
		messages = append(messages, *msg)
		return nil
	}

	req := pubsub.SubscribeRequest{
		Topic: testTopic,
		Metadata: map[string]string{
			"requireAllProperties": "true",
		},
	}
	err = eh.Subscribe(context.Background(), req, handler)
	assert.NoError(t, err)

	// Note: azure-event-hubs-go SDK defaultLeasePersistenceInterval is 5s
	// Sleep long enough so that the azure event hubs SDK has time to persist updated checkpoints
	// before the test process exits.
	time.Sleep(10 * time.Second)

	assert.Greater(t, len(messages), 0, "Failed to receive any IotHub events")
	logger.Infof("Received %d messages", len(messages))
	for _, r := range messages {
		logger.Infof("Message metadata: %v", r.Metadata)
		assert.Equal(t, r.Topic, testTopic, "Message topic doesn't match subscription")
		assert.Contains(t, string(r.Data), "Integration test message")

		// Verify expected IoT Hub device event metadata exists
		// TODO: add device messages than can populate the sysPropPartitionKey and sysPropIotHubConnectionModuleID metadata
		assert.Contains(t, r.Metadata, "x-opt-sequence-number", "IoT device event missing: %s", "x-opt-sequence-number")
		assert.Contains(t, r.Metadata, "x-opt-enqueued-time", "IoT device event missing: %s", "x-opt-enqueued-time")
		assert.Contains(t, r.Metadata, "x-opt-offset", "IoT device event missing: %s", "x-opt-offset")
		assert.Contains(t, r.Metadata, "iothub-connection-device-id", "IoT device event missing: %s", "iothub-connection-device-id")
		assert.Contains(t, r.Metadata, "iothub-connection-auth-generation-id", "IoT device event missing: %s", "iothub-connection-auth-generation-id")
		assert.Contains(t, r.Metadata, "iothub-connection-auth-method", "IoT device event missing: %s", "iothub-connection-auth-method")
		assert.Contains(t, r.Metadata, "iothub-enqueuedtime", "IoT device event missing: %s", "iothub-enqueuedtime")
		assert.Contains(t, r.Metadata, "message-id", "IoT device event missing: %s", "message-id")

		// Verify sent custom application property is received in IoT Hub device event metadata
		assert.Contains(t, r.Metadata, applicationProperty, "IoT device event missing: %s", applicationProperty)
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
