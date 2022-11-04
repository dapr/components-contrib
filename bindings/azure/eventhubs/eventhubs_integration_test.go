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

package eventhubs

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
)

const (
	// Note: Reuse the environment variables names from the conformance tests where possible to support reuse of setup workflows

	// iotHubConnectionStringEnvKey defines the key containing the integration test connection string
	// For the default EventHub endpoint for an Azure IoT Hub, it will resemble:
	// Endpoint=sb://<iotHubGeneratedNamespace>.servicebus.windows.net/;SharedAccessKeyName=service;SharedAccessKey=<key>;EntityPath=<iotHubGeneratedPath>
	iotHubConnectionStringEnvKey = "AzureIotHubEventHubConnectionString"
	iotHubConsumerGroupEnvKey    = "AzureIotHubBindingsConsumerGroup"
	iotHubNameEnvKey             = "AzureIotHubName"
	storageAccountNameEnvKey     = "AzureBlobStorageAccount"
	storageAccountKeyEnvKey      = "AzureBlobStorageAccessKey"
	azureCredentialsEnvKey       = "AZURE_CREDENTIALS"
	testStorageContainerName     = "iothub-bindings-integration-test"

	// use for AAD integration test
	azureServicePrincipalClientIdEnvKey     = "AzureCertificationServicePrincipalClientId"
	azureServicePrincipalClientSecretEnvKey = "AzureCertificationServicePrincipalClientSecret"
	azureTenantIdEnvKey                     = "AzureCertificationTenantId"
	azureBlobStorageAccountEnvKey           = "AzureBlobStorageAccount"
	eventHubsBindingsContainerEnvKey        = "AzureEventHubsBindingsContainer"
	eventHubBindingsHubEnvKey               = "AzureEventHubsBindingsHub"
	eventHubBindingsNamespaceEnvKey         = "AzureEventHubsBindingsNamespace"
	eventHubsBindingsConsumerGroupEnvKey    = "AzureEventHubsBindingsConsumerGroup"
)

func createIotHubBindingsMetadata() bindings.Metadata {
	metadata := bindings.Metadata{
		Base: metadata.Base{
			Properties: map[string]string{
				connectionString:     os.Getenv(iotHubConnectionStringEnvKey),
				consumerGroup:        os.Getenv(iotHubConsumerGroupEnvKey),
				storageAccountName:   os.Getenv(storageAccountNameEnvKey),
				storageAccountKey:    os.Getenv(storageAccountKeyEnvKey),
				storageContainerName: testStorageContainerName,
			},
		},
	}

	return metadata
}

func createEventHubsBindingsAADMetadata() bindings.Metadata {
	metadata := bindings.Metadata{
		Base: metadata.Base{
			Properties: map[string]string{
				consumerGroup:        os.Getenv(eventHubsBindingsConsumerGroupEnvKey),
				storageAccountName:   os.Getenv(azureBlobStorageAccountEnvKey),
				storageContainerName: os.Getenv(eventHubsBindingsContainerEnvKey),
				"eventHub":           os.Getenv(eventHubBindingsHubEnvKey),
				"eventHubNamespace":  os.Getenv(eventHubBindingsNamespaceEnvKey),
				"azureTenantId":      os.Getenv(azureTenantIdEnvKey),
				"azureClientId":      os.Getenv(azureServicePrincipalClientIdEnvKey),
				"azureClientSecret":  os.Getenv(azureServicePrincipalClientSecretEnvKey),
			},
		},
	}

	return metadata
}

func testEventHubsBindingsAADAuthentication(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	logger := logger.NewLogger("bindings.azure.eventhubs.integration.test")
	metadata := createEventHubsBindingsAADMetadata()
	eventHubsBindings := NewAzureEventHubs(logger)

	err := eventHubsBindings.Init(metadata)
	assert.NoError(t, err)

	req := &bindings.InvokeRequest{
		Data: []byte("Integration test message"),
	}
	_, err = eventHubsBindings.Invoke(ctx, req)
	assert.NoError(t, err)

	// Setup Read binding to capture readResponses in a closure so that test asserts can be
	// performed on the main thread, including the case where the handler is never invoked.
	var readResponses []bindings.ReadResponse
	var handler bindings.Handler = func(_ context.Context, data *bindings.ReadResponse) ([]byte, error) {
		readResponses = append(readResponses, *data)
		return nil, nil
	}

	_, err = eventHubsBindings.Invoke(ctx, req)
	assert.NoError(t, err)

	eventHubsBindings.Read(ctx, handler)

	time.Sleep(1 * time.Second)
	_, err = eventHubsBindings.Invoke(ctx, req)
	assert.NoError(t, err)

	// Note: azure-event-hubs-go SDK defaultLeasePersistenceInterval is 5s
	// Sleep long enough so that the azure event hubs SDK has time to persist updated checkpoints
	// before the test process exits.
	time.Sleep(10 * time.Second)

	assert.Greater(t, len(readResponses), 0, "Failed to receive any EventHub events")
	logger.Infof("Received %d messages", len(readResponses))
	for _, r := range readResponses {
		logger.Infof("Message metadata: %v", r.Metadata)
		assert.Contains(t, string(r.Data), "Integration test message")
	}
}

func testReadIotHubEvents(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	logger := logger.NewLogger("bindings.azure.eventhubs.integration.test")
	eh := NewAzureEventHubs(logger)
	err := eh.Init(createIotHubBindingsMetadata())
	assert.Nil(t, err)

	// Invoke az CLI via bash script to send test IoT device events
	// Requires the AZURE_CREDENTIALS environment variable to be already set (output of `az ad sp create-for-rbac`)
	cmd := exec.Command("/bin/bash", "../../../tests/scripts/send-iot-device-events.sh")
	cmd.Env = append(os.Environ(), fmt.Sprintf("IOT_HUB_NAME=%s", os.Getenv(iotHubNameEnvKey)))
	out, err := cmd.CombinedOutput()
	assert.Nil(t, err, "Error in send-iot-device-events.sh:\n%s", out)

	// Setup Read binding to capture readResponses in a closure so that test asserts can be
	// performed on the main thread, including the case where the handler is never invoked.
	var readResponses []bindings.ReadResponse
	handler := func(_ context.Context, data *bindings.ReadResponse) ([]byte, error) {
		readResponses = append(readResponses, *data)
		return nil, nil
	}

	eh.Read(ctx, handler)

	// Note: azure-event-hubs-go SDK defaultLeasePersistenceInterval is 5s
	// Sleep long enough so that the azure event hubs SDK has time to persist updated checkpoints
	// before the test process exits.
	time.Sleep(10 * time.Second)

	assert.Greater(t, len(readResponses), 0, "Failed to receive any IotHub events")
	logger.Infof("Received %d messages", len(readResponses))
	for _, r := range readResponses {
		logger.Infof("Message metadata: %v", r.Metadata)
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
		assert.Contains(t, r.Metadata, sysPropMessageID, "IoT device event missing: %s", sysPropMessageID)
	}

	cancel()
	if c, ok := eh.(io.Closer); ok {
		c.Close()
	}
}

func TestIntegrationCases(t *testing.T) {
	connectionString := os.Getenv(iotHubConnectionStringEnvKey)
	if connectionString != "" {
		t.Run("Read IoT Hub events", testReadIotHubEvents)
	}
	serviceprincipal := os.Getenv(azureServicePrincipalClientIdEnvKey)
	if serviceprincipal != "" {
		t.Run("Event Hubs Binding AAD authentication", testEventHubsBindingsAADAuthentication)
	}
}
