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
	"errors"
	"fmt"
	"sync"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azeventhubs"

	azauth "github.com/dapr/components-contrib/internal/authentication/azure"
	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/components-contrib/pubsub/azure/eventhubs/conn"
	"github.com/dapr/kit/logger"
)

// AzureEventHubs allows sending/receiving Azure Event Hubs events.
type AzureEventHubs struct {
	metadata *azureEventHubsMetadata
	logger   logger.Logger

	clientsLock     *sync.RWMutex
	producerClients map[string]*azeventhubs.ProducerClient
}

// NewAzureEventHubs returns a new Azure Event hubs instance.
func NewAzureEventHubs(logger logger.Logger) pubsub.PubSub {
	return &AzureEventHubs{
		logger:          logger,
		clientsLock:     &sync.RWMutex{},
		producerClients: make(map[string]*azeventhubs.ProducerClient, 1),
	}
}

// Init connects to Azure Event Hubs.
func (aeh *AzureEventHubs) Init(metadata pubsub.Metadata) error {
	m, err := parseEventHubsMetadata(metadata, aeh.logger)
	if err != nil {
		return err
	}
	aeh.metadata = m

	if aeh.metadata.ConnectionString != "" {
		// Connect using the connection string
		var parsedConn *conn.ParsedConn
		parsedConn, err = conn.ParsedConnectionFromStr(aeh.metadata.ConnectionString)
		if err != nil {
			return fmt.Errorf("connectionString is invalid: %w", err)
		}

		if parsedConn.HubName != "" {
			aeh.logger.Infof(`The provided connection string is specific to the Event Hub ("entity path") '%s'; publishing or subscribing to a topic that does not match this Event Hub will fail when attempted`, parsedConn.HubName)
		} else {
			aeh.logger.Infof(`The provided connection string does not contain an Event Hub name ("entity path"); the connection will be established on first publish/subscribe and req.Topic field in incoming requests will be honored`)
		}

		aeh.metadata.hubName = parsedConn.HubName
	} else {
		// Connect via Azure AD
		var env azauth.EnvironmentSettings
		env, err = azauth.NewEnvironmentSettings("eventhubs", metadata.Properties)
		if err != nil {
			return fmt.Errorf("failed to initialize Azure AD credentials: %w", err)
		}
		aeh.metadata.aadTokenProvider, err = env.GetTokenCredential()
		if err != nil {
			return fmt.Errorf("failed to get Azure AD token credentials provider: %w", err)
		}

		aeh.logger.Info("connecting to Azure Event Hub using Azure AD; the connection will be established on first publish/subscribe and req.Topic field in incoming requests will be honored")

		/*if aeh.metadata.EnableEntityManagement {
			if err := aeh.validateEnitityManagementMetadata(); err != nil {
				return err
			}

			// Create hubManager for eventHub management with AAD.
			if managerCreateErr := aeh.createHubManager(); managerCreateErr != nil {
				return managerCreateErr
			}

			// Get Azure Management plane settings for creating consumer groups using event hubs management client.
			settings, err := azauth.NewEnvironmentSettings("azure", metadata.Properties)
			if err != nil {
				return err
			}
			aeh.managementSettings = settings
		}*/
	}

	// Connect to the Azure Storage account
	/*if m.StorageAccountKey != "" {
		metadata.Properties["accountKey"] = m.StorageAccountKey
	}
	var storageCredsErr error
	aeh.storageCredential, aeh.azureEnvironment, storageCredsErr = azauth.GetAzureStorageBlobCredentials(aeh.logger, m.StorageAccountName, metadata.Properties)
	if storageCredsErr != nil {
		return fmt.Errorf("invalid storage credentials with error: %w", storageCredsErr)
	}*/

	return nil
}

func (aeh *AzureEventHubs) Features() []pubsub.Feature {
	return nil
}

// Publish sends a message to Azure Event Hubs.
func (aeh *AzureEventHubs) Publish(ctx context.Context, req *pubsub.PublishRequest) error {
	if req.Topic == "" {
		return errors.New("parameter 'topic' is required")
	}

	// Get the producer client
	client, err := aeh.getProducerClientForTopic(ctx, req.Topic)
	if err != nil {
		return fmt.Errorf("error trying to establish a connection: %w", err)
	}

	// Build the batch of messages
	batchOpts := &azeventhubs.EventDataBatchOptions{}
	if pk := req.Metadata["partitionKey"]; pk != "" {
		batchOpts.PartitionKey = &pk
	}
	batch, err := client.NewEventDataBatch(ctx, batchOpts)
	if err != nil {
		return fmt.Errorf("error creating batch: %w", err)
	}
	err = batch.AddEventData(&azeventhubs.EventData{
		Body:        req.Data,
		ContentType: req.ContentType,
	}, nil)
	if err != nil {
		return fmt.Errorf("error adding message to batch: %w", err)
	}

	// Send the message
	client.SendEventDataBatch(ctx, batch, nil)
	if err != nil {
		return fmt.Errorf("error publishing batch: %w", err)
	}

	return nil
}

func (aeh *AzureEventHubs) getProducerClientForTopic(ctx context.Context, topic string) (client *azeventhubs.ProducerClient, err error) {
	// Check if we have the producer client in the cache
	aeh.clientsLock.RLock()
	client = aeh.producerClients[topic]
	aeh.clientsLock.RUnlock()
	if client != nil {
		return client, nil
	}

	// After acquiring a write lock, check again if the producer exists in the cache just in case another goroutine created it in the meanwhile
	aeh.clientsLock.Lock()
	defer aeh.clientsLock.Unlock()

	client = aeh.producerClients[topic]
	if client != nil {
		return client, nil
	}

	// Start by creating a new entity if needed
	if aeh.metadata.EnableEntityManagement {
		// TODO: Create a new entity
	}

	clientOpts := &azeventhubs.ProducerClientOptions{
		ApplicationID: "dapr-" + logger.DaprVersion,
	}

	// Check if we're authenticating using a connection string
	if aeh.metadata.ConnectionString != "" {
		var connString string
		connString, err = aeh.constructConnectionStringFromTopic(topic)
		if err != nil {
			return nil, err
		}
		client, err = azeventhubs.NewProducerClientFromConnectionString(connString, "", clientOpts)
		if err != nil {
			return nil, fmt.Errorf("unable to connect to Azure Event Hub using a connection string: %w", err)
		}
	} else {
		client, err = azeventhubs.NewProducerClient(aeh.metadata.EventHubNamespace, topic, aeh.metadata.aadTokenProvider, nil)
		if err != nil {
			return nil, fmt.Errorf("unable to connect to Azure Event Hub using Azure AD: %w", err)
		}
	}

	// Store in the cache before returning
	aeh.producerClients[topic] = client
	return client, nil
}

// BulkPublish sends data to Azure Event Hubs in bulk.
func (aeh *AzureEventHubs) BulkPublish(ctx context.Context, req *pubsub.BulkPublishRequest) (pubsub.BulkPublishResponse, error) {
	return pubsub.BulkPublishResponse{}, nil
}

// Subscribe receives data from Azure Event Hubs.
func (aeh *AzureEventHubs) Subscribe(subscribeCtx context.Context, req pubsub.SubscribeRequest, handler pubsub.Handler) error {
	return nil
}

func (aeh *AzureEventHubs) Close() (err error) {
	return nil
}

// Returns a connection string with the Event Hub name (entity path) set if not present
func (aeh *AzureEventHubs) constructConnectionStringFromTopic(topic string) (string, error) {
	if aeh.metadata.hubName != "" {
		if aeh.metadata.hubName != topic {
			return "", fmt.Errorf("the requested topic '%s' does not match the Event Hub name in the connection string", topic)
		}
		return aeh.metadata.ConnectionString, nil
	}

	connString := aeh.metadata.ConnectionString + ";EntityPath=" + topic
	return connString, nil
}
