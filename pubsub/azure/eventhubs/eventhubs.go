// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package eventhubs

import (
	"context"
	"errors"
	"fmt"

	eventhub "github.com/Azure/azure-event-hubs-go"
	"github.com/Azure/azure-event-hubs-go/eph"
	"github.com/Azure/azure-event-hubs-go/storage"
	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/Azure/go-autorest/autorest/azure"
	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/dapr/pkg/logger"
)

const (
	// metadata

	connectionString = "connectionString"
	consumerID       = "consumerID" // passed by dapr runtime
	// required by subscriber
	storageAccountName   = "storageAccountName"
	storageAccountKey    = "storageAccountKey"
	storageContainerName = "storageContainerName"

	// errors

	missingConnectionStringErrorMsg     = "error: connectionString is a required attribute"
	missingStorageAccountNameErrorMsg   = "error: storageAccountName is a required attribute"
	missingStorageAccountKeyErrorMsg    = "error: storageAccountKey is a required attribute"
	missingStorageContainerNameErrorMsg = "error: storageContainerName is a required attribute"
	missingConsumerIDErrorMsg           = "error: missing consumerID attribute"
)

// AzureEventHubs allows sending/receiving Azure Event Hubs events
type AzureEventHubs struct {
	hub      *eventhub.Hub
	metadata azureEventHubsMetadata

	logger logger.Logger
}

type azureEventHubsMetadata struct {
	connectionString     string
	consumerGroup        string
	storageAccountName   string
	storageAccountKey    string
	storageContainerName string
}

// NewAzureEventHubs returns a new Azure Event hubs instance
func NewAzureEventHubs(logger logger.Logger) *AzureEventHubs {
	return &AzureEventHubs{logger: logger}
}

func parseEventHubsMetadata(meta pubsub.Metadata) (azureEventHubsMetadata, error) {
	m := azureEventHubsMetadata{}

	if val, ok := meta.Properties[connectionString]; ok && val != "" {
		m.connectionString = val
	} else {
		return m, errors.New(missingConnectionStringErrorMsg)
	}

	if val, ok := meta.Properties[storageAccountName]; ok && val != "" {
		m.storageAccountName = val
	} else {
		return m, errors.New(missingStorageAccountNameErrorMsg)
	}

	if val, ok := meta.Properties[storageAccountKey]; ok && val != "" {
		m.storageAccountKey = val
	} else {
		return m, errors.New(missingStorageAccountKeyErrorMsg)
	}

	if val, ok := meta.Properties[storageContainerName]; ok && val != "" {
		m.storageContainerName = val
	} else {
		return m, errors.New(missingStorageContainerNameErrorMsg)
	}

	if val, ok := meta.Properties[consumerID]; ok && val != "" {
		m.consumerGroup = val
	} else {
		return m, errors.New(missingConsumerIDErrorMsg)
	}

	return m, nil
}

// Init connects to Azure Event Hubs
func (aeh *AzureEventHubs) Init(metadata pubsub.Metadata) error {
	m, err := parseEventHubsMetadata(metadata)
	if err != nil {
		return err
	}
	aeh.metadata = m
	hub, err := eventhub.NewHubFromConnectionString(aeh.metadata.connectionString)
	if err != nil {
		return fmt.Errorf("unable to connect to azure event hubs: %v", err)
	}

	aeh.hub = hub

	return nil
}

// Publish sends data to Azure Event Hubs
func (aeh *AzureEventHubs) Publish(req *pubsub.PublishRequest) error {
	err := aeh.hub.Send(context.Background(), &eventhub.Event{Data: req.Data})
	if err != nil {
		return fmt.Errorf("error from publish: %s", err)
	}

	return nil
}

// Subscribe receives data from Azure Event Hubs
func (aeh *AzureEventHubs) Subscribe(req pubsub.SubscribeRequest, handler func(msg *pubsub.NewMessage) error) error {
	cred, err := azblob.NewSharedKeyCredential(aeh.metadata.storageAccountName, aeh.metadata.storageAccountKey)
	if err != nil {
		return err
	}

	leaserCheckpointer, err := storage.NewStorageLeaserCheckpointer(cred, aeh.metadata.storageAccountName, aeh.metadata.storageContainerName, azure.PublicCloud)
	if err != nil {
		return err
	}

	processor, err := eph.NewFromConnectionString(context.Background(), aeh.metadata.connectionString, leaserCheckpointer, leaserCheckpointer, eph.WithNoBanner(), eph.WithConsumerGroup(aeh.metadata.consumerGroup))
	if err != nil {
		return err
	}

	_, err = processor.RegisterHandler(context.Background(),
		func(c context.Context, e *eventhub.Event) error {
			return handler(&pubsub.NewMessage{Data: e.Data, Topic: req.Topic})
		})
	if err != nil {
		return err
	}

	err = processor.StartNonBlocking(context.Background())
	if err != nil {
		return err
	}

	return nil
}

func (aeh *AzureEventHubs) Close() error {
	return aeh.hub.Close(context.TODO())
}
