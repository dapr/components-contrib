// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package eventhubs

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	eventhub "github.com/Azure/azure-event-hubs-go"
	"github.com/Azure/azure-event-hubs-go/eph"
	"github.com/Azure/azure-event-hubs-go/storage"
	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/Azure/go-autorest/autorest/azure"
	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/dapr/pkg/logger"
)

const (
	// metadata
	connectionString = "connectionString"

	// required by subscriber
	consumerGroup        = "consumerGroup"
	storageAccountName   = "storageAccountName"
	storageAccountKey    = "storageAccountKey"
	storageContainerName = "storageContainerName"

	// errors
	missingConnectionStringErrorMsg     = "error: connectionString is a required attribute"
	missingStorageAccountNameErrorMsg   = "error: storageAccountName is a required attribute"
	missingStorageAccountKeyErrorMsg    = "error: storageAccountKey is a required attribute"
	missingStorageContainerNameErrorMsg = "error: storageContainerName is a required attribute"
	missingConsumerGroupErrorMsg        = "error: consumerGroup is a required attribute"
)

// AzureEventHubs allows sending/receiving Azure Event Hubs events
type AzureEventHubs struct {
	hub      *eventhub.Hub
	metadata *azureEventHubsMetadata

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

// Init performs metadata init
func (a *AzureEventHubs) Init(metadata bindings.Metadata) error {
	m, err := parseMetadata(metadata)
	if err != nil {
		return err
	}
	a.metadata = m
	hub, err := eventhub.NewHubFromConnectionString(a.metadata.connectionString)

	if err != nil {
		return fmt.Errorf("unable to connect to azure event hubs: %v", err)
	}

	a.hub = hub
	return nil
}

func parseMetadata(meta bindings.Metadata) (*azureEventHubsMetadata, error) {
	m := &azureEventHubsMetadata{}

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

	if val, ok := meta.Properties[consumerGroup]; ok && val != "" {
		m.consumerGroup = val
	} else {
		return m, errors.New(missingConsumerGroupErrorMsg)
	}

	return m, nil
}

// Write posts an event hubs message
func (a *AzureEventHubs) Write(req *bindings.WriteRequest) error {
	err := a.hub.Send(context.Background(), &eventhub.Event{
		Data: req.Data,
	})
	if err != nil {
		return err
	}

	return nil
}

// Read gets messages from eventhubs in a non-blocking fashion
func (a *AzureEventHubs) Read(handler func(*bindings.ReadResponse) error) error {

	cred, err := azblob.NewSharedKeyCredential(a.metadata.storageAccountName, a.metadata.storageAccountKey)
	if err != nil {
		return err
	}

	leaserCheckpointer, err := storage.NewStorageLeaserCheckpointer(cred, a.metadata.storageAccountName, a.metadata.storageContainerName, azure.PublicCloud)
	if err != nil {
		return err
	}

	processor, err := eph.NewFromConnectionString(context.Background(), a.metadata.connectionString, leaserCheckpointer, leaserCheckpointer, eph.WithNoBanner(), eph.WithConsumerGroup(a.metadata.consumerGroup))

	if err != nil {
		return err
	}

	_, err = processor.RegisterHandler(context.Background(),
		func(c context.Context, e *eventhub.Event) error {
			return handler(&bindings.ReadResponse{Data: e.Data})
		})
	if err != nil {
		return err
	}

	err = processor.StartNonBlocking(context.Background())
	if err != nil {
		return err
	}

	// close Event Hubs when application exits

	exitChan := make(chan os.Signal, 1)
	signal.Notify(exitChan, os.Interrupt, syscall.SIGTERM)
	<-exitChan

	err = a.hub.Close(context.Background())

	return nil
}
