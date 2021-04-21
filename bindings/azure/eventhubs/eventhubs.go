// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
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
	"github.com/dapr/kit/logger"
)

const (
	// metadata
	connectionString = "connectionString"

	// required by subscriber
	consumerGroup        = "consumerGroup"
	storageAccountName   = "storageAccountName"
	storageAccountKey    = "storageAccountKey"
	storageContainerName = "storageContainerName"

	// optional
	partitionKeyName = "partitionKey"
	partitionIDName  = "partitionID"

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
	partitionID          string
	partitionKey         string
}

func (m azureEventHubsMetadata) partitioned() bool {
	return m.partitionID != ""
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

	// Create paritioned sender if the partitionID is configured
	if a.metadata.partitioned() {
		hub, err = eventhub.NewHubFromConnectionString(a.metadata.connectionString,
			eventhub.HubWithPartitionedSender(a.metadata.partitionID))
	}

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

	if val, ok := meta.Properties[partitionKeyName]; ok {
		m.partitionKey = val
	}

	if val, ok := meta.Properties[partitionIDName]; ok {
		m.partitionID = val
	}

	return m, nil
}

func (a *AzureEventHubs) Operations() []bindings.OperationKind {
	return []bindings.OperationKind{bindings.CreateOperation}
}

// Write posts an event hubs message
func (a *AzureEventHubs) Invoke(req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	event := &eventhub.Event{
		Data: req.Data,
	}

	// Send partitionKey in event
	if a.metadata.partitionKey != "" {
		event.PartitionKey = &a.metadata.partitionKey
	} else {
		partitionKey, ok := req.Metadata[partitionKeyName]
		if partitionKey != "" && ok {
			event.PartitionKey = &partitionKey
		}
	}

	err := a.hub.Send(context.Background(), event)
	if err != nil {
		return nil, err
	}

	return nil, nil
}

// Read gets messages from eventhubs in a non-blocking fashion
func (a *AzureEventHubs) Read(handler func(*bindings.ReadResponse) ([]byte, error)) error {
	if !a.metadata.partitioned() {
		if err := a.RegisterEventProcessor(handler); err != nil {
			return err
		}
	} else {
		if err := a.RegisterPartitionedEventProcessor(handler); err != nil {
			return err
		}
	}

	// close Event Hubs when application exits
	exitChan := make(chan os.Signal, 1)
	signal.Notify(exitChan, os.Interrupt, syscall.SIGTERM)
	<-exitChan

	a.hub.Close(context.Background())

	return nil
}

// RegisterPartitionedEventProcessor - receive eventhub messages by partitionID
func (a *AzureEventHubs) RegisterPartitionedEventProcessor(handler func(*bindings.ReadResponse) ([]byte, error)) error {
	ctx := context.Background()

	runtimeInfo, err := a.hub.GetRuntimeInformation(ctx)
	if err != nil {
		return err
	}

	callback := func(c context.Context, event *eventhub.Event) error {
		if event != nil {
			handler(&bindings.ReadResponse{
				Data: event.Data,
			})
		}

		return nil
	}

	ops := []eventhub.ReceiveOption{
		eventhub.ReceiveWithLatestOffset(),
	}

	if a.metadata.consumerGroup != "" {
		a.logger.Infof("eventhubs: using consumer group %s", a.metadata.consumerGroup)
		ops = append(ops, eventhub.ReceiveWithConsumerGroup(a.metadata.consumerGroup))
	}

	if contains(runtimeInfo.PartitionIDs, a.metadata.partitionID) {
		a.logger.Infof("eventhubs: using partition id %s", a.metadata.partitionID)

		_, err := a.hub.Receive(ctx, a.metadata.partitionID, callback, ops...)
		if err != nil {
			return err
		}
	}

	return nil
}

func contains(arr []string, str string) bool {
	for _, a := range arr {
		if a == str {
			return true
		}
	}

	return false
}

// RegisterEventProcessor - receive eventhub messages by eventprocessor
// host by balancing partitions
func (a *AzureEventHubs) RegisterEventProcessor(handler func(*bindings.ReadResponse) ([]byte, error)) error {
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
			_, err = handler(&bindings.ReadResponse{Data: e.Data})

			return err
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
