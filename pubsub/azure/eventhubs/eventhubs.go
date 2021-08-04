// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package eventhubs

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	eventhub "github.com/Azure/azure-event-hubs-go/v3"
	"github.com/Azure/azure-event-hubs-go/v3/eph"
	"github.com/Azure/azure-event-hubs-go/v3/storage"
	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/Azure/go-autorest/autorest/azure"
	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/kit/logger"
	"github.com/dapr/kit/retry"
)

const (
	// metadata.
	connectionString = "connectionString"
	consumerID       = "consumerID" // passed by dapr runtime

	// required by subscriber.
	storageAccountName   = "storageAccountName"
	storageAccountKey    = "storageAccountKey"
	storageContainerName = "storageContainerName"

	// errors.
	missingConnectionStringErrorMsg     = "error: connectionString is a required attribute"
	missingStorageAccountNameErrorMsg   = "error: storageAccountName is a required attribute"
	missingStorageAccountKeyErrorMsg    = "error: storageAccountKey is a required attribute"
	missingStorageContainerNameErrorMsg = "error: storageContainerName is a required attribute"
	missingConsumerIDErrorMsg           = "error: missing consumerID attribute"

	// Event Hubs SystemProperties names for metadata passthrough.
	sysPropSequenceNumber             = "x-opt-sequence-number"
	sysPropEnqueuedTime               = "x-opt-enqueued-time"
	sysPropOffset                     = "x-opt-offset"
	sysPropPartitionID                = "x-opt-partition-id"
	sysPropPartitionKey               = "x-opt-partition-key"
	sysPropIotHubDeviceConnectionID   = "iothub-connection-device-id"
	sysPropIotHubAuthGenerationID     = "iothub-connection-auth-generation-id"
	sysPropIotHubConnectionAuthMethod = "iothub-connection-auth-method"
	sysPropIotHubConnectionModuleID   = "iothub-connection-module-id"
	sysPropIotHubEnqueuedTime         = "iothub-enqueuedtime"
)

func subscribeHandler(ctx context.Context, topic string, e *eventhub.Event, handler pubsub.Handler) error {
	res := pubsub.NewMessage{Data: e.Data, Topic: topic, Metadata: map[string]string{}}
	if e.SystemProperties.SequenceNumber != nil {
		res.Metadata[sysPropSequenceNumber] = strconv.FormatInt(*e.SystemProperties.SequenceNumber, 10)
	}
	if e.SystemProperties.EnqueuedTime != nil {
		res.Metadata[sysPropEnqueuedTime] = e.SystemProperties.EnqueuedTime.Format(time.RFC3339)
	}
	if e.SystemProperties.Offset != nil {
		res.Metadata[sysPropOffset] = strconv.FormatInt(*e.SystemProperties.Offset, 10)
	}
	// According to azure-event-hubs-go docs, this will always be nil.
	if e.SystemProperties.PartitionID != nil {
		res.Metadata[sysPropPartitionID] = strconv.Itoa(int(*e.SystemProperties.PartitionID))
	}
	// The following metadata properties are only present if event was generated by Azure IoT Hub
	if e.SystemProperties.PartitionKey != nil {
		res.Metadata[sysPropPartitionKey] = *e.SystemProperties.PartitionKey
	}
	if e.SystemProperties.IoTHubDeviceConnectionID != nil {
		res.Metadata[sysPropIotHubDeviceConnectionID] = *e.SystemProperties.IoTHubDeviceConnectionID
	}
	if e.SystemProperties.IoTHubAuthGenerationID != nil {
		res.Metadata[sysPropIotHubAuthGenerationID] = *e.SystemProperties.IoTHubAuthGenerationID
	}
	if e.SystemProperties.IoTHubConnectionAuthMethod != nil {
		res.Metadata[sysPropIotHubConnectionAuthMethod] = *e.SystemProperties.IoTHubConnectionAuthMethod
	}
	if e.SystemProperties.IoTHubConnectionModuleID != nil {
		res.Metadata[sysPropIotHubConnectionModuleID] = *e.SystemProperties.IoTHubConnectionModuleID
	}
	if e.SystemProperties.IoTHubEnqueuedTime != nil {
		res.Metadata[sysPropIotHubEnqueuedTime] = e.SystemProperties.IoTHubEnqueuedTime.Format(time.RFC3339)
	}

	return handler(ctx, &res)
}

// AzureEventHubs allows sending/receiving Azure Event Hubs events.
type AzureEventHubs struct {
	hub      *eventhub.Hub
	metadata azureEventHubsMetadata

	logger        logger.Logger
	ctx           context.Context
	cancel        context.CancelFunc
	backOffConfig retry.Config
}

type azureEventHubsMetadata struct {
	connectionString     string
	consumerGroup        string
	storageAccountName   string
	storageAccountKey    string
	storageContainerName string
}

// NewAzureEventHubs returns a new Azure Event hubs instance.
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

// Init connects to Azure Event Hubs.
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
	aeh.ctx, aeh.cancel = context.WithCancel(context.Background())

	// Default retry configuration is used if no backOff properties are set.
	if err := retry.DecodeConfigWithPrefix(
		&aeh.backOffConfig,
		metadata.Properties,
		"backOff"); err != nil {
		return err
	}

	return nil
}

// Publish sends data to Azure Event Hubs.
func (aeh *AzureEventHubs) Publish(req *pubsub.PublishRequest) error {
	err := aeh.hub.Send(aeh.ctx, &eventhub.Event{Data: req.Data})
	if err != nil {
		return fmt.Errorf("error from publish: %s", err)
	}

	return nil
}

// Subscribe receives data from Azure Event Hubs.
func (aeh *AzureEventHubs) Subscribe(req pubsub.SubscribeRequest, handler pubsub.Handler) error {
	cred, err := azblob.NewSharedKeyCredential(aeh.metadata.storageAccountName, aeh.metadata.storageAccountKey)
	if err != nil {
		return err
	}

	leaserCheckpointer, err := storage.NewStorageLeaserCheckpointer(cred, aeh.metadata.storageAccountName, aeh.metadata.storageContainerName, azure.PublicCloud)
	if err != nil {
		return err
	}

	processor, err := eph.NewFromConnectionString(aeh.ctx, aeh.metadata.connectionString, leaserCheckpointer, leaserCheckpointer, eph.WithNoBanner(), eph.WithConsumerGroup(aeh.metadata.consumerGroup))
	if err != nil {
		return err
	}

	_, err = processor.RegisterHandler(aeh.ctx,
		func(c context.Context, e *eventhub.Event) error {
			b := aeh.backOffConfig.NewBackOffWithContext(aeh.ctx)

			return retry.NotifyRecover(func() error {
				aeh.logger.Debugf("Processing EventHubs event %s/%s", req.Topic, e.ID)

				return subscribeHandler(aeh.ctx, req.Topic, e, handler)
			}, b, func(err error, d time.Duration) {
				aeh.logger.Errorf("Error processing EventHubs event: %s/%s. Retrying...", req.Topic, e.ID)
			}, func() {
				aeh.logger.Errorf("Successfully processed EventHubs event after it previously failed: %s/%s", req.Topic, e.ID)
			})
		})
	if err != nil {
		return err
	}

	err = processor.StartNonBlocking(aeh.ctx)
	if err != nil {
		return err
	}

	return nil
}

func (aeh *AzureEventHubs) Close() error {
	aeh.cancel()

	return aeh.hub.Close(aeh.ctx)
}

func (aeh *AzureEventHubs) Features() []pubsub.Feature {
	return nil
}
