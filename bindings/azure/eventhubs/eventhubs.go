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
	"reflect"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azeventhubs"

	"github.com/dapr/components-contrib/bindings"
	impl "github.com/dapr/components-contrib/internal/component/azure/eventhubs"
	contribMetadata "github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
	"github.com/dapr/kit/ptr"
)

// AzureEventHubs allows sending/receiving Azure Event Hubs events.
type AzureEventHubs struct {
	*impl.AzureEventHubs
}

// NewAzureEventHubs returns a new Azure Event hubs instance.
func NewAzureEventHubs(logger logger.Logger) bindings.InputOutputBinding {
	return &AzureEventHubs{
		AzureEventHubs: impl.NewAzureEventHubs(logger, true),
	}
}

// Init performs metadata init.
func (a *AzureEventHubs) Init(_ context.Context, metadata bindings.Metadata) error {
	return a.AzureEventHubs.Init(metadata.Properties)
}

func (a *AzureEventHubs) Operations() []bindings.OperationKind {
	return []bindings.OperationKind{
		bindings.CreateOperation,
	}
}

// Write posts an event hubs message.
func (a *AzureEventHubs) Invoke(ctx context.Context, req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	// Get the partition key and content type
	batchOpts := &azeventhubs.EventDataBatchOptions{}
	if pk := req.Metadata["partitionKey"]; pk != "" {
		batchOpts.PartitionKey = &pk
	}
	var contentType *string
	if ct := req.Metadata["contentType"]; ct != "" {
		contentType = ptr.Of(req.Metadata["contentType"])
	}

	// Publish the message
	messages := []*azeventhubs.EventData{
		{
			Body:        req.Data,
			ContentType: contentType,
		},
	}

	// Publish the message
	err := a.AzureEventHubs.Publish(ctx, a.AzureEventHubs.EventHubName(), messages, batchOpts)
	if err != nil {
		return nil, err
	}
	return nil, nil
}

// Read gets messages from Event Hubs in a non-blocking way.
func (a *AzureEventHubs) Read(ctx context.Context, handler bindings.Handler) error {
	// Start the subscription
	// This is non-blocking
	topic := a.AzureEventHubs.EventHubName()
	bindingsHandler := a.AzureEventHubs.GetBindingsHandlerFunc(topic, false, handler)
	// Setting `maxBulkSubCount` to 1 as bindings are not supported for bulk subscriptions
	// Setting `CheckPointFrequencyPerPartition` to default value of 1
	return a.AzureEventHubs.Subscribe(ctx, impl.SubscribeConfig{
		Topic:                           topic,
		MaxBulkSubCount:                 1,
		MaxBulkSubAwaitDurationMs:       impl.DefaultMaxBulkSubAwaitDurationMs,
		CheckPointFrequencyPerPartition: impl.DefaultCheckpointFrequencyPerPartition,
		Handler:                         bindingsHandler,
	})
}

func (a *AzureEventHubs) Close() error {
	return a.AzureEventHubs.Close()
}

// GetComponentMetadata returns the metadata of the component.
func (a *AzureEventHubs) GetComponentMetadata() (metadataInfo contribMetadata.MetadataMap) {
	metadataStruct := impl.AzureEventHubsMetadata{}
	contribMetadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, contribMetadata.BindingType)
	return
}
