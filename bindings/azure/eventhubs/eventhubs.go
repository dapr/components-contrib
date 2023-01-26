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

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azeventhubs"

	"github.com/dapr/components-contrib/bindings"
	impl "github.com/dapr/components-contrib/internal/component/azure/eventhubs"
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
func (a *AzureEventHubs) Init(ctx context.Context, metadata bindings.Metadata) error {
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
	return a.AzureEventHubs.Subscribe(ctx, a.AzureEventHubs.EventHubName(), false, func(ctx context.Context, data []byte, metadata map[string]string) error {
		res := bindings.ReadResponse{
			Data:     data,
			Metadata: metadata,
		}
		_, hErr := handler(ctx, &res)
		return hErr
	})
}

func (a *AzureEventHubs) Close() error {
	return a.AzureEventHubs.Close()
}
