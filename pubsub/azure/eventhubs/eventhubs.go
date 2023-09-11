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
	"reflect"
	"strconv"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azeventhubs"

	impl "github.com/dapr/components-contrib/internal/component/azure/eventhubs"
	"github.com/dapr/components-contrib/internal/utils"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/kit/logger"
	"github.com/dapr/kit/ptr"
)

// AzureEventHubs allows sending/receiving Azure Event Hubs events.
type AzureEventHubs struct {
	*impl.AzureEventHubs
}

// NewAzureEventHubs returns a new Azure Event hubs instance.
func NewAzureEventHubs(logger logger.Logger) pubsub.PubSub {
	return &AzureEventHubs{
		AzureEventHubs: impl.NewAzureEventHubs(logger, false),
	}
}

// Init the object.
func (aeh *AzureEventHubs) Init(_ context.Context, metadata pubsub.Metadata) error {
	return aeh.AzureEventHubs.Init(metadata.Properties)
}

func (aeh *AzureEventHubs) Features() []pubsub.Feature {
	return []pubsub.Feature{pubsub.FeatureBulkPublish}
}

// Publish sends a message to Azure Event Hubs.
func (aeh *AzureEventHubs) Publish(ctx context.Context, req *pubsub.PublishRequest) error {
	if req.Topic == "" {
		return errors.New("parameter 'topic' is required")
	}

	// Get the partition key and create the batch of messages
	batchOpts := &azeventhubs.EventDataBatchOptions{}
	if pk := req.Metadata["partitionKey"]; pk != "" {
		batchOpts.PartitionKey = &pk
	}
	messages := []*azeventhubs.EventData{
		{
			Body:        req.Data,
			ContentType: req.ContentType,
		},
	}

	// Publish the message
	return aeh.AzureEventHubs.Publish(ctx, req.Topic, messages, batchOpts)
}

// BulkPublish sends data to Azure Event Hubs in bulk.
func (aeh *AzureEventHubs) BulkPublish(ctx context.Context, req *pubsub.BulkPublishRequest) (pubsub.BulkPublishResponse, error) {
	var err error

	if req.Topic == "" {
		err = errors.New("parameter 'topic' is required")
		return pubsub.NewBulkPublishResponse(req.Entries, err), err
	}

	// Batch options
	batchOpts := &azeventhubs.EventDataBatchOptions{}
	if val := req.Metadata[metadata.MaxBulkPubBytesKey]; val != "" {
		var maxBytes uint64
		maxBytes, err = strconv.ParseUint(val, 10, 63)
		if err == nil && maxBytes > 0 {
			batchOpts.MaxBytes = maxBytes
		}
	}

	// Build the batch of messages
	messages := make([]*azeventhubs.EventData, len(req.Entries))
	for i, entry := range req.Entries {
		messages[i] = &azeventhubs.EventData{
			Body: entry.Event,
		}
		if entry.ContentType != "" {
			messages[i].ContentType = ptr.Of(entry.ContentType)
		}
		if val := entry.Metadata["partitionKey"]; val != "" {
			if batchOpts.PartitionKey != nil && *batchOpts.PartitionKey != val {
				err = errors.New("cannot send messages to different partitions")
				return pubsub.NewBulkPublishResponse(req.Entries, err), err
			}
			batchOpts.PartitionKey = &val
		}
	}

	// Publish the message
	err = aeh.AzureEventHubs.Publish(ctx, req.Topic, messages, batchOpts)
	if err != nil {
		// Partial success is not supported by Azure Event Hubs.
		// If an error occurs, all events are considered failed.
		return pubsub.NewBulkPublishResponse(req.Entries, err), err
	}

	return pubsub.BulkPublishResponse{}, nil
}

// Subscribe receives messages from Azure Event Hubs.
func (aeh *AzureEventHubs) Subscribe(ctx context.Context, req pubsub.SubscribeRequest, handler pubsub.Handler) error {
	topic := req.Topic
	if topic == "" {
		return errors.New("parameter 'topic' is required")
	}

	// Check if requireAllProperties is set and is truthy
	getAllProperties := utils.IsTruthy(req.Metadata["requireAllProperties"])
	checkPointFrequencyPerPartition := utils.GetIntValFromString(req.Metadata["checkPointFrequencyPerPartition"], impl.DefaultCheckpointFrequencyPerPartition)

	pubsubHandler := aeh.GetPubSubHandlerFunc(topic, getAllProperties, handler)

	subscribeConfig := impl.SubscribeConfig{
		Topic:                           topic,
		MaxBulkSubCount:                 1,
		MaxBulkSubAwaitDurationMs:       impl.DefaultMaxBulkSubAwaitDurationMs,
		CheckPointFrequencyPerPartition: checkPointFrequencyPerPartition,
		Handler:                         pubsubHandler,
	}
	// Start the subscription
	// This is non-blocking
	return aeh.AzureEventHubs.Subscribe(ctx, subscribeConfig)
}

// BulkSubscribe receives bulk messages from Azure Event Hubs.
func (aeh *AzureEventHubs) BulkSubscribe(ctx context.Context, req pubsub.SubscribeRequest, handler pubsub.BulkHandler) error {
	topic := req.Topic
	if topic == "" {
		return errors.New("parameter 'topic' is required")
	}

	// Check if requireAllProperties is set and is truthy
	getAllProperties := utils.IsTruthy(req.Metadata["requireAllProperties"])
	checkPointFrequencyPerPartition := utils.GetIntValFromString(req.Metadata["checkPointFrequencyPerPartition"], impl.DefaultCheckpointFrequencyPerPartition)
	maxBulkSubCount := utils.GetIntValOrDefault(req.BulkSubscribeConfig.MaxMessagesCount, impl.DefaultMaxBulkSubCount)
	maxBulkSubAwaitDurationMs := utils.GetIntValOrDefault(req.BulkSubscribeConfig.MaxAwaitDurationMs, impl.DefaultMaxBulkSubAwaitDurationMs)

	bulkPubsubHandler := aeh.GetBulkPubSubHandlerFunc(topic, getAllProperties, handler)

	subscribeConfig := impl.SubscribeConfig{
		Topic:                           topic,
		MaxBulkSubCount:                 maxBulkSubCount,
		MaxBulkSubAwaitDurationMs:       maxBulkSubAwaitDurationMs,
		CheckPointFrequencyPerPartition: checkPointFrequencyPerPartition,
		Handler:                         bulkPubsubHandler,
	}

	// Start the subscription
	// This is non-blocking
	return aeh.AzureEventHubs.Subscribe(ctx, subscribeConfig)
}

func (aeh *AzureEventHubs) Close() (err error) {
	return aeh.AzureEventHubs.Close()
}

// GetComponentMetadata returns the metadata of the component.
func (aeh *AzureEventHubs) GetComponentMetadata() (metadataInfo metadata.MetadataMap) {
	metadataStruct := impl.AzureEventHubsMetadata{}
	metadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, metadata.PubSubType)
	return
}
