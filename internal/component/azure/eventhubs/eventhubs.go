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
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azeventhubs"
	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azeventhubs/checkpoints"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"golang.org/x/exp/maps"

	azauth "github.com/dapr/components-contrib/internal/authentication/azure"
	"github.com/dapr/components-contrib/internal/component/azure/blobstorage"
	"github.com/dapr/kit/logger"
	"github.com/dapr/kit/retry"
)

// AzureEventHubs allows sending/receiving Azure Event Hubs events.
// This is an abstract class used by both the pubsub and binding components.
type AzureEventHubs struct {
	metadata  *AzureEventHubsMetadata
	logger    logger.Logger
	isBinding bool

	backOffConfig        retry.Config
	producersLock        *sync.RWMutex
	producers            map[string]*azeventhubs.ProducerClient
	checkpointStoreCache azeventhubs.CheckpointStore
	checkpointStoreLock  *sync.RWMutex

	managementCreds azcore.TokenCredential

	// TODO(@ItalyPaleAle): Remove in Dapr 1.13
	isFailed atomic.Bool
}

// NewAzureEventHubs returns a new Azure Event hubs instance.
func NewAzureEventHubs(logger logger.Logger, isBinding bool) *AzureEventHubs {
	return &AzureEventHubs{
		logger:              logger,
		isBinding:           isBinding,
		producersLock:       &sync.RWMutex{},
		producers:           make(map[string]*azeventhubs.ProducerClient, 1),
		checkpointStoreLock: &sync.RWMutex{},
	}
}

// Init connects to Azure Event Hubs.
func (aeh *AzureEventHubs) Init(metadata map[string]string) error {
	m, err := parseEventHubsMetadata(metadata, aeh.isBinding, aeh.logger)
	if err != nil {
		return err
	}
	aeh.metadata = m

	aeh.metadata.azEnvSettings, err = azauth.NewEnvironmentSettings(metadata)
	if err != nil {
		return fmt.Errorf("failed to initialize Azure environment: %w", err)
	}

	if aeh.metadata.ConnectionString == "" {
		aeh.logger.Info("connecting to Azure Event Hub using Azure AD; the connection will be established on first publish/subscribe and req.Topic field in incoming requests will be honored")

		if aeh.metadata.EnableEntityManagement {
			err = aeh.initEntityManagement()
			if err != nil {
				return fmt.Errorf("failed to initialize entity manager: %w", err)
			}
		}
	}

	// Default retry configuration is used if no backOff properties are set
	// backOff max retry config is set to 3, which means 3 retries by default
	aeh.backOffConfig = retry.DefaultConfig()
	aeh.backOffConfig.MaxRetries = 3
	err = retry.DecodeConfigWithPrefix(&aeh.backOffConfig, metadata, "backOff")
	if err != nil {
		return fmt.Errorf("failed to decode backoff configuration")
	}

	return nil
}

// EventHubName returns the parsed eventHub property from the metadata.
// It's used by the binding only.
func (aeh *AzureEventHubs) EventHubName() string {
	return aeh.metadata.hubName
}

// Publish a batch of messages.
func (aeh *AzureEventHubs) Publish(ctx context.Context, topic string, messages []*azeventhubs.EventData, batchOpts *azeventhubs.EventDataBatchOptions) error {
	// Get the producer client
	client, err := aeh.getProducerClientForTopic(ctx, topic)
	if err != nil {
		return fmt.Errorf("error trying to establish a connection: %w", err)
	}

	// Build the batch of messages
	batch, err := client.NewEventDataBatch(ctx, batchOpts)
	if err != nil {
		return fmt.Errorf("error creating event batch: %w", err)
	}

	// Add all messages
	for _, msg := range messages {
		err = batch.AddEventData(msg, nil)
		if err != nil {
			return fmt.Errorf("error adding messages to batch: %w", err)
		}
	}

	// Send the message
	err = client.SendEventDataBatch(ctx, batch, nil)
	if err != nil {
		return fmt.Errorf("error publishing batch: %w", err)
	}

	return nil
}

// Subscribe receives data from Azure Event Hubs in background.
func (aeh *AzureEventHubs) Subscribe(subscribeCtx context.Context, topic string, getAllProperties bool, handler SubscribeHandler) (err error) {
	if aeh.metadata.ConsumerGroup == "" {
		return errors.New("property consumerID is required to subscribe to an Event Hub topic")
	}

	// Get the processor client
	processor, err := aeh.getProcessorForTopic(subscribeCtx, topic)
	if err != nil {
		return fmt.Errorf("error trying to establish a connection: %w", err)
	}

	// Ensure that no subscriber using the old "track 1" SDK is active
	// TODO(@ItalyPaleAle): Remove this for Dapr 1.13
	{
		// If a previous topic already failed, no need to try with other topics, as we're about to panic anyways
		if aeh.isFailed.Load() {
			return errors.New("subscribing to another topic on this component failed and Dapr is scheduled to crash; will not try subscribing to a new topic")
		}

		ctx, cancel := context.WithTimeout(subscribeCtx, 2*time.Minute)
		err = aeh.ensureNoTrack1Subscribers(ctx, topic)
		cancel()
		if err != nil {
			// If there's a timeout, it means that the other client was still active after the timeout
			// In this case, we return an error here so Dapr can continue the initialization and report a "healthy" status (but this subscription won't be active)
			// After 2 minutes, then, we panic, which ensures that during a rollout Kubernetes will see that this pod is unhealthy and re-creates that. Hopefully, by then other instances of the app will have been updated and no more locks will be present
			if errors.Is(err, context.DeadlineExceeded) {
				aeh.isFailed.Store(true)
				errMsg := fmt.Sprintf("Another instance is currently subscribed to the topic %s in this Event Hub using an old version of Dapr, and this is not supported. Please ensure that all applications subscribed to the same topic, with this consumer group, are using Dapr 1.10 or newer.", topic)
				aeh.logger.Error(errMsg + " ⚠️⚠️⚠️ Dapr will crash in 2 minutes to force the orchestrator to restart the process after the rollout of other instances is complete.")
				go func() {
					time.Sleep(2 * time.Minute)
					aeh.logger.Fatalf("Another instance is currently subscribed to the topic %s in this Event Hub using an old version of Dapr, and this is not supported. Please ensure that all applications subscribed to the same topic, with this consumer group, are using Dapr 1.10 or newer.", topic)
				}()
				return fmt.Errorf("another instance is currently subscribed to the topic %s in this Event Hub using an old version of Dapr", topic)
			}

			// In case of other errors, just return the error
			return fmt.Errorf("failed to check for subscribers using an old version of Dapr: %w", err)
		}
	}

	// This component has built-in retries because Event Hubs doesn't support N/ACK for messages
	retryHandler := func(ctx context.Context, data []byte, metadata map[string]string) error {
		b := aeh.backOffConfig.NewBackOffWithContext(subscribeCtx)

		mID := metadata[sysPropMessageID]
		if mID == "" {
			mID = "(nil)"
		}
		// This method is synchronous so no risk of race conditions if using side effects
		var attempts int
		retryerr := retry.NotifyRecover(func() error {
			attempts++
			aeh.logger.Debugf("Processing EventHubs event %s/%s (attempt: %d)", topic, mID, attempts)

			if attempts > 1 {
				metadata["dapr-attempt"] = strconv.Itoa(attempts)
			}

			return handler(ctx, data, metadata)
		}, b, func(_ error, _ time.Duration) {
			aeh.logger.Warnf("Error processing EventHubs event: %s/%s. Retrying...", topic, mID)
		}, func() {
			aeh.logger.Warnf("Successfully processed EventHubs event after it previously failed: %s/%s", topic, mID)
		})
		if retryerr != nil {
			aeh.logger.Errorf("Too many failed attempts at processing Eventhubs event: %s/%s. Error: %v", topic, mID, err)
		}
		return retryerr
	}

	// Get the subscribe handler
	eventHandler := subscribeHandler(subscribeCtx, getAllProperties, retryHandler)

	// Process all partition clients as they come in
	go func() {
		for {
			// This will block until a new partition client is available
			// It returns nil if processor.Run terminates or if the context is canceled
			partitionClient := processor.NextPartitionClient(subscribeCtx)
			if partitionClient == nil {
				return
			}
			aeh.logger.Debugf("Received client for partition %s", partitionClient.PartitionID())

			// Once we get a partition client, process the events in a separate goroutine
			go func() {
				processErr := aeh.processEvents(subscribeCtx, topic, partitionClient, eventHandler)
				// Do not log context.Canceled which happens at shutdown
				if processErr != nil && !errors.Is(processErr, context.Canceled) {
					aeh.logger.Errorf("Error processing events from partition client: %v", processErr)
				}
			}()
		}
	}()

	// Start the processor
	go func() {
		// This is a blocking call that runs until the context is canceled
		err = processor.Run(subscribeCtx)
		// Do not log context.Canceled which happens at shutdown
		if err != nil && !errors.Is(err, context.Canceled) {
			aeh.logger.Errorf("Error from event processor: %v", err)
		}
	}()

	return nil
}

func (aeh *AzureEventHubs) processEvents(subscribeCtx context.Context, topic string, partitionClient *azeventhubs.ProcessorPartitionClient, eventHandler func(e *azeventhubs.ReceivedEventData) error) error {
	// At the end of the method we need to do some cleanup and close the partition client
	defer func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), resourceGetTimeout)
		defer closeCancel()
		closeErr := partitionClient.Close(closeCtx)
		if closeErr != nil {
			aeh.logger.Errorf("Error while closing partition client: %v", closeErr)
		}
	}()

	// Loop to receive messages
	var (
		ctx    context.Context
		cancel context.CancelFunc
		events []*azeventhubs.ReceivedEventData
		err    error
	)
	for {
		// TODO: Support setting a batch size
		const batchSize = 1
		ctx, cancel = context.WithTimeout(subscribeCtx, time.Minute)
		events, err = partitionClient.ReceiveEvents(ctx, batchSize, nil)
		cancel()

		// A DeadlineExceeded error means that the context timed out before we received the full batch of messages, and that's fine
		if err != nil && !errors.Is(err, context.DeadlineExceeded) {
			// If we get an error like ErrorCodeOwnershipLost, it means that the partition was rebalanced and we lost it
			// We'll just stop this subscription and return
			eventHubError := (*azeventhubs.Error)(nil)
			if errors.As(err, &eventHubError) && eventHubError.Code == azeventhubs.ErrorCodeOwnershipLost {
				aeh.logger.Debugf("Client lost ownership of partition %s for topic %s", partitionClient.PartitionID(), topic)
				return nil
			}

			return fmt.Errorf("error receiving events: %w", err)
		}

		aeh.logger.Debugf("Received batch with %d events on topic %s, partition %s", len(events), topic, partitionClient.PartitionID())

		if len(events) != 0 {
			for _, event := range events {
				// Process the event in its own goroutine
				go eventHandler(event)
			}

			// Update the checkpoint with the last event received. If we lose ownership of this partition or have to restart the next owner will start from this point.
			// This context inherits from the background one in case subscriptionCtx gets canceled
			ctx, cancel = context.WithTimeout(context.Background(), resourceCreationTimeout)
			err = partitionClient.UpdateCheckpoint(ctx, events[len(events)-1], nil)
			cancel()
			if err != nil {
				return fmt.Errorf("failed to update checkpoint: %w", err)
			}
		}
	}
}

func (aeh *AzureEventHubs) Close() (err error) {
	// Acquire locks
	aeh.checkpointStoreLock.Lock()
	defer aeh.checkpointStoreLock.Unlock()
	aeh.producersLock.Lock()
	defer aeh.producersLock.Unlock()

	// Close all producers
	wg := sync.WaitGroup{}
	for _, producer := range aeh.producers {
		if producer == nil {
			continue
		}
		wg.Add(1)
		go func(producer *azeventhubs.ProducerClient) {
			closeCtx, closeCancel := context.WithTimeout(context.Background(), resourceGetTimeout)
			defer closeCancel()
			producer.Close(closeCtx)
			wg.Done()
		}(producer)
	}
	wg.Wait()
	maps.Clear(aeh.producers)

	// Remove the cached checkpoint store and metadata
	aeh.checkpointStoreCache = nil
	aeh.metadata = nil

	return nil
}

// Returns a producer client for a given topic.
// If the client doesn't exist in the cache, it will create one.
func (aeh *AzureEventHubs) getProducerClientForTopic(ctx context.Context, topic string) (client *azeventhubs.ProducerClient, err error) {
	// Check if we have the producer client in the cache
	aeh.producersLock.RLock()
	client = aeh.producers[topic]
	aeh.producersLock.RUnlock()
	if client != nil {
		return client, nil
	}

	// After acquiring a write lock, check again if the producer exists in the cache just in case another goroutine created it in the meanwhile
	aeh.producersLock.Lock()
	defer aeh.producersLock.Unlock()

	client = aeh.producers[topic]
	if client != nil {
		return client, nil
	}

	// Create a new entity if needed
	if aeh.metadata.EnableEntityManagement {
		err = aeh.ensureEventHubEntity(ctx, topic)
		if err != nil {
			return nil, fmt.Errorf("failed to create Event Hub entity %s: %w", topic, err)
		}
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
		// Use Azure AD
		cred, tokenErr := aeh.metadata.azEnvSettings.GetTokenCredential()
		if tokenErr != nil {
			return nil, fmt.Errorf("failed to get credentials from Azure AD: %w", tokenErr)
		}
		client, err = azeventhubs.NewProducerClient(aeh.metadata.EventHubNamespace, topic, cred, clientOpts)
		if err != nil {
			return nil, fmt.Errorf("unable to connect to Azure Event Hub using Azure AD: %w", err)
		}
	}

	// Store in the cache before returning it
	aeh.producers[topic] = client
	return client, nil
}

// Creates a processor for a given topic.
func (aeh *AzureEventHubs) getProcessorForTopic(ctx context.Context, topic string) (*azeventhubs.Processor, error) {
	// Get the checkpoint store
	checkpointStore, err := aeh.getCheckpointStore(ctx)
	if err != nil {
		return nil, fmt.Errorf("unable to connect to the checkpoint store: %w", err)
	}

	// Create a new entity if needed
	if aeh.metadata.EnableEntityManagement {
		// First ensure that the Event Hub entity exists
		// We need to acquire a lock on producers, as creating a producer can perform the same operations
		aeh.producersLock.Lock()
		err = aeh.ensureEventHubEntity(ctx, topic)
		aeh.producersLock.Unlock()
		if err != nil {
			return nil, fmt.Errorf("failed to create Event Hub entity %s: %w", topic, err)
		}

		// Abuse on the lock on checkpoints which are used by all tasks creating processors
		aeh.checkpointStoreLock.Lock()
		err = aeh.ensureSubscription(ctx, topic)
		aeh.checkpointStoreLock.Unlock()
		if err != nil {
			return nil, fmt.Errorf("failed to create Event Hub subscription to entity %s: %w", topic, err)
		}
	}

	// Create a consumer client
	var consumerClient *azeventhubs.ConsumerClient
	clientOpts := &azeventhubs.ConsumerClientOptions{
		ApplicationID: "dapr-" + logger.DaprVersion,
	}

	// Check if we're authenticating using a connection string
	if aeh.metadata.ConnectionString != "" {
		var connString string
		connString, err = aeh.constructConnectionStringFromTopic(topic)
		if err != nil {
			return nil, err
		}
		consumerClient, err = azeventhubs.NewConsumerClientFromConnectionString(connString, "", aeh.metadata.ConsumerGroup, clientOpts)
		if err != nil {
			return nil, fmt.Errorf("unable to connect to Azure Event Hub using a connection string: %w", err)
		}
	} else {
		// Use Azure AD
		cred, tokenErr := aeh.metadata.azEnvSettings.GetTokenCredential()
		if tokenErr != nil {
			return nil, fmt.Errorf("failed to get credentials from Azure AD: %w", tokenErr)
		}
		consumerClient, err = azeventhubs.NewConsumerClient(aeh.metadata.EventHubNamespace, topic, aeh.metadata.ConsumerGroup, cred, clientOpts)
		if err != nil {
			return nil, fmt.Errorf("unable to connect to Azure Event Hub using Azure AD: %w", err)
		}
	}

	// Create the processor from the consumer client and checkpoint store
	processor, err := azeventhubs.NewProcessor(consumerClient, checkpointStore, nil)
	if err != nil {
		return nil, fmt.Errorf("unable to create the processor: %w", err)
	}

	return processor, nil
}

// Returns the checkpoint store from the object. If it doesn't exist, it lazily initializes it.
func (aeh *AzureEventHubs) getCheckpointStore(ctx context.Context) (azeventhubs.CheckpointStore, error) {
	// Check if we have the checkpoint store
	aeh.checkpointStoreLock.RLock()
	if aeh.checkpointStoreCache != nil {
		aeh.checkpointStoreLock.RUnlock()
		return aeh.checkpointStoreCache, nil
	}
	aeh.checkpointStoreLock.RUnlock()

	// After acquiring a write lock, check again if the checkpoint store exists in case another goroutine created it in the meanwhile
	aeh.checkpointStoreLock.Lock()
	defer aeh.checkpointStoreLock.Unlock()

	if aeh.checkpointStoreCache != nil {
		return aeh.checkpointStoreCache, nil
	}

	// Init the checkpoint store and store it in the object
	var err error
	aeh.checkpointStoreCache, err = aeh.createCheckpointStore(ctx)
	if err != nil {
		return nil, fmt.Errorf("unable to connect to the checkpoint store: %w", err)
	}

	return aeh.checkpointStoreCache, nil
}

// Initializes a new checkpoint store
func (aeh *AzureEventHubs) createCheckpointStore(ctx context.Context) (checkpointStore azeventhubs.CheckpointStore, err error) {
	if aeh.metadata.StorageAccountName == "" {
		return nil, errors.New("property storageAccountName is required to subscribe to an Event Hub topic")
	}
	if aeh.metadata.StorageContainerName == "" {
		return nil, errors.New("property storageContainerName is required to subscribe to an Event Hub topic")
	}

	// Get the Azure Blob Storage client and ensure the container exists
	client, err := aeh.createStorageClient(ctx, true)
	if err != nil {
		return nil, err
	}

	// Create the checkpoint store
	checkpointStore, err = checkpoints.NewBlobStore(client, &checkpoints.BlobStoreOptions{
		ClientOptions: policy.ClientOptions{
			Telemetry: policy.TelemetryOptions{
				ApplicationID: "dapr-" + logger.DaprVersion,
			},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("error creating checkpointer: %w", err)
	}
	return checkpointStore, nil
}

// Creates a client to access Azure Blob Storage.
// TODO(@ItalyPaleAle): Remove ensureContainer option (and default to true) for Dapr 1.13
func (aeh *AzureEventHubs) createStorageClient(ctx context.Context, ensureContainer bool) (*container.Client, error) {
	m := blobstorage.ContainerClientOpts{
		ConnectionString: aeh.metadata.StorageConnectionString,
		ContainerName:    aeh.metadata.StorageContainerName,
		AccountName:      aeh.metadata.StorageAccountName,
		AccountKey:       aeh.metadata.StorageAccountKey,
		RetryCount:       3,
	}
	client, err := m.InitContainerClient(aeh.metadata.azEnvSettings)
	if err != nil {
		return nil, err
	}

	if ensureContainer {
		// Ensure the container exists
		// We're setting "accessLevel" to nil to make sure it's private
		err = m.EnsureContainer(ctx, client, nil)
		if err != nil {
			return nil, err
		}
	}

	return client, nil
}

// Returns a connection string with the Event Hub name (entity path) set if not present.
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
