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
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/Azure/azure-amqp-common-go/v3/aad"
	"github.com/Azure/azure-amqp-common-go/v3/conn"
	eventhub "github.com/Azure/azure-event-hubs-go/v3"
	"github.com/Azure/azure-event-hubs-go/v3/eph"
	"github.com/Azure/azure-event-hubs-go/v3/storage"
	mgmt "github.com/Azure/azure-sdk-for-go/services/eventhub/mgmt/2017-04-01/eventhub"
	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/Azure/go-autorest/autorest/azure"

	azauth "github.com/dapr/components-contrib/internal/authentication/azure"
	"github.com/dapr/components-contrib/internal/utils"
	contribMetadata "github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/kit/logger"
	"github.com/dapr/kit/retry"
)

const (

	// connection string entity path key.
	entityPathKey = "EntityPath"
	// metadata partitionKey key.
	partitionKeyMetadataKey = "partitionKey"

	// errors.
	hubManagerCreationErrorMsg               = "error: creating eventHub manager client"
	invalidConnectionStringErrorMsg          = "error: connectionString is invalid"
	missingConnectionStringNamespaceErrorMsg = "error: connectionString or eventHubNamespace is required"
	missingStorageAccountNameErrorMsg        = "error: storageAccountName is a required attribute for subscribe"
	missingStorageAccountKeyErrorMsg         = "error: storageAccountKey is required for subscribe when connectionString is provided"
	missingStorageContainerNameErrorMsg      = "error: storageContainerName is a required attribute for subscribe"
	missingConsumerIDErrorMsg                = "error: missing consumerID attribute for subscribe"
	bothConnectionStringNamespaceErrorMsg    = "error: both connectionString and eventHubNamespace are given, only one should be given"
	missingResourceGroupNameMsg              = "error: missing resourceGroupName attribute required for entityManagement"
	missingSubscriptionIDMsg                 = "error: missing subscriptionID attribute required for entityManagement"
	entityManagementConnectionStrMsg         = "error: entity management support is not available with connectionString"
	differentTopicConnectionStringErrorTmpl  = "error: specified topic %s does not match the event hub name in the provided connectionString"

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
	sysPropMessageID                  = "message-id"

	defaultMessageRetentionInDays = 1
	defaultPartitionCount         = 1

	resourceCheckMaxRetry                       = 5
	resourceCheckMaxRetryInterval time.Duration = 5 * time.Minute
	resourceCreationTimeout       time.Duration = 15 * time.Second
	resourceGetTimeout            time.Duration = 5 * time.Second

	// See https://docs.microsoft.com/en-us/azure/event-hubs/event-hubs-quotas for numbers.
	maxMessageRetention = int32(90)
	maxPartitionCount   = int32(1024)
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
	// The following metadata properties are only present if event was generated by Azure IoT Hub.
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
	// azure-event-hubs-go SDK pulls out the AMQP message-id property to the Event.ID property, map it from there.
	if e.ID != "" {
		res.Metadata[sysPropMessageID] = e.ID
	}

	return handler(ctx, &res)
}

// AzureEventHubs allows sending/receiving Azure Event Hubs events.
type AzureEventHubs struct {
	metadata           *azureEventHubsMetadata
	logger             logger.Logger
	publishCtx         context.Context
	publishCancel      context.CancelFunc
	backOffConfig      retry.Config
	hubClients         map[string]*eventhub.Hub
	eventProcessors    map[string]*eph.EventProcessorHost
	hubManager         *eventhub.HubManager
	eventHubSettings   azauth.EnvironmentSettings
	managementSettings azauth.EnvironmentSettings
	cgClient           *mgmt.ConsumerGroupsClient
	tokenProvider      *aad.TokenProvider
	storageCredential  azblob.Credential
	azureEnvironment   *azure.Environment
}

type azureEventHubsMetadata struct {
	ConnectionString       string `json:"connectionString,omitempty"`
	EventHubNamespace      string `json:"eventHubNamespace,omitempty"`
	ConsumerGroup          string `json:"consumerID"`
	StorageAccountName     string `json:"storageAccountName,omitempty"`
	StorageAccountKey      string `json:"storageAccountKey,omitempty"`
	StorageContainerName   string `json:"storageContainerName,omitempty"`
	EnableEntityManagement bool   `json:"enableEntityManagement,omitempty,string"`
	MessageRetentionInDays int32  `json:"messageRetentionInDays,omitempty,string"`
	PartitionCount         int32  `json:"partitionCount,omitempty,string"`
	SubscriptionID         string `json:"subscriptionID,omitempty"`
	ResourceGroupName      string `json:"resourceGroupName,omitempty"`
}

// NewAzureEventHubs returns a new Azure Event hubs instance.
func NewAzureEventHubs(logger logger.Logger) pubsub.PubSub {
	return &AzureEventHubs{logger: logger}
}

func parseEventHubsMetadata(meta pubsub.Metadata) (*azureEventHubsMetadata, error) {
	b, err := json.Marshal(meta.Properties)
	if err != nil {
		return nil, err
	}

	m := azureEventHubsMetadata{}
	err = json.Unmarshal(b, &m)
	if err != nil {
		return nil, err
	}

	if m.ConnectionString == "" && m.EventHubNamespace == "" {
		return &m, errors.New(missingConnectionStringNamespaceErrorMsg)
	}

	if m.ConnectionString != "" && m.EventHubNamespace != "" {
		return &m, errors.New(bothConnectionStringNamespaceErrorMsg)
	}

	return &m, nil
}

func validateAndGetHubName(connectionString string) (string, error) {
	parsed, err := conn.ParsedConnectionFromStr(connectionString)
	if err != nil {
		return "", err
	}
	return parsed.HubName, nil
}

func (aeh *AzureEventHubs) ensureEventHub(ctx context.Context, hubName string) error {
	if aeh.hubManager == nil {
		aeh.logger.Errorf("hubManager client not initialized properly.")
		return fmt.Errorf("hubManager client not initialized properly")
	}
	entity, err := aeh.getHubEntity(ctx, hubName)
	if err != nil {
		return err
	}
	if entity == nil {
		if err := aeh.createHubEntity(ctx, hubName); err != nil {
			return err
		}
	}
	return nil
}

func (aeh *AzureEventHubs) ensureSubscription(ctx context.Context, hubName string) error {
	err := aeh.ensureEventHub(ctx, hubName)
	if err != nil {
		return err
	}
	_, err = aeh.getConsumerGroupsClient()
	if err != nil {
		return err
	}
	return aeh.createConsumerGroup(ctx, hubName)
}

func (aeh *AzureEventHubs) getConsumerGroupsClient() (*mgmt.ConsumerGroupsClient, error) {
	if aeh.cgClient != nil {
		return aeh.cgClient, nil
	}
	client := mgmt.NewConsumerGroupsClientWithBaseURI(aeh.managementSettings.AzureEnvironment.ResourceManagerEndpoint,
		aeh.metadata.SubscriptionID)
	a, err := aeh.managementSettings.GetAuthorizer()
	if err != nil {
		return nil, err
	}
	client.Authorizer = a
	aeh.cgClient = &client
	return aeh.cgClient, nil
}

func (aeh *AzureEventHubs) createConsumerGroup(parentCtx context.Context, hubName string) error {
	create := false
	backOffConfig := retry.DefaultConfig()
	backOffConfig.Policy = retry.PolicyExponential
	backOffConfig.MaxInterval = resourceCheckMaxRetryInterval
	backOffConfig.MaxRetries = resourceCheckMaxRetry

	b := backOffConfig.NewBackOffWithContext(parentCtx)

	err := retry.NotifyRecover(func() error {
		c, err := aeh.shouldCreateConsumerGroup(parentCtx, hubName)
		if err == nil {
			create = c
			return nil
		}
		return err
	}, b, func(_ error, _ time.Duration) {
		aeh.logger.Errorf("Error checking for consumer group for EventHub : %s. Retrying...", hubName)
	}, func() {
		aeh.logger.Warnf("Successfully checked for consumer group in EventHub %s after it previously failed.", hubName)
	})
	if err != nil {
		return err
	}
	if create {
		ctx, cancel := context.WithTimeout(parentCtx, resourceCreationTimeout)
		_, err = aeh.cgClient.CreateOrUpdate(ctx, aeh.metadata.ResourceGroupName, aeh.metadata.EventHubNamespace, hubName, aeh.metadata.ConsumerGroup, mgmt.ConsumerGroup{})
		cancel()
		if err != nil {
			return err
		}
	}
	return nil
}

func (aeh *AzureEventHubs) shouldCreateConsumerGroup(parentCtx context.Context, hubName string) (bool, error) {
	ctx, cancel := context.WithTimeout(parentCtx, resourceGetTimeout)
	g, err := aeh.cgClient.Get(ctx, aeh.metadata.ResourceGroupName, aeh.metadata.EventHubNamespace, hubName, aeh.metadata.ConsumerGroup)
	cancel()
	if err != nil {
		if g.HasHTTPStatus(404) {
			return true, nil
		}
		return false, err
	}
	if *g.Name == aeh.metadata.ConsumerGroup {
		aeh.logger.Infof("consumer group %s exists for the requested topic/eventHub %s", aeh.metadata.ConsumerGroup, hubName)
	}
	return false, nil
}

func (aeh *AzureEventHubs) getHubEntity(parentCtx context.Context, hubName string) (*eventhub.HubEntity, error) {
	ctx, cancel := context.WithTimeout(parentCtx, resourceGetTimeout)
	defer cancel()
	return aeh.hubManager.Get(ctx, hubName)
}

func (aeh *AzureEventHubs) createHubEntity(parentCtx context.Context, hubName string) error {
	ctx, cancel := context.WithTimeout(parentCtx, resourceCreationTimeout)
	_, err := aeh.hubManager.Put(ctx, hubName,
		eventhub.HubWithMessageRetentionInDays(aeh.metadata.MessageRetentionInDays),
		eventhub.HubWithPartitionCount(aeh.metadata.PartitionCount))
	cancel()
	if err != nil {
		aeh.logger.Errorf("error creating event hub %s: %s", hubName, err)
		return fmt.Errorf("error creating event hub %s: %s", hubName, err)
	}
	return nil
}

func (aeh *AzureEventHubs) ensurePublisherClient(ctx context.Context, hubName string) error {
	if aeh.metadata.EnableEntityManagement {
		if err := aeh.ensureEventHub(ctx, hubName); err != nil {
			return err
		}
	}
	userAgent := "dapr-" + logger.DaprVersion
	if aeh.metadata.ConnectionString != "" {
		// Connect with connection string.
		newConnectionString, err := aeh.constructConnectionStringFromTopic(hubName)
		if err != nil {
			return err
		}

		hub, err := eventhub.NewHubFromConnectionString(newConnectionString,
			eventhub.HubWithUserAgent(userAgent))
		if err != nil {
			aeh.logger.Debugf("unable to connect to azure event hubs: %v", err)
			return fmt.Errorf("unable to connect to azure event hubs: %v", err)
		}
		aeh.hubClients[hubName] = hub
	} else {
		if hubName == "" {
			return errors.New("error: missing topic/hubName attribute with AAD connection")
		}

		hub, err := eventhub.NewHub(aeh.metadata.EventHubNamespace, hubName, aeh.tokenProvider, eventhub.HubWithUserAgent(userAgent))
		if err != nil {
			return fmt.Errorf("unable to connect to azure event hubs: %v", err)
		}
		aeh.hubClients[hubName] = hub
	}

	return nil
}

func (aeh *AzureEventHubs) ensureSubscriberClient(ctx context.Context, topic string, leaserCheckpointer *storage.LeaserCheckpointer) (*eph.EventProcessorHost, error) {
	// connectionString given.
	if aeh.metadata.ConnectionString != "" {
		hubName, err := validateAndGetHubName(aeh.metadata.ConnectionString)
		if err != nil {
			return nil, fmt.Errorf("error parsing connection string %s", err)
		}
		if hubName != "" && hubName != topic {
			return nil, fmt.Errorf("error: component cannot subscribe to requested topic %s with the given connectionString", topic)
		}
		if hubName == "" {
			aeh.logger.Debugf("eventhub namespace connection string given. using topic as event hub entity path")
		}
		connectionString, err := aeh.constructConnectionStringFromTopic(topic)
		if err != nil {
			return nil, err
		}
		processor, err := eph.NewFromConnectionString(
			ctx, connectionString,
			leaserCheckpointer,
			leaserCheckpointer,
			eph.WithNoBanner(),
			eph.WithConsumerGroup(aeh.metadata.ConsumerGroup),
		)
		if err != nil {
			return nil, err
		}
		aeh.logger.Debugf("processor initialized via connection string for topic %s", topic)
		return processor, nil
	}
	// AAD connection.
	processor, err := eph.New(ctx,
		aeh.metadata.EventHubNamespace,
		topic,
		aeh.tokenProvider,
		leaserCheckpointer,
		leaserCheckpointer,
		eph.WithNoBanner(),
		eph.WithConsumerGroup(aeh.metadata.ConsumerGroup),
	)
	if err != nil {
		return nil, err
	}
	aeh.logger.Debugf("processor initialized via AAD for topic %s", topic)

	return processor, nil
}

func (aeh *AzureEventHubs) createHubManager() error {
	// Only AAD based authentication supported.
	hubManager, err := eventhub.NewHubManagerFromAzureEnvironment(aeh.metadata.EventHubNamespace, aeh.tokenProvider, *aeh.eventHubSettings.AzureEnvironment)
	if err != nil {
		return fmt.Errorf("%s %s", hubManagerCreationErrorMsg, err)
	}
	aeh.hubManager = hubManager

	return nil
}

func (aeh *AzureEventHubs) constructConnectionStringFromTopic(requestedTopic string) (string, error) {
	hubName, err := validateAndGetHubName(aeh.metadata.ConnectionString)
	if err != nil {
		return "", err
	}
	if hubName != "" && hubName == requestedTopic {
		return aeh.metadata.ConnectionString, nil
	} else if hubName != "" {
		return "", fmt.Errorf(differentTopicConnectionStringErrorTmpl, requestedTopic)
	}
	return aeh.metadata.ConnectionString + ";" + entityPathKey + "=" + requestedTopic, nil
}

func (aeh *AzureEventHubs) validateEnitityManagementMetadata() error {
	if aeh.metadata.MessageRetentionInDays <= 0 || aeh.metadata.MessageRetentionInDays > maxMessageRetention {
		aeh.logger.Warnf("invalid/no message retention time period is given with entity management enabled, default value of %d is used", defaultMessageRetentionInDays)
		aeh.metadata.MessageRetentionInDays = defaultMessageRetentionInDays
	}
	if aeh.metadata.PartitionCount <= 0 || aeh.metadata.PartitionCount > maxPartitionCount {
		aeh.logger.Warnf("invalid/no partition count is given with entity management enabled, default value of %d is used", defaultPartitionCount)
		aeh.metadata.PartitionCount = defaultPartitionCount
	}
	if aeh.metadata.ResourceGroupName == "" {
		return errors.New(missingResourceGroupNameMsg)
	}
	if aeh.metadata.SubscriptionID == "" {
		return errors.New(missingSubscriptionIDMsg)
	}
	return nil
}

func (aeh *AzureEventHubs) validateSubscriptionAttributes() error {
	m := *aeh.metadata

	if m.StorageAccountName == "" {
		return errors.New(missingStorageAccountNameErrorMsg)
	}

	if m.StorageAccountKey == "" && m.ConnectionString != "" {
		return errors.New(missingStorageAccountKeyErrorMsg)
	}

	if m.StorageContainerName == "" {
		return errors.New(missingStorageContainerNameErrorMsg)
	}

	if m.ConsumerGroup == "" {
		return errors.New(missingConsumerIDErrorMsg)
	}
	return nil
}

func (aeh *AzureEventHubs) getStoragePrefixString(topic string) string {
	// empty string in the end of slice to have a suffix "-".
	return strings.Join([]string{"dapr", topic, aeh.metadata.ConsumerGroup, ""}, "-")
}

// Init connects to Azure Event Hubs.
func (aeh *AzureEventHubs) Init(metadata pubsub.Metadata) error {
	m, parseErr := parseEventHubsMetadata(metadata)
	if parseErr != nil {
		return parseErr
	}

	aeh.metadata = m
	aeh.eventProcessors = map[string]*eph.EventProcessorHost{}
	aeh.hubClients = map[string]*eventhub.Hub{}

	if aeh.metadata.ConnectionString != "" {
		// Validate connectionString.
		hubName, validateErr := validateAndGetHubName(aeh.metadata.ConnectionString)
		if validateErr != nil {
			return errors.New(invalidConnectionStringErrorMsg)
		}
		if hubName != "" {
			aeh.logger.Infof("connectionString provided is specific to event hub %q. Publishing or subscribing to a topic that does not match this event hub will fail when attempted.", hubName)
		} else {
			aeh.logger.Infof("hubName not given in connectionString. connection established on first publish/subscribe")
			aeh.logger.Debugf("req.Topic field in incoming requests honored")
		}
		if aeh.metadata.EnableEntityManagement {
			// See https://docs.microsoft.com/en-us/azure/event-hubs/event-hubs-management-libraries
			return errors.New(entityManagementConnectionStrMsg)
		}
	} else {
		// Connect via AAD.
		settings, sErr := azauth.NewEnvironmentSettings(azauth.AzureEventHubsResourceName, metadata.Properties)
		if sErr != nil {
			return sErr
		}
		aeh.eventHubSettings = settings
		tokenProvider, err := aeh.eventHubSettings.GetAMQPTokenProvider()
		if err != nil {
			return fmt.Errorf("%s %s", hubManagerCreationErrorMsg, err)
		}
		aeh.tokenProvider = tokenProvider
		aeh.logger.Info("connecting to Azure EventHubs via AAD. connection established on first publish/subscribe")
		aeh.logger.Debugf("req.Topic field in incoming requests honored")

		if aeh.metadata.EnableEntityManagement {
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
		}
	}

	// connect to the storage account.
	if m.StorageAccountKey != "" {
		metadata.Properties["accountKey"] = m.StorageAccountKey
	}
	var storageCredsErr error
	aeh.storageCredential, aeh.azureEnvironment, storageCredsErr = azauth.GetAzureStorageBlobCredentials(aeh.logger, m.StorageAccountName, metadata.Properties)
	if storageCredsErr != nil {
		return fmt.Errorf("invalid storage credentials with error: %w", storageCredsErr)
	}

	aeh.publishCtx, aeh.publishCancel = context.WithCancel(context.Background())

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
	if _, ok := aeh.hubClients[req.Topic]; !ok {
		if err := aeh.ensurePublisherClient(aeh.publishCtx, req.Topic); err != nil {
			return fmt.Errorf("error on establishing hub connection: %s", err)
		}
	}
	event := &eventhub.Event{Data: req.Data}
	val, ok := req.Metadata[partitionKeyMetadataKey]
	if ok {
		event.PartitionKey = &val
	}
	err := aeh.hubClients[req.Topic].Send(aeh.publishCtx, event)
	if err != nil {
		return fmt.Errorf("error from publish: %s", err)
	}

	return nil
}

// BulkPublish sends data to Azure Event Hubs in bulk.
func (aeh *AzureEventHubs) BulkPublish(ctx context.Context, req *pubsub.BulkPublishRequest) (pubsub.BulkPublishResponse, error) {
	if _, ok := aeh.hubClients[req.Topic]; !ok {
		if err := aeh.ensurePublisherClient(ctx, req.Topic); err != nil {
			err = fmt.Errorf("error on establishing hub connection: %s", err)
			return pubsub.NewBulkPublishResponse(req.Entries, pubsub.PublishFailed, err), err
		}
	}

	// Create a slice of events to send.
	events := make([]*eventhub.Event, len(req.Entries))
	for i, entry := range req.Entries {
		events[i] = &eventhub.Event{Data: entry.Event}
		if val, ok := entry.Metadata[partitionKeyMetadataKey]; ok {
			events[i].PartitionKey = &val
		}
	}

	// Configure options for sending events.
	opts := []eventhub.BatchOption{
		eventhub.BatchWithMaxSizeInBytes(utils.GetElemOrDefaultFromMap(
			req.Metadata, contribMetadata.MaxBulkPubBytesKey, int(eventhub.DefaultMaxMessageSizeInBytes))),
	}

	// Send events.
	err := aeh.hubClients[req.Topic].SendBatch(ctx, eventhub.NewEventBatchIterator(events...), opts...)
	if err != nil {
		// Partial success is not supported by Azure Event Hubs.
		// If an error occurs, all events are considered failed.
		return pubsub.NewBulkPublishResponse(req.Entries, pubsub.PublishFailed, err), err
	}

	return pubsub.NewBulkPublishResponse(req.Entries, pubsub.PublishSucceeded, nil), nil
}

// Subscribe receives data from Azure Event Hubs.
func (aeh *AzureEventHubs) Subscribe(subscribeCtx context.Context, req pubsub.SubscribeRequest, handler pubsub.Handler) error {
	err := aeh.validateSubscriptionAttributes()
	if err != nil {
		return fmt.Errorf("error : error on subscribe %s", err)
	}
	if aeh.metadata.EnableEntityManagement {
		if err = aeh.ensureSubscription(subscribeCtx, req.Topic); err != nil {
			return err
		}
	}

	// Set topic name, consumerID prefix for partition checkpoint lease blob path.
	// This is needed to support multiple consumers for the topic using the same storage container.
	leaserPrefixOpt := storage.WithPrefixInBlobPath(aeh.getStoragePrefixString(req.Topic))
	leaserCheckpointer, err := storage.NewStorageLeaserCheckpointer(aeh.storageCredential, aeh.metadata.StorageAccountName, aeh.metadata.StorageContainerName, *aeh.azureEnvironment, leaserPrefixOpt)
	if err != nil {
		return err
	}

	processor, err := aeh.ensureSubscriberClient(subscribeCtx, req.Topic, leaserCheckpointer)
	if err != nil {
		return err
	}

	aeh.logger.Debugf("registering handler for topic %s", req.Topic)
	_, err = processor.RegisterHandler(subscribeCtx,
		func(_ context.Context, e *eventhub.Event) error {
			// This component has built-in retries because Event Hubs doesn't support N/ACK for messages
			b := aeh.backOffConfig.NewBackOffWithContext(subscribeCtx)

			retryerr := retry.NotifyRecover(func() error {
				aeh.logger.Debugf("Processing EventHubs event %s/%s", req.Topic, e.ID)

				return subscribeHandler(subscribeCtx, req.Topic, e, handler)
			}, b, func(_ error, _ time.Duration) {
				aeh.logger.Warnf("Error processing EventHubs event: %s/%s. Retrying...", req.Topic, e.ID)
			}, func() {
				aeh.logger.Warnf("Successfully processed EventHubs event after it previously failed: %s/%s", req.Topic, e.ID)
			})
			if retryerr != nil {
				aeh.logger.Errorf("Too many failed attempts at processing Eventhubs event: %s/%s. Error: %v.", req.Topic, e.ID, err)
			}
			return retryerr
		})
	if err != nil {
		return err
	}

	err = processor.StartNonBlocking(subscribeCtx)
	if err != nil {
		return err
	}
	aeh.eventProcessors[req.Topic] = processor

	// Listen for context cancelation and stop processing messages
	// This seems to be necessary because otherwise the processor isn't automatically closed on context cancelation
	go func() {
		<-subscribeCtx.Done()
		stopCtx, stopCancel := context.WithTimeout(context.Background(), resourceGetTimeout)
		stopErr := processor.Close(stopCtx)
		stopCancel()
		if stopErr != nil {
			aeh.logger.Warnf("Error closing subscribe processor: %v", stopErr)
		}
	}()

	return nil
}

func (aeh *AzureEventHubs) Close() (err error) {
	if aeh.publishCancel != nil {
		aeh.publishCancel()
	}

	flag := false
	var ctx context.Context
	var cancel context.CancelFunc
	for topic, client := range aeh.hubClients {
		ctx, cancel = context.WithTimeout(context.Background(), resourceGetTimeout)
		err = client.Close(ctx)
		cancel()
		if err != nil {
			flag = true
			aeh.logger.Warnf("error closing publish client properly for topic/eventHub %s: %s", topic, err)
		}
	}
	aeh.hubClients = map[string]*eventhub.Hub{}
	for topic, client := range aeh.eventProcessors {
		ctx, cancel = context.WithTimeout(context.Background(), resourceGetTimeout)
		err = client.Close(ctx)
		cancel()
		if err != nil {
			flag = true
			aeh.logger.Warnf("error closing event processor host client properly for topic/eventHub %s: %s", topic, err)
		}
	}
	aeh.eventProcessors = map[string]*eph.EventProcessorHost{}
	if flag {
		return errors.New("error closing event hub clients in a proper fashion")
	}
	return nil
}

func (aeh *AzureEventHubs) Features() []pubsub.Feature {
	return nil
}
