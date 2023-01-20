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
	"net/http"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/arm"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/eventhub/armeventhub"

	azauth "github.com/dapr/components-contrib/internal/authentication/azure"
	"github.com/dapr/kit/logger"
	"github.com/dapr/kit/ptr"
	"github.com/dapr/kit/retry"
)

const (
	defaultMessageRetentionInDays = 1
	defaultPartitionCount         = 1

	resourceCheckMaxRetry         = 5
	resourceCheckMaxRetryInterval = 5 * time.Minute
	resourceCreationTimeout       = 15 * time.Second
	resourceGetTimeout            = 5 * time.Second

	// See https://docs.microsoft.com/en-us/azure/event-hubs/event-hubs-quotas for numbers.
	maxMessageRetention = int32(90)
	maxPartitionCount   = int32(1024)
)

// Intializes the entity management capabilities. This method is invoked by Init.
func (aeh *AzureEventHubs) initEntityManagement() error {
	// Validate the metadata
	err := aeh.validateEnitityManagementMetadata()
	if err != nil {
		return err
	}

	// Get Azure Management plane credentials object
	settings, err := azauth.NewEnvironmentSettings("azure", aeh.metadata.properties)
	if err != nil {
		return err
	}
	creds, err := settings.GetTokenCredential()
	if err != nil {
		return fmt.Errorf("failed to obtain Azure AD management credentials: %w", err)
	}
	aeh.managementCreds = creds
	return nil
}

func (aeh *AzureEventHubs) validateEnitityManagementMetadata() error {
	if aeh.metadata.MessageRetentionInDays <= 0 || aeh.metadata.MessageRetentionInDays > maxMessageRetention {
		aeh.logger.Warnf("Property messageRetentionInDays for entity management has an empty or invalid value; using the default value %d", defaultMessageRetentionInDays)
		aeh.metadata.MessageRetentionInDays = defaultMessageRetentionInDays
	}
	if aeh.metadata.PartitionCount <= 0 || aeh.metadata.PartitionCount > maxPartitionCount {
		aeh.logger.Warnf("Property partitionCount for entity management has an empty or invalid value; using the default value %d", defaultPartitionCount)
		aeh.metadata.PartitionCount = defaultPartitionCount
	}
	if aeh.metadata.ResourceGroupName == "" {
		return errors.New("property resourceGroupName is required for entity management")
	}
	if aeh.metadata.SubscriptionID == "" {
		return errors.New("property subscriptionID is required for entity management")
	}
	return nil
}

// Ensures that the Event Hub entity exists.
// This is used during the creation of both producers and consumers.
func (aeh *AzureEventHubs) ensureEventHubEntity(parentCtx context.Context, topic string) error {
	client, err := armeventhub.NewEventHubsClient(aeh.metadata.SubscriptionID, aeh.managementCreds, &arm.ClientOptions{
		ClientOptions: policy.ClientOptions{
			Telemetry: policy.TelemetryOptions{
				ApplicationID: "dapr-" + logger.DaprVersion,
			},
		},
	})
	if err != nil {
		return fmt.Errorf("failed to create Event Hubs ARM client: %w", err)
	}

	aeh.logger.Debugf("Checking if entity %s exists on Event Hub namespace %s", topic, aeh.metadata.EventHubNamespace)

	// Check if the entity exists
	ctx, cancel := context.WithTimeout(parentCtx, resourceGetTimeout)
	defer cancel()
	_, err = client.Get(ctx, aeh.metadata.ResourceGroupName, aeh.metadata.EventHubNamespace, topic, nil)
	if err == nil {
		// If there's no error, the entity already exists, so all good
		aeh.logger.Debugf("Entity %s already exists on Event Hub namespace %s", topic, aeh.metadata.EventHubNamespace)
		return nil
	}

	// Check if the error is NotFound or something else
	resErr := &azcore.ResponseError{}
	if !errors.As(err, &resErr) || resErr.StatusCode != http.StatusNotFound {
		// We have another error, just return it
		return fmt.Errorf("failed to retrieve Event Hub entity %s: %w", topic, err)
	}

	// Create the entity
	aeh.logger.Infof("Will create entity %s on Event Hub namespace %s", topic, aeh.metadata.EventHubNamespace)
	params := armeventhub.Eventhub{
		Properties: &armeventhub.Properties{
			MessageRetentionInDays: ptr.Of(int64(aeh.metadata.MessageRetentionInDays)),
			PartitionCount:         ptr.Of(int64(aeh.metadata.PartitionCount)),
		},
	}
	ctx, cancel = context.WithTimeout(parentCtx, resourceCreationTimeout)
	defer cancel()
	_, err = client.CreateOrUpdate(ctx, aeh.metadata.ResourceGroupName, aeh.metadata.EventHubNamespace, topic, params, nil)
	if err != nil {
		return fmt.Errorf("failed to create Event Hub entity %s: %w", topic, err)
	}

	aeh.logger.Infof("Entity %s created on Event Hub namespace %s", topic, aeh.metadata.EventHubNamespace)
	return nil
}

// Ensures that the subscription (consumer group) exists.
// This is used during the creation of consumers only.
func (aeh *AzureEventHubs) ensureSubscription(parentCtx context.Context, hubName string) error {
	client, err := armeventhub.NewConsumerGroupsClient(aeh.metadata.SubscriptionID, aeh.managementCreds, &arm.ClientOptions{
		ClientOptions: policy.ClientOptions{
			Telemetry: policy.TelemetryOptions{
				ApplicationID: "dapr-" + logger.DaprVersion,
			},
		},
	})
	if err != nil {
		return fmt.Errorf("failed to create consumer group ARM client: %w", err)
	}

	aeh.logger.Debugf("Checking if consumer group %s exists in entity %s", aeh.metadata.ConsumerGroup, hubName)

	// Check if the consumer group exists
	// We need to use a retry logic here
	backOffConfig := retry.DefaultConfig()
	backOffConfig.Policy = retry.PolicyExponential
	backOffConfig.MaxInterval = resourceCheckMaxRetryInterval
	backOffConfig.MaxRetries = resourceCheckMaxRetry
	b := backOffConfig.NewBackOffWithContext(parentCtx)
	create, err := retry.NotifyRecoverWithData(func() (bool, error) {
		c, cErr := aeh.shouldCreateConsumerGroup(parentCtx, client, hubName)
		if cErr != nil {
			return false, cErr
		}
		return c, nil
	}, b, func(_ error, _ time.Duration) {
		aeh.logger.Errorf("Error checking for consumer group for Event Hub: %s. Retrying…", hubName)
	}, func() {
		aeh.logger.Warnf("Successfully checked for consumer group in Event Hub %s after it previously failed", hubName)
	})
	if err != nil {
		return err
	}
	if !create {
		// Already exists
		aeh.logger.Debugf("Consumer group %s already exists in entity %s", aeh.metadata.ConsumerGroup, hubName)
		return nil
	}

	// Need to create the consumer group
	aeh.logger.Infof("Will create consumer group %s exists in entity %s", aeh.metadata.ConsumerGroup, hubName)

	ctx, cancel := context.WithTimeout(parentCtx, resourceCreationTimeout)
	defer cancel()
	_, err = client.CreateOrUpdate(ctx, aeh.metadata.ResourceGroupName, aeh.metadata.EventHubNamespace, hubName, aeh.metadata.ConsumerGroup, armeventhub.ConsumerGroup{}, nil)
	if err != nil {
		return fmt.Errorf("failed to create consumer group  %s: %w", aeh.metadata.ConsumerGroup, err)
	}

	aeh.logger.Infof("Consumer group %s created in entity %s", aeh.metadata.ConsumerGroup, hubName)
	return nil
}

func (aeh *AzureEventHubs) shouldCreateConsumerGroup(parentCtx context.Context, client *armeventhub.ConsumerGroupsClient, hubName string) (bool, error) {
	ctx, cancel := context.WithTimeout(parentCtx, resourceGetTimeout)
	defer cancel()
	_, err := client.Get(ctx, aeh.metadata.ResourceGroupName, aeh.metadata.EventHubNamespace, hubName, aeh.metadata.ConsumerGroup, nil)
	if err == nil {
		// If there's no error, the consumer group already exists, so all good
		return true, nil
	}

	// Check if the error is NotFound or something else
	resErr := &azcore.ResponseError{}
	if !errors.As(err, &resErr) || resErr.StatusCode != http.StatusNotFound {
		// We have another error, just return it
		return false, err
	}

	// Consumer group doesn't exist
	return false, nil
}
