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
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/cenkalti/backoff/v4"

	azauth "github.com/dapr/components-contrib/internal/authentication/azure"
	"github.com/dapr/kit/logger"
	"github.com/dapr/kit/retry"
)

// This method ensures that there are currently no active subscribers to the same Event Hub topic that are using the old ("track 1") SDK of Azure Event Hubs. This is the SDK that was in use until Dapr 1.9.
// Because the new SDK stores checkpoints in a different way, clients using the new ("track 2") and the old SDK cannot coexist.
// To ensure this doesn't happen, when we create a new subscription to the same topic and with the same consumer group, we check if there's a file in Azure Storage with the checkpoint created by the old SDK and with a still-active lease. If that's true, we wait (until the context expires) before we crash Dapr with a log message describing what's happening.
// These conflicts should be transient anyways, as mixed versions of Dapr should only happen during a rollout of a new version of Dapr.
// TODO(@ItalyPaleAle): Remove this for Dapr 1.13
func (aeh *AzureEventHubs) ensureNoTrack1Subscribers(parentCtx context.Context, topic string) error {
	// Get a client to Azure Blob Storage
	client, err := aeh.createContainerStorageClient()
	if err != nil {
		return err
	}

	// In the old version of the SDK, checkpoints were stored in the root of the storage account and were named like:
	// `dapr-(topic)-(consumer-group)-(partition-key)`
	// We need to list those up and check if they have an active lease
	prefix := fmt.Sprintf("dapr-%s-%s-", topic, aeh.metadata.ConsumerGroup)

	// Retry until we find no active leases - or the context expires
	backOffConfig := retry.DefaultConfig()
	backOffConfig.Policy = retry.PolicyExponential
	backOffConfig.MaxInterval = time.Minute
	backOffConfig.MaxElapsedTime = 0
	backOffConfig.MaxRetries = -1
	b := backOffConfig.NewBackOffWithContext(parentCtx)
	err = backoff.Retry(func() error {
		pager := client.NewListBlobsFlatPager(&container.ListBlobsFlatOptions{
			Prefix: &prefix,
		})
		for pager.More() {
			ctx, cancel := context.WithTimeout(parentCtx, resourceGetTimeout)
			resp, innerErr := pager.NextPage(ctx)
			cancel()
			if innerErr != nil {
				return fmt.Errorf("failed to list blobs: %w", innerErr)
			}
			for _, blob := range resp.Segment.BlobItems {
				if blob == nil || blob.Name == nil || blob.Properties == nil || blob.Properties.LeaseState == nil {
					continue
				}
				aeh.logger.Debugf("Found checkpoint from an older Dapr version %s", *blob.Name)
				// If the blob is locked, it means that there's another Dapr process with an old version of the SDK running, so we need to wait
				if *blob.Properties.LeaseStatus == "locked" {
					aeh.logger.Warnf("Found active lease on checkpoint %s from an older Dapr version - waiting for lease to expire", *blob.Name)
					return fmt.Errorf("found active lease on checkpoint %s from an old Dapr version", *blob.Name)
				}
			}
		}
		return nil
	}, b)

	// If the error is a DeadlineExceeded on the operation and not on parentCtx, handle that separately to avoid crashing Dapr needlessly
	if err != nil && errors.Is(err, context.DeadlineExceeded) && parentCtx.Err() != context.DeadlineExceeded {
		err = errors.New("failed to list blobs: request timed out")
	}
	return err
}

func (aeh *AzureEventHubs) createContainerStorageClient() (*container.Client, error) {
	options := container.ClientOptions{
		ClientOptions: azcore.ClientOptions{
			Telemetry: policy.TelemetryOptions{
				ApplicationID: "dapr-" + logger.DaprVersion,
			},
		},
	}

	var (
		err    error
		client *container.Client
	)
	// Use the global URL for Azure Storage
	containerURL := fmt.Sprintf("https://%s.blob.%s/%s", aeh.metadata.StorageAccountName, "core.windows.net", aeh.metadata.StorageContainerName)

	if aeh.metadata.StorageConnectionString != "" {
		// Authenticate with a connection string
		client, err = container.NewClientFromConnectionString(aeh.metadata.StorageConnectionString, aeh.metadata.StorageContainerName, &options)
		if err != nil {
			return nil, fmt.Errorf("error creating Azure Storage client from connection string: %w", err)
		}
	} else if aeh.metadata.StorageAccountKey != "" {
		// Authenticate with a shared key
		credential, newSharedKeyErr := azblob.NewSharedKeyCredential(aeh.metadata.StorageAccountName, aeh.metadata.StorageAccountKey)
		if newSharedKeyErr != nil {
			return nil, fmt.Errorf("invalid Azure Storage shared key credentials with error: %w", newSharedKeyErr)
		}
		client, err = container.NewClientWithSharedKeyCredential(containerURL, credential, &options)
		if err != nil {
			return nil, fmt.Errorf("error creating Azure Storage client from shared key credentials: %w", err)
		}
	} else {
		// Use Azure AD
		settings, err := azauth.NewEnvironmentSettings("storage", aeh.metadata.properties)
		if err != nil {
			return nil, err
		}
		credential, tokenErr := settings.GetTokenCredential()
		if tokenErr != nil {
			return nil, fmt.Errorf("invalid Azure Storage token credentials with error: %w", tokenErr)
		}
		client, err = container.NewClient(containerURL, credential, &options)
		if err != nil {
			return nil, fmt.Errorf("error creating Azure Storage client from token credentials: %w", err)
		}
	}

	return client, nil
}
