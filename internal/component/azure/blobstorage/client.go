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

package blobstorage

import (
	"context"
	"fmt"
	"net/url"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"

	azauth "github.com/dapr/components-contrib/internal/authentication/azure"
	mdutils "github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
)

const (
	// Specifies the maximum number of HTTP requests that will be made to retry blob operations. A value
	// of zero means that no additional HTTP requests will be made.
	defaultBlobRetryCount = 3
)

func CreateContainerStorageClient(log logger.Logger, meta map[string]string) (*container.Client, *BlobStorageMetadata, error) {
	m, err := parseMetadata(meta)
	if err != nil {
		return nil, nil, err
	}

	userAgent := "dapr-" + logger.DaprVersion
	options := container.ClientOptions{
		ClientOptions: azcore.ClientOptions{
			Retry: policy.RetryOptions{
				MaxRetries: m.RetryCount,
			},
			Telemetry: policy.TelemetryOptions{
				ApplicationID: userAgent,
			},
		},
	}

	settings, err := azauth.NewEnvironmentSettings("storage", meta)
	if err != nil {
		return nil, nil, err
	}
	var customEndpoint string
	if val, ok := mdutils.GetMetadataProperty(meta, azauth.StorageEndpointKeys...); ok && val != "" {
		customEndpoint = val
	}
	var URL *url.URL
	if customEndpoint != "" {
		var parseErr error
		URL, parseErr = url.Parse(fmt.Sprintf("%s/%s/%s", customEndpoint, m.AccountName, m.ContainerName))
		if parseErr != nil {
			return nil, nil, parseErr
		}
	} else {
		env := settings.AzureEnvironment
		URL, _ = url.Parse(fmt.Sprintf("https://%s.blob.%s/%s", m.AccountName, env.StorageEndpointSuffix, m.ContainerName))
	}

	var clientErr error
	var client *container.Client
	// Try using shared key credentials first
	if m.AccountKey != "" {
		credential, newSharedKeyErr := azblob.NewSharedKeyCredential(m.AccountName, m.AccountKey)
		if newSharedKeyErr != nil {
			return nil, nil, fmt.Errorf("invalid shared key credentials with error: %w", newSharedKeyErr)
		}
		client, clientErr = container.NewClientWithSharedKeyCredential(URL.String(), credential, &options)
		if clientErr != nil {
			return nil, nil, fmt.Errorf("cannot init Blobstorage container client: %w", clientErr)
		}
	} else {
		// fallback to AAD
		credential, tokenErr := settings.GetTokenCredential()
		if tokenErr != nil {
			return nil, nil, fmt.Errorf("invalid token credentials with error: %w", tokenErr)
		}
		client, clientErr = container.NewClient(URL.String(), credential, &options)
	}
	if clientErr != nil {
		return nil, nil, fmt.Errorf("cannot init Blobstorage client: %w", clientErr)
	}

	createContainerOptions := container.CreateOptions{
		Access:   &m.PublicAccessLevel,
		Metadata: map[string]string{},
	}
	timeoutCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	_, err = client.Create(timeoutCtx, &createContainerOptions)
	cancel()
	// Don't return error, container might already exist
	log.Debugf("error creating container: %v", err)

	return client, m, nil
}
