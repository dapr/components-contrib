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
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"

	azauth "github.com/dapr/components-contrib/common/authentication/azure"
	mdutils "github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
)

const (
	// Specifies the maximum number of HTTP requests that will be made to retry blob operations. A value
	// of zero means that no additional HTTP requests will be made.
	defaultBlobRetryCount = 3
)

// CreateContainerStorageClient returns a container.Client and the parsed metadata from the metadata dictionary.
func CreateContainerStorageClient(parentCtx context.Context, log logger.Logger, meta map[string]string) (*container.Client, *BlobStorageMetadata, error) {
	// Parse the metadata and set the properties in the object
	m, err := parseMetadata(meta)
	if err != nil {
		return nil, nil, err
	}

	azEnvSettings, err := azauth.NewEnvironmentSettings(meta)
	if err != nil {
		return nil, nil, err
	}

	// Check if using a custom endpoint
	err = m.setCustomEndpoint(log, meta, azEnvSettings)
	if err != nil {
		return nil, nil, err
	}

	// Get the container client
	client, err := m.InitContainerClient(azEnvSettings)
	if err != nil {
		return nil, nil, err
	}

	// if entity management is disabled, do not attempt to create the container
	if !m.DisableEntityManagement {
		// Create the container if it doesn't already exist
		var accessLevel *azblob.PublicAccessType
		if m.PublicAccessLevel != "" && m.PublicAccessLevel != "none" {
			accessLevel = &m.PublicAccessLevel
		}
		ctx, cancel := context.WithTimeout(parentCtx, 30*time.Second)
		defer cancel()
		err = m.EnsureContainer(ctx, client, accessLevel)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to create Azure Storage container %s: %w", m.ContainerName, err)
		}
	}

	return client, m, nil
}

// Sets the customEndpoint property if needed
func (opts *ContainerClientOpts) setCustomEndpoint(log logger.Logger, meta map[string]string, azEnvSettings azauth.EnvironmentSettings) error {
	val, _ := mdutils.GetMetadataProperty(meta, azauth.MetadataKeys["StorageEndpoint"]...)
	if val == "" {
		return nil
	}

	endpointURL, err := url.Parse(val)
	if err != nil {
		return fmt.Errorf("failed to parse custom endpoint %q: %w", val, err)
	}

	// Check if the custom endpoint is set to an Azure Blob Storage public endpoint
	azbURL := opts.getAzureBlobStorageContainerURL(azEnvSettings)
	if endpointURL.Hostname() == azbURL.Hostname() && azbURL.Path == endpointURL.Path {
		log.Warn("Metadata property endpoint is set to an Azure Blob Storage endpoint and will be ignored")
	} else {
		log.Info("Using custom endpoint for Azure Blob Storage")
		opts.customEndpoint = strings.TrimSuffix(endpointURL.String(), "/")
	}

	return nil
}

// GetContainerURL returns the URL of the container, needed by some auth methods.
func (opts *ContainerClientOpts) GetContainerURL(azEnvSettings azauth.EnvironmentSettings) (u *url.URL, err error) {
	if opts.customEndpoint != "" {
		u, err = url.Parse(fmt.Sprintf("%s/%s/%s", opts.customEndpoint, opts.AccountName, opts.ContainerName))
		if err != nil {
			return nil, errors.New("failed to get container's URL with custom endpoint")
		}
	} else {
		u = opts.getAzureBlobStorageContainerURL(azEnvSettings)
	}
	return u, nil
}

func (opts *ContainerClientOpts) getAzureBlobStorageContainerURL(azEnvSettings azauth.EnvironmentSettings) *url.URL {
	u, _ := url.Parse(fmt.Sprintf("https://%s.blob.%s/%s", opts.AccountName, azEnvSettings.EndpointSuffix(azauth.ServiceAzureStorage), opts.ContainerName))
	return u
}

// InitContainerClient returns a new container.Client object from the given options.
func (opts *ContainerClientOpts) InitContainerClient(azEnvSettings azauth.EnvironmentSettings) (client *container.Client, err error) {
	clientOpts := &container.ClientOptions{
		ClientOptions: azcore.ClientOptions{
			Retry: policy.RetryOptions{
				MaxRetries: opts.RetryCount,
			},
			Telemetry: policy.TelemetryOptions{
				ApplicationID: "dapr-" + logger.DaprVersion,
			},
		},
	}

	switch {
	// Use a connection string
	case opts.ConnectionString != "":
		client, err = container.NewClientFromConnectionString(opts.ConnectionString, opts.ContainerName, clientOpts)
		if err != nil {
			return nil, fmt.Errorf("cannot init blob storage container client with connection string: %w", err)
		}

	// Use a shared account key
	case opts.AccountKey != "" && opts.AccountName != "":
		var (
			credential *azblob.SharedKeyCredential
			u          *url.URL
		)
		credential, err = azblob.NewSharedKeyCredential(opts.AccountName, opts.AccountKey)
		if err != nil {
			return nil, fmt.Errorf("invalid shared key credentials with error: %w", err)
		}
		u, err = opts.GetContainerURL(azEnvSettings)
		if err != nil {
			return nil, err
		}
		client, err = container.NewClientWithSharedKeyCredential(u.String(), credential, clientOpts)
		if err != nil {
			return nil, fmt.Errorf("cannot init blob storage container client with shared key: %w", err)
		}

	// Use Azure AD as fallback
	default:
		credential, tokenErr := azEnvSettings.GetTokenCredential()
		if tokenErr != nil {
			return nil, fmt.Errorf("invalid token credentials with error: %w", tokenErr)
		}
		var u *url.URL
		u, err = opts.GetContainerURL(azEnvSettings)
		if err != nil {
			return nil, err
		}
		client, err = container.NewClient(u.String(), credential, clientOpts)
		if err != nil {
			return nil, fmt.Errorf("cannot init blob storage container client with Azure AD token: %w", err)
		}
	}

	return client, nil
}

// EnsureContainer creates the container if it doesn't already exist.
// Property "accessLevel" indicates the public access level; nil-value means the container is private
func (opts *ContainerClientOpts) EnsureContainer(ctx context.Context, client *container.Client, accessLevel *azblob.PublicAccessType) error {
	// Create the container
	// This will return an error if it already exists
	_, err := client.Create(ctx, &container.CreateOptions{
		Access: accessLevel,
	})
	if err != nil {
		// Check if it's an Azure Storage error
		resErr := &azcore.ResponseError{}
		// If the container already exists, return no error
		if errors.As(err, &resErr) && (resErr.ErrorCode == "ContainerAlreadyExists" || resErr.ErrorCode == "ResourceAlreadyExists") {
			return nil
		}
		return err
	}

	return nil
}
