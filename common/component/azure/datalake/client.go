/*
Copyright 2025 The Dapr Authors
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

package datalake

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azdatalake"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azdatalake/filesystem"

	azauth "github.com/dapr/components-contrib/common/authentication/azure"
	mdutils "github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
)

// CreateFileSystemStorageClient returns a filesystem.Client and the parsed metadata from the metadata dictionary.
func CreateFileSystemStorageClient(parentCtx context.Context, log logger.Logger, meta map[string]string) (*filesystem.Client, *DataLakeMetadata, error) {
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

	// Get the filesystem client
	client, err := m.InitFileSystemClient(azEnvSettings)
	if err != nil {
		return nil, nil, err
	}

	// if entity management is disabled, do not attempt to create the filesystem
	if !m.DisableEntityManagement {
		ctx, cancel := context.WithTimeout(parentCtx, 30*time.Second)
		defer cancel()
		err = m.EnsureFileSystem(ctx, client)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to create Azure Data Lake Storage filesystem %s: %w", m.FileSystemName, err)
		}
	}

	return client, m, nil
}

// Sets the customEndpoint property if needed
func (opts *FileSystemClientOpts) setCustomEndpoint(log logger.Logger, meta map[string]string, azEnvSettings azauth.EnvironmentSettings) error {
	val, _ := mdutils.GetMetadataProperty(meta, azauth.MetadataKeys["StorageEndpoint"]...)
	if val == "" {
		return nil
	}

	endpointURL, err := url.Parse(val)
	if err != nil {
		return fmt.Errorf("failed to parse custom endpoint %q: %w", val, err)
	}

	// Check if the custom endpoint is set to an Azure Data Lake Storage public endpoint
	adlsURL := opts.getAzureDataLakeStorageFileSystemURL(azEnvSettings)
	if endpointURL.Hostname() == adlsURL.Hostname() && adlsURL.Path == endpointURL.Path {
		log.Warn("Metadata property endpoint is set to an Azure Data Lake Storage endpoint and will be ignored")
	} else {
		log.Info("Using custom endpoint for Azure Data Lake Storage")
		opts.customEndpoint = strings.TrimSuffix(endpointURL.String(), "/")
	}

	return nil
}

// GetFileSystemURL returns the URL of the filesystem, needed by some auth methods.
func (opts *FileSystemClientOpts) GetFileSystemURL(azEnvSettings azauth.EnvironmentSettings) (u *url.URL, err error) {
	if opts.customEndpoint != "" {
		u, err = url.Parse(fmt.Sprintf("%s/%s/%s", opts.customEndpoint, opts.AccountName, opts.FileSystemName))
		if err != nil {
			return nil, errors.New("failed to get filesystem's URL with custom endpoint")
		}
	} else {
		u = opts.getAzureDataLakeStorageFileSystemURL(azEnvSettings)
	}
	return u, nil
}

func (opts *FileSystemClientOpts) getAzureDataLakeStorageFileSystemURL(azEnvSettings azauth.EnvironmentSettings) *url.URL {
	u, _ := url.Parse(fmt.Sprintf("https://%s.dfs.%s/%s", opts.AccountName, azEnvSettings.EndpointSuffix(azauth.ServiceAzureStorage), opts.FileSystemName))
	return u
}

// InitFileSystemClient returns a new filesystem.Client object from the given options.
func (opts *FileSystemClientOpts) InitFileSystemClient(azEnvSettings azauth.EnvironmentSettings) (client *filesystem.Client, err error) {
	clientOpts := &filesystem.ClientOptions{
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
		client, err = filesystem.NewClientFromConnectionString(opts.ConnectionString, opts.FileSystemName, clientOpts)
		if err != nil {
			return nil, fmt.Errorf("cannot init data lake filesystem client with connection string: %w", err)
		}

	// Use a shared account key
	case opts.AccountKey != "" && opts.AccountName != "":
		var (
			credential *filesystem.SharedKeyCredential
			u          *url.URL
		)
		credential, err = azdatalake.NewSharedKeyCredential(opts.AccountName, opts.AccountKey)
		if err != nil {
			return nil, fmt.Errorf("invalid shared key credentials with error: %w", err)
		}
		u, err = opts.GetFileSystemURL(azEnvSettings)
		if err != nil {
			return nil, err
		}
		client, err = filesystem.NewClientWithSharedKeyCredential(u.String(), credential, clientOpts)
		if err != nil {
			return nil, fmt.Errorf("cannot init data lake filesystem client with shared key: %w", err)
		}
	// Use Azure AD as fallback
	default:
		credential, tokenErr := azEnvSettings.GetTokenCredential()
		if tokenErr != nil {
			return nil, fmt.Errorf("invalid token credentials with error: %w", tokenErr)
		}
		var u *url.URL
		u, err = opts.GetFileSystemURL(azEnvSettings)
		if err != nil {
			return nil, err
		}
		client, err = filesystem.NewClient(u.String(), credential, clientOpts)
		if err != nil {
			return nil, fmt.Errorf("cannot init data lake filesystem client with Azure AD token: %w", err)
		}
	}

	return client, nil
}

// EnsureFileSystem creates the filesystem if it doesn't already exist.
func (opts *FileSystemClientOpts) EnsureFileSystem(ctx context.Context, client *filesystem.Client) error {
	// Create the filesystem
	// This will return an error if it already exists
	_, err := client.Create(ctx, nil)
	if err != nil {
		// Check if it's an Azure Storage error
		resErr := &azcore.ResponseError{}
		// If the filesystem already exists, return no error
		if errors.As(err, &resErr) && (resErr.ErrorCode == "FileSystemAlreadyExists" || resErr.ErrorCode == "ResourceAlreadyExists") {
			return nil
		}
		return err
	}

	return nil
}
