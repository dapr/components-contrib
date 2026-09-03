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

// Package datalake provides a BinaryStore implementation backed by
// Azure Data Lake Storage Gen2. Each named file maps 1:1 to a path inside the
// configured filesystem.
package datalake

import (
	"context"
	"fmt"
	"reflect"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azdatalake/datalakeerror"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azdatalake/file"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azdatalake/filesystem"

	"github.com/dapr/components-contrib/binarystore"
	storagecommon "github.com/dapr/components-contrib/common/component/azure/datalake"
	contribMetadata "github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
)

// AzureDataLakeStorage implements binarystore.BinaryStore using Azure Data
// Lake Storage Gen2.
type AzureDataLakeStorage struct {
	metadata         *storagecommon.DataLakeMetadata
	fileSystemClient *filesystem.Client
	logger           logger.Logger
}

// NewAzureDataLakeStorage returns a new AzureDataLakeStorage binary store.
func NewAzureDataLakeStorage(log logger.Logger) binarystore.BinaryStore {
	return &AzureDataLakeStorage{logger: log}
}

// Init initialises the Azure Data Lake Storage client from the component metadata.
func (a *AzureDataLakeStorage) Init(ctx context.Context, md binarystore.Metadata) error {
	var err error
	a.fileSystemClient, a.metadata, err = storagecommon.CreateFileSystemStorageClient(ctx, a.logger, md.Properties)
	return err
}

// Features returns the optional features supported by this component.
func (a *AzureDataLakeStorage) Features() []binarystore.Feature {
	return []binarystore.Feature{}
}

// Set uploads binary data to Azure Data Lake Storage.
//
// When req.Overwrite is false, the file is created with an If-None-Match: *
// access condition so that the server rejects the operation atomically if
// the path already exists, returning binarystore.ErrFileAlreadyExists. When
// req.Overwrite is true, the file is created or replaced without a condition
// check.
func (a *AzureDataLakeStorage) Set(ctx context.Context, req *binarystore.SetRequest) error {
	if req.FileName == "" {
		return binarystore.ErrMissingFileName
	}

	fileClient := a.fileSystemClient.NewFileClient(req.FileName)

	createOpts := &file.CreateOptions{}
	if !req.Overwrite {
		// If-None-Match: * instructs the service to reject the create if any
		// version of the path already exists (HTTP 409 PathAlreadyExists).
		etagAny := azcore.ETagAny
		createOpts.AccessConditions = &file.AccessConditions{
			ModifiedAccessConditions: &file.ModifiedAccessConditions{
				IfNoneMatch: &etagAny,
			},
		}
	}

	_, err := fileClient.Create(ctx, createOpts)
	if err != nil {
		if datalakeerror.HasCode(err, datalakeerror.PathAlreadyExists, datalakeerror.ConditionNotMet) {
			return binarystore.ErrFileAlreadyExists
		}
		return fmt.Errorf("error creating file %q: %w", req.FileName, err)
	}

	if err = fileClient.UploadStream(ctx, req.Data, nil); err != nil {
		return fmt.Errorf("error uploading file %q: %w", req.FileName, err)
	}

	return nil
}

// Get downloads binary data from Azure Data Lake Storage as a streaming reader.
//
// The Data field of the returned GetResponse wraps the HTTP response body and
// must be closed by the caller when reading is complete.
func (a *AzureDataLakeStorage) Get(ctx context.Context, req *binarystore.GetRequest) (*binarystore.GetResponse, error) {
	if req.FileName == "" {
		return nil, binarystore.ErrMissingFileName
	}

	fileClient := a.fileSystemClient.NewFileClient(req.FileName)
	resp, err := fileClient.DownloadStream(ctx, nil)
	if err != nil {
		if datalakeerror.HasCode(err, datalakeerror.PathNotFound) {
			return nil, binarystore.ErrFileNotFound
		}
		return nil, fmt.Errorf("error downloading file %q: %w", req.FileName, err)
	}

	return &binarystore.GetResponse{
		Data: resp.Body,
	}, nil
}

// Delete removes a path from Azure Data Lake Storage. If the path does not
// exist, binarystore.ErrFileNotFound is returned.
func (a *AzureDataLakeStorage) Delete(ctx context.Context, req *binarystore.DeleteRequest) error {
	if req.FileName == "" {
		return binarystore.ErrMissingFileName
	}

	fileClient := a.fileSystemClient.NewFileClient(req.FileName)
	_, err := fileClient.Delete(ctx, nil)
	if err != nil {
		if datalakeerror.HasCode(err, datalakeerror.PathNotFound) {
			return binarystore.ErrFileNotFound
		}
		return fmt.Errorf("error deleting file %q: %w", req.FileName, err)
	}

	return nil
}

// GetComponentMetadata returns the metadata schema for this component, used by
// the Dapr metadata linter.
func (a *AzureDataLakeStorage) GetComponentMetadata() (metadataInfo contribMetadata.MetadataMap) {
	metadataStruct := storagecommon.DataLakeMetadata{}
	contribMetadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, contribMetadata.BinaryStoreType)
	return
}

// Close is a no-op; the Azure SDK manages connection lifecycle internally.
func (a *AzureDataLakeStorage) Close() error {
	return nil
}
