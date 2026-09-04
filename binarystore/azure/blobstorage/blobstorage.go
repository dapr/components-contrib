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

// Package blobstorage provides a BinaryStore implementation backed by
// Azure Blob Storage. Each named file maps 1:1 to a block blob inside the
// configured container.
package blobstorage

import (
	"context"
	"fmt"
	"reflect"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"

	"github.com/dapr/components-contrib/binarystore"
	storagecommon "github.com/dapr/components-contrib/common/component/azure/blobstorage"
	contribMetadata "github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
)

// AzureBlobStorage implements binarystore.BinaryStore using Azure Blob Storage.
type AzureBlobStorage struct {
	metadata        *storagecommon.BlobStorageMetadata
	containerClient *container.Client
	logger          logger.Logger
}

// NewAzureBlobStorage returns a new AzureBlobStorage binary store.
func NewAzureBlobStorage(log logger.Logger) binarystore.BinaryStore {
	return &AzureBlobStorage{logger: log}
}

// Init initialises the Azure Blob Storage client from the component metadata.
func (a *AzureBlobStorage) Init(ctx context.Context, md binarystore.Metadata) error {
	var err error
	a.containerClient, a.metadata, err = storagecommon.CreateContainerStorageClient(ctx, a.logger, md.Properties)
	return err
}

// Features returns the optional features supported by this component.
func (a *AzureBlobStorage) Features() []binarystore.Feature {
	return []binarystore.Feature{}
}

// Set uploads binary data to Azure Blob Storage.
//
// When req.Overwrite is false an If-None-Match: * access condition is applied
// so that the server rejects the upload atomically if the blob already exists,
// returning binarystore.ErrFileAlreadyExists. When req.Overwrite is true the
// blob is created or replaced without a condition check.
func (a *AzureBlobStorage) Set(ctx context.Context, req *binarystore.SetRequest) error {
	if req.FileName == "" {
		return binarystore.ErrMissingFileName
	}

	opts := &blockblob.UploadStreamOptions{}
	if !req.Overwrite {
		// If-None-Match: * instructs the service to reject the write if any
		// version of the blob already exists (HTTP 412 / ConditionNotMet).
		etagAny := azcore.ETagAny
		opts.AccessConditions = &blob.AccessConditions{
			ModifiedAccessConditions: &blob.ModifiedAccessConditions{
				IfNoneMatch: &etagAny,
			},
		}
	}

	blockBlobClient := a.containerClient.NewBlockBlobClient(binarystore.ObjectPath(a.metadata.Prefix, req.FileName))
	_, err := blockBlobClient.UploadStream(ctx, req.Data, opts)
	if err != nil {
		if bloberror.HasCode(err, bloberror.BlobAlreadyExists) ||
			bloberror.HasCode(err, bloberror.ConditionNotMet) {
			return binarystore.ErrFileAlreadyExists
		}
		return fmt.Errorf("error uploading blob %q: %w", req.FileName, err)
	}

	return nil
}

// Get downloads binary data from Azure Blob Storage as a streaming reader.
//
// The Data field of the returned GetResponse wraps the HTTP response body and
// must be closed by the caller when reading is complete.
func (a *AzureBlobStorage) Get(ctx context.Context, req *binarystore.GetRequest) (*binarystore.GetResponse, error) {
	if req.FileName == "" {
		return nil, binarystore.ErrMissingFileName
	}

	blockBlobClient := a.containerClient.NewBlockBlobClient(binarystore.ObjectPath(a.metadata.Prefix, req.FileName))
	resp, err := blockBlobClient.DownloadStream(ctx, nil)
	if err != nil {
		if bloberror.HasCode(err, bloberror.BlobNotFound) {
			return nil, binarystore.ErrFileNotFound
		}
		return nil, fmt.Errorf("error downloading blob %q: %w", req.FileName, err)
	}

	return &binarystore.GetResponse{
		Data: resp.Body,
	}, nil
}

// Delete removes a blob from Azure Blob Storage. If the blob does not exist,
// binarystore.ErrFileNotFound is returned.
func (a *AzureBlobStorage) Delete(ctx context.Context, req *binarystore.DeleteRequest) error {
	if req.FileName == "" {
		return binarystore.ErrMissingFileName
	}

	blockBlobClient := a.containerClient.NewBlockBlobClient(binarystore.ObjectPath(a.metadata.Prefix, req.FileName))
	_, err := blockBlobClient.Delete(ctx, nil)
	if err != nil {
		if bloberror.HasCode(err, bloberror.BlobNotFound) {
			return binarystore.ErrFileNotFound
		}
		return fmt.Errorf("error deleting blob %q: %w", req.FileName, err)
	}

	return nil
}

// GetComponentMetadata returns the metadata schema for this component, used by
// the Dapr metadata linter.
func (a *AzureBlobStorage) GetComponentMetadata() (metadataInfo contribMetadata.MetadataMap) {
	metadataStruct := storagecommon.BlobStorageMetadata{}
	contribMetadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, contribMetadata.BinaryStoreType)
	return
}

// Close is a no-op; the Azure SDK manages connection lifecycle internally.
func (a *AzureBlobStorage) Close() error {
	return nil
}
