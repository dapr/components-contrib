/*
Copyright 2026 The Dapr Authors
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
	"io"
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
	kitmd "github.com/dapr/kit/metadata"
)

const (
	// maxBlobBlocks is the maximum number of blocks in a block blob.
	maxBlobBlocks int64 = 50_000
	// maxBlockSize is the maximum size of a single block: 4000 MiB.
	maxBlockSize int64 = 4000 << 20
)

// effectiveMaxObjectSize returns the largest blob uploadable with the given
// block size: a block blob is limited to 50,000 blocks.
func effectiveMaxObjectSize(blockSize int64) int64 {
	return maxBlobBlocks * blockSize
}

// uploadTuningMetadata holds binary-store-specific upload options on top of
// the shared Azure Blob Storage metadata.
type uploadTuningMetadata struct {
	// PartSize is the upload block size, as a byte size quantity. Larger
	// blocks raise the maximum uploadable blob size (50,000 blocks per blob)
	// at the cost of more memory per in-flight upload.
	PartSize kitmd.ByteSize `json:"partSize" mapstructure:"partSize"`
}

// AzureBlobStorage implements binarystore.BinaryStore using Azure Blob Storage.
type AzureBlobStorage struct {
	metadata      *storagecommon.BlobStorageMetadata
	client        blobStoreClient
	logger        logger.Logger
	maxObjectSize int64
}

// blobStoreClient isolates the SDK calls the component makes, mirroring the
// client seams used by the other binary store providers so that behaviour can
// be unit tested without a live storage account.
type blobStoreClient interface {
	putObject(ctx context.Context, name string, data io.Reader, overwrite bool) error
	getObject(ctx context.Context, name string) (io.ReadCloser, error)
	deleteObject(ctx context.Context, name string) error
	close() error
}

type azureBlobClient struct {
	containerClient *container.Client
	partSize        int64
}

// NewAzureBlobStorage returns a new AzureBlobStorage binary store.
func NewAzureBlobStorage(log logger.Logger) binarystore.BinaryStore {
	return &AzureBlobStorage{logger: log}
}

// Init initialises the Azure Blob Storage client from the component metadata.
func (a *AzureBlobStorage) Init(ctx context.Context, md binarystore.Metadata) error {
	containerClient, m, err := storagecommon.CreateContainerStorageClient(ctx, a.logger, md.Properties)
	if err != nil {
		return err
	}

	tuning := uploadTuningMetadata{PartSize: kitmd.NewByteSize(binarystore.DefaultUploadPartSize)}
	if err = kitmd.DecodeMetadata(md.Properties, &tuning); err != nil {
		return fmt.Errorf("failed to decode metadata: %w", err)
	}
	partSize, err := tuning.PartSize.GetBytes()
	if err != nil {
		return fmt.Errorf("invalid value for partSize: %w", err)
	}
	if partSize <= 0 || partSize > maxBlockSize {
		return fmt.Errorf("partSize must be between 1 and %d bytes", maxBlockSize)
	}

	a.metadata = m
	a.maxObjectSize = effectiveMaxObjectSize(partSize)
	a.client = &azureBlobClient{containerClient: containerClient, partSize: partSize}
	return nil
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

	name, err := binarystore.ObjectPath(a.metadata.Prefix, req.FileName)
	if err != nil {
		return err
	}
	if err = a.client.putObject(ctx, name, binarystore.NewSizeLimitReader(req.Data, a.maxObjectSize), req.Overwrite); err != nil {
		// Only a create-only write can fail because the blob already exists;
		// when overwriting, a condition failure signals an unrelated
		// condition that must be surfaced to the caller.
		if !req.Overwrite && isPreconditionFailed(err) {
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

	name, err := binarystore.ObjectPath(a.metadata.Prefix, req.FileName)
	if err != nil {
		return nil, err
	}
	body, err := a.client.getObject(ctx, name)
	if err != nil {
		if isNotFound(err) {
			return nil, binarystore.ErrFileNotFound
		}
		return nil, fmt.Errorf("error downloading blob %q: %w", req.FileName, err)
	}

	return &binarystore.GetResponse{Data: body}, nil
}

// Delete removes a blob from Azure Blob Storage. If the blob does not exist,
// binarystore.ErrFileNotFound is returned.
func (a *AzureBlobStorage) Delete(ctx context.Context, req *binarystore.DeleteRequest) error {
	if req.FileName == "" {
		return binarystore.ErrMissingFileName
	}

	name, err := binarystore.ObjectPath(a.metadata.Prefix, req.FileName)
	if err != nil {
		return err
	}
	if err = a.client.deleteObject(ctx, name); err != nil {
		if isNotFound(err) {
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
	_ = contribMetadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, contribMetadata.BinaryStoreType)
	_ = contribMetadata.GetMetadataInfoFromStructType(reflect.TypeOf(uploadTuningMetadata{}), &metadataInfo, contribMetadata.BinaryStoreType)
	return
}

// Close closes the underlying Azure Blob Storage client.
func (a *AzureBlobStorage) Close() error {
	if a.client == nil {
		return nil
	}
	return a.client.close()
}

func (c *azureBlobClient) putObject(ctx context.Context, name string, data io.Reader, overwrite bool) error {
	opts := &blockblob.UploadStreamOptions{
		// Block size and concurrency are set explicitly so buffering matches
		// the other binary store providers rather than the SDK defaults of 1
		// MiB blocks staged one at a time, which also cap a block blob at
		// ~48.8 GiB given the 50,000 block limit.
		BlockSize:   c.partSize,
		Concurrency: binarystore.DefaultUploadConcurrency,
	}
	if !overwrite {
		// If-None-Match: * instructs the service to reject the write if any
		// version of the blob already exists (HTTP 412 / ConditionNotMet).
		etagAny := azcore.ETagAny
		opts.AccessConditions = &blob.AccessConditions{
			ModifiedAccessConditions: &blob.ModifiedAccessConditions{
				IfNoneMatch: &etagAny,
			},
		}
	}

	_, err := c.containerClient.NewBlockBlobClient(name).UploadStream(ctx, data, opts)
	return err
}

func (c *azureBlobClient) getObject(ctx context.Context, name string) (io.ReadCloser, error) {
	resp, err := c.containerClient.NewBlockBlobClient(name).DownloadStream(ctx, nil)
	if err != nil {
		return nil, err
	}
	return resp.Body, nil
}

func (c *azureBlobClient) deleteObject(ctx context.Context, name string) error {
	_, err := c.containerClient.NewBlockBlobClient(name).Delete(ctx, nil)
	return err
}

// close is a no-op; the Azure SDK manages connection lifecycle internally.
func (c *azureBlobClient) close() error {
	return nil
}

// isNotFound reports whether err indicates the requested blob does not exist.
func isNotFound(err error) bool {
	return bloberror.HasCode(err, bloberror.BlobNotFound)
}

// isPreconditionFailed reports whether err indicates that a create-only write
// (If-None-Match: *) was rejected because the blob already exists.
func isPreconditionFailed(err error) bool {
	return bloberror.HasCode(err, bloberror.BlobAlreadyExists, bloberror.ConditionNotMet)
}
