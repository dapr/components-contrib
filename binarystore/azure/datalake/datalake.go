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
	"io"
	"reflect"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azdatalake/datalakeerror"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azdatalake/file"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azdatalake/filesystem"
	"github.com/google/uuid"

	"github.com/dapr/components-contrib/binarystore"
	storagecommon "github.com/dapr/components-contrib/common/component/azure/datalake"
	contribMetadata "github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
)

// cleanupTimeout bounds the best-effort removal of a temporary file after a
// failed upload, which must not inherit an already-cancelled request context.
const cleanupTimeout = 30 * time.Second

// AzureDataLakeStorage implements binarystore.BinaryStore using Azure Data
// Lake Storage Gen2.
type AzureDataLakeStorage struct {
	metadata *storagecommon.DataLakeMetadata
	client   datalakeStoreClient
	logger   logger.Logger
}

// datalakeStoreClient isolates the SDK calls the component makes, mirroring
// the client seams used by the other binary store providers so that behaviour
// can be unit tested without a live storage account.
type datalakeStoreClient interface {
	putObject(ctx context.Context, name string, data io.Reader, overwrite bool) error
	getObject(ctx context.Context, name string) (io.ReadCloser, error)
	deleteObject(ctx context.Context, name string) error
	close() error
}

type azureDataLakeClient struct {
	fileSystemClient *filesystem.Client
	logger           logger.Logger
}

// NewAzureDataLakeStorage returns a new AzureDataLakeStorage binary store.
func NewAzureDataLakeStorage(log logger.Logger) binarystore.BinaryStore {
	return &AzureDataLakeStorage{logger: log}
}

// Init initialises the Azure Data Lake Storage client from the component metadata.
func (a *AzureDataLakeStorage) Init(ctx context.Context, md binarystore.Metadata) error {
	fileSystemClient, m, err := storagecommon.CreateFileSystemStorageClient(ctx, a.logger, md.Properties)
	if err != nil {
		return err
	}

	a.metadata = m
	a.client = &azureDataLakeClient{
		fileSystemClient: fileSystemClient,
		logger:           a.logger,
	}
	return nil
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

	if err := a.client.putObject(ctx, binarystore.ObjectPath(a.metadata.Prefix, req.FileName), req.Data, req.Overwrite); err != nil {
		// Only a create-only write can fail because the path already exists;
		// when overwriting, a condition failure signals an unrelated
		// condition that must be surfaced to the caller.
		if !req.Overwrite && isPreconditionFailed(err) {
			return binarystore.ErrFileAlreadyExists
		}
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

	body, err := a.client.getObject(ctx, binarystore.ObjectPath(a.metadata.Prefix, req.FileName))
	if err != nil {
		if isNotFound(err) {
			return nil, binarystore.ErrFileNotFound
		}
		return nil, fmt.Errorf("error downloading file %q: %w", req.FileName, err)
	}

	return &binarystore.GetResponse{Data: body}, nil
}

// Delete removes a path from Azure Data Lake Storage. If the path does not
// exist, binarystore.ErrFileNotFound is returned.
func (a *AzureDataLakeStorage) Delete(ctx context.Context, req *binarystore.DeleteRequest) error {
	if req.FileName == "" {
		return binarystore.ErrMissingFileName
	}

	if err := a.client.deleteObject(ctx, binarystore.ObjectPath(a.metadata.Prefix, req.FileName)); err != nil {
		if isNotFound(err) {
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
	_ = contribMetadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, contribMetadata.BinaryStoreType)
	return
}

// Close closes the underlying Azure Data Lake Storage client.
func (a *AzureDataLakeStorage) Close() error {
	if a.client == nil {
		return nil
	}
	return a.client.close()
}

// putObject uploads to a temporary path and renames it onto the target path,
// so that a failed upload never leaves a partial file behind and the
// create-only condition is evaluated atomically by the service.
//
// Parent directories are not created up front: on a hierarchical namespace
// the create normally succeeds for nested paths, and pre-creating one
// directory per path segment would cost a metadata round trip per segment on
// every write. Instead, the rare PathNotFound is handled lazily by creating
// the parent directory once and retrying.
func (c *azureDataLakeClient) putObject(ctx context.Context, name string, data io.Reader, overwrite bool) error {
	tempPath := fmt.Sprintf("%s.tmp-%s", name, uuid.NewString())
	tempClient := c.fileSystemClient.NewFileClient(tempPath)

	if err := c.createFile(ctx, tempClient, tempPath); err != nil {
		return err
	}

	uploadOpts := &file.UploadStreamOptions{
		// Chunk size and concurrency are set explicitly so buffering matches
		// the other binary store providers rather than the SDK defaults.
		ChunkSize:   binarystore.DefaultUploadPartSize,
		Concurrency: binarystore.DefaultUploadConcurrency,
	}
	if err := tempClient.UploadStream(ctx, data, uploadOpts); err != nil {
		c.cleanupFile(ctx, tempClient, tempPath)
		return err
	}

	renameOpts := &file.RenameOptions{}
	if !overwrite {
		// If-None-Match: * instructs the service to reject the rename if any
		// version of the destination path already exists (HTTP 409 PathAlreadyExists).
		etagAny := azcore.ETagAny
		renameOpts.AccessConditions = &file.AccessConditions{
			ModifiedAccessConditions: &file.ModifiedAccessConditions{
				IfNoneMatch: &etagAny,
			},
		}
	}

	if _, err := tempClient.Rename(ctx, name, renameOpts); err != nil {
		c.cleanupFile(ctx, tempClient, tempPath)
		return err
	}

	return nil
}

// createFile creates path, creating the parent directory hierarchy and
// retrying once if the service reports that the parent does not exist.
// Creating a directory with a nested path creates the intermediate
// directories in a single request, so at most one extra round trip is spent,
// and only the first time a given directory is written to.
func (c *azureDataLakeClient) createFile(ctx context.Context, client *file.Client, path string) error {
	return createWithParent(ctx, path,
		func(ctx context.Context) error {
			_, err := client.Create(ctx, nil)
			return err
		},
		func(ctx context.Context, dir string) error {
			_, err := c.fileSystemClient.NewDirectoryClient(dir).Create(ctx, nil)
			return err
		},
	)
}

// createWithParent runs create, and if the service reports that the parent
// path does not exist, creates the parent directory once and retries.
func createWithParent(ctx context.Context, path string, create func(context.Context) error, createDir func(context.Context, string) error) error {
	err := create(ctx)
	if err == nil || !datalakeerror.HasCode(err, datalakeerror.PathNotFound) {
		return err
	}

	idx := strings.LastIndex(path, "/")
	if idx <= 0 {
		return err
	}
	parent := path[:idx]

	if dirErr := createDir(ctx, parent); dirErr != nil && !datalakeerror.HasCode(dirErr, datalakeerror.PathAlreadyExists) {
		return fmt.Errorf("error creating parent directory %q: %w", parent, dirErr)
	}

	return create(ctx)
}

func (c *azureDataLakeClient) getObject(ctx context.Context, name string) (io.ReadCloser, error) {
	resp, err := c.fileSystemClient.NewFileClient(name).DownloadStream(ctx, nil)
	if err != nil {
		return nil, err
	}
	return resp.Body, nil
}

func (c *azureDataLakeClient) deleteObject(ctx context.Context, name string) error {
	_, err := c.fileSystemClient.NewFileClient(name).Delete(ctx, nil)
	return err
}

// close is a no-op; the Azure SDK manages connection lifecycle internally.
func (c *azureDataLakeClient) close() error {
	return nil
}

// cleanupFile removes a temporary file on a best-effort basis, logging rather
// than swallowing failures so that leaked temporary files are diagnosable.
func (c *azureDataLakeClient) cleanupFile(ctx context.Context, client *file.Client, path string) {
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), cleanupTimeout)
	defer cancel()

	if _, err := client.Delete(cleanupCtx, nil); err != nil && !datalakeerror.HasCode(err, datalakeerror.PathNotFound) {
		c.logger.Warnf("failed to remove temporary file %q: %v", path, err)
	}
}

// isNotFound reports whether err indicates the requested path does not exist.
func isNotFound(err error) bool {
	return datalakeerror.HasCode(err, datalakeerror.PathNotFound)
}

// isPreconditionFailed reports whether err indicates that a create-only write
// (If-None-Match: *) was rejected because the path already exists.
func isPreconditionFailed(err error) bool {
	return datalakeerror.HasCode(err, datalakeerror.PathAlreadyExists, datalakeerror.ConditionNotMet)
}
