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

/*
Azure Blob Storage state store.

Sample configuration in yaml:

	apiVersion: dapr.io/v1alpha1
	kind: Component
	metadata:
	  name: statestore
	spec:
	  type: state.azure.blobstorage
	  metadata:
	  - name: accountName
		value: <storage account name>
	  - name: accountKey
		value: <key>
	  - name: containerName
		value: <container Name>

Concurrency is supported with ETags according to https://docs.microsoft.com/en-us/azure/storage/common/storage-concurrency#managing-concurrency-in-blob-storage
*/

package blobstorage

import (
	"context"
	"fmt"
	"io"
	"reflect"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	jsoniter "github.com/json-iterator/go"

	storageinternal "github.com/dapr/components-contrib/internal/component/azure/blobstorage"
	mdutils "github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/state"
	"github.com/dapr/kit/logger"
	"github.com/dapr/kit/ptr"
)

const (
	keyDelimiter = "||"
)

// StateStore Type.
type StateStore struct {
	state.BulkStore

	containerClient *container.Client
	logger          logger.Logger
}

// Init the connection to blob storage, optionally creates a blob container if it doesn't exist.
func (r *StateStore) Init(ctx context.Context, metadata state.Metadata) error {
	var err error
	r.containerClient, _, err = storageinternal.CreateContainerStorageClient(ctx, r.logger, metadata.Properties)
	if err != nil {
		return err
	}
	return nil
}

// Features returns the features available in this state store.
func (r *StateStore) Features() []state.Feature {
	return []state.Feature{state.FeatureETag}
}

// Delete the state.
func (r *StateStore) Delete(ctx context.Context, req *state.DeleteRequest) error {
	return r.deleteFile(ctx, req)
}

// Get the state.
func (r *StateStore) Get(ctx context.Context, req *state.GetRequest) (*state.GetResponse, error) {
	return r.readFile(ctx, req)
}

// Set the state.
func (r *StateStore) Set(ctx context.Context, req *state.SetRequest) error {
	return r.writeFile(ctx, req)
}

func (r *StateStore) Ping(ctx context.Context) error {
	if _, err := r.containerClient.GetProperties(ctx, nil); err != nil {
		return fmt.Errorf("blob storage: error connecting to Blob storage at %s: %s", r.containerClient.URL(), err)
	}

	return nil
}

func (r *StateStore) GetComponentMetadata() (metadataInfo mdutils.MetadataMap) {
	metadataStruct := storageinternal.BlobStorageMetadata{}
	mdutils.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, mdutils.StateStoreType)
	return
}

// NewAzureBlobStorageStore instance.
func NewAzureBlobStorageStore(logger logger.Logger) state.Store {
	s := &StateStore{
		logger: logger,
	}
	s.BulkStore = state.NewDefaultBulkStore(s)
	return s
}

func (r *StateStore) readFile(ctx context.Context, req *state.GetRequest) (*state.GetResponse, error) {
	blockBlobClient := r.containerClient.NewBlockBlobClient(getFileName(req.Key))
	blobDownloadResponse, err := blockBlobClient.DownloadStream(ctx, nil)
	if err != nil {
		if isNotFoundError(err) {
			return &state.GetResponse{}, nil
		}

		return &state.GetResponse{}, err
	}

	reader := blobDownloadResponse.Body
	defer reader.Close()
	blobData, err := io.ReadAll(reader)
	if err != nil {
		return &state.GetResponse{}, fmt.Errorf("error reading az blob: %w", err)
	}

	contentType := blobDownloadResponse.ContentType

	return &state.GetResponse{
		Data:        blobData,
		ETag:        ptr.Of(string(*blobDownloadResponse.ETag)),
		ContentType: contentType,
	}, nil
}

func (r *StateStore) writeFile(ctx context.Context, req *state.SetRequest) error {
	modifiedAccessConditions := blob.ModifiedAccessConditions{}

	if req.HasETag() {
		modifiedAccessConditions.IfMatch = ptr.Of(azcore.ETag(*req.ETag))
	}
	if req.Options.Concurrency == state.FirstWrite && !req.HasETag() {
		modifiedAccessConditions.IfNoneMatch = ptr.Of(azcore.ETagAny)
	}

	accessConditions := blob.AccessConditions{
		ModifiedAccessConditions: &modifiedAccessConditions,
	}

	blobHTTPHeaders, err := storageinternal.CreateBlobHTTPHeadersFromRequest(req.Metadata, req.ContentType, r.logger)
	if err != nil {
		return err
	}

	uploadOptions := azblob.UploadBufferOptions{
		AccessConditions: &accessConditions,
		Metadata:         storageinternal.SanitizeMetadata(r.logger, req.Metadata),
		HTTPHeaders:      &blobHTTPHeaders,
	}

	blockBlobClient := r.containerClient.NewBlockBlobClient(getFileName(req.Key))
	_, err = blockBlobClient.UploadBuffer(ctx, r.marshal(req), &uploadOptions)

	if err != nil {
		// Check if the error is due to ETag conflict
		if req.HasETag() && isETagConflictError(err) {
			return state.NewETagError(state.ETagMismatch, err)
		}

		return fmt.Errorf("error uploading az blob: %w", err)
	}

	return nil
}

func (r *StateStore) deleteFile(ctx context.Context, req *state.DeleteRequest) error {
	blockBlobClient := r.containerClient.NewBlockBlobClient(getFileName(req.Key))

	modifiedAccessConditions := blob.ModifiedAccessConditions{}
	if req.HasETag() {
		modifiedAccessConditions.IfMatch = ptr.Of(azcore.ETag(*req.ETag))
	}

	deleteOptions := blob.DeleteOptions{
		DeleteSnapshots: nil,
		AccessConditions: &blob.AccessConditions{
			ModifiedAccessConditions: &modifiedAccessConditions,
		},
	}

	_, err := blockBlobClient.Delete(ctx, &deleteOptions)
	if err != nil {
		if req.HasETag() && isETagConflictError(err) {
			return state.NewETagError(state.ETagMismatch, err)
		} else if isNotFoundError(err) {
			// deleting an item that doesn't exist without specifying an ETAG is a noop
			return nil
		}

		return err
	}

	return nil
}

func getFileName(key string) string {
	pr := strings.Split(key, keyDelimiter)
	if len(pr) != 2 {
		return pr[0]
	}

	return pr[1]
}

func (r *StateStore) marshal(req *state.SetRequest) []byte {
	var v string
	b, ok := req.Value.([]byte)
	if ok {
		v = string(b)
	} else {
		v, _ = jsoniter.MarshalToString(req.Value)
	}

	return []byte(v)
}

func isNotFoundError(err error) bool {
	return bloberror.HasCode(err, bloberror.BlobNotFound)
}

func isETagConflictError(err error) bool {
	return bloberror.HasCode(err, bloberror.ConditionNotMet)
}
