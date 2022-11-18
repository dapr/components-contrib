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
	b64 "encoding/base64"
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
	keyDelimiter       = "||"
	contentType        = "ContentType"
	contentMD5         = "ContentMD5"
	contentEncoding    = "ContentEncoding"
	contentLanguage    = "ContentLanguage"
	contentDisposition = "ContentDisposition"
	cacheControl       = "CacheControl"
	endpointKey        = "endpoint"
)

// StateStore Type.
type StateStore struct {
	state.DefaultBulkStore
	containerClient *container.Client
	json            jsoniter.API

	features []state.Feature
	logger   logger.Logger
}

// Init the connection to blob storage, optionally creates a blob container if it doesn't exist.
func (r *StateStore) Init(metadata state.Metadata) error {
	var err error
	r.containerClient, _, err = storageinternal.CreateContainerStorageClient(r.logger, metadata.Properties)
	if err != nil {
		return err
	}
	return nil
}

// Features returns the features available in this state store.
func (r *StateStore) Features() []state.Feature {
	return r.features
}

// Delete the state.
func (r *StateStore) Delete(req *state.DeleteRequest) error {
	r.logger.Debugf("delete %s", req.Key)
	return r.deleteFile(context.Background(), req)
}

// Get the state.
func (r *StateStore) Get(req *state.GetRequest) (*state.GetResponse, error) {
	r.logger.Debugf("get %s", req.Key)
	return r.readFile(context.Background(), req)
}

// Set the state.
func (r *StateStore) Set(req *state.SetRequest) error {
	r.logger.Debugf("saving %s", req.Key)
	return r.writeFile(context.Background(), req)
}

func (r *StateStore) Ping() error {
	getPropertiesOptions := container.GetPropertiesOptions{
		LeaseAccessConditions: &container.LeaseAccessConditions{},
	}
	if _, err := r.containerClient.GetProperties(context.Background(), &getPropertiesOptions); err != nil {
		return fmt.Errorf("blob storage: error connecting to Blob storage at %s: %s", r.containerClient.URL(), err)
	}

	return nil
}

func (r *StateStore) GetComponentMetadata() map[string]string {
	metadataStruct := storageinternal.BlobStorageMetadata{}
	metadataInfo := map[string]string{}
	mdutils.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo)
	return metadataInfo
}

// NewAzureBlobStorageStore instance.
func NewAzureBlobStorageStore(logger logger.Logger) state.Store {
	s := &StateStore{
		json:     jsoniter.ConfigFastest,
		features: []state.Feature{state.FeatureETag},
		logger:   logger,
	}
	s.DefaultBulkStore = state.NewDefaultBulkStore(s)

	return s
}

func (r *StateStore) readFile(ctx context.Context, req *state.GetRequest) (*state.GetResponse, error) {
	blockBlobClient := r.containerClient.NewBlockBlobClient(getFileName(req.Key))

	downloadOptions := azblob.DownloadStreamOptions{
		AccessConditions: &blob.AccessConditions{},
	}

	blobDownloadResponse, err := blockBlobClient.DownloadStream(ctx, &downloadOptions)
	if err != nil {
		r.logger.Debugf("download file %s, err %s", req.Key, err)

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
	err = reader.Close()
	if err != nil {
		return &state.GetResponse{}, fmt.Errorf("error closing az blob reader: %w", err)
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

	if req.ETag != nil && *req.ETag != "" {
		modifiedAccessConditions.IfMatch = ptr.Of(azcore.ETag(*req.ETag))
	}
	if req.Options.Concurrency == state.FirstWrite && (req.ETag == nil || *req.ETag == "") {
		modifiedAccessConditions.IfNoneMatch = ptr.Of(azcore.ETagAny)
	}

	accessConditions := blob.AccessConditions{
		ModifiedAccessConditions: &modifiedAccessConditions,
	}

	blobHTTPHeaders, err := r.createBlobHTTPHeadersFromRequest(req)
	if err != nil {
		return err
	}

	uploadOptions := azblob.UploadBufferOptions{
		AccessConditions: &accessConditions,
		Metadata:         storageinternal.SanitizeMetadata(r.logger, req.Metadata),
		HTTPHeaders:      &blobHTTPHeaders,
		Concurrency:      16,
	}

	blockBlobClient := r.containerClient.NewBlockBlobClient(getFileName(req.Key))
	_, err = blockBlobClient.UploadBuffer(ctx, r.marshal(req), &uploadOptions)

	if err != nil {
		// Check if the error is due to ETag conflict
		if req.ETag != nil && isETagConflictError(err) {
			return state.NewETagError(state.ETagMismatch, err)
		}

		return fmt.Errorf("error uploading az blob: %w", err)
	}

	return nil
}

func (r *StateStore) createBlobHTTPHeadersFromRequest(req *state.SetRequest) (blob.HTTPHeaders, error) {
	blobHTTPHeaders := blob.HTTPHeaders{}
	if val, ok := req.Metadata[contentType]; ok && val != "" {
		blobHTTPHeaders.BlobContentType = &val
		delete(req.Metadata, contentType)
	}

	if req.ContentType != nil {
		if blobHTTPHeaders.BlobContentType != nil {
			r.logger.Warnf("ContentType received from request Metadata %s, as well as ContentType property %s, choosing value from contentType property", blobHTTPHeaders.BlobContentType, req.ContentType)
		}
		blobHTTPHeaders.BlobContentType = req.ContentType
	}

	if val, ok := req.Metadata[contentMD5]; ok && val != "" {
		sDec, err := b64.StdEncoding.DecodeString(val)
		if err != nil || len(sDec) != 16 {
			return blob.HTTPHeaders{}, fmt.Errorf("the MD5 value specified in Content MD5 is invalid, MD5 value must be 128 bits and base64 encoded")
		}
		blobHTTPHeaders.BlobContentMD5 = sDec
		delete(req.Metadata, contentMD5)
	}
	if val, ok := req.Metadata[contentEncoding]; ok && val != "" {
		blobHTTPHeaders.BlobContentEncoding = &val
		delete(req.Metadata, contentEncoding)
	}
	if val, ok := req.Metadata[contentLanguage]; ok && val != "" {
		blobHTTPHeaders.BlobContentLanguage = &val
		delete(req.Metadata, contentLanguage)
	}
	if val, ok := req.Metadata[contentDisposition]; ok && val != "" {
		blobHTTPHeaders.BlobContentDisposition = &val
		delete(req.Metadata, contentDisposition)
	}
	if val, ok := req.Metadata[cacheControl]; ok && val != "" {
		blobHTTPHeaders.BlobCacheControl = &val
		delete(req.Metadata, cacheControl)
	}
	return blobHTTPHeaders, nil
}

func (r *StateStore) deleteFile(ctx context.Context, req *state.DeleteRequest) error {
	blockBlobClient := r.containerClient.NewBlockBlobClient(getFileName(req.Key))

	modifiedAccessConditions := blob.ModifiedAccessConditions{}
	if req.ETag != nil && *req.ETag != "" {
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
		r.logger.Debugf("delete file %s, err %s", req.Key, err)

		if req.ETag != nil && isETagConflictError(err) {
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
