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
	"net"
	"net/url"
	"reflect"
	"strings"

	"github.com/Azure/azure-storage-blob-go/azblob"
	jsoniter "github.com/json-iterator/go"

	azauth "github.com/dapr/components-contrib/internal/authentication/azure"
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
)

// StateStore Type.
type StateStore struct {
	state.DefaultBulkStore
	containerURL azblob.ContainerURL
	json         jsoniter.API

	features []state.Feature
	logger   logger.Logger
}

type blobStorageMetadata struct {
	AccountName   string
	ContainerName string
}

// Init the connection to blob storage, optionally creates a blob container if it doesn't exist.
func (r *StateStore) Init(metadata state.Metadata) error {
	meta, err := getBlobStorageMetadata(metadata.Properties)
	if err != nil {
		return err
	}

	credential, env, err := azauth.GetAzureStorageBlobCredentials(r.logger, meta.AccountName, metadata.Properties)
	if err != nil {
		return fmt.Errorf("invalid credentials with error: %s", err.Error())
	}

	userAgent := "dapr-" + logger.DaprVersion
	options := azblob.PipelineOptions{
		Telemetry: azblob.TelemetryOptions{Value: userAgent},
	}
	p := azblob.NewPipeline(credential, options)

	var URL *url.URL
	customEndpoint, ok := mdutils.GetMetadataProperty(metadata.Properties, azauth.StorageEndpointKeys...)
	if ok && customEndpoint != "" {
		URL, err = url.Parse(fmt.Sprintf("%s/%s/%s", customEndpoint, meta.AccountName, meta.ContainerName))
	} else {
		URL, err = url.Parse(fmt.Sprintf("https://%s.blob.%s/%s", meta.AccountName, env.StorageEndpointSuffix, meta.ContainerName))
	}
	if err != nil {
		return err
	}
	containerURL := azblob.NewContainerURL(*URL, p)

	_, err = net.LookupHost(URL.Hostname())
	if err != nil {
		return err
	}

	ctx := context.Background()
	_, err = containerURL.Create(ctx, azblob.Metadata{}, azblob.PublicAccessNone)
	r.logger.Debugf("error creating container: %s", err)

	r.containerURL = containerURL
	r.logger.Debugf("using container '%s'", meta.ContainerName)

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
	accessConditions := azblob.BlobAccessConditions{}

	if _, err := r.containerURL.GetProperties(context.Background(), accessConditions.LeaseAccessConditions); err != nil {
		return fmt.Errorf("blob storage: error connecting to Blob storage at %s: %s", r.containerURL.URL().Host, err)
	}

	return nil
}

func (r *StateStore) GetComponentMetadata() map[string]string {
	metadataStruct := blobStorageMetadata{}
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

func getBlobStorageMetadata(meta map[string]string) (*blobStorageMetadata, error) {
	m := blobStorageMetadata{}
	err := mdutils.DecodeMetadata(meta, &m)

	if val, ok := mdutils.GetMetadataProperty(meta, azauth.StorageAccountNameKeys...); ok && val != "" {
		m.AccountName = val
	} else {
		return nil, fmt.Errorf("missing or empty %s field from metadata", azauth.StorageAccountNameKeys[0])
	}

	if val, ok := mdutils.GetMetadataProperty(meta, azauth.StorageContainerNameKeys...); ok && val != "" {
		m.ContainerName = val
	} else {
		return nil, fmt.Errorf("missing or empty %s field from metadata", azauth.StorageContainerNameKeys[0])
	}

	return &m, err
}

func (r *StateStore) readFile(ctx context.Context, req *state.GetRequest) (*state.GetResponse, error) {
	blobURL := r.containerURL.NewBlockBlobURL(getFileName(req.Key))

	resp, err := blobURL.Download(ctx, 0, azblob.CountToEnd, azblob.BlobAccessConditions{}, false)
	if err != nil {
		r.logger.Debugf("download file %s, err %s", req.Key, err)

		if isNotFoundError(err) {
			return &state.GetResponse{}, nil
		}

		return &state.GetResponse{}, err
	}

	bodyStream := resp.Body(azblob.RetryReaderOptions{})
	data, err := io.ReadAll(bodyStream)
	if err != nil {
		r.logger.Debugf("read file %s, err %s", req.Key, err)
		return &state.GetResponse{}, err
	}

	contentType := resp.ContentType()

	return &state.GetResponse{
		Data:        data,
		ETag:        ptr.Of(string(resp.ETag())),
		ContentType: &contentType,
	}, nil
}

func (r *StateStore) writeFile(ctx context.Context, req *state.SetRequest) error {
	accessConditions := azblob.BlobAccessConditions{}

	if req.ETag != nil && *req.ETag != "" {
		accessConditions.IfMatch = azblob.ETag(*req.ETag)
	}
	if req.Options.Concurrency == state.FirstWrite && (req.ETag == nil || *req.ETag == "") {
		accessConditions.IfNoneMatch = azblob.ETag("*")
	}

	blobURL := r.containerURL.NewBlockBlobURL(getFileName(req.Key))

	blobHTTPHeaders, err := r.createBlobHTTPHeadersFromRequest(req)
	if err != nil {
		return err
	}
	_, err = azblob.UploadBufferToBlockBlob(ctx, r.marshal(req), blobURL, azblob.UploadToBlockBlobOptions{
		Metadata:         req.Metadata,
		AccessConditions: accessConditions,
		BlobHTTPHeaders:  blobHTTPHeaders,
	})
	if err != nil {
		r.logger.Debugf("write file %s, err %s", req.Key, err)

		// Check if the error is due to ETag conflict
		if req.ETag != nil && isETagConflictError(err) {
			return state.NewETagError(state.ETagMismatch, err)
		}

		return err
	}

	return nil
}

func (r *StateStore) createBlobHTTPHeadersFromRequest(req *state.SetRequest) (azblob.BlobHTTPHeaders, error) {
	var blobHTTPHeaders azblob.BlobHTTPHeaders
	if val, ok := req.Metadata[contentType]; ok && val != "" {
		blobHTTPHeaders.ContentType = val
		delete(req.Metadata, contentType)
	}

	if req.ContentType != nil {
		if blobHTTPHeaders.ContentType != "" {
			r.logger.Warnf("ContentType received from request Metadata %s, as well as ContentType property %s, choosing value from contentType property", blobHTTPHeaders.ContentType, *req.ContentType)
		}
		blobHTTPHeaders.ContentType = *req.ContentType
	}

	if val, ok := req.Metadata[contentMD5]; ok && val != "" {
		sDec, err := b64.StdEncoding.DecodeString(val)
		if err != nil || len(sDec) != 16 {
			return azblob.BlobHTTPHeaders{}, fmt.Errorf("the MD5 value specified in Content MD5 is invalid, MD5 value must be 128 bits and base64 encoded")
		}
		blobHTTPHeaders.ContentMD5 = sDec
		delete(req.Metadata, contentMD5)
	}
	if val, ok := req.Metadata[contentEncoding]; ok && val != "" {
		blobHTTPHeaders.ContentEncoding = val
		delete(req.Metadata, contentEncoding)
	}
	if val, ok := req.Metadata[contentLanguage]; ok && val != "" {
		blobHTTPHeaders.ContentLanguage = val
		delete(req.Metadata, contentLanguage)
	}
	if val, ok := req.Metadata[contentDisposition]; ok && val != "" {
		blobHTTPHeaders.ContentDisposition = val
		delete(req.Metadata, contentDisposition)
	}
	if val, ok := req.Metadata[cacheControl]; ok && val != "" {
		blobHTTPHeaders.CacheControl = val
		delete(req.Metadata, cacheControl)
	}
	return blobHTTPHeaders, nil
}

func (r *StateStore) deleteFile(ctx context.Context, req *state.DeleteRequest) error {
	blobURL := r.containerURL.NewBlockBlobURL(getFileName(req.Key))
	accessConditions := azblob.BlobAccessConditions{}

	if req.ETag != nil && *req.ETag != "" {
		accessConditions.IfMatch = azblob.ETag(*req.ETag)
	}

	_, err := blobURL.Delete(ctx, azblob.DeleteSnapshotsOptionNone, accessConditions)
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
	azureError, ok := err.(azblob.StorageError)

	return ok && azureError.ServiceCode() == azblob.ServiceCodeBlobNotFound
}

func isETagConflictError(err error) bool {
	azureError, ok := err.(azblob.StorageError)

	return ok && azureError.ServiceCode() == azblob.ServiceCodeConditionNotMet
}
