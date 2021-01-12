// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

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
	"bytes"
	"context"
	"fmt"
	"net/url"
	"strings"

	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/dapr/components-contrib/state"
	"github.com/dapr/dapr/pkg/logger"
	jsoniter "github.com/json-iterator/go"
)

const (
	keyDelimiter     = "||"
	accountNameKey   = "accountName"
	accountKeyKey    = "accountKey"
	containerNameKey = "containerName"
)

// StateStore Type
type StateStore struct {
	state.DefaultBulkStore
	containerURL azblob.ContainerURL
	json         jsoniter.API

	logger logger.Logger
}

type blobStorageMetadata struct {
	accountName   string
	accountKey    string
	containerName string
}

// Init the connection to blob storage, optionally creates a blob container if it doesn't exist.
func (r *StateStore) Init(metadata state.Metadata) error {
	meta, err := getBlobStorageMetadata(metadata.Properties)
	if err != nil {
		return err
	}

	credential, err := azblob.NewSharedKeyCredential(meta.accountName, meta.accountKey)
	if err != nil {
		return fmt.Errorf("invalid credentials with error: %s", err.Error())
	}

	p := azblob.NewPipeline(credential, azblob.PipelineOptions{})

	URL, _ := url.Parse(fmt.Sprintf("https://%s.blob.core.windows.net/%s", meta.accountName, meta.containerName))
	containerURL := azblob.NewContainerURL(*URL, p)

	ctx := context.Background()
	_, err = containerURL.Create(ctx, azblob.Metadata{}, azblob.PublicAccessNone)
	r.logger.Debugf("error creating container: %s", err)

	r.containerURL = containerURL
	r.logger.Debugf("using container '%s'", meta.containerName)

	return nil
}

// Delete the state
func (r *StateStore) Delete(req *state.DeleteRequest) error {
	r.logger.Debugf("delete %s", req.Key)

	return r.deleteFile(req)
}

// Get the state
func (r *StateStore) Get(req *state.GetRequest) (*state.GetResponse, error) {
	r.logger.Debugf("fetching %s", req.Key)
	data, etag, err := r.readFile(req)
	if err != nil {
		r.logger.Debugf("error %s", err)

		if isNotFoundError(err) {
			return &state.GetResponse{}, nil
		}

		return &state.GetResponse{}, err
	}

	return &state.GetResponse{
		Data: data,
		ETag: etag,
	}, err
}

// Set the state
func (r *StateStore) Set(req *state.SetRequest) error {
	r.logger.Debugf("saving %s", req.Key)

	return r.writeFile(req)
}

// NewAzureBlobStorageStore instance
func NewAzureBlobStorageStore(logger logger.Logger) *StateStore {
	s := &StateStore{
		json:   jsoniter.ConfigFastest,
		logger: logger,
	}
	s.DefaultBulkStore = state.NewDefaultBulkStore(s)

	return s
}

func getBlobStorageMetadata(metadata map[string]string) (*blobStorageMetadata, error) {
	meta := blobStorageMetadata{}

	if val, ok := metadata[accountNameKey]; ok && val != "" {
		meta.accountName = val
	} else {
		return nil, fmt.Errorf("missing or empty %s field from metadata", accountNameKey)
	}

	if val, ok := metadata[accountKeyKey]; ok && val != "" {
		meta.accountKey = val
	} else {
		return nil, fmt.Errorf("missing of empty %s field from metadata", accountKeyKey)
	}

	if val, ok := metadata[containerNameKey]; ok && val != "" {
		meta.containerName = val
	} else {
		return nil, fmt.Errorf("missing of empty %s field from metadata", containerNameKey)
	}

	return &meta, nil
}

func (r *StateStore) readFile(req *state.GetRequest) ([]byte, string, error) {
	blobURL := r.containerURL.NewBlockBlobURL(getFileName(req.Key))

	resp, err := blobURL.Download(context.Background(), 0, azblob.CountToEnd, azblob.BlobAccessConditions{}, false)
	if err != nil {
		r.logger.Debugf("download file %s, err %s", req.Key, err)

		return nil, "", err
	}

	bodyStream := resp.Body(azblob.RetryReaderOptions{})
	data := bytes.Buffer{}
	_, err = data.ReadFrom(bodyStream)

	if err != nil {
		r.logger.Debugf("read file %s, err %s", req.Key, err)

		return nil, "", err
	}

	return data.Bytes(), string(resp.ETag()), nil
}

func (r *StateStore) writeFile(req *state.SetRequest) error {
	blobURL := r.containerURL.NewBlockBlobURL(getFileName(req.Key))

	accessConditions := azblob.BlobAccessConditions{}

	if req.Options.Concurrency == state.LastWrite {
		accessConditions.IfMatch = azblob.ETag(req.ETag)
	}

	_, err := azblob.UploadBufferToBlockBlob(context.Background(), r.marshal(req), blobURL, azblob.UploadToBlockBlobOptions{
		Parallelism:      16,
		Metadata:         req.Metadata,
		AccessConditions: accessConditions,
	})
	if err != nil {
		r.logger.Debugf("write file %s, err %s", req.Key, err)

		if req.ETag != "" {
			return state.NewETagError(state.ETagMismatch, err)
		}

		return err
	}

	return nil
}

func (r *StateStore) deleteFile(req *state.DeleteRequest) error {
	blobURL := r.containerURL.NewBlockBlobURL(getFileName((req.Key)))
	accessConditions := azblob.BlobAccessConditions{}

	if req.Options.Concurrency == state.LastWrite {
		accessConditions.IfMatch = azblob.ETag(req.ETag)
	}

	_, err := blobURL.Delete(context.Background(), azblob.DeleteSnapshotsOptionNone, accessConditions)
	if err != nil {
		r.logger.Debugf("delete file %s, err %s", req.Key, err)

		if req.ETag != "" {
			return state.NewETagError(state.ETagMismatch, err)
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
