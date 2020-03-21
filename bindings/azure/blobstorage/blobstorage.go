// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package blobstorage

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	b64 "encoding/base64"

	"github.com/dapr/dapr/pkg/logger"
	"github.com/google/uuid"

	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/dapr/components-contrib/bindings"
)

const (
	blobName           = "blobName"
	contentType        = "ContentType"
	contentMD5         = "ContentMD5"
	contentEncoding    = "ContentEncoding"
	contentLanguage    = "ContentLanguage"
	contentDisposition = "ContentDisposition"
	cacheControl       = "CacheControl"
)

// AzureBlobStorage allows saving blobs to an Azure Blob Storage account
type AzureBlobStorage struct {
	metadata     *blobStorageMetadata
	containerURL azblob.ContainerURL

	logger logger.Logger
}

type blobStorageMetadata struct {
	StorageAccount   string `json:"storageAccount"`
	StorageAccessKey string `json:"storageAccessKey"`
	Container        string `json:"container"`
}

// NewAzureBlobStorage returns a new Azure Blob Storage instance
func NewAzureBlobStorage(logger logger.Logger) *AzureBlobStorage {
	return &AzureBlobStorage{logger: logger}
}

// Init performs metadata parsing
func (a *AzureBlobStorage) Init(metadata bindings.Metadata) error {
	m, err := a.parseMetadata(metadata)
	if err != nil {
		return err
	}
	a.metadata = m
	credential, err := azblob.NewSharedKeyCredential(m.StorageAccount, m.StorageAccessKey)
	if err != nil {
		return fmt.Errorf("invalid credentials with error: %s", err.Error())
	}
	p := azblob.NewPipeline(credential, azblob.PipelineOptions{})

	containerName := a.metadata.Container
	URL, _ := url.Parse(
		fmt.Sprintf("https://%s.blob.core.windows.net/%s", m.StorageAccount, containerName))
	containerURL := azblob.NewContainerURL(*URL, p)

	ctx := context.Background()
	_, err = containerURL.Create(ctx, azblob.Metadata{}, azblob.PublicAccessNone)
	// Don't return error, container might already exist
	a.logger.Debugf("error creating container: %s", err)
	a.containerURL = containerURL
	return nil
}

func (a *AzureBlobStorage) parseMetadata(metadata bindings.Metadata) (*blobStorageMetadata, error) {
	connInfo := metadata.Properties
	b, err := json.Marshal(connInfo)
	if err != nil {
		return nil, err
	}

	var m blobStorageMetadata
	err = json.Unmarshal(b, &m)
	if err != nil {
		return nil, err
	}
	return &m, nil
}

func (a *AzureBlobStorage) Write(req *bindings.WriteRequest) error {
	name := ""
	if val, ok := req.Metadata[blobName]; ok && val != "" {
		name = val
		delete(req.Metadata, blobName)
	} else {
		name = uuid.New().String()
	}
	blobURL := a.containerURL.NewBlockBlobURL(name)

	var blobHTTPHeaders azblob.BlobHTTPHeaders
	if val, ok := req.Metadata[contentType]; ok && val != "" {
		blobHTTPHeaders.ContentType = val
		delete(req.Metadata, contentType)
	}
	if val, ok := req.Metadata[contentMD5]; ok && val != "" {
		sDec, err := b64.StdEncoding.DecodeString(val)
		if err != nil || len(sDec) != 16 {
			return fmt.Errorf("The MD5 value specified in ContentMD5 is invalid. MD5 value must be 128 bits and base64 encoded.")
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
	
	_, err := azblob.UploadBufferToBlockBlob(context.Background(), req.Data, blobURL, azblob.UploadToBlockBlobOptions{
		Parallelism: 	 16,
		Metadata:    	 req.Metadata,
		BlobHTTPHeaders: blobHTTPHeaders,
	})
	return err
}
