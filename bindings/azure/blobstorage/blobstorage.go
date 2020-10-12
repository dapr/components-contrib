// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package blobstorage

import (
	"bytes"
	"context"
	b64 "encoding/base64"
	"encoding/json"
	"fmt"
	"net/url"
	"strconv"

	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/dapr/pkg/logger"
	"github.com/google/uuid"
)

const (
	blobName                 = "blobName"
	contentType              = "ContentType"
	contentMD5               = "ContentMD5"
	contentEncoding          = "ContentEncoding"
	contentLanguage          = "ContentLanguage"
	contentDisposition       = "ContentDisposition"
	cacheControl             = "CacheControl"
	defaultGetBlobRetryCount = 10
)

// AzureBlobStorage allows saving blobs to an Azure Blob Storage account
type AzureBlobStorage struct {
	metadata     *blobStorageMetadata
	containerURL azblob.ContainerURL

	logger logger.Logger
}

type blobStorageMetadata struct {
	StorageAccount    string `json:"storageAccount"`
	StorageAccessKey  string `json:"storageAccessKey"`
	Container         string `json:"container"`
	DecodeBase64      string `json:"decodeBase64"`
	GetBlobRetryCount int    `json:"getBlobRetryCount"`
}

type createResponse struct {
	BlobURL string `json:"blobURL"`
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

	if m.GetBlobRetryCount == 0 {
		m.GetBlobRetryCount = defaultGetBlobRetryCount
	}

	return &m, nil
}

func (a *AzureBlobStorage) Operations() []bindings.OperationKind {
	return []bindings.OperationKind{bindings.CreateOperation, bindings.GetOperation}
}

func (a *AzureBlobStorage) create(blobURL azblob.BlockBlobURL, req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	var blobHTTPHeaders azblob.BlobHTTPHeaders
	if val, ok := req.Metadata[contentType]; ok && val != "" {
		blobHTTPHeaders.ContentType = val
		delete(req.Metadata, contentType)
	}
	if val, ok := req.Metadata[contentMD5]; ok && val != "" {
		sDec, err := b64.StdEncoding.DecodeString(val)
		if err != nil || len(sDec) != 16 {
			return nil, fmt.Errorf("the MD5 value specified in Content MD5 is invalid, MD5 value must be 128 bits and base64 encoded")
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

	// Unescape data which will still be a JSON string
	unescapedData, unescapeError := strconv.Unquote(string(req.Data))

	if unescapeError != nil {
		return nil, unescapeError
	}

	data := []byte(unescapedData)

	// The "true" is the only allowed positive value. Other positive variations like "True" not acceptable.
	if a.metadata.DecodeBase64 == "true" {
		decoded, decodeError := b64.StdEncoding.DecodeString(unescapedData)
		if decodeError != nil {
			return nil, decodeError
		}
		data = decoded
	}

	_, err := azblob.UploadBufferToBlockBlob(context.Background(), data, blobURL, azblob.UploadToBlockBlobOptions{
		Parallelism:     16,
		Metadata:        req.Metadata,
		BlobHTTPHeaders: blobHTTPHeaders,
	})
	if err != nil {
		return nil, fmt.Errorf("error uploading az blob: %s", err)
	}

	resp := createResponse{
		BlobURL: blobURL.String(),
	}
	b, err := json.Marshal(resp)
	if err != nil {
		return nil, fmt.Errorf("error marshalling create response for azure blob: %s", err)
	}

	return &bindings.InvokeResponse{
		Data: b,
	}, nil
}

func (a *AzureBlobStorage) get(blobURL azblob.BlockBlobURL, req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	resp, err := blobURL.Download(context.TODO(), 0, azblob.CountToEnd, azblob.BlobAccessConditions{}, false)
	if err != nil {
		return nil, fmt.Errorf("error downloading az blob: %s", err)
	}

	bodyStream := resp.Body(azblob.RetryReaderOptions{MaxRetryRequests: a.metadata.GetBlobRetryCount})

	b := bytes.Buffer{}
	_, err = b.ReadFrom(bodyStream)
	if err != nil {
		return nil, fmt.Errorf("error reading az blob body: %s", err)
	}

	return &bindings.InvokeResponse{
		Data: b.Bytes(),
	}, nil
}

func (a *AzureBlobStorage) Invoke(req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	name := ""
	if val, ok := req.Metadata[blobName]; ok && val != "" {
		name = val
		delete(req.Metadata, blobName)
	} else {
		name = uuid.New().String()
	}

	blobURL := a.containerURL.NewBlockBlobURL(name)
	switch req.Operation {
	case bindings.CreateOperation:
		return a.create(blobURL, req)
	case bindings.GetOperation:
		return a.get(blobURL, req)
	case bindings.DeleteOperation, bindings.ListOperation:
		fallthrough
	default:
		return nil, fmt.Errorf("unsupported operation %s", req.Operation)
	}
}
