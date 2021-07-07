// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
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
	"github.com/dapr/kit/logger"
	"github.com/google/uuid"
)

const (
	blobName                 = "blobName"
	includeCopy              = "includeCopy"
	includeMetadata          = "includeMetadata"
	includeSnapshots         = "includeSnapshots"
	includeUncommittedBlobs  = "includeUncommittedBlobs"
	includeDeleted           = "includeDeleted"
	prefix                   = "prefix"
	maxResults               = "maxResults"
	contentType              = "ContentType"
	contentMD5               = "ContentMD5"
	contentEncoding          = "ContentEncoding"
	contentLanguage          = "ContentLanguage"
	contentDisposition       = "ContentDisposition"
	cacheControl             = "CacheControl"
	deleteSnapshotOptions    = "DeleteSnapshotOptions"
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
	GetBlobRetryCount int    `json:"getBlobRetryCount,string"`
	PublicAccessLevel string `json:"publicAccessLevel"`
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
		return fmt.Errorf("invalid credentials with error: %w", err)
	}
	p := azblob.NewPipeline(credential, azblob.PipelineOptions{})

	containerName := a.metadata.Container
	URL, _ := url.Parse(
		fmt.Sprintf("https://%s.blob.core.windows.net/%s", m.StorageAccount, containerName))
	containerURL := azblob.NewContainerURL(*URL, p)

	ctx := context.Background()
	_, err = containerURL.Create(ctx, azblob.Metadata{}, azblob.PublicAccessType(m.PublicAccessLevel))
	// Don't return error, container might already exist
	a.logger.Debugf("error creating container: %w", err)
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
	return []bindings.OperationKind{bindings.CreateOperation, bindings.GetOperation, bindings.DeleteOperation}
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

	d, err := strconv.Unquote(string(req.Data))
	if err == nil {
		req.Data = []byte(d)
	}

	// The "true" is the only allowed positive value. Other positive variations like "True" not acceptable.
	if a.metadata.DecodeBase64 == "true" {
		decoded, decodeError := b64.StdEncoding.DecodeString(string(req.Data))
		if decodeError != nil {
			return nil, decodeError
		}
		req.Data = decoded
	}

	_, err = azblob.UploadBufferToBlockBlob(context.Background(), req.Data, blobURL, azblob.UploadToBlockBlobOptions{
		Parallelism:     16,
		Metadata:        req.Metadata,
		BlobHTTPHeaders: blobHTTPHeaders,
	})
	if err != nil {
		return nil, fmt.Errorf("error uploading az blob: %w", err)
	}

	resp := createResponse{
		BlobURL: blobURL.String(),
	}
	b, err := json.Marshal(resp)
	if err != nil {
		return nil, fmt.Errorf("error marshalling create response for azure blob: %w", err)
	}

	return &bindings.InvokeResponse{
		Data: b,
	}, nil
}

func (a *AzureBlobStorage) get(blobURL azblob.BlockBlobURL, req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	ctx := context.TODO()
	resp, err := blobURL.Download(ctx, 0, azblob.CountToEnd, azblob.BlobAccessConditions{}, false)
	if err != nil {
		return nil, fmt.Errorf("error downloading az blob: %w", err)
	}

	bodyStream := resp.Body(azblob.RetryReaderOptions{MaxRetryRequests: a.metadata.GetBlobRetryCount})

	b := bytes.Buffer{}
	_, err = b.ReadFrom(bodyStream)
	if err != nil {
		return nil, fmt.Errorf("error reading az blob body: %w", err)
	}

	var metadata map[string]string
	fetchMetadata, err := req.GetMetadataAsBool(includeMetadata)
	if err != nil {
		return nil, fmt.Errorf("error parsing metadata: %w", err)
	}

	if fetchMetadata {
		props, err := blobURL.GetProperties(ctx, azblob.BlobAccessConditions{})
		if err != nil {
			return nil, fmt.Errorf("error reading blob metadata: %w", err)
		}

		metadata = props.NewMetadata()
	}

	return &bindings.InvokeResponse{
		Data:     b.Bytes(),
		Metadata: metadata,
	}, nil
}

func (a *AzureBlobStorage) delete(blobURL azblob.BlockBlobURL, req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	_, err := blobURL.Delete(context.Background(), azblob.DeleteSnapshotsOptionType(req.Metadata[deleteSnapshotOptions]), azblob.BlobAccessConditions{})

	return nil, err
}

func (a *AzureBlobStorage) list(req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	listingDetails := azblob.BlobListingDetails{}
	options := azblob.ListBlobsSegmentOptions{Details: listingDetails}

	if boolVal, err := req.GetMetadataAsBool(includeCopy); boolVal {
		listingDetails.Copy = boolVal
	} else if err != nil {
		return nil, fmt.Errorf("error parsing metadata: %w", err)
	}

	if boolVal, err := req.GetMetadataAsBool(includeMetadata); boolVal {
		listingDetails.Metadata = boolVal
	} else if err != nil {
		return nil, fmt.Errorf("error parsing metadata: %w", err)
	}

	if boolVal, err := req.GetMetadataAsBool(includeSnapshots); boolVal {
		listingDetails.Snapshots = boolVal
	} else if err != nil {
		return nil, fmt.Errorf("error parsing metadata: %w", err)
	}

	if boolVal, err := req.GetMetadataAsBool(includeUncommittedBlobs); boolVal {
		listingDetails.UncommittedBlobs = boolVal
	} else if err != nil {
		return nil, fmt.Errorf("error parsing metadata: %w", err)
	}

	if boolVal, err := req.GetMetadataAsBool(includeDeleted); boolVal {
		listingDetails.Deleted = boolVal
	} else if err != nil {
		return nil, fmt.Errorf("error parsing metadata: %w", err)
	}

	if intVal, err := req.GetMetadataAsInt64(maxResults, 32); intVal != 0 {
		options.MaxResults = int32(intVal)
	} else if err != nil {
		return nil, fmt.Errorf("error parsing metadata: %w", err)
	}

	if val, ok := req.Metadata[prefix]; ok && val != "" {
		options.Prefix = val
	}

	var blobs []azblob.BlobItem
	marker := azblob.Marker{}
	for {
		response, err := a.containerURL.ListBlobsFlatSegment(context.Background(), marker, options)
		if err != nil {
			return nil, fmt.Errorf("error listing blobs: %w", err)
		}

		blobs = append(blobs, response.Segment.BlobItems...)

		if marker := response.NextMarker; marker.Val == nil {
			break
		}
	}

	jsonResponse, err := json.Marshal(blobs)
	if err != nil {
		return nil, fmt.Errorf("cannot marshal blobs to json: %w", err)
	}

	return &bindings.InvokeResponse{
		Data: jsonResponse,
	}, nil
}

func (a *AzureBlobStorage) Invoke(req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	switch req.Operation {
	case bindings.CreateOperation:
		return a.create(a.getBlobURL(req.Metadata), req)
	case bindings.GetOperation:
		return a.get(a.getBlobURL(req.Metadata), req)
	case bindings.DeleteOperation:
		return a.delete(a.getBlobURL(req.Metadata), req)
	case bindings.ListOperation:
		return a.list(req)
	default:
		return nil, fmt.Errorf("unsupported operation %s", req.Operation)
	}
}

func (a *AzureBlobStorage) getBlobURL(metadata map[string]string) azblob.BlockBlobURL {
	name := ""
	if val, ok := metadata[blobName]; ok && val != "" {
		name = val
		delete(metadata, blobName)
	} else {
		name = uuid.New().String()
	}

	blobURL := a.containerURL.NewBlockBlobURL(name)

	return blobURL
}
