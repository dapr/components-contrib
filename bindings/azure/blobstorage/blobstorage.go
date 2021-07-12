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
	"errors"
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
	marker                   = "marker"
	number                   = "number"
	includeMetadata          = "includeMetadata"
	deleteSnapshots          = "deleteSnapshots"
	contentType              = "contentType"
	contentMD5               = "contentMD5"
	contentEncoding          = "contentEncoding"
	contentLanguage          = "contentLanguage"
	contentDisposition       = "contentDisposition"
	cacheControl             = "cacheControl"
	defaultGetBlobRetryCount = 10
	maxResults               = 5000

	// TODO: remove the pascal case support when the component moves to GA
	// See: https://github.com/dapr/components-contrib/pull/999#issuecomment-876890210
	contentTypeBC           = "ContentType"
	contentMD5BC            = "ContentMD5"
	contentEncodingBC       = "ContentEncoding"
	contentLanguageBC       = "ContentLanguage"
	contentDispositionBC    = "ContentDisposition"
	cacheControlBC          = "CacheControl"
	deleteSnapshotOptionsBC = "DeleteSnapshotOptions"
)

var ErrMissingBlobName = errors.New("blobName is a required attribute")

// AzureBlobStorage allows saving blobs to an Azure Blob Storage account
type AzureBlobStorage struct {
	metadata     *blobStorageMetadata
	containerURL azblob.ContainerURL

	logger logger.Logger
}

type blobStorageMetadata struct {
	StorageAccount    string                  `json:"storageAccount"`
	StorageAccessKey  string                  `json:"storageAccessKey"`
	Container         string                  `json:"container"`
	GetBlobRetryCount int                     `json:"getBlobRetryCount,string"`
	DecodeBase64      bool                    `json:"decodeBase64,string"`
	PublicAccessLevel azblob.PublicAccessType `json:"publicAccessLevel"`
}

type createResponse struct {
	BlobURL string `json:"blobURL"`
}

type listInclude struct {
	Copy             bool `json:"copy"`
	Metadata         bool `json:"metadata"`
	Snapshots        bool `json:"snapshots"`
	UncommittedBlobs bool `json:"uncommittedBlobs"`
	Deleted          bool `json:"deleted"`
}

type listPayload struct {
	Marker     string      `json:"marker"`
	Prefix     string      `json:"prefix"`
	MaxResults int32       `json:"maxResults"`
	Include    listInclude `json:"include"`
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
	_, err = containerURL.Create(ctx, azblob.Metadata{}, m.PublicAccessLevel)
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

	if !a.isValidPublicAccessType(m.PublicAccessLevel) {
		return nil, fmt.Errorf("invalid public access level: %s; allowed: %s",
			m.PublicAccessLevel, azblob.PossiblePublicAccessTypeValues())
	}

	return &m, nil
}

func (a *AzureBlobStorage) Operations() []bindings.OperationKind {
	return []bindings.OperationKind{
		bindings.CreateOperation,
		bindings.GetOperation,
		bindings.DeleteOperation,
		bindings.ListOperation,
	}
}

func (a *AzureBlobStorage) create(req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	var blobHTTPHeaders azblob.BlobHTTPHeaders
	var blobURL azblob.BlockBlobURL
	if val, ok := req.Metadata[blobName]; ok && val != "" {
		blobURL = a.getBlobURL(val)
		delete(req.Metadata, blobName)
	} else {
		blobURL = a.getBlobURL(uuid.New().String())
	}

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

	if a.metadata.DecodeBase64 {
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

func (a *AzureBlobStorage) get(req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	var blobURL azblob.BlockBlobURL
	if val, ok := req.Metadata[blobName]; ok && val != "" {
		blobURL = a.getBlobURL(val)
	} else {
		return nil, ErrMissingBlobName
	}

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

func (a *AzureBlobStorage) delete(req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	var blobURL azblob.BlockBlobURL
	if val, ok := req.Metadata[blobName]; ok && val != "" {
		blobURL = a.getBlobURL(val)
	} else {
		return nil, ErrMissingBlobName
	}

	deleteSnapshotsOptions := azblob.DeleteSnapshotsOptionNone
	if val, ok := req.Metadata[deleteSnapshots]; ok && val != "" {
		deleteSnapshotsOptions = azblob.DeleteSnapshotsOptionType(val)
		if !a.isValidDeleteSnapshotsOptionType(deleteSnapshotsOptions) {
			return nil, fmt.Errorf("invalid delete snapshot option type: %s; allowed: %s",
				deleteSnapshotsOptions, azblob.PossibleDeleteSnapshotsOptionTypeValues())
		}
	}

	_, err := blobURL.Delete(context.Background(), deleteSnapshotsOptions, azblob.BlobAccessConditions{})

	return nil, err
}

func (a *AzureBlobStorage) list(req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	options := azblob.ListBlobsSegmentOptions{}

	var payload listPayload
	err := json.Unmarshal(req.Data, &payload)
	if err != nil {
		return nil, err
	}

	options.Details.Copy = payload.Include.Copy
	options.Details.Metadata = payload.Include.Metadata
	options.Details.Snapshots = payload.Include.Snapshots
	options.Details.UncommittedBlobs = payload.Include.UncommittedBlobs
	options.Details.Deleted = payload.Include.Deleted

	if payload.MaxResults != int32(0) {
		options.MaxResults = payload.MaxResults
	} else {
		options.MaxResults = maxResults
	}

	if payload.Prefix != "" {
		options.Prefix = payload.Prefix
	}

	var initialMarker azblob.Marker
	if payload.Marker != "" {
		initialMarker = azblob.Marker{Val: &payload.Marker}
	} else {
		initialMarker = azblob.Marker{}
	}

	var blobs []azblob.BlobItem
	metadata := map[string]string{}
	ctx := context.Background()
	for currentMaker := initialMarker; currentMaker.NotDone(); {
		var listBlob *azblob.ListBlobsFlatSegmentResponse
		listBlob, err = a.containerURL.ListBlobsFlatSegment(ctx, currentMaker, options)
		if err != nil {
			return nil, fmt.Errorf("error listing blobs: %w", err)
		}

		blobs = append(blobs, listBlob.Segment.BlobItems...)

		numBlobs := len(blobs)
		currentMaker = listBlob.NextMarker
		metadata[marker] = *currentMaker.Val
		metadata[number] = strconv.FormatInt(int64(numBlobs), 10)

		if numBlobs == int(options.MaxResults) {
			break
		}
	}

	jsonResponse, err := json.Marshal(blobs)
	if err != nil {
		return nil, fmt.Errorf("cannot marshal blobs to json: %w", err)
	}

	return &bindings.InvokeResponse{
		Data:     jsonResponse,
		Metadata: metadata,
	}, nil
}

func (a *AzureBlobStorage) Invoke(req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	req.Metadata = a.handleBackwardCompatibilityForMetadata(req.Metadata)

	switch req.Operation {
	case bindings.CreateOperation:
		return a.create(req)
	case bindings.GetOperation:
		return a.get(req)
	case bindings.DeleteOperation:
		return a.delete(req)
	case bindings.ListOperation:
		return a.list(req)
	default:
		return nil, fmt.Errorf("unsupported operation %s", req.Operation)
	}
}

func (a *AzureBlobStorage) getBlobURL(name string) azblob.BlockBlobURL {
	blobURL := a.containerURL.NewBlockBlobURL(name)

	return blobURL
}

func (a *AzureBlobStorage) isValidPublicAccessType(accessType azblob.PublicAccessType) bool {
	validTypes := azblob.PossiblePublicAccessTypeValues()
	for _, item := range validTypes {
		if item == accessType {
			return true
		}
	}

	return false
}

func (a *AzureBlobStorage) isValidDeleteSnapshotsOptionType(accessType azblob.DeleteSnapshotsOptionType) bool {
	validTypes := azblob.PossibleDeleteSnapshotsOptionTypeValues()
	for _, item := range validTypes {
		if item == accessType {
			return true
		}
	}

	return false
}

// TODO: remove the pascal case support when the component moves to GA
// See: https://github.com/dapr/components-contrib/pull/999#issuecomment-876890210
func (a *AzureBlobStorage) handleBackwardCompatibilityForMetadata(metadata map[string]string) map[string]string {
	if val, ok := metadata[contentTypeBC]; ok && val != "" {
		metadata[contentType] = val
		delete(metadata, contentTypeBC)
	}

	if val, ok := metadata[contentMD5BC]; ok && val != "" {
		metadata[contentMD5] = val
		delete(metadata, contentMD5BC)
	}

	if val, ok := metadata[contentEncodingBC]; ok && val != "" {
		metadata[contentEncoding] = val
		delete(metadata, contentEncodingBC)
	}

	if val, ok := metadata[contentLanguageBC]; ok && val != "" {
		metadata[contentLanguage] = val
		delete(metadata, contentLanguageBC)
	}

	if val, ok := metadata[contentDispositionBC]; ok && val != "" {
		metadata[contentDisposition] = val
		delete(metadata, contentDispositionBC)
	}

	if val, ok := metadata[cacheControlBC]; ok && val != "" {
		metadata[cacheControl] = val
		delete(metadata, cacheControlBC)
	}

	if val, ok := metadata[deleteSnapshotOptionsBC]; ok && val != "" {
		metadata[deleteSnapshots] = val
		delete(metadata, deleteSnapshotOptionsBC)
	}

	return metadata
}
