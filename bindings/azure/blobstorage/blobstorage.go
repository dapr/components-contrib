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
	"github.com/google/uuid"

	azauth "github.com/dapr/components-contrib/authentication/azure"
	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/kit/logger"
)

const (
	// Used to reference the blob relative to the container.
	metadataKeyBlobName = "blobName"
	// A string value that identifies the portion of the list to be returned with the next list operation.
	// The operation returns a marker value within the response body if the list returned was not complete. The marker
	// value may then be used in a subsequent call to request the next set of list items.
	// See: https://docs.microsoft.com/en-us/rest/api/storageservices/list-blobs#uri-parameters
	metadataKeyMarker = "marker"
	// The number of blobs that will be returned in a list operation.
	metadataKeyNumber = "number"
	// Defines if the user defined metadata should be returned in the get operation.
	metadataKeyIncludeMetadata = "includeMetadata"
	// Defines the delete snapshots option for the delete operation.
	// See: https://docs.microsoft.com/en-us/rest/api/storageservices/delete-blob#request-headers
	metadataKeyDeleteSnapshots = "deleteSnapshots"
	// HTTP headers to be associated with the blob.
	// See: https://docs.microsoft.com/en-us/rest/api/storageservices/put-blob#request-headers-all-blob-types
	metadataKeyContentType        = "contentType"
	metadataKeyContentMD5         = "contentMD5"
	metadataKeyContentEncoding    = "contentEncoding"
	metadataKeyContentLanguage    = "contentLanguage"
	metadataKeyContentDisposition = "contentDisposition"
	metadataKeyCacheControl       = "cacheControl"
	// Specifies the maximum number of HTTP GET requests that will be made while reading from a RetryReader. A value
	// of zero means that no additional HTTP GET requests will be made.
	defaultGetBlobRetryCount = 10
	// Specifies the maximum number of blobs to return, including all BlobPrefix elements. If the request does not
	// specify maxresults the server will return up to 5,000 items.
	// See: https://docs.microsoft.com/en-us/rest/api/storageservices/list-blobs#uri-parameters
	maxResults  = 5000
	endpointKey = "endpoint"
)

var ErrMissingBlobName = errors.New("blobName is a required attribute")

// AzureBlobStorage allows saving blobs to an Azure Blob Storage account.
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
	BlobURL  string `json:"blobURL"`
	BlobName string `json:"blobName"`
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

// NewAzureBlobStorage returns a new Azure Blob Storage instance.
func NewAzureBlobStorage(logger logger.Logger) *AzureBlobStorage {
	return &AzureBlobStorage{logger: logger}
}

// Init performs metadata parsing.
func (a *AzureBlobStorage) Init(metadata bindings.Metadata) error {
	m, err := a.parseMetadata(metadata)
	if err != nil {
		return err
	}
	a.metadata = m

	if m.StorageAccessKey != "" {
		metadata.Properties["accountKey"] = m.StorageAccessKey
	}
	credential, env, err := azauth.GetAzureStorageCredentials(a.logger, m.StorageAccount, metadata.Properties)
	if err != nil {
		return fmt.Errorf("invalid credentials with error: %s", err.Error())
	}

	userAgent := "dapr-" + logger.DaprVersion
	options := azblob.PipelineOptions{
		Telemetry: azblob.TelemetryOptions{Value: userAgent},
	}
	p := azblob.NewPipeline(credential, options)

	var containerURL azblob.ContainerURL
	customEndpoint, ok := metadata.Properties[endpointKey]
	if ok && customEndpoint != "" {
		URL, parseErr := url.Parse(fmt.Sprintf("%s/%s/%s", customEndpoint, m.StorageAccount, m.Container))
		if parseErr != nil {
			return parseErr
		}
		containerURL = azblob.NewContainerURL(*URL, p)
	} else {
		URL, _ := url.Parse(fmt.Sprintf("https://%s.blob.%s/%s", m.StorageAccount, env.StorageEndpointSuffix, m.Container))
		containerURL = azblob.NewContainerURL(*URL, p)
	}

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

	// per the Dapr documentation "none" is a valid value
	if m.PublicAccessLevel == "none" {
		m.PublicAccessLevel = ""
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
	var blobName string
	if val, ok := req.Metadata[metadataKeyBlobName]; ok && val != "" {
		blobName = val
		delete(req.Metadata, metadataKeyBlobName)
	} else {
		blobName = uuid.New().String()
	}
	blobURL = a.getBlobURL(blobName)

	if val, ok := req.Metadata[metadataKeyContentType]; ok && val != "" {
		blobHTTPHeaders.ContentType = val
		delete(req.Metadata, metadataKeyContentType)
	}
	if val, ok := req.Metadata[metadataKeyContentMD5]; ok && val != "" {
		sDec, err := b64.StdEncoding.DecodeString(val)
		if err != nil || len(sDec) != 16 {
			return nil, fmt.Errorf("the MD5 value specified in Content MD5 is invalid, MD5 value must be 128 bits and base64 encoded")
		}
		blobHTTPHeaders.ContentMD5 = sDec
		delete(req.Metadata, metadataKeyContentMD5)
	}
	if val, ok := req.Metadata[metadataKeyContentEncoding]; ok && val != "" {
		blobHTTPHeaders.ContentEncoding = val
		delete(req.Metadata, metadataKeyContentEncoding)
	}
	if val, ok := req.Metadata[metadataKeyContentLanguage]; ok && val != "" {
		blobHTTPHeaders.ContentLanguage = val
		delete(req.Metadata, metadataKeyContentLanguage)
	}
	if val, ok := req.Metadata[metadataKeyContentDisposition]; ok && val != "" {
		blobHTTPHeaders.ContentDisposition = val
		delete(req.Metadata, metadataKeyContentDisposition)
	}
	if val, ok := req.Metadata[metadataKeyCacheControl]; ok && val != "" {
		blobHTTPHeaders.CacheControl = val
		delete(req.Metadata, metadataKeyCacheControl)
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

	createResponseMetadata := map[string]string{
		"blobName": blobName,
	}

	return &bindings.InvokeResponse{
		Data:     b,
		Metadata: createResponseMetadata,
	}, nil
}

func (a *AzureBlobStorage) get(req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	var blobURL azblob.BlockBlobURL
	if val, ok := req.Metadata[metadataKeyBlobName]; ok && val != "" {
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
	fetchMetadata, err := req.GetMetadataAsBool(metadataKeyIncludeMetadata)
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
	if val, ok := req.Metadata[metadataKeyBlobName]; ok && val != "" {
		blobURL = a.getBlobURL(val)
	} else {
		return nil, ErrMissingBlobName
	}

	deleteSnapshotsOptions := azblob.DeleteSnapshotsOptionNone
	if val, ok := req.Metadata[metadataKeyDeleteSnapshots]; ok && val != "" {
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

	hasPayload := false
	var payload listPayload
	if req.Data != nil {
		err := json.Unmarshal(req.Data, &payload)
		if err != nil {
			return nil, err
		}
		hasPayload = true
	}

	if hasPayload {
		options.Details.Copy = payload.Include.Copy
		options.Details.Metadata = payload.Include.Metadata
		options.Details.Snapshots = payload.Include.Snapshots
		options.Details.UncommittedBlobs = payload.Include.UncommittedBlobs
		options.Details.Deleted = payload.Include.Deleted
	}

	if hasPayload && payload.MaxResults != int32(0) {
		options.MaxResults = payload.MaxResults
	} else {
		options.MaxResults = maxResults
	}

	if hasPayload && payload.Prefix != "" {
		options.Prefix = payload.Prefix
	}

	var initialMarker azblob.Marker
	if hasPayload && payload.Marker != "" {
		initialMarker = azblob.Marker{Val: &payload.Marker}
	} else {
		initialMarker = azblob.Marker{}
	}

	var blobs []azblob.BlobItem
	metadata := map[string]string{}
	ctx := context.Background()
	for currentMaker := initialMarker; currentMaker.NotDone(); {
		var listBlob *azblob.ListBlobsFlatSegmentResponse
		listBlob, err := a.containerURL.ListBlobsFlatSegment(ctx, currentMaker, options)
		if err != nil {
			return nil, fmt.Errorf("error listing blobs: %w", err)
		}

		blobs = append(blobs, listBlob.Segment.BlobItems...)

		numBlobs := len(blobs)
		currentMaker = listBlob.NextMarker
		metadata[metadataKeyMarker] = *currentMaker.Val
		metadata[metadataKeyNumber] = strconv.FormatInt(int64(numBlobs), 10)

		if options.MaxResults-maxResults > 0 {
			options.MaxResults -= maxResults
		} else {
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
