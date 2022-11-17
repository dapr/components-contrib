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
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/google/uuid"

	"github.com/dapr/components-contrib/bindings"
	azauth "github.com/dapr/components-contrib/internal/authentication/azure"
	mdutils "github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
	"github.com/dapr/kit/ptr"
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
	metadata *blobStorageMetadata
	// containerURL azblob.ContainerURL
	containerClient *container.Client

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
func NewAzureBlobStorage(logger logger.Logger) bindings.OutputBinding {
	return &AzureBlobStorage{logger: logger}
}

// Init performs metadata parsing.
func (a *AzureBlobStorage) Init(metadata bindings.Metadata) error {
	m, err := a.parseMetadata(metadata)
	if err != nil {
		return err
	}
	a.metadata = m

	userAgent := "dapr-" + logger.DaprVersion
	options := container.ClientOptions{
		ClientOptions: azcore.ClientOptions{
			Retry: policy.RetryOptions{
				MaxRetries: int32(a.metadata.GetBlobRetryCount),
			},
			Telemetry: policy.TelemetryOptions{
				ApplicationID: userAgent,
			},
		},
	}

	settings, err := azauth.NewEnvironmentSettings("storage", metadata.Properties)
	if err != nil {
		return err
	}
	customEndpoint, ok := metadata.Properties[endpointKey]
	var URL *url.URL
	if ok && customEndpoint != "" {
		var parseErr error
		URL, parseErr = url.Parse(fmt.Sprintf("%s/%s/%s", customEndpoint, m.StorageAccount, m.Container))
		if parseErr != nil {
			return parseErr
		}
	} else {
		env := settings.AzureEnvironment
		URL, _ = url.Parse(fmt.Sprintf("https://%s.blob.%s/%s", m.StorageAccount, env.StorageEndpointSuffix, m.Container))
	}

	var clientErr error
	var client *container.Client
	// Try using shared key credentials first
	if m.StorageAccessKey != "" {
		credential, newSharedKeyErr := azblob.NewSharedKeyCredential(m.StorageAccount, m.StorageAccessKey)
		if err != nil {
			return fmt.Errorf("invalid credentials with error: %w", newSharedKeyErr)
		}
		client, clientErr = container.NewClientWithSharedKeyCredential(URL.String(), credential, &options)
		if clientErr != nil {
			return fmt.Errorf("cannot init Blobstorage container client: %w", err)
		}
		container.NewClientWithSharedKeyCredential(URL.String(), credential, &options)
		a.containerClient = client
	} else {
		// fallback to AAD
		credential, tokenErr := settings.GetTokenCredential()
		if err != nil {
			return fmt.Errorf("invalid credentials with error: %w", tokenErr)
		}
		client, clientErr = container.NewClient(URL.String(), credential, &options)
	}
	if clientErr != nil {
		return fmt.Errorf("cannot init Blobstorage client: %w", clientErr)
	}

	createContainerOptions := container.CreateOptions{
		Access:   &m.PublicAccessLevel,
		Metadata: map[string]string{},
	}
	timeoutCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	_, err = client.Create(timeoutCtx, &createContainerOptions)
	cancel()
	// Don't return error, container might already exist
	a.logger.Debugf("error creating container: %w", err)
	a.containerClient = client

	return nil
}

func (a *AzureBlobStorage) parseMetadata(meta bindings.Metadata) (*blobStorageMetadata, error) {
	m := blobStorageMetadata{
		GetBlobRetryCount: defaultGetBlobRetryCount,
	}
	mdutils.DecodeMetadata(meta.Properties, &m)

	if val, ok := mdutils.GetMetadataProperty(meta.Properties, azauth.StorageAccountNameKeys...); ok && val != "" {
		m.StorageAccount = val
	} else {
		return nil, fmt.Errorf("missing or empty %s field from metadata", azauth.StorageAccountNameKeys[0])
	}

	if val, ok := mdutils.GetMetadataProperty(meta.Properties, azauth.StorageContainerNameKeys...); ok && val != "" {
		m.Container = val
	} else {
		return nil, fmt.Errorf("missing or empty %s field from metadata", azauth.StorageContainerNameKeys[0])
	}

	// per the Dapr documentation "none" is a valid value
	if m.PublicAccessLevel == "none" {
		m.PublicAccessLevel = ""
	}
	if m.PublicAccessLevel != "" && !a.isValidPublicAccessType(m.PublicAccessLevel) {
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

func (a *AzureBlobStorage) create(ctx context.Context, req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	var blobName string
	if val, ok := req.Metadata[metadataKeyBlobName]; ok && val != "" {
		blobName = val
		delete(req.Metadata, metadataKeyBlobName)
	} else {
		blobName = uuid.New().String()
	}

	blobHTTPHeaders := blob.HTTPHeaders{}

	if val, ok := req.Metadata[metadataKeyContentType]; ok && val != "" {
		blobHTTPHeaders.BlobContentType = ptr.Of(val)
		delete(req.Metadata, metadataKeyContentType)
	}
	var contentMD5 *[]byte

	if val, ok := req.Metadata[metadataKeyContentMD5]; ok && val != "" {
		sDec, err := b64.StdEncoding.DecodeString(val)
		if err != nil || len(sDec) != 16 {
			return nil, fmt.Errorf("the MD5 value specified in Content MD5 is invalid, MD5 value must be 128 bits and base64 encoded")
		}
		blobHTTPHeaders.BlobContentMD5 = sDec
		contentMD5 = &sDec
		delete(req.Metadata, metadataKeyContentMD5)
	}
	if val, ok := req.Metadata[metadataKeyContentEncoding]; ok && val != "" {
		blobHTTPHeaders.BlobContentEncoding = ptr.Of(val)
		delete(req.Metadata, metadataKeyContentEncoding)
	}
	if val, ok := req.Metadata[metadataKeyContentLanguage]; ok && val != "" {
		blobHTTPHeaders.BlobContentLanguage = ptr.Of(val)
		delete(req.Metadata, metadataKeyContentLanguage)
	}
	if val, ok := req.Metadata[metadataKeyContentDisposition]; ok && val != "" {
		blobHTTPHeaders.BlobContentDisposition = ptr.Of(val)
		delete(req.Metadata, metadataKeyContentDisposition)
	}
	if val, ok := req.Metadata[metadataKeyCacheControl]; ok && val != "" {
		blobHTTPHeaders.BlobCacheControl = ptr.Of(val)
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

	uploadOptions := azblob.UploadBufferOptions{
		Metadata:                a.sanitizeMetadata(req.Metadata),
		HTTPHeaders:             &blobHTTPHeaders,
		Concurrency:             16,
		TransactionalContentMD5: contentMD5,
	}

	blockBlobClient := a.containerClient.NewBlockBlobClient(blobName)
	_, err = blockBlobClient.UploadBuffer(ctx, req.Data, &uploadOptions)

	if err != nil {
		return nil, fmt.Errorf("error uploading az blob: %w", err)
	}

	resp := createResponse{
		BlobURL: blockBlobClient.URL(),
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

func (a *AzureBlobStorage) get(ctx context.Context, req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	var blockBlobClient *blockblob.Client
	if val, ok := req.Metadata[metadataKeyBlobName]; ok && val != "" {
		blockBlobClient = a.containerClient.NewBlockBlobClient(val)
	} else {
		return nil, ErrMissingBlobName
	}

	downloadOptions := azblob.DownloadStreamOptions{
		AccessConditions: &blob.AccessConditions{},
	}

	blobDownloadResponse, err := blockBlobClient.DownloadStream(ctx, &downloadOptions)
	if err != nil {
		return nil, fmt.Errorf("error downloading az blob: %w", err)
	}
	blobData := &bytes.Buffer{}
	reader := blobDownloadResponse.Body
	_, err = blobData.ReadFrom(reader)
	if err != nil {
		return nil, fmt.Errorf("error reading az blob: %w", err)
	}
	err = reader.Close()
	if err != nil {
		return nil, fmt.Errorf("error closing az blob reader: %w", err)
	}

	var metadata map[string]string
	fetchMetadata, err := req.GetMetadataAsBool(metadataKeyIncludeMetadata)
	if err != nil {
		return nil, fmt.Errorf("error parsing metadata: %w", err)
	}

	getPropertiesOptions := blob.GetPropertiesOptions{
		AccessConditions: &blob.AccessConditions{},
	}

	if fetchMetadata {
		props, err := blockBlobClient.GetProperties(ctx, &getPropertiesOptions)
		if err != nil {
			return nil, fmt.Errorf("error reading blob metadata: %w", err)
		}

		metadata = props.Metadata
	}

	return &bindings.InvokeResponse{
		Data:     blobData.Bytes(),
		Metadata: metadata,
	}, nil
}

func (a *AzureBlobStorage) delete(ctx context.Context, req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	var blockBlobClient *blockblob.Client
	val, ok := req.Metadata[metadataKeyBlobName]
	if !ok || val == "" {
		return nil, ErrMissingBlobName
	}

	var deleteSnapshotsOptions blob.DeleteSnapshotsOptionType
	if deleteSnapShotOption, ok := req.Metadata[metadataKeyDeleteSnapshots]; ok && val != "" {
		deleteSnapshotsOptions = azblob.DeleteSnapshotsOptionType(deleteSnapShotOption)
		if !a.isValidDeleteSnapshotsOptionType(deleteSnapshotsOptions) {
			return nil, fmt.Errorf("invalid delete snapshot option type: %s; allowed: %s",
				deleteSnapshotsOptions, azblob.PossibleDeleteSnapshotsOptionTypeValues())
		}
	}

	deleteOptions := blob.DeleteOptions{
		DeleteSnapshots:  &deleteSnapshotsOptions,
		AccessConditions: &blob.AccessConditions{},
	}

	blockBlobClient = a.containerClient.NewBlockBlobClient(val)
	_, err := blockBlobClient.Delete(context.Background(), &deleteOptions)

	return nil, err
}

func (a *AzureBlobStorage) list(ctx context.Context, req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	options := container.ListBlobsFlatOptions{}

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
		options.Include.Copy = payload.Include.Copy
		options.Include.Metadata = payload.Include.Metadata
		options.Include.Snapshots = payload.Include.Snapshots
		options.Include.UncommittedBlobs = payload.Include.UncommittedBlobs
		options.Include.Deleted = payload.Include.Deleted
	}

	if hasPayload && payload.MaxResults != int32(0) {
		options.MaxResults = ptr.Of(payload.MaxResults)
	} else {
		options.MaxResults = ptr.Of(int32(maxResults))
	}

	if hasPayload && payload.Prefix != "" {
		options.Prefix = ptr.Of(payload.Prefix)
	}

	var initialMarker string
	if hasPayload && payload.Marker != "" {
		initialMarker = payload.Marker
	} else {
		initialMarker = ""
	}
	options.Marker = ptr.Of(initialMarker)

	metadata := map[string]string{}
	blobs := []*container.BlobItem{}
	pager := a.containerClient.NewListBlobsFlatPager(&options)

	for pager.More() {
		resp, err := pager.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("error listing blobs: %w", err)
		}

		blobs = append(blobs, resp.Segment.BlobItems...)
		numBlobs := len(blobs)
		metadata[metadataKeyNumber] = strconv.FormatInt(int64(numBlobs), 10)
		metadata[metadataKeyMarker] = ""
		if resp.Marker != nil {
			metadata[metadataKeyMarker] = *resp.Marker
		}

		if *options.MaxResults-maxResults > 0 {
			*options.MaxResults -= maxResults
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

func (a *AzureBlobStorage) Invoke(ctx context.Context, req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	switch req.Operation {
	case bindings.CreateOperation:
		return a.create(ctx, req)
	case bindings.GetOperation:
		return a.get(ctx, req)
	case bindings.DeleteOperation:
		return a.delete(ctx, req)
	case bindings.ListOperation:
		return a.list(ctx, req)
	default:
		return nil, fmt.Errorf("unsupported operation %s", req.Operation)
	}
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

func (a *AzureBlobStorage) sanitizeMetadata(metadata map[string]string) map[string]string {
	for key, val := range metadata {
		// Keep only letters and digits
		n := 0
		newKey := make([]byte, len(key))
		for i := 0; i < len(key); i++ {
			if (key[i] >= 'A' && key[i] <= 'Z') ||
				(key[i] >= 'a' && key[i] <= 'z') ||
				(key[i] >= '0' && key[i] <= '9') {
				newKey[n] = key[i]
				n++
			}
		}

		if n != len(key) {
			nks := string(newKey[:n])
			a.logger.Warnf("metadata key %s contains disallowed characters, sanitized to %s", key, nks)
			delete(metadata, key)
			metadata[nks] = val
			key = nks
		}

		// Remove all non-ascii characters
		n = 0
		newVal := make([]byte, len(val))
		for i := 0; i < len(val); i++ {
			if val[i] > 127 {
				continue
			}
			newVal[n] = val[i]
			n++
		}
		metadata[key] = string(newVal[:n])
	}

	return metadata
}
