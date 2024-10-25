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
	"context"
	b64 "encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strconv"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/google/uuid"

	"github.com/dapr/components-contrib/bindings"
	storagecommon "github.com/dapr/components-contrib/common/component/azure/blobstorage"
	contribMetadata "github.com/dapr/components-contrib/metadata"
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
	// Defines the response metadata key for the number of pages traversed in a list response.
	metadataKeyPagesTraversed = "pagesTraversed"
	// Defines if the user defined metadata should be returned in the get operation.
	metadataKeyIncludeMetadata = "includeMetadata"
	// Defines the delete snapshots option for the delete operation.
	// See: https://docs.microsoft.com/en-us/rest/api/storageservices/delete-blob#request-headers
	metadataKeyDeleteSnapshots = "deleteSnapshots"
	// Specifies the maximum number of blobs to return, including all BlobPrefix elements. If the request does not
	// specify maxresults the server will return up to 5,000 items.
	// See: https://docs.microsoft.com/en-us/rest/api/storageservices/list-blobs#uri-parameters
	maxResults  int32 = 5000
	endpointKey       = "endpoint"
)

var ErrMissingBlobName = errors.New("blobName is a required attribute")

// AzureBlobStorage allows saving blobs to an Azure Blob Storage account.
type AzureBlobStorage struct {
	metadata        *storagecommon.BlobStorageMetadata
	containerClient *container.Client

	logger logger.Logger
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
func (a *AzureBlobStorage) Init(ctx context.Context, metadata bindings.Metadata) error {
	var err error
	a.containerClient, a.metadata, err = storagecommon.CreateContainerStorageClient(ctx, a.logger, metadata.Properties)
	if err != nil {
		return err
	}
	return nil
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
		id, err := uuid.NewRandom()
		if err != nil {
			return nil, err
		}
		blobName = id.String()
	}

	blobHTTPHeaders, err := storagecommon.CreateBlobHTTPHeadersFromRequest(req.Metadata, nil, a.logger)
	if err != nil {
		return nil, err
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
		Metadata:                storagecommon.SanitizeMetadata(a.logger, req.Metadata),
		HTTPHeaders:             &blobHTTPHeaders,
		TransactionalContentMD5: blobHTTPHeaders.BlobContentMD5,
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
		if bloberror.HasCode(err, bloberror.BlobNotFound) {
			return nil, errors.New("blob not found")
		}
		return nil, fmt.Errorf("error downloading az blob: %w", err)
	}
	reader := blobDownloadResponse.Body
	defer reader.Close()
	blobData, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("error reading az blob: %w", err)
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

		if len(props.Metadata) > 0 {
			metadata = make(map[string]string, len(props.Metadata))
			for k, v := range props.Metadata {
				if v == nil {
					continue
				}
				metadata[k] = *v
			}
		}
	}

	return &bindings.InvokeResponse{
		Data:     blobData,
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
	_, err := blockBlobClient.Delete(ctx, &deleteOptions)

	if bloberror.HasCode(err, bloberror.BlobNotFound) {
		return nil, errors.New("blob not found")
	}

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

	if hasPayload && payload.MaxResults > 0 {
		options.MaxResults = &payload.MaxResults
	} else {
		options.MaxResults = ptr.Of(maxResults) // cannot get address of constant directly
	}

	if hasPayload && payload.Prefix != "" {
		options.Prefix = &payload.Prefix
	}

	var initialMarker string
	if hasPayload && payload.Marker != "" {
		initialMarker = payload.Marker
	} else {
		initialMarker = ""
	}
	options.Marker = &initialMarker

	metadata := make(map[string]string, 3)
	blobs := []*container.BlobItem{}
	pager := a.containerClient.NewListBlobsFlatPager(&options)

	metadata[metadataKeyMarker] = ""
	numBlobs := 0
	pagesTraversed := 0
	for pager.More() {
		resp, err := pager.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("error listing blobs: %w", err)
		}
		pagesTraversed++

		blobs = append(blobs, resp.Segment.BlobItems...)
		numBlobs += len(resp.Segment.BlobItems)
		if resp.Marker != nil {
			metadata[metadataKeyMarker] = *resp.Marker
		} else {
			metadata[metadataKeyMarker] = ""
		}

		if numBlobs >= int(*options.MaxResults) {
			break
		}
	}
	metadata[metadataKeyNumber] = strconv.FormatInt(int64(numBlobs), 10)
	metadata[metadataKeyPagesTraversed] = strconv.FormatInt(int64(pagesTraversed), 10)

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

func (a *AzureBlobStorage) isValidDeleteSnapshotsOptionType(accessType azblob.DeleteSnapshotsOptionType) bool {
	validTypes := azblob.PossibleDeleteSnapshotsOptionTypeValues()
	for _, item := range validTypes {
		if item == accessType {
			return true
		}
	}

	return false
}

// GetComponentMetadata returns the metadata of the component.
func (a *AzureBlobStorage) GetComponentMetadata() (metadataInfo contribMetadata.MetadataMap) {
	metadataStruct := storagecommon.BlobStorageMetadata{}
	contribMetadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, contribMetadata.BindingType)
	return
}

func (a *AzureBlobStorage) Close() error {
	return nil
}
