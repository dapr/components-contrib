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
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"

	securejoin "github.com/cyphar/filepath-securejoin"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/google/uuid"
	"golang.org/x/sync/errgroup"

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

	bulkGetOperation    bindings.OperationKind = "bulkGet"
	bulkCreateOperation bindings.OperationKind = "bulkCreate"
	bulkDeleteOperation bindings.OperationKind = "bulkDelete"

	metadataKeyFilePath = "filePath"

	defaultConcurrency int32 = 10

	// Azure Blob Batch API supports up to 256 sub-requests per batch.
	maxBatchDeleteSize = 256
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

type bulkGetItem struct {
	BlobName string  `json:"blobName"`
	FilePath *string `json:"filePath,omitempty"`
}

type bulkGetPayload struct {
	// Items specifies explicit blob-to-file mappings.
	Items []bulkGetItem `json:"items,omitempty"`
	// Prefix lists blobs by prefix; requires DestinationDir.
	Prefix *string `json:"prefix,omitempty"`
	// DestinationDir is used as the base directory for prefix-discovered blobs.
	DestinationDir *string `json:"destinationDir,omitempty"`
	Concurrency    *int32  `json:"concurrency,omitempty"`
}

type bulkGetResponseItem struct {
	BlobName string `json:"blobName"`
	FilePath string `json:"filePath,omitempty"`
	// Data holds the blob content when using inline mode (no filePath).
	// JSON-marshalled as base64 since it is []byte, which is safe for binary blobs.
	Data  []byte `json:"data,omitempty"`
	Error string `json:"error,omitempty"`
}

type bulkCreateItem struct {
	BlobName    string  `json:"blobName"`
	SourcePath  string  `json:"sourcePath,omitempty"`
	Data        *string `json:"data,omitempty"`
	ContentType *string `json:"contentType,omitempty"`
}

type bulkCreatePayload struct {
	Items       []bulkCreateItem `json:"items"`
	Concurrency *int32           `json:"concurrency,omitempty"`
}

type bulkCreateResponseItem struct {
	BlobName string `json:"blobName"`
	BlobURL  string `json:"blobURL,omitempty"`
	Error    string `json:"error,omitempty"`
}

type bulkDeletePayload struct {
	Prefix          *string  `json:"prefix,omitempty"`
	BlobNames       []string `json:"blobNames,omitempty"`
	DeleteSnapshots *string  `json:"deleteSnapshots,omitempty"`
	Concurrency     *int32   `json:"concurrency,omitempty"`
}

type bulkDeleteResponseItem struct {
	BlobName string `json:"blobName"`
	Error    string `json:"error,omitempty"`
}

var (
	ErrMissingBlobSelection       = errors.New("at least one of items or prefix is required")
	ErrEmptyBulkCreateItems       = errors.New("items array must not be empty")
	ErrMissingDestinationDir      = errors.New("destinationDir is required when using prefix")
	ErrInvalidConcurrency         = errors.New("concurrency must be greater than 0")
	ErrMissingBulkDeleteSelection = errors.New("at least one of prefix or blobNames is required")
)

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
		bulkGetOperation,
		bulkCreateOperation,
		bulkDeleteOperation,
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

	// If filePath is set, stream directly to file via DownloadFile.
	if filePath, ok := req.Metadata[metadataKeyFilePath]; ok && filePath != "" {
		return a.getToFile(ctx, blockBlobClient, filePath)
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

// downloadBlobToFile downloads a blob directly to a local file, creating parent
// directories as needed. On error, any partially-written file is removed.
func downloadBlobToFile(ctx context.Context, blockBlobClient *blockblob.Client, filePath string) error {
	if mkdirErr := os.MkdirAll(filepath.Dir(filePath), 0o755); mkdirErr != nil {
		return fmt.Errorf("error creating directory for %q: %w", filePath, mkdirErr)
	}

	f, createErr := os.Create(filePath)
	if createErr != nil {
		return fmt.Errorf("error creating file %q: %w", filePath, createErr)
	}
	defer f.Close()

	if _, dlErr := blockBlobClient.DownloadFile(ctx, f, nil); dlErr != nil {
		os.Remove(filePath)
		if bloberror.HasCode(dlErr, bloberror.BlobNotFound) {
			return errors.New("blob not found")
		}
		return fmt.Errorf("error downloading blob to file: %w", dlErr)
	}

	return nil
}

func (a *AzureBlobStorage) getToFile(ctx context.Context, blockBlobClient *blockblob.Client, filePath string) (*bindings.InvokeResponse, error) {
	if err := downloadBlobToFile(ctx, blockBlobClient, filePath); err != nil {
		return nil, err
	}

	return &bindings.InvokeResponse{
		Metadata: map[string]string{
			metadataKeyFilePath: filePath,
		},
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

func getConcurrency(val *int32) (int, error) {
	if val == nil {
		return int(defaultConcurrency), nil
	}
	if *val <= 0 {
		return 0, ErrInvalidConcurrency
	}
	return int(*val), nil
}

func (a *AzureBlobStorage) resolveBlobs(ctx context.Context, prefix *string, blobNames []string) ([]string, error) {
	seen := make(map[string]struct{}, len(blobNames))
	result := make([]string, 0, len(blobNames))

	// Add explicit blob names first.
	for _, name := range blobNames {
		if name == "" {
			continue
		}
		if _, ok := seen[name]; !ok {
			seen[name] = struct{}{}
			result = append(result, name)
		}
	}

	// If prefix is set, list blobs and merge.
	if prefix != nil && *prefix != "" {
		options := container.ListBlobsFlatOptions{
			Prefix: prefix,
		}
		pager := a.containerClient.NewListBlobsFlatPager(&options)
		for pager.More() {
			resp, err := pager.NextPage(ctx)
			if err != nil {
				return nil, fmt.Errorf("error listing blobs with prefix %q: %w", *prefix, err)
			}
			for _, item := range resp.Segment.BlobItems {
				if item.Name == nil {
					continue
				}
				if _, ok := seen[*item.Name]; !ok {
					seen[*item.Name] = struct{}{}
					result = append(result, *item.Name)
				}
			}
		}
	}

	return result, nil
}

func (a *AzureBlobStorage) bulkGet(ctx context.Context, req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	var payload bulkGetPayload
	if req.Data != nil {
		if err := json.Unmarshal(req.Data, &payload); err != nil {
			return nil, fmt.Errorf("error parsing bulkGet payload: %w", err)
		}
	}

	concurrency, err := getConcurrency(payload.Concurrency)
	if err != nil {
		return nil, err
	}

	// Build the work list from explicit items and/or prefix discovery.
	items, err := a.resolveBulkGetItems(ctx, &payload)
	if err != nil {
		return nil, err
	}

	if len(items) == 0 {
		return nil, ErrMissingBlobSelection
	}

	results := make([]bulkGetResponseItem, len(items))

	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(concurrency)

	for i, item := range items {
		g.Go(func() error {
			results[i].BlobName = item.BlobName
			blockBlobClient := a.containerClient.NewBlockBlobClient(item.BlobName)

			if item.FilePath != nil && *item.FilePath != "" {
				// Stream to file via shared download helper.
				filePath := *item.FilePath
				if dlErr := downloadBlobToFile(gctx, blockBlobClient, filePath); dlErr != nil {
					results[i].Error = dlErr.Error()
					return nil
				}

				results[i].FilePath = filePath
			} else {
				// Inline mode: download to memory and return data in response.
				resp, dlErr := blockBlobClient.DownloadStream(gctx, nil)
				if dlErr != nil {
					if bloberror.HasCode(dlErr, bloberror.BlobNotFound) {
						results[i].Error = "blob not found"
					} else {
						results[i].Error = dlErr.Error()
					}
					return nil
				}
				defer resp.Body.Close()

				var buf bytes.Buffer
				if _, copyErr := io.Copy(&buf, resp.Body); copyErr != nil {
					results[i].Error = copyErr.Error()
					return nil
				}

				results[i].Data = buf.Bytes()
			}
			return nil
		})
	}

	if waitErr := g.Wait(); waitErr != nil {
		return nil, fmt.Errorf("error in bulkGet: %w", waitErr)
	}

	jsonResponse, err := json.Marshal(results)
	if err != nil {
		return nil, fmt.Errorf("error marshalling bulkGet response: %w", err)
	}

	return &bindings.InvokeResponse{
		Data: jsonResponse,
		Metadata: map[string]string{
			metadataKeyNumber: strconv.Itoa(len(results)),
		},
	}, nil
}

// resolveBulkGetItems builds the work list for bulkGet.
// Explicit items are preserved as-is (including duplicates with different filePaths).
// Prefix-discovered blobs use destinationDir as base, skipping any already covered by explicit items.
func (a *AzureBlobStorage) resolveBulkGetItems(ctx context.Context, payload *bulkGetPayload) ([]bulkGetItem, error) {
	items := make([]bulkGetItem, 0, len(payload.Items))

	// Track blob names from explicit items so prefix discovery doesn't duplicate them.
	explicitNames := make(map[string]struct{})

	// Add explicit items as-is (no dedup — same blob with different filePaths is valid).
	for _, item := range payload.Items {
		if item.BlobName == "" {
			continue
		}
		explicitNames[item.BlobName] = struct{}{}
		items = append(items, item)
	}

	// If prefix is set, discover blobs and map them into destinationDir.
	if payload.Prefix != nil && *payload.Prefix != "" {
		if payload.DestinationDir == nil || *payload.DestinationDir == "" {
			return nil, ErrMissingDestinationDir
		}

		seen := make(map[string]struct{})
		options := container.ListBlobsFlatOptions{
			Prefix: payload.Prefix,
		}
		pager := a.containerClient.NewListBlobsFlatPager(&options)
		for pager.More() {
			resp, err := pager.NextPage(ctx)
			if err != nil {
				return nil, fmt.Errorf("error listing blobs with prefix %q: %w", *payload.Prefix, err)
			}
			for _, blobItem := range resp.Segment.BlobItems {
				if blobItem.Name == nil {
					continue
				}
				// Skip blobs already covered by explicit items.
				if _, ok := explicitNames[*blobItem.Name]; ok {
					continue
				}
				if _, ok := seen[*blobItem.Name]; ok {
					continue
				}
				seen[*blobItem.Name] = struct{}{}

				destPath, err := securejoin.SecureJoin(*payload.DestinationDir, *blobItem.Name)
				if err != nil {
					return nil, fmt.Errorf("unsafe blob name %q: %w", *blobItem.Name, err)
				}
				items = append(items, bulkGetItem{
					BlobName: *blobItem.Name,
					FilePath: ptr.Of(destPath),
				})
			}
		}
	}

	return items, nil
}

func (a *AzureBlobStorage) bulkCreate(ctx context.Context, req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	var payload bulkCreatePayload
	if req.Data != nil {
		if err := json.Unmarshal(req.Data, &payload); err != nil {
			return nil, fmt.Errorf("error parsing bulkCreate payload: %w", err)
		}
	}

	if len(payload.Items) == 0 {
		return nil, ErrEmptyBulkCreateItems
	}

	for i, item := range payload.Items {
		if item.BlobName == "" {
			return nil, fmt.Errorf("item at index %d is missing blobName", i)
		}
		if item.SourcePath == "" && item.Data == nil {
			return nil, fmt.Errorf("item at index %d is missing both data and sourcePath", i)
		}
	}

	concurrency, err := getConcurrency(payload.Concurrency)
	if err != nil {
		return nil, err
	}

	results := make([]bulkCreateResponseItem, len(payload.Items))

	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(concurrency)

	for i, item := range payload.Items {
		g.Go(func() error {
			results[i].BlobName = item.BlobName

			blockBlobClient := a.containerClient.NewBlockBlobClient(item.BlobName)

			// Build HTTP headers if contentType is specified.
			var httpHeaders *blob.HTTPHeaders
			if item.ContentType != nil && *item.ContentType != "" {
				httpHeaders = &blob.HTTPHeaders{
					BlobContentType: item.ContentType,
				}
			}

			if item.Data != nil {
				// Inline data mode: upload from the provided string.
				var r io.Reader = strings.NewReader(*item.Data)
				if a.metadata.DecodeBase64 {
					r = b64.NewDecoder(b64.StdEncoding, r)
				}
				if _, uploadErr := blockBlobClient.UploadStream(gctx, r, &blockblob.UploadStreamOptions{
					HTTPHeaders: httpHeaders,
				}); uploadErr != nil {
					results[i].Error = "error uploading blob: " + uploadErr.Error()
					return nil
				}
			} else {
				// File-based mode: upload from sourcePath.
				f, openErr := os.Open(item.SourcePath)
				if openErr != nil {
					results[i].Error = "error opening source file: " + openErr.Error()
					return nil
				}
				defer f.Close()

				var uploadErr error
				if a.metadata.DecodeBase64 {
					decoder := b64.NewDecoder(b64.StdEncoding, f)
					_, uploadErr = blockBlobClient.UploadStream(gctx, decoder, &blockblob.UploadStreamOptions{
						HTTPHeaders: httpHeaders,
					})
				} else {
					_, uploadErr = blockBlobClient.UploadFile(gctx, f, &blockblob.UploadFileOptions{
						HTTPHeaders: httpHeaders,
					})
				}
				if uploadErr != nil {
					results[i].Error = "error uploading blob: " + uploadErr.Error()
					return nil
				}
			}

			results[i].BlobURL = blockBlobClient.URL()
			return nil
		})
	}

	if waitErr := g.Wait(); waitErr != nil {
		return nil, fmt.Errorf("error in bulkCreate: %w", waitErr)
	}

	jsonResponse, err := json.Marshal(results)
	if err != nil {
		return nil, fmt.Errorf("error marshalling bulkCreate response: %w", err)
	}

	return &bindings.InvokeResponse{
		Data: jsonResponse,
		Metadata: map[string]string{
			metadataKeyNumber: strconv.Itoa(len(results)),
		},
	}, nil
}

func (a *AzureBlobStorage) bulkDelete(ctx context.Context, req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	var payload bulkDeletePayload
	if req.Data != nil {
		if err := json.Unmarshal(req.Data, &payload); err != nil {
			return nil, fmt.Errorf("error parsing bulkDelete payload: %w", err)
		}
	}

	if payload.DeleteSnapshots != nil && *payload.DeleteSnapshots != "" {
		deleteSnapshotsOption := azblob.DeleteSnapshotsOptionType(*payload.DeleteSnapshots)
		if !a.isValidDeleteSnapshotsOptionType(deleteSnapshotsOption) {
			return nil, fmt.Errorf("invalid delete snapshot option type: %s; allowed: %s",
				*payload.DeleteSnapshots, azblob.PossibleDeleteSnapshotsOptionTypeValues())
		}
	}

	concurrency, err := getConcurrency(payload.Concurrency)
	if err != nil {
		return nil, err
	}

	names, err := a.resolveBlobs(ctx, payload.Prefix, payload.BlobNames)
	if err != nil {
		return nil, err
	}

	if len(names) == 0 {
		return nil, ErrMissingBulkDeleteSelection
	}

	// Build a name→indices map to handle duplicates correctly.
	nameIndices := make(map[string][]int, len(names))
	results := make([]bulkDeleteResponseItem, len(names))
	for i, name := range names {
		results[i].BlobName = name
		nameIndices[name] = append(nameIndices[name], i)
	}

	var batchDeleteOpts *container.BatchDeleteOptions
	if payload.DeleteSnapshots != nil && *payload.DeleteSnapshots != "" {
		dsOpt := azblob.DeleteSnapshotsOptionType(*payload.DeleteSnapshots)
		batchDeleteOpts = &container.BatchDeleteOptions{
			DeleteOptions: blob.DeleteOptions{
				DeleteSnapshots: &dsOpt,
			},
		}
	}

	// Try batch API first (more efficient), fall back to individual deletes on error.
	// completedCount tracks how many names were successfully processed by batch chunks,
	// so the fallback only retries the remaining names.
	completedCount, batchErr := a.bulkDeleteBatch(ctx, names, nameIndices, results, batchDeleteOpts)
	if batchErr != nil {
		// Batch API may fail (e.g. SAS auth without proper permissions).
		// Fall back to concurrent individual deletes for remaining names only.
		a.logger.Warnf("batch delete failed after %d/%d items, falling back to individual deletes: %v", completedCount, len(names), batchErr)

		remainingNames := names[completedCount:]
		remainingOffset := completedCount

		deleteOptions := blob.DeleteOptions{
			AccessConditions: &blob.AccessConditions{},
		}
		if batchDeleteOpts != nil {
			deleteOptions.DeleteSnapshots = batchDeleteOpts.DeleteSnapshots
		}

		g, gctx := errgroup.WithContext(ctx)
		g.SetLimit(concurrency)

		for j, name := range remainingNames {
			resultIdx := remainingOffset + j
			g.Go(func() error {
				blockBlobClient := a.containerClient.NewBlockBlobClient(name)
				if _, delErr := blockBlobClient.Delete(gctx, &deleteOptions); delErr != nil {
					if bloberror.HasCode(delErr, bloberror.BlobNotFound) {
						results[resultIdx].Error = "blob not found"
					} else {
						results[resultIdx].Error = delErr.Error()
					}
				}
				return nil
			})
		}

		if waitErr := g.Wait(); waitErr != nil {
			return nil, fmt.Errorf("error in bulkDelete: %w", waitErr)
		}
	}

	jsonResponse, err := json.Marshal(results)
	if err != nil {
		return nil, fmt.Errorf("error marshalling bulkDelete response: %w", err)
	}

	return &bindings.InvokeResponse{
		Data: jsonResponse,
		Metadata: map[string]string{
			metadataKeyNumber: strconv.Itoa(len(results)),
		},
	}, nil
}

// bulkDeleteBatch uses the Azure Blob Batch API to delete blobs in chunks of 256.
// Returns the number of items in completed chunks (for fallback offset) and any error.
func (a *AzureBlobStorage) bulkDeleteBatch(ctx context.Context, names []string, nameIndices map[string][]int, results []bulkDeleteResponseItem, opts *container.BatchDeleteOptions) (int, error) {
	for batchStart := 0; batchStart < len(names); batchStart += maxBatchDeleteSize {
		batchEnd := batchStart + maxBatchDeleteSize
		if batchEnd > len(names) {
			batchEnd = len(names)
		}
		batch := names[batchStart:batchEnd]

		bb, bbErr := a.containerClient.NewBatchBuilder()
		if bbErr != nil {
			return batchStart, fmt.Errorf("error creating batch builder: %w", bbErr)
		}

		for _, name := range batch {
			if delErr := bb.Delete(name, opts); delErr != nil {
				return batchStart, fmt.Errorf("error adding delete to batch for %q: %w", name, delErr)
			}
		}

		resp, submitErr := a.containerClient.SubmitBatch(ctx, bb, nil)
		if submitErr != nil {
			return batchStart, fmt.Errorf("error submitting batch delete: %w", submitErr)
		}

		// Map batch responses back to results.
		for _, item := range resp.Responses {
			if item.BlobName == nil {
				continue
			}
			if item.Error != nil {
				for _, idx := range nameIndices[*item.BlobName] {
					results[idx].Error = item.Error.Error()
				}
			}
		}
	}

	return len(names), nil
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
	case bulkGetOperation:
		return a.bulkGet(ctx, req)
	case bulkCreateOperation:
		return a.bulkCreate(ctx, req)
	case bulkDeleteOperation:
		return a.bulkDelete(ctx, req)
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
