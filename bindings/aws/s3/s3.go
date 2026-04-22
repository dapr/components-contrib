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

package s3

import (
	"bytes"
	"context"
	"crypto/tls"
	b64 "encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/google/uuid"

	"github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager"
	tmtypes "github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager/types"

	"github.com/dapr/components-contrib/bindings"
	awsCommon "github.com/dapr/components-contrib/common/aws"
	awsCommonAuth "github.com/dapr/components-contrib/common/aws/auth"
	commonutils "github.com/dapr/components-contrib/common/utils"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
	kitmd "github.com/dapr/kit/metadata"
	"github.com/dapr/kit/ptr"
	kitstrings "github.com/dapr/kit/strings"
)

const (
	metadataDecodeBase64 = "decodeBase64"
	metadataEncodeBase64 = "encodeBase64"
	metadataFilePath     = "filePath"
	metadataPresignTTL   = "presignTTL"
	metadataStorageClass = "storageClass"
	metadataTags         = "tags"

	metatadataContentType = "Content-Type"
	metadataKey           = "key"

	defaultMaxResults      = 1000
	presignOperation       = "presign"
	bulkGetOperation       = "bulkGet"
	bulkCreateOperation    = "bulkCreate"
	bulkDeleteOperation    = "bulkDelete"
	defaultBulkConcurrency = 10
	maxDeleteBatchSize     = 1000
)

// AWSS3 is a binding for an AWS S3 storage bucket.
type AWSS3 struct {
	metadata  *s3Metadata
	s3Client  *s3.Client
	tmClient  *transfermanager.Client
	presigner *s3.PresignClient
	logger    logger.Logger
}

type s3Metadata struct {
	// Ignored by metadata parser because included in built-in authentication profile
	AccessKey    string `json:"accessKey" mapstructure:"accessKey" mdignore:"true"`
	SecretKey    string `json:"secretKey" mapstructure:"secretKey" mdignore:"true"`
	SessionToken string `json:"sessionToken" mapstructure:"sessionToken" mdignore:"true"`

	Region         string `json:"region" mapstructure:"region" mapstructurealiases:"awsRegion" mdignore:"true"`
	Endpoint       string `json:"endpoint" mapstructure:"endpoint"`
	Bucket         string `json:"bucket" mapstructure:"bucket"`
	DecodeBase64   bool   `json:"decodeBase64,string" mapstructure:"decodeBase64"`
	EncodeBase64   bool   `json:"encodeBase64,string" mapstructure:"encodeBase64"`
	ForcePathStyle bool   `json:"forcePathStyle,string" mapstructure:"forcePathStyle"`
	DisableSSL     bool   `json:"disableSSL,string" mapstructure:"disableSSL"`
	InsecureSSL    bool   `json:"insecureSSL,string" mapstructure:"insecureSSL"`
	FilePath       string `json:"filePath" mapstructure:"filePath"   mdignore:"true"`
	PresignTTL     string `json:"presignTTL" mapstructure:"presignTTL"  mdignore:"true"`
	StorageClass   string `json:"storageClass" mapstructure:"storageClass"  mdignore:"true"`
}

type createResponse struct {
	Location   string  `json:"location"`
	VersionID  *string `json:"versionID"`
	PresignURL string  `json:"presignURL,omitempty"`
}

type presignResponse struct {
	PresignURL string `json:"presignURL"`
}

type listPayload struct {
	Marker     string `json:"marker"`
	Prefix     string `json:"prefix"`
	MaxResults int32  `json:"maxResults"`
	Delimiter  string `json:"delimiter"`
}

type bulkGetItem struct {
	Key      string  `json:"key"`
	FilePath *string `json:"filePath,omitempty"`
}

type bulkGetPayload struct {
	Items       []bulkGetItem `json:"items"`
	Concurrency *int          `json:"concurrency,omitempty"`
}

type bulkCreateItem struct {
	Key         string  `json:"key"`
	Data        *string `json:"data,omitempty"`
	ContentType *string `json:"contentType,omitempty"`
	FilePath    *string `json:"filePath,omitempty"`
}

type bulkCreatePayload struct {
	Items       []bulkCreateItem `json:"items"`
	Concurrency *int             `json:"concurrency,omitempty"`
}

type bulkDeletePayload struct {
	Keys []string `json:"keys"`
}

type bulkItemResult struct {
	Key      string `json:"key"`
	Data     string `json:"data,omitempty"`
	Location string `json:"location,omitempty"`
	Error    string `json:"error,omitempty"`
}

// NewAWSS3 returns a new AWSS3 instance.
func NewAWSS3(logger logger.Logger) bindings.OutputBinding {
	return &AWSS3{logger: logger}
}

// Init does metadata parsing and connection creation.
func (s *AWSS3) Init(ctx context.Context, metadata bindings.Metadata) error {
	m, err := s.parseMetadata(metadata)
	if err != nil {
		return err
	}
	s.metadata = m

	if s.metadata.DisableSSL && s.metadata.Endpoint != "" && !strings.HasPrefix(s.metadata.Endpoint, "http://") && !strings.HasPrefix(s.metadata.Endpoint, "https://") {
		s.metadata.Endpoint = "http://" + s.metadata.Endpoint
	}

	configOpts := awsCommonAuth.Options{
		Logger:       s.logger,
		Properties:   metadata.Properties,
		Region:       m.Region,
		Endpoint:     m.Endpoint,
		AccessKey:    m.AccessKey,
		SecretKey:    m.SecretKey,
		SessionToken: m.SessionToken,
	}

	var awsCfg aws.Config
	if s.metadata.InsecureSSL {
		customTransport := http.DefaultTransport.(*http.Transport).Clone()
		customTransport.TLSClientConfig = &tls.Config{
			//nolint:gosec
			InsecureSkipVerify: true,
		}
		client := &http.Client{Transport: customTransport}
		awsCfg, err = awsCommon.NewConfig(ctx, configOpts, awsCommon.WithHTTPClient(client))
		if err == nil {
			s.logger.Infof("aws s3: you are using 'insecureSSL' to skip server config verify which is unsafe!")
		}
	} else {
		awsCfg, err = awsCommon.NewConfig(ctx, configOpts)
	}
	if err != nil {
		return err
	}

	s.s3Client = s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.UsePathStyle = s.metadata.ForcePathStyle
	})
	// transfermanager for multipart/managed uploads
	s.tmClient = transfermanager.New(s.s3Client)
	s.presigner = s3.NewPresignClient(s.s3Client)

	return nil
}

func (s *AWSS3) Close() error {
	// nothing to cleanup
	return nil
}

func (s *AWSS3) Operations() []bindings.OperationKind {
	return []bindings.OperationKind{
		bindings.CreateOperation,
		bindings.GetOperation,
		bindings.DeleteOperation,
		bindings.ListOperation,
		presignOperation,
		bulkGetOperation,
		bulkCreateOperation,
		bulkDeleteOperation,
	}
}

func (s *AWSS3) create(ctx context.Context, req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	metadata, err := s.metadata.mergeWithRequestMetadata(req)
	if err != nil {
		return nil, fmt.Errorf("s3 binding error: error merging metadata: %w", err)
	}

	key := req.Metadata[metadataKey]
	if key == "" {
		var u uuid.UUID
		u, err = uuid.NewRandom()
		if err != nil {
			return nil, fmt.Errorf("s3 binding error: failed to generate UUID: %w", err)
		}
		key = u.String()
		s.logger.Debugf("s3 binding error: key not found. generating key %s", key)
	}

	var contentType *string
	contentTypeStr := strings.TrimSpace(req.Metadata[metatadataContentType])
	if contentTypeStr != "" {
		contentType = &contentTypeStr
	}

	var tagging *string
	if rawTags, ok := req.Metadata[metadataTags]; ok {
		tagging, err = s.parseS3Tags(rawTags)
		if err != nil {
			return nil, fmt.Errorf("s3 binding error: parsing tags falied error: %w", err)
		}
	}

	var r io.Reader
	if metadata.FilePath != "" {
		r, err = os.Open(metadata.FilePath)
		if err != nil {
			return nil, fmt.Errorf("s3 binding error: file read error: %w", err)
		}
	} else {
		r = strings.NewReader(commonutils.Unquote(req.Data))
	}

	if metadata.DecodeBase64 {
		r = b64.NewDecoder(b64.StdEncoding, r)
	}

	var storageClass *string
	if metadata.StorageClass != "" {
		storageClass = aws.String(metadata.StorageClass)
	}

	uploadIn := &transfermanager.UploadObjectInput{
		Bucket: ptr.Of(metadata.Bucket),
		Key:    ptr.Of(key),
		Body:   r,
	}
	if contentType != nil {
		uploadIn.ContentType = contentType
	}
	if storageClass != nil {
		uploadIn.StorageClass = tmtypes.StorageClass(*storageClass)
	}
	if tagging != nil {
		uploadIn.Tagging = tagging
	}

	uploadOut, err := s.tmClient.UploadObject(ctx, uploadIn)
	if err != nil {
		return nil, fmt.Errorf("s3 binding error: uploading failed: %w", err)
	}

	var uploadLocation *string
	if uploadOut != nil {
		uploadLocation = uploadOut.Location
	}
	resultLocation := s.buildLocation(uploadLocation, metadata.Bucket, key)
	var versionID *string
	if uploadOut != nil {
		versionID = uploadOut.VersionID
	}
	var presignURL string
	if metadata.PresignTTL != "" {
		url, presignErr := s.presignObject(ctx, metadata.Bucket, key, metadata.PresignTTL)
		if presignErr != nil {
			return nil, fmt.Errorf("s3 binding error: %s", presignErr)
		}

		presignURL = url
	}

	jsonResponse, err := json.Marshal(createResponse{
		Location:   resultLocation,
		VersionID:  versionID,
		PresignURL: presignURL,
	})
	if err != nil {
		return nil, fmt.Errorf("s3 binding error: error marshalling create response: %w", err)
	}

	return &bindings.InvokeResponse{
		Data: jsonResponse,
		Metadata: map[string]string{
			metadataKey: key,
		},
	}, nil
}

func (s *AWSS3) presign(ctx context.Context, req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	metadata, err := s.metadata.mergeWithRequestMetadata(req)
	if err != nil {
		return nil, fmt.Errorf("s3 binding error: error merging metadata: %w", err)
	}

	key := req.Metadata[metadataKey]
	if key == "" {
		return nil, fmt.Errorf("s3 binding error: required metadata '%s' missing", metadataKey)
	}

	if metadata.PresignTTL == "" {
		return nil, fmt.Errorf("s3 binding error: required metadata '%s' missing", metadataPresignTTL)
	}

	url, err := s.presignObject(ctx, metadata.Bucket, key, metadata.PresignTTL)
	if err != nil {
		return nil, fmt.Errorf("s3 binding error: %w", err)
	}

	jsonResponse, err := json.Marshal(presignResponse{
		PresignURL: url,
	})
	if err != nil {
		return nil, fmt.Errorf("s3 binding error: error marshalling presign response: %w", err)
	}

	return &bindings.InvokeResponse{
		Data: jsonResponse,
	}, nil
}

func (s *AWSS3) presignObject(ctx context.Context, bucket, key, ttl string) (string, error) {
	d, err := time.ParseDuration(ttl)
	if err != nil {
		return "", fmt.Errorf("s3 binding error: cannot parse duration %s: %w", ttl, err)
	}
	presignResult, err := s.presigner.PresignGetObject(ctx, &s3.GetObjectInput{
		Bucket: ptr.Of(bucket),
		Key:    ptr.Of(key),
	}, func(po *s3.PresignOptions) { po.Expires = d })
	if err != nil {
		return "", fmt.Errorf("s3 binding error: failed to presign URL: %w", err)
	}
	return presignResult.URL, nil
}

func (s *AWSS3) get(ctx context.Context, req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	metadata, err := s.metadata.mergeWithRequestMetadata(req)
	if err != nil {
		return nil, fmt.Errorf("s3 binding error: error merging metadata : %w", err)
	}

	key := req.Metadata[metadataKey]
	if key == "" {
		return nil, fmt.Errorf("s3 binding error: required metadata '%s' missing", metadataKey)
	}

	resp, err := s.s3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: ptr.Of(s.metadata.Bucket),
		Key:    ptr.Of(key),
	})
	if err != nil {
		var apiErr smithy.APIError
		if errors.As(err, &apiErr) && apiErr.ErrorCode() == "NoSuchKey" {
			return nil, errors.New("object not found")
		}
		return nil, fmt.Errorf("s3 binding error: error downloading S3 object: %w", err)
	}
	defer resp.Body.Close()

	dataBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("s3 binding error: error reading S3 object body: %w", err)
	}

	var data []byte
	if metadata.EncodeBase64 {
		encoded := b64.StdEncoding.EncodeToString(dataBytes)
		data = []byte(encoded)
	} else {
		data = dataBytes
	}

	return &bindings.InvokeResponse{
		Data:     data,
		Metadata: nil,
	}, nil
}

func (s *AWSS3) delete(ctx context.Context, req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	key := req.Metadata[metadataKey]
	if key == "" {
		return nil, fmt.Errorf("s3 binding error: required metadata '%s' missing", metadataKey)
	}
	_, err := s.s3Client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: ptr.Of(s.metadata.Bucket),
		Key:    ptr.Of(key),
	})
	if err != nil {
		var apiErr smithy.APIError
		if errors.As(err, &apiErr) && apiErr.ErrorCode() == "NoSuchKey" {
			return nil, errors.New("object not found")
		}
		return nil, fmt.Errorf("s3 binding error: delete operation failed: %w", err)
	}

	return nil, nil
}

func (s *AWSS3) list(ctx context.Context, req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	payload := listPayload{}
	if req.Data != nil {
		if err := json.Unmarshal(req.Data, &payload); err != nil {
			return nil, fmt.Errorf("s3 binding (List Operation) - unable to parse Data property - %v", err)
		}
	}

	if payload.MaxResults < 1 {
		payload.MaxResults = defaultMaxResults
	}
	result, err := s.s3Client.ListObjects(ctx, &s3.ListObjectsInput{
		Bucket:    ptr.Of(s.metadata.Bucket),
		MaxKeys:   ptr.Of(payload.MaxResults),
		Marker:    ptr.Of(payload.Marker),
		Prefix:    ptr.Of(payload.Prefix),
		Delimiter: ptr.Of(payload.Delimiter),
	})
	if err != nil {
		return nil, fmt.Errorf("s3 binding error: list operation failed: %w", err)
	}

	jsonResponse, err := json.Marshal(result)
	if err != nil {
		return nil, fmt.Errorf("s3 binding error: list operation: cannot marshal list to json: %w", err)
	}

	return &bindings.InvokeResponse{
		Data: jsonResponse,
	}, nil
}

// resolveConcurrency validates and resolves the concurrency pointer.
// Returns defaultBulkConcurrency if nil, or an error if the value is <= 0.
func resolveConcurrency(c *int) (int, error) {
	if c == nil {
		return defaultBulkConcurrency, nil
	}
	if *c <= 0 {
		return 0, fmt.Errorf("s3 binding error: concurrency must be greater than 0, got %d", *c)
	}
	return *c, nil
}

// buildLocation constructs the object URL from the upload output or falls back to endpoint/bucket/key conventions.
func (s *AWSS3) buildLocation(uploadLocation *string, bucket, key string) string {
	if uploadLocation != nil && *uploadLocation != "" {
		return *uploadLocation
	}
	if s.metadata.Endpoint != "" {
		ep := strings.TrimRight(s.metadata.Endpoint, "/")
		return fmt.Sprintf("%s/%s/%s", ep, bucket, key)
	}
	if s.metadata.ForcePathStyle {
		return fmt.Sprintf("https://s3.amazonaws.com/%s/%s", bucket, key)
	}
	return fmt.Sprintf("https://%s.s3.amazonaws.com/%s", bucket, key)
}

func (s *AWSS3) bulkGet(ctx context.Context, req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	metadata, err := s.metadata.mergeWithRequestMetadata(req)
	if err != nil {
		return nil, fmt.Errorf("s3 binding error: error merging metadata: %w", err)
	}

	var payload bulkGetPayload
	if err = json.Unmarshal(req.Data, &payload); err != nil {
		return nil, fmt.Errorf("s3 binding error: invalid bulkGet payload: %w", err)
	}
	if len(payload.Items) == 0 {
		return nil, errors.New("s3 binding error: bulkGet requires at least one item")
	}

	concurrency, err := resolveConcurrency(payload.Concurrency)
	if err != nil {
		return nil, err
	}

	results := make([]bulkItemResult, len(payload.Items))
	sem := make(chan struct{}, concurrency)
	var wg sync.WaitGroup

	for i, item := range payload.Items {
		if item.Key == "" {
			results[i] = bulkItemResult{Key: "", Error: "key is required"}
			continue
		}

		wg.Add(1)
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			results[i] = bulkItemResult{Key: item.Key, Error: ctx.Err().Error()}
			wg.Done()
			continue
		}
		go func(idx int, it bulkGetItem) {
			defer wg.Done()
			defer func() { <-sem }()

			resp, gerr := s.s3Client.GetObject(ctx, &s3.GetObjectInput{
				Bucket: ptr.Of(metadata.Bucket),
				Key:    ptr.Of(it.Key),
			})
			if gerr != nil {
				var apiErr smithy.APIError
				if errors.As(gerr, &apiErr) && apiErr.ErrorCode() == "NoSuchKey" {
					results[idx] = bulkItemResult{Key: it.Key, Error: "object not found"}
				} else {
					results[idx] = bulkItemResult{Key: it.Key, Error: gerr.Error()}
				}
				return
			}
			defer resp.Body.Close()

			// If filePath is specified, stream directly to file
			if it.FilePath != nil {
				f, ferr := os.Create(*it.FilePath)
				if ferr != nil {
					results[idx] = bulkItemResult{Key: it.Key, Error: ferr.Error()}
					return
				}
				defer f.Close()

				var w io.Writer = f
				if metadata.EncodeBase64 {
					encoder := b64.NewEncoder(b64.StdEncoding, f)
					w = encoder
					defer func() {
						if cerr := encoder.Close(); cerr != nil && results[idx].Error == "" {
							results[idx].Error = cerr.Error()
						}
					}()
				}

				if _, gerr = io.Copy(w, resp.Body); gerr != nil {
					results[idx] = bulkItemResult{Key: it.Key, Error: gerr.Error()}
					return
				}
				results[idx] = bulkItemResult{Key: it.Key}
				return
			}

			// No filePath: stream into buffer with optional base64 encoding
			var buf bytes.Buffer
			if metadata.EncodeBase64 {
				encoder := b64.NewEncoder(b64.StdEncoding, &buf)
				if _, gerr = io.Copy(encoder, resp.Body); gerr != nil {
					results[idx] = bulkItemResult{Key: it.Key, Error: gerr.Error()}
					return
				}
				if gerr = encoder.Close(); gerr != nil {
					results[idx] = bulkItemResult{Key: it.Key, Error: gerr.Error()}
					return
				}
			} else {
				if _, gerr = io.Copy(&buf, resp.Body); gerr != nil {
					results[idx] = bulkItemResult{Key: it.Key, Error: gerr.Error()}
					return
				}
			}

			results[idx] = bulkItemResult{Key: it.Key, Data: buf.String()}
		}(i, item)
	}
	wg.Wait()

	jsonResponse, err := json.Marshal(results)
	if err != nil {
		return nil, fmt.Errorf("s3 binding error: error marshalling bulkGet response: %w", err)
	}

	return &bindings.InvokeResponse{Data: jsonResponse}, nil
}

func (s *AWSS3) bulkCreate(ctx context.Context, req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	metadata, err := s.metadata.mergeWithRequestMetadata(req)
	if err != nil {
		return nil, fmt.Errorf("s3 binding error: error merging metadata: %w", err)
	}

	var payload bulkCreatePayload
	if err = json.Unmarshal(req.Data, &payload); err != nil {
		return nil, fmt.Errorf("s3 binding error: invalid bulkCreate payload: %w", err)
	}
	if len(payload.Items) == 0 {
		return nil, errors.New("s3 binding error: bulkCreate requires at least one item")
	}

	concurrency, err := resolveConcurrency(payload.Concurrency)
	if err != nil {
		return nil, err
	}

	results := make([]bulkItemResult, len(payload.Items))
	sem := make(chan struct{}, concurrency)
	var wg sync.WaitGroup

	for i, item := range payload.Items {
		if item.Key == "" {
			results[i] = bulkItemResult{Key: "", Error: "key is required"}
			continue
		}

		wg.Add(1)
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			results[i] = bulkItemResult{Key: item.Key, Error: ctx.Err().Error()}
			wg.Done()
			continue
		}
		go func(idx int, it bulkCreateItem) {
			defer wg.Done()
			defer func() { <-sem }()

			var r io.Reader
			if it.FilePath != nil {
				f, ferr := os.Open(*it.FilePath)
				if ferr != nil {
					results[idx] = bulkItemResult{Key: it.Key, Error: ferr.Error()}
					return
				}
				defer f.Close()
				r = f
			} else if it.Data != nil {
				r = strings.NewReader(*it.Data)
			} else {
				results[idx] = bulkItemResult{Key: it.Key, Error: "either data or filePath is required"}
				return
			}

			if metadata.DecodeBase64 {
				r = b64.NewDecoder(b64.StdEncoding, r)
			}

			uploadIn := &transfermanager.UploadObjectInput{
				Bucket: ptr.Of(metadata.Bucket),
				Key:    ptr.Of(it.Key),
				Body:   r,
			}
			if it.ContentType != nil {
				uploadIn.ContentType = it.ContentType
			}

			uploadOut, uerr := s.tmClient.UploadObject(ctx, uploadIn)
			if uerr != nil {
				results[idx] = bulkItemResult{Key: it.Key, Error: uerr.Error()}
				return
			}

			var uploadLocation *string
			if uploadOut != nil {
				uploadLocation = uploadOut.Location
			}
			results[idx] = bulkItemResult{Key: it.Key, Location: s.buildLocation(uploadLocation, metadata.Bucket, it.Key)}
		}(i, item)
	}
	wg.Wait()

	jsonResponse, err := json.Marshal(results)
	if err != nil {
		return nil, fmt.Errorf("s3 binding error: error marshalling bulkCreate response: %w", err)
	}

	return &bindings.InvokeResponse{Data: jsonResponse}, nil
}

func (s *AWSS3) bulkDelete(ctx context.Context, req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	metadata, err := s.metadata.mergeWithRequestMetadata(req)
	if err != nil {
		return nil, fmt.Errorf("s3 binding error: error merging metadata: %w", err)
	}

	var payload bulkDeletePayload
	if err = json.Unmarshal(req.Data, &payload); err != nil {
		return nil, fmt.Errorf("s3 binding error: invalid bulkDelete payload: %w", err)
	}
	if len(payload.Keys) == 0 {
		return nil, errors.New("s3 binding error: bulkDelete requires at least one key")
	}

	results := make([]bulkItemResult, len(payload.Keys))
	// Initialize results and validate keys; track valid keys for batching Build
	// a key->indices map to handle duplicates correctly
	keyIndices := make(map[string][]int, len(payload.Keys))
	validKeys := make([]string, 0, len(payload.Keys))
	for i, key := range payload.Keys {
		results[i] = bulkItemResult{Key: key}
		if strings.TrimSpace(key) == "" {
			results[i].Error = "key is required"
			continue
		}
		if _, exists := keyIndices[key]; !exists {
			validKeys = append(validKeys, key)
		}
		keyIndices[key] = append(keyIndices[key], i)
	}

	// Process valid keys in batches of 1000 (S3 DeleteObjects limit)
	for batchStart := 0; batchStart < len(validKeys); batchStart += maxDeleteBatchSize {
		batchEnd := batchStart + maxDeleteBatchSize
		if batchEnd > len(validKeys) {
			batchEnd = len(validKeys)
		}
		batch := validKeys[batchStart:batchEnd]

		objects := make([]s3types.ObjectIdentifier, len(batch))
		for i, key := range batch {
			objects[i] = s3types.ObjectIdentifier{Key: ptr.Of(key)}
		}

		var output *s3.DeleteObjectsOutput
		output, err = s.s3Client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
			Bucket: ptr.Of(metadata.Bucket),
			Delete: &s3types.Delete{
				Objects: objects,
				Quiet:   ptr.Of(false),
			},
		})
		if err != nil {
			// If the entire batch call failed, mark all keys in this batch as failed
			for _, key := range batch {
				for _, idx := range keyIndices[key] {
					results[idx].Error = err.Error()
				}
			}
			continue
		}

		// Map individual errors from S3 response to all matching indices
		if output != nil {
			for _, s3err := range output.Errors {
				if s3err.Key != nil {
					msg := "unknown error"
					if s3err.Message != nil {
						msg = *s3err.Message
					}
					for _, idx := range keyIndices[*s3err.Key] {
						results[idx].Error = msg
					}
				}
			}
		}
	}

	jsonResponse, err := json.Marshal(results)
	if err != nil {
		return nil, fmt.Errorf("s3 binding error: error marshalling bulkDelete response: %w", err)
	}

	return &bindings.InvokeResponse{Data: jsonResponse}, nil
}

func (s *AWSS3) Invoke(ctx context.Context, req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	switch req.Operation {
	case bindings.CreateOperation:
		return s.create(ctx, req)
	case bindings.GetOperation:
		return s.get(ctx, req)
	case bindings.DeleteOperation:
		return s.delete(ctx, req)
	case bindings.ListOperation:
		return s.list(ctx, req)
	case presignOperation:
		return s.presign(ctx, req)
	case bulkGetOperation:
		return s.bulkGet(ctx, req)
	case bulkCreateOperation:
		return s.bulkCreate(ctx, req)
	case bulkDeleteOperation:
		return s.bulkDelete(ctx, req)
	default:
		return nil, fmt.Errorf("s3 binding error: unsupported operation %s", req.Operation)
	}
}

func (s *AWSS3) parseMetadata(md bindings.Metadata) (*s3Metadata, error) {
	var m s3Metadata
	err := kitmd.DecodeMetadata(md.Properties, &m)
	if err != nil {
		return nil, err
	}
	return &m, nil
}

// Helper for parsing s3 tags metadata
func (s *AWSS3) parseS3Tags(raw string) (*string, error) {
	tagEntries := strings.Split(raw, ",")
	pairs := make([]string, 0, len(tagEntries))
	for _, tagEntry := range tagEntries {
		kv := strings.SplitN(strings.TrimSpace(tagEntry), "=", 2)
		isInvalidTag := len(kv) != 2 || strings.TrimSpace(kv[0]) == "" || strings.TrimSpace(kv[1]) == ""
		if isInvalidTag {
			return nil, fmt.Errorf("invalid tag format: '%s' (expected key=value)", tagEntry)
		}
		pairs = append(pairs, fmt.Sprintf("%s=%s", strings.TrimSpace(kv[0]), strings.TrimSpace(kv[1])))
	}

	if len(pairs) == 0 {
		return nil, nil
	}

	return ptr.Of(strings.Join(pairs, "&")), nil
}

// Helper to merge config and request metadata.
func (metadata s3Metadata) mergeWithRequestMetadata(req *bindings.InvokeRequest) (s3Metadata, error) {
	merged := metadata

	if val, ok := req.Metadata[metadataDecodeBase64]; ok && val != "" {
		merged.DecodeBase64 = kitstrings.IsTruthy(val)
	}

	if val, ok := req.Metadata[metadataEncodeBase64]; ok && val != "" {
		merged.EncodeBase64 = kitstrings.IsTruthy(val)
	}

	if val, ok := req.Metadata[metadataFilePath]; ok && val != "" {
		merged.FilePath = val
	}

	if val, ok := req.Metadata[metadataPresignTTL]; ok && val != "" {
		merged.PresignTTL = val
	}

	if val, ok := req.Metadata[metadataStorageClass]; ok && val != "" {
		merged.StorageClass = val
	}

	return merged, nil
}

// GetComponentMetadata returns the metadata of the component.
func (s *AWSS3) GetComponentMetadata() (metadataInfo metadata.MetadataMap) {
	metadataStruct := s3Metadata{}
	metadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, metadata.BindingType)
	return
}
