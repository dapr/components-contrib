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
	"context"
	"crypto/tls"
	b64 "encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"reflect"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/google/uuid"

	awsCommon "github.com/dapr/components-contrib/common/aws"
	awsCommonAuth "github.com/dapr/components-contrib/common/aws/auth"

	"github.com/dapr/components-contrib/bindings"
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

	defaultMaxResults = 1000
	presignOperation  = "presign"
)

// AWSS3 is a binding for an AWS S3 storage bucket.
type AWSS3 struct {
	metadata *s3Metadata
	logger   logger.Logger

	s3Client *s3.Client

	s3Downloader    *manager.Downloader
	s3Uploader      *manager.Uploader
	s3PresignClient *s3.PresignClient
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

	opts := awsCommonAuth.Options{
		Logger: s.logger,

		Properties: metadata.Properties,

		Region:       m.Region,
		Endpoint:     m.Endpoint,
		AccessKey:    m.AccessKey,
		SecretKey:    m.SecretKey,
		SessionToken: m.SessionToken,

		// TODO: Add ARN provider config support
	}

	awsConfig, err := awsCommon.NewConfig(ctx, opts)
	if err != nil {
		return fmt.Errorf("s3 binding error: cannot create AWS config: %w", err)
	}

	var s3Options []func(*s3.Options)

	if m.ForcePathStyle {
		s3Options = append(s3Options, func(o *s3.Options) {
			o.UsePathStyle = true
		})
	}

	if m.DisableSSL {
		awsConfig.HTTPClient = &http.Client{
			Transport: &http.Transport{
				//nolint:gosec
				TLSClientConfig: &tls.Config{},
			},
		}
	}

	if m.InsecureSSL {
		customTransport := &http.Transport{}
		if awsConfig.HTTPClient != nil {
			if client, ok := awsConfig.HTTPClient.(*http.Client); ok {
				if t, ok := client.Transport.(*http.Transport); ok {
					customTransport = t.Clone()
				}
			}
		}
		if customTransport.TLSClientConfig == nil {
			//nolint:gosec
			customTransport.TLSClientConfig = &tls.Config{}
		}
		customTransport.TLSClientConfig.InsecureSkipVerify = true
		awsConfig.HTTPClient = &http.Client{
			Transport: customTransport,
		}
		s.logger.Infof("aws s3: you are using 'insecureSSL' to skip server config verify which is unsafe!")
	}

	if m.Endpoint != "" {
		s3Options = append(s3Options, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(m.Endpoint)
		})
	}

	s.s3Client = s3.NewFromConfig(awsConfig, s3Options...)

	if s.s3Client == nil {
		return errors.New("s3 binding error: cannot create S3 client")
	}

	s.s3Downloader = manager.NewDownloader(s.s3Client)
	s.s3Uploader = manager.NewUploader(s.s3Client)
	s.s3PresignClient = s3.NewPresignClient(s.s3Client)

	return nil
}

func (s *AWSS3) Close() error {
	// No resources to close
	return nil
}

func (s *AWSS3) Operations() []bindings.OperationKind {
	return []bindings.OperationKind{
		bindings.CreateOperation,
		bindings.GetOperation,
		bindings.DeleteOperation,
		bindings.ListOperation,
		presignOperation,
	}
}

func (s *AWSS3) create(ctx context.Context, req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	md, err := s.metadata.mergeWithRequestMetadata(req)
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
	if md.FilePath != "" {
		r, err = os.Open(md.FilePath)
		if err != nil {
			return nil, fmt.Errorf("s3 binding error: file read error: %w", err)
		}
	} else {
		r = strings.NewReader(commonutils.Unquote(req.Data))
	}

	if md.DecodeBase64 {
		r = b64.NewDecoder(b64.StdEncoding, r)
	}

	var storageClass types.StorageClass
	if md.StorageClass != "" {
		storageClass = types.StorageClass(md.StorageClass)
	}

	resultUpload, err := s.s3Uploader.Upload(ctx, &s3.PutObjectInput{
		Bucket:       ptr.Of(md.Bucket),
		Key:          ptr.Of(key),
		Body:         r,
		ContentType:  contentType,
		StorageClass: storageClass,
		Tagging:      tagging,
	})
	if err != nil {
		return nil, fmt.Errorf("s3 binding error: uploading failed: %w", err)
	}

	var presignURL string
	if md.PresignTTL != "" {
		url, presignErr := s.presignObject(ctx, md.Bucket, key, md.PresignTTL)
		if presignErr != nil {
			return nil, fmt.Errorf("s3 binding error: %s", presignErr)
		}

		presignURL = url
	}

	jsonResponse, err := json.Marshal(createResponse{
		Location:   resultUpload.Location,
		VersionID:  resultUpload.VersionID,
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
	md, err := s.metadata.mergeWithRequestMetadata(req)
	if err != nil {
		return nil, fmt.Errorf("s3 binding error: error merging metadata: %w", err)
	}

	key := req.Metadata[metadataKey]
	if key == "" {
		return nil, fmt.Errorf("s3 binding error: required metadata '%s' missing", metadataKey)
	}

	if md.PresignTTL == "" {
		return nil, fmt.Errorf("s3 binding error: required metadata '%s' missing", metadataPresignTTL)
	}

	url, err := s.presignObject(ctx, md.Bucket, key, md.PresignTTL)
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

	presigned, err := s.s3PresignClient.PresignGetObject(ctx, &s3.GetObjectInput{
		Bucket: ptr.Of(bucket),
		Key:    ptr.Of(key),
	}, func(po *s3.PresignOptions) {
		po.Expires = d
	})
	if err != nil {
		return "", fmt.Errorf("s3 binding error: failed to presign URL: %w", err)
	}

	return presigned.URL, nil
}

func (s *AWSS3) get(ctx context.Context, req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	md, err := s.metadata.mergeWithRequestMetadata(req)
	if err != nil {
		return nil, fmt.Errorf("s3 binding error: error merging metadata : %w", err)
	}

	key := req.Metadata[metadataKey]
	if key == "" {
		return nil, fmt.Errorf("s3 binding error: required metadata '%s' missing", metadataKey)
	}

	buff := manager.WriteAtBuffer{}
	_, err = s.s3Downloader.Download(ctx,
		&buff,
		&s3.GetObjectInput{
			Bucket: ptr.Of(s.metadata.Bucket),
			Key:    ptr.Of(key),
		},
	)
	if err != nil {
		var noSuchKey *types.NoSuchKey
		if errors.As(err, &noSuchKey) {
			return nil, errors.New("object not found")
		}
		return nil, fmt.Errorf("s3 binding error: error downloading S3 object: %w", err)
	}

	var data []byte
	if md.EncodeBase64 {
		encoded := b64.StdEncoding.EncodeToString(buff.Bytes())
		data = []byte(encoded)
	} else {
		data = buff.Bytes()
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
	_, err := s.s3Client.DeleteObject(
		ctx,
		&s3.DeleteObjectInput{
			Bucket: ptr.Of(s.metadata.Bucket),
			Key:    ptr.Of(key),
		},
	)
	if err != nil {
		var noSuchKey *types.NoSuchKey
		if errors.As(err, &noSuchKey) {
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
		s.logger.Errorf("s3 binding warning: list operation: maxResults must be between 1 and %d. Using default value %d", math.MaxInt32, defaultMaxResults)
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

	return aws.String(strings.Join(pairs, "&")), nil
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
	err := metadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, metadata.BindingType)
	if err != nil {
		s.logger.Warnf("error getting metadata info: %v", err)
	}
	return
}
