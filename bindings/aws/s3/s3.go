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
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/google/uuid"

	"github.com/dapr/components-contrib/bindings"
	awsAuth "github.com/dapr/components-contrib/internal/authentication/aws"
	"github.com/dapr/components-contrib/internal/utils"
	"github.com/dapr/kit/logger"
)

const (
	metadataDecodeBase64 = "decodeBase64"
	metadataEncodeBase64 = "encodeBase64"
	metadataFilePath     = "filePath"
	metadataPresignTTL   = "presignTTL"

	metadataKey = "key"

	maxResults       = 1000
	presignOperation = "presign"
)

// AWSS3 is a binding for an AWS S3 storage bucket.
type AWSS3 struct {
	metadata   *s3Metadata
	s3Client   *s3.S3
	uploader   *s3manager.Uploader
	downloader *s3manager.Downloader
	logger     logger.Logger
}

type s3Metadata struct {
	Region         string `json:"region"`
	Endpoint       string `json:"endpoint"`
	AccessKey      string `json:"accessKey"`
	SecretKey      string `json:"secretKey"`
	SessionToken   string `json:"sessionToken"`
	Bucket         string `json:"bucket"`
	DecodeBase64   bool   `json:"decodeBase64,string"`
	EncodeBase64   bool   `json:"encodeBase64,string"`
	ForcePathStyle bool   `json:"forcePathStyle,string"`
	DisableSSL     bool   `json:"disableSSL,string"`
	InsecureSSL    bool   `json:"insecureSSL,string"`
	FilePath       string
	PresignTTL     string
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
func (s *AWSS3) Init(metadata bindings.Metadata) error {
	m, err := s.parseMetadata(metadata)
	if err != nil {
		return err
	}
	session, err := s.getSession(m)
	if err != nil {
		return err
	}

	cfg := aws.NewConfig().
		WithS3ForcePathStyle(m.ForcePathStyle).
		WithDisableSSL(m.DisableSSL)

	// Use a custom HTTP client to allow self-signed certs
	if m.InsecureSSL {
		customTransport := http.DefaultTransport.(*http.Transport).Clone()
		customTransport.TLSClientConfig = &tls.Config{
			//nolint:gosec
			InsecureSkipVerify: true,
		}
		client := &http.Client{
			Transport: customTransport,
		}
		cfg = cfg.WithHTTPClient(client)
	}

	s.metadata = m
	s.s3Client = s3.New(session, cfg)
	s.downloader = s3manager.NewDownloaderWithClient(s.s3Client)
	s.uploader = s3manager.NewUploaderWithClient(s.s3Client)

	return nil
}

func (s *AWSS3) Close() error {
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
	metadata, err := s.metadata.mergeWithRequestMetadata(req)
	if err != nil {
		return nil, fmt.Errorf("s3 binding error: error merging metadata: %w", err)
	}
	var key string
	if val, ok := req.Metadata[metadataKey]; ok && val != "" {
		key = val
	} else {
		key = uuid.New().String()
		s.logger.Debugf("s3 binding: key not found. generating key %s", key)
	}

	d, err := strconv.Unquote(string(req.Data))
	if err == nil {
		req.Data = []byte(d)
	}

	var r io.Reader
	if metadata.FilePath != "" {
		r, err = os.Open(metadata.FilePath)
		if err != nil {
			return nil, fmt.Errorf("s3 file read error: %s", err)
		}
	} else {
		r = bytes.NewReader(req.Data)
	}

	if metadata.DecodeBase64 {
		r = b64.NewDecoder(b64.StdEncoding, r)
	}

	resultUpload, err := s.uploader.UploadWithContext(ctx, &s3manager.UploadInput{
		Bucket: aws.String(metadata.Bucket),
		Key:    aws.String(key),
		Body:   r,
	})
	if err != nil {
		return nil, fmt.Errorf("s3 binding error: Uploading: %w", err)
	}

	var presignURL string
	if metadata.PresignTTL != "" {
		url, presignErr := s.presignObject(metadata.Bucket, key, metadata.PresignTTL)
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
		return nil, fmt.Errorf("s3 binding error: Error marshalling create response: %w", err)
	}

	return &bindings.InvokeResponse{
		Data: jsonResponse,
	}, nil
}

func (s *AWSS3) presign(ctx context.Context, req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	metadata, err := s.metadata.mergeWithRequestMetadata(req)
	if err != nil {
		return nil, fmt.Errorf("s3 binding error: error merging metadata: %w", err)
	}
	var key string
	if val, ok := req.Metadata[metadataKey]; ok && val != "" {
		key = val
	} else {
		return nil, fmt.Errorf("s3 binding error: required metadata '%s' missing", metadataKey)
	}

	if metadata.PresignTTL == "" {
		return nil, fmt.Errorf("s3 binding error: required metadata '%s' missing", metadataPresignTTL)
	}

	url, err := s.presignObject(metadata.Bucket, key, metadata.PresignTTL)
	if err != nil {
		return nil, fmt.Errorf("s3 binding error: %w", err)
	}

	jsonResponse, err := json.Marshal(presignResponse{
		PresignURL: url,
	})
	if err != nil {
		return nil, fmt.Errorf("s3 binding error: Error marshalling presign response: %w", err)
	}

	return &bindings.InvokeResponse{
		Data: jsonResponse,
	}, nil
}

func (s *AWSS3) presignObject(bucket, key, ttl string) (string, error) {
	d, err := time.ParseDuration(ttl)
	if err != nil {
		return "", fmt.Errorf("cannot parse duration %s: %w", ttl, err)
	}

	objReq, _ := s.s3Client.GetObjectRequest(&s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	url, err := objReq.Presign(d)
	if err != nil {
		return "", fmt.Errorf("failed to presign URL: %w", err)
	}

	return url, nil
}

func (s *AWSS3) get(ctx context.Context, req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	metadata, err := s.metadata.mergeWithRequestMetadata(req)
	if err != nil {
		return nil, fmt.Errorf("s3 binding error. error merge metadata : %w", err)
	}

	var key string
	if val, ok := req.Metadata[metadataKey]; ok && val != "" {
		key = val
	} else {
		return nil, fmt.Errorf("s3 binding error: can't read key value")
	}

	buff := &aws.WriteAtBuffer{}

	_, err = s.downloader.DownloadWithContext(ctx,
		buff,
		&s3.GetObjectInput{
			Bucket: aws.String(s.metadata.Bucket),
			Key:    aws.String(key),
		},
	)
	if err != nil {
		return nil, fmt.Errorf("s3 binding error: error downloading S3 object: %w", err)
	}

	var data []byte
	if metadata.EncodeBase64 {
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
	var key string
	if val, ok := req.Metadata[metadataKey]; ok && val != "" {
		key = val
	} else {
		return nil, fmt.Errorf("s3 binding error: can't read key value")
	}

	_, err := s.s3Client.DeleteObjectWithContext(
		ctx,
		&s3.DeleteObjectInput{
			Bucket: aws.String(s.metadata.Bucket),
			Key:    aws.String(key),
		},
	)

	return nil, err
}

func (s *AWSS3) list(ctx context.Context, req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	var payload listPayload
	err := json.Unmarshal(req.Data, &payload)
	if err != nil {
		return nil, err
	}

	if payload.MaxResults == int32(0) {
		payload.MaxResults = maxResults
	}

	result, err := s.s3Client.ListObjectsWithContext(ctx, &s3.ListObjectsInput{
		Bucket:    aws.String(s.metadata.Bucket),
		MaxKeys:   aws.Int64(int64(payload.MaxResults)),
		Marker:    aws.String(payload.Marker),
		Prefix:    aws.String(payload.Prefix),
		Delimiter: aws.String(payload.Delimiter),
	})
	if err != nil {
		return nil, fmt.Errorf("s3 binding error. list operation. cannot marshal blobs to json: %w", err)
	}

	jsonResponse, err := json.Marshal(result)
	if err != nil {
		return nil, fmt.Errorf("s3 binding error. list operation. cannot marshal blobs to json: %w", err)
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
		return nil, fmt.Errorf("s3 binding error. unsupported operation %s", req.Operation)
	}
}

func (s *AWSS3) parseMetadata(metadata bindings.Metadata) (*s3Metadata, error) {
	b, err := json.Marshal(metadata.Properties)
	if err != nil {
		return nil, err
	}

	var m s3Metadata
	err = json.Unmarshal(b, &m)
	if err != nil {
		return nil, err
	}

	return &m, nil
}

func (s *AWSS3) getSession(metadata *s3Metadata) (*session.Session, error) {
	sess, err := awsAuth.GetClient(metadata.AccessKey, metadata.SecretKey, metadata.SessionToken, metadata.Region, metadata.Endpoint)
	if err != nil {
		return nil, err
	}

	return sess, nil
}

// Helper to merge config and request metadata.
func (metadata s3Metadata) mergeWithRequestMetadata(req *bindings.InvokeRequest) (s3Metadata, error) {
	merged := metadata

	if val, ok := req.Metadata[metadataDecodeBase64]; ok && val != "" {
		merged.DecodeBase64 = utils.IsTruthy(val)
	}

	if val, ok := req.Metadata[metadataEncodeBase64]; ok && val != "" {
		merged.EncodeBase64 = utils.IsTruthy(val)
	}

	if val, ok := req.Metadata[metadataFilePath]; ok && val != "" {
		merged.FilePath = val
	}

	if val, ok := req.Metadata[metadataPresignTTL]; ok && val != "" {
		merged.PresignTTL = val
	}

	return merged, nil
}
