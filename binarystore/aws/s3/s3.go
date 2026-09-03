/*
Copyright 2025 The Dapr Authors
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

// Package s3 provides a BinaryStore implementation backed by Amazon S3 or any
// S3-compatible object storage service (e.g. MinIO, Ceph, Cloudflare R2).
// Each named file maps 1:1 to an object inside the configured bucket.
package s3

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net/http"
	"reflect"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/dapr/kit/ptr"

	"github.com/dapr/components-contrib/binarystore"
	awsCommon "github.com/dapr/components-contrib/common/aws"
	awsCommonAuth "github.com/dapr/components-contrib/common/aws/auth"
	contribMetadata "github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
)

// AWSS3 implements binarystore.BinaryStore using Amazon S3 or an
// S3-compatible object storage service.
type AWSS3 struct {
	metadata *s3Metadata
	s3Client *s3.Client
	tmClient *transfermanager.Client
	logger   logger.Logger
}

// NewAWSS3 returns a new AWSS3 binary store.
func NewAWSS3(log logger.Logger) binarystore.BinaryStore {
	return &AWSS3{logger: log}
}

// Init initialises the S3 client from the component metadata.
func (s *AWSS3) Init(ctx context.Context, md binarystore.Metadata) error {
	m, err := parseMetadata(md.Properties)
	if err != nil {
		return err
	}
	s.metadata = m

	configOpts := awsCommonAuth.Options{
		Logger:       s.logger,
		Properties:   md.Properties,
		Region:       m.Region,
		Endpoint:     m.Endpoint,
		AccessKey:    m.AccessKey,
		SecretKey:    m.SecretKey,
		SessionToken: m.SessionToken,
	}

	var awsCfg aws.Config
	if m.InsecureSSL {
		customTransport := http.DefaultTransport.(*http.Transport).Clone()
		customTransport.TLSClientConfig = &tls.Config{
			//nolint:gosec
			InsecureSkipVerify: true,
		}
		client := &http.Client{Transport: customTransport}
		awsCfg, err = awsCommon.NewConfig(ctx, configOpts, awsCommon.WithHTTPClient(client))
		if err == nil {
			s.logger.Infof("binarystore aws.s3: you are using 'insecureSSL' to skip server config verify which is unsafe!")
		}
	} else {
		awsCfg, err = awsCommon.NewConfig(ctx, configOpts)
	}
	if err != nil {
		return err
	}

	s.s3Client = s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.UsePathStyle = m.ForcePathStyle
	})
	s.tmClient = transfermanager.New(s.s3Client)

	return nil
}

// Features returns the optional features supported by this component.
func (s *AWSS3) Features() []binarystore.Feature {
	return []binarystore.Feature{}
}

// Set uploads binary data to S3.
//
// When req.Overwrite is false, an If-None-Match: * condition is applied so
// that the server rejects the upload atomically if the object already
// exists, returning binarystore.ErrFileAlreadyExists. Not all S3-compatible
// providers support conditional writes; in that case the underlying error is
// surfaced unless it maps to a PreconditionFailed-style response.
//
// When req.Overwrite is true, the object is created or replaced without a
// condition check.
func (s *AWSS3) Set(ctx context.Context, req *binarystore.SetRequest) error {
	if req.FileName == "" {
		return binarystore.ErrMissingFileName
	}

	input := &transfermanager.UploadObjectInput{
		Bucket: ptr.Of(s.metadata.Bucket),
		Key:    ptr.Of(req.FileName),
		Body:   req.Data,
	}
	if !req.Overwrite {
		input.IfNoneMatch = ptr.Of("*")
	}

	_, err := s.tmClient.UploadObject(ctx, input)
	if err != nil {
		if isPreconditionFailed(err) {
			return binarystore.ErrFileAlreadyExists
		}
		return fmt.Errorf("error uploading object %q: %w", req.FileName, err)
	}

	return nil
}

// Get downloads binary data from S3 as a streaming reader.
//
// The Data field of the returned GetResponse wraps the HTTP response body and
// must be closed by the caller when reading is complete.
func (s *AWSS3) Get(ctx context.Context, req *binarystore.GetRequest) (*binarystore.GetResponse, error) {
	if req.FileName == "" {
		return nil, binarystore.ErrMissingFileName
	}

	resp, err := s.s3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: ptr.Of(s.metadata.Bucket),
		Key:    ptr.Of(req.FileName),
	})
	if err != nil {
		if isNotFound(err) {
			return nil, binarystore.ErrFileNotFound
		}
		return nil, fmt.Errorf("error downloading object %q: %w", req.FileName, err)
	}

	return &binarystore.GetResponse{
		Data: resp.Body,
	}, nil
}

// Delete removes an object from S3. If the object does not exist,
// binarystore.ErrFileNotFound is returned.
func (s *AWSS3) Delete(ctx context.Context, req *binarystore.DeleteRequest) error {
	if req.FileName == "" {
		return binarystore.ErrMissingFileName
	}

	// S3 requires the object to exist to distinguish delete from no-op; check
	// first so callers reliably receive ErrFileNotFound (S3's DeleteObject is
	// idempotent and does not error when the key is missing).
	_, err := s.s3Client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: ptr.Of(s.metadata.Bucket),
		Key:    ptr.Of(req.FileName),
	})
	if err != nil {
		if isNotFound(err) {
			return binarystore.ErrFileNotFound
		}
		return fmt.Errorf("error checking object %q: %w", req.FileName, err)
	}

	_, err = s.s3Client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: ptr.Of(s.metadata.Bucket),
		Key:    ptr.Of(req.FileName),
	})
	if err != nil {
		return fmt.Errorf("error deleting object %q: %w", req.FileName, err)
	}

	return nil
}

// GetComponentMetadata returns the metadata schema for this component, used
// by the Dapr metadata linter.
func (s *AWSS3) GetComponentMetadata() (metadataInfo contribMetadata.MetadataMap) {
	metadataStruct := s3Metadata{}
	contribMetadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, contribMetadata.BinaryStoreType)
	return
}

// Close is a no-op; the AWS SDK manages connection lifecycle internally.
func (s *AWSS3) Close() error {
	return nil
}

// isNotFound reports whether err indicates the requested S3 object does not
// exist (NoSuchKey, or a 404 from a HeadObject/GetObject call).
func isNotFound(err error) bool {
	var nsk *types.NoSuchKey
	if errors.As(err, &nsk) {
		return true
	}

	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		switch apiErr.ErrorCode() {
		case "NoSuchKey", "NotFound":
			return true
		}
	}

	var re interface{ HTTPStatusCode() int }
	if errors.As(err, &re) {
		return re.HTTPStatusCode() == http.StatusNotFound
	}

	return false
}

// isPreconditionFailed reports whether err indicates that a conditional write
// (If-None-Match) was rejected because the object already exists.
func isPreconditionFailed(err error) bool {
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		switch apiErr.ErrorCode() {
		case "PreconditionFailed", "ConditionalRequestConflict":
			return true
		}
	}

	var re interface{ HTTPStatusCode() int }
	if errors.As(err, &re) {
		return re.HTTPStatusCode() == http.StatusPreconditionFailed || re.HTTPStatusCode() == http.StatusConflict
	}

	return false
}
