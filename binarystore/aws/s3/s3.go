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
	"io"
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
	client   s3StoreClient
	logger   logger.Logger
}

// s3StoreClient isolates the SDK calls the component makes, mirroring the
// client seams used by the other binary store providers so that behaviour can
// be unit tested without a live endpoint.
type s3StoreClient interface {
	putObject(ctx context.Context, name string, data io.Reader, overwrite bool) error
	getObject(ctx context.Context, name string) (io.ReadCloser, error)
	headObject(ctx context.Context, name string) error
	deleteObject(ctx context.Context, name string) error
	close() error
}

type awsS3Client struct {
	bucket   string
	s3Client *s3.Client
	tmClient *transfermanager.Client
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

	s3Client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.UsePathStyle = m.ForcePathStyle
	})

	s.metadata = m
	s.client = &awsS3Client{
		bucket:   m.Bucket,
		s3Client: s3Client,
		// Part size and concurrency are set explicitly so buffering matches
		// the other binary store providers rather than the SDK defaults.
		tmClient: transfermanager.New(s3Client, func(o *transfermanager.Options) {
			o.PartSizeBytes = binarystore.DefaultUploadPartSize
			o.Concurrency = binarystore.DefaultUploadConcurrency
		}),
	}

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
// exists, returning binarystore.ErrFileAlreadyExists. This relies on the
// endpoint honouring conditional writes: AWS S3 and other conforming
// S3-compatible providers do, but a provider that does not recognize
// If-None-Match may silently ignore the header and perform a normal
// PutObject, overwriting any existing object without error. Only use
// req.Overwrite = false against endpoints that are known to support
// conditional writes.
//
// When req.Overwrite is true, the object is created or replaced without a
// condition check.
func (s *AWSS3) Set(ctx context.Context, req *binarystore.SetRequest) error {
	if req.FileName == "" {
		return binarystore.ErrMissingFileName
	}

	if err := s.client.putObject(ctx, binarystore.ObjectPath(s.metadata.Prefix, req.FileName), req.Data, req.Overwrite); err != nil {
		// Only a create-only write can fail because the object already
		// exists; when overwriting, a conflict signals an unrelated
		// condition (e.g. OperationAborted from racing writers) that must be
		// surfaced to the caller rather than masked as ErrFileAlreadyExists.
		if !req.Overwrite && isPreconditionFailed(err) {
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

	body, err := s.client.getObject(ctx, binarystore.ObjectPath(s.metadata.Prefix, req.FileName))
	if err != nil {
		if isNotFound(err) {
			return nil, binarystore.ErrFileNotFound
		}
		return nil, fmt.Errorf("error downloading object %q: %w", req.FileName, err)
	}

	return &binarystore.GetResponse{Data: body}, nil
}

// Delete removes an object from S3. If the object does not exist,
// binarystore.ErrFileNotFound is returned.
//
// Because S3's DeleteObject is idempotent and succeeds for missing keys, an
// existence check is issued first, which requires the s3:GetObject permission.
// When that permission is absent S3 answers HeadObject for a missing key with
// 403 AccessDenied rather than 404 so that key existence is not leaked; in
// that case the delete proceeds without the existence guarantee rather than
// failing outright.
func (s *AWSS3) Delete(ctx context.Context, req *binarystore.DeleteRequest) error {
	if req.FileName == "" {
		return binarystore.ErrMissingFileName
	}

	name := binarystore.ObjectPath(s.metadata.Prefix, req.FileName)

	// Unlike the other providers, S3's DeleteObject is idempotent and does not
	// error when the key is missing, so an existence check is needed to return
	// ErrFileNotFound consistently with them.
	if err := s.client.headObject(ctx, name); err != nil {
		switch {
		case isNotFound(err):
			return binarystore.ErrFileNotFound
		case isAccessDenied(err):
			// Existence cannot be determined without s3:GetObject; fall
			// through to the delete so a delete-only policy still works.
		default:
			return fmt.Errorf("error checking object %q: %w", req.FileName, err)
		}
	}

	if err := s.client.deleteObject(ctx, name); err != nil {
		return fmt.Errorf("error deleting object %q: %w", req.FileName, err)
	}

	return nil
}

// GetComponentMetadata returns the metadata schema for this component, used
// by the Dapr metadata linter.
func (s *AWSS3) GetComponentMetadata() (metadataInfo contribMetadata.MetadataMap) {
	metadataStruct := s3Metadata{}
	_ = contribMetadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, contribMetadata.BinaryStoreType)
	return
}

// Close closes the underlying S3 client.
func (s *AWSS3) Close() error {
	if s.client == nil {
		return nil
	}
	return s.client.close()
}

func (c *awsS3Client) putObject(ctx context.Context, name string, data io.Reader, overwrite bool) error {
	input := &transfermanager.UploadObjectInput{
		Bucket: ptr.Of(c.bucket),
		Key:    ptr.Of(name),
		Body:   data,
	}
	if !overwrite {
		input.IfNoneMatch = ptr.Of("*")
	}

	_, err := c.tmClient.UploadObject(ctx, input)
	return err
}

func (c *awsS3Client) getObject(ctx context.Context, name string) (io.ReadCloser, error) {
	resp, err := c.s3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: ptr.Of(c.bucket),
		Key:    ptr.Of(name),
	})
	if err != nil {
		return nil, err
	}
	return resp.Body, nil
}

func (c *awsS3Client) headObject(ctx context.Context, name string) error {
	_, err := c.s3Client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: ptr.Of(c.bucket),
		Key:    ptr.Of(name),
	})
	return err
}

func (c *awsS3Client) deleteObject(ctx context.Context, name string) error {
	_, err := c.s3Client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: ptr.Of(c.bucket),
		Key:    ptr.Of(name),
	})
	return err
}

// close is a no-op; the AWS SDK manages connection lifecycle internally.
func (c *awsS3Client) close() error {
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

// isAccessDenied reports whether err indicates that the caller is not
// authorised for the request. S3 returns this instead of a 404 for HeadObject
// on a missing key when the caller lacks s3:ListBucket, so that key existence
// is not disclosed.
func isAccessDenied(err error) bool {
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		switch apiErr.ErrorCode() {
		case "AccessDenied", "Forbidden":
			return true
		}
	}

	var re interface{ HTTPStatusCode() int }
	if errors.As(err, &re) {
		return re.HTTPStatusCode() == http.StatusForbidden
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
		// 409 is deliberately not matched here: S3 also returns it for
		// conditions unrelated to object existence, such as OperationAborted.
		// The ConditionalRequestConflict error code above covers the
		// existence case.
		return re.HTTPStatusCode() == http.StatusPreconditionFailed
	}

	return false
}
