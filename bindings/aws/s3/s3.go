// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package s3

import (
	"bytes"
	b64 "encoding/base64"
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	aws_auth "github.com/dapr/components-contrib/authentication/aws"
	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/kit/logger"
	"github.com/google/uuid"
)

const (
	metadataDecodeBase64 = "decodeBase64"
	metadataKey          = "key"
)

// AWSS3 is a binding for an AWS S3 storage bucket
type AWSS3 struct {
	metadata   *s3Metadata
	s3Client   *s3.S3
	uploader   *s3manager.Uploader
	downloader *s3manager.Downloader
	logger     logger.Logger
}

type s3Metadata struct {
	Region       string `json:"region"`
	Endpoint     string `json:"endpoint"`
	AccessKey    string `json:"accessKey"`
	SecretKey    string `json:"secretKey"`
	SessionToken string `json:"sessionToken"`
	Bucket       string `json:"bucket"`
	DecodeBase64 bool   `json:"decodeBase64,string"`
}

type createResponse struct {
	Location  string  `json:"Location"`
	VersionID *string `json:"VersionID"`
}

// NewAWSS3 returns a new AWSS3 instance
func NewAWSS3(logger logger.Logger) *AWSS3 {
	return &AWSS3{logger: logger}
}

// Init does metadata parsing and connection creation
func (s *AWSS3) Init(metadata bindings.Metadata) error {
	m, err := s.parseMetadata(metadata)
	if err != nil {
		return err
	}
	session, err := s.getSession(m)
	if err != nil {
		return err
	}
	s.metadata = m
	s.s3Client = s3.New(session)
	s.downloader = s3manager.NewDownloader(session)
	s.uploader = s3manager.NewUploader(session)
	return nil
}

func (s *AWSS3) Operations() []bindings.OperationKind {
	return []bindings.OperationKind{
		bindings.CreateOperation,
		bindings.GetOperation,
		bindings.DeleteOperation,
	}
}

func (s *AWSS3) create(req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {

	metadata, err := s.metadata.mergeWithRequestMetadata(req)

	if err != nil {
		return nil, fmt.Errorf("s3 binding error. error merge metadata : %w", err)
	}
	key := ""
	if val, ok := req.Metadata[metadataKey]; ok && val != "" {
		key = val
	} else {
		key = uuid.New().String()
		s.logger.Debugf("key not found. generating key %s", key)
	}

	d, err := strconv.Unquote(string(req.Data))
	if err == nil {
		req.Data = []byte(d)
	}

	if metadata.DecodeBase64 {
		decoded, decodeError := b64.StdEncoding.DecodeString(string(req.Data))
		if decodeError != nil {
			return nil, fmt.Errorf("s3 binding error. decode : %w", decodeError)
		}
		req.Data = decoded
	}

	r := bytes.NewReader(req.Data)

	resultUpload, err := s.uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(metadata.Bucket),
		Key:    aws.String(key),
		Body:   r,
	})

	if err != nil {
		return nil, fmt.Errorf("s3 binding error. Uploading: %w", err)
	}

	jsonResponse, err := json.Marshal(createResponse{
		Location:  resultUpload.Location,
		VersionID: resultUpload.VersionID,
	})
	if err != nil {
		return nil, fmt.Errorf("s3 binding error. Error marshalling create response: %w", err)
	}

	return &bindings.InvokeResponse{
		Data: jsonResponse,
	}, nil
}

func (s *AWSS3) get(req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	key := ""
	if val, ok := req.Metadata[metadataKey]; ok && val != "" {
		key = val
	} else {
		return nil, fmt.Errorf("s3 binding error: can't read key value")
	}

	buff := &aws.WriteAtBuffer{}

	_, err := s.downloader.Download(buff,
		&s3.GetObjectInput{
			Bucket: aws.String(s.metadata.Bucket),
			Key:    aws.String(key),
		})

	if err != nil {
		return nil, fmt.Errorf("s3 binding error: error downloading S3 object: %w", err)
	}

	return &bindings.InvokeResponse{
		Data:     buff.Bytes(),
		Metadata: nil,
	}, nil
}

func (s *AWSS3) delete(req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	key := ""
	if val, ok := req.Metadata[metadataKey]; ok && val != "" {
		key = val
	} else {
		return nil, fmt.Errorf("s3 binding error: can't read key value")
	}

	_, err := s.s3Client.DeleteObject(
		&s3.DeleteObjectInput{
			Bucket: aws.String(s.metadata.Bucket),
			Key:    aws.String(key),
		})

	return nil, err
}

func (s *AWSS3) Invoke(req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {

	switch req.Operation {
	case bindings.CreateOperation:
		return s.create(req)
	case bindings.GetOperation:
		return s.get(req)
	case bindings.DeleteOperation:
		return s.delete(req)
	default:
		return nil, fmt.Errorf("unsupported operation %s", req.Operation)
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
	sess, err := aws_auth.GetClient(metadata.AccessKey, metadata.SecretKey, metadata.SessionToken, metadata.Region, metadata.Endpoint)
	if err != nil {
		return nil, err
	}

	return sess, nil
}

// Helper to merge config and request metadata
func (metadata s3Metadata) mergeWithRequestMetadata(req *bindings.InvokeRequest) (s3Metadata, error) {
	merged := metadata

	if val, ok := req.Metadata[metadataDecodeBase64]; ok && val != "" {
		valBool, err := strconv.ParseBool(val)
		if err != nil {
			return merged, err
		} else {
			merged.DecodeBase64 = valBool
		}
	}

	return merged, nil
}
