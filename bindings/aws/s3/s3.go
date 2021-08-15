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
	metadata *s3Metadata
	uploader *s3manager.Uploader
	logger   logger.Logger
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
	uploader, err := s.getClient(m)
	if err != nil {
		return err
	}
	s.metadata = m
	s.uploader = uploader

	return nil
}

func (s *AWSS3) Operations() []bindings.OperationKind {
	return []bindings.OperationKind{bindings.CreateOperation}
}

func (s *AWSS3) Invoke(req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {

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

func (s *AWSS3) getClient(metadata *s3Metadata) (*s3manager.Uploader, error) {
	sess, err := aws_auth.GetClient(metadata.AccessKey, metadata.SecretKey, metadata.SessionToken, metadata.Region, metadata.Endpoint)
	if err != nil {
		return nil, err
	}

	uploader := s3manager.NewUploader(sess)

	return uploader, nil
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
