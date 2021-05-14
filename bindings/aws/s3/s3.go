// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package s3

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/url"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	aws_auth "github.com/dapr/components-contrib/authentication/aws"
	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/kit/logger"
	"github.com/google/uuid"
)

const (
	CopyOperation    bindings.OperationKind = "copy"
)


// AWSS3 is a binding for an AWS S3 storage bucket
type AWSS3 struct {
	metadata   *s3Metadata
	uploader   *s3manager.Uploader
	downloader *s3manager.Downloader
	svc        *s3.S3
	logger     logger.Logger
}

type s3Metadata struct {
	Region       string `json:"region"`
	Endpoint     string `json:"endpoint"`
	AccessKey    string `json:"accessKey"`
	SecretKey    string `json:"secretKey"`
	SessionToken string `json:"sessionToken"`
	Bucket       string `json:"bucket"`
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
	uploader, downloader, svc, err := s.getClient(m)
	if err != nil {
		return err
	}
	s.metadata = m
	s.uploader = uploader
	s.downloader = downloader
	s.svc = svc

	return nil
}

func (s *AWSS3) Operations() []bindings.OperationKind {
	return []bindings.OperationKind{bindings.CreateOperation, bindings.GetOperation, bindings.DeleteOperation, CopyOperation}
}

func (s *AWSS3) Invoke(req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	bucket := s.metadata.Bucket
	data := []byte{}
	key := ""
	err := (error)(nil)
	if (req.Operation == bindings.CreateOperation) {
		key = s.getKey(req)
	} else {
		key, err = s.getReqParam(req, "key")
		if (err != nil) {
			return nil, err
		}
	}

	switch op := req.Operation; op {
	case bindings.CreateOperation:
		r := bytes.NewReader(req.Data)
		_, err := s.uploader.Upload(&s3manager.UploadInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
			Body:   r,
		})
		if (err != nil) {
			s.logger.Errorf("s3 CREATE operation exceptions: %s", err)
		}

	case bindings.GetOperation:
		buff := &aws.WriteAtBuffer{}
		_, err = s.downloader.Download(buff, &s3.GetObjectInput{
			Bucket: aws.String(s.metadata.Bucket),
			Key:    aws.String(key),
		})
		if (err == nil) {
			data = buff.Bytes()
		} else {
			s.logger.Debugf("s3 GET operation exceptions: %s", err)
		}
		
	case bindings.DeleteOperation:
		_, err = s.svc.DeleteObject(&s3.DeleteObjectInput{
			Bucket: aws.String(s.metadata.Bucket),
			Key:    aws.String(key),
		})
		if (err != nil) {
			s.logger.Errorf("s3 DELETE operation exceptions: %s", err)
		}

	case CopyOperation:
		source, err := s.getReqParam(req, "source")
		if (err != nil) {
			return nil, err
		}
		_, err = s.svc.CopyObject(&s3.CopyObjectInput{

			CopySource: aws.String(url.PathEscape(source)),
			Bucket: aws.String(s.metadata.Bucket),
			Key: aws.String(key),
		})
		if (err != nil) {
			s.logger.Errorf("s3 COPY operation exceptions: %s", err)
		}
	}

	resp := &bindings.InvokeResponse{
		Data:     data,
		Metadata: map[string]string{"bucket": bucket, "key": key},
	}
	return resp, err
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

func (s *AWSS3) getReqParam(req *bindings.InvokeRequest, paramName string) (string, error) {
	value := ""
	err := (error)(nil)

	if val, ok := req.Metadata[paramName]; ok && val != "" {
		value = val
	} else {
		err = fmt.Errorf("param %s is required", paramName)
	}

	return value, err
}

func (s *AWSS3) getKey(req *bindings.InvokeRequest) (string) {
	key := ""
	if val, ok := req.Metadata["key"]; ok && val != "" {
		key = val
	} else {
		key = uuid.New().String()
		s.logger.Debugf("key not found. generating key %s", key)
	}

	return key
}

func (s *AWSS3) getClient(metadata *s3Metadata) (*s3manager.Uploader, *s3manager.Downloader,  *s3.S3, error) {
	sess, err := aws_auth.GetClient(metadata.AccessKey, metadata.SecretKey, metadata.SessionToken, metadata.Region, metadata.Endpoint)
	if err != nil {
		return nil, nil, nil, err
	}

	uploader := s3manager.NewUploader(sess)
	downloader := s3manager.NewDownloader(sess)
	svc := s3.New(sess)

	return uploader, downloader, svc, nil
}
