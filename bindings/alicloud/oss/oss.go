// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package oss

import (
	"bytes"
	"encoding/json"

	"github.com/aliyun/aliyun-oss-go-sdk/oss"
	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/kit/logger"
	"github.com/google/uuid"
)

// AliCloudOSS is a binding for an AliCloud OSS storage bucket
type AliCloudOSS struct {
	metadata *ossMetadata
	client   *oss.Client
	logger   logger.Logger
}

type ossMetadata struct {
	Endpoint    string `json:"endpoint"`
	AccessKeyID string `json:"accessKeyID"`
	AccessKey   string `json:"accessKey"`
	Bucket      string `json:"bucket"`
}

// NewAliCloudOSS returns a new  instance
func NewAliCloudOSS(logger logger.Logger) *AliCloudOSS {
	return &AliCloudOSS{logger: logger}
}

// Init does metadata parsing and connection creation
func (s *AliCloudOSS) Init(metadata bindings.Metadata) error {
	m, err := s.parseMetadata(metadata)
	if err != nil {
		return err
	}
	client, err := s.getClient(m)
	if err != nil {
		return err
	}
	s.metadata = m
	s.client = client

	return nil
}

func (s *AliCloudOSS) Operations() []bindings.OperationKind {
	return []bindings.OperationKind{bindings.CreateOperation}
}

func (s *AliCloudOSS) Invoke(req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	key := ""
	if val, ok := req.Metadata["key"]; ok && val != "" {
		key = val
	} else {
		key = uuid.New().String()
		s.logger.Debugf("key not found. generating key %s", key)
	}

	bucket, err := s.client.Bucket(s.metadata.Bucket)
	if err != nil {
		return nil, err
	}

	// Upload a byte array.
	err = bucket.PutObject(key, bytes.NewReader(req.Data))
	if err != nil {
		return nil, err
	}

	return nil, err
}

func (s *AliCloudOSS) parseMetadata(metadata bindings.Metadata) (*ossMetadata, error) {
	b, err := json.Marshal(metadata.Properties)
	if err != nil {
		return nil, err
	}

	var m ossMetadata
	err = json.Unmarshal(b, &m)
	if err != nil {
		return nil, err
	}

	return &m, nil
}

func (s *AliCloudOSS) getClient(metadata *ossMetadata) (*oss.Client, error) {
	client, err := oss.New(metadata.Endpoint, metadata.AccessKeyID, metadata.AccessKey)
	if err != nil {
		return nil, err
	}

	return client, nil
}
