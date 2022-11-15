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

package oss

import (
	"bytes"
	"context"

	"github.com/aliyun/aliyun-oss-go-sdk/oss"
	"github.com/google/uuid"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
)

// AliCloudOSS is a binding for an AliCloud OSS storage bucket.
type AliCloudOSS struct {
	metadata *ossMetadata
	client   *oss.Client
	logger   logger.Logger
}

type ossMetadata struct {
	Endpoint    string `json:"endpoint" mapstructure:"endpoint"`
	AccessKeyID string `json:"accessKeyID" mapstructure:"accessKeyID"`
	AccessKey   string `json:"accessKey" mapstructure:"accessKey"`
	Bucket      string `json:"bucket" mapstructure:"bucket"`
}

// NewAliCloudOSS returns a new  instance.
func NewAliCloudOSS(logger logger.Logger) bindings.OutputBinding {
	return &AliCloudOSS{logger: logger}
}

// Init does metadata parsing and connection creation.
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

func (s *AliCloudOSS) Invoke(_ context.Context, req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
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

func (s *AliCloudOSS) parseMetadata(meta bindings.Metadata) (*ossMetadata, error) {
	var m ossMetadata
	err := metadata.DecodeMetadata(meta.Properties, &m)
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
