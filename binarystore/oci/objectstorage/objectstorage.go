/*
Copyright 2026 The Dapr Authors
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

// Package objectstorage provides a BinaryStore implementation backed by OCI
// Object Storage. Each named file maps 1:1 to an object inside the configured
// bucket.
package objectstorage

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"strings"

	"github.com/dapr/components-contrib/binarystore"
	contribMetadata "github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
	kitmd "github.com/dapr/kit/metadata"
)

const (
	instancePrincipalAuthenticationKey = "instancePrincipalAuthentication"
	configFileAuthenticationKey        = "configFileAuthentication"
	configFilePathKey                  = "configFilePath"
	tenancyKey                         = "tenancyOCID"
	compartmentKey                     = "compartmentOCID"
	regionKey                          = "region"
	fingerPrintKey                     = "fingerPrint"
	privateKeyKey                      = "privateKey"
	userKey                            = "userOCID"
	bucketNameKey                      = "bucketName"
)

// ObjectStorage implements binarystore.BinaryStore using OCI Object Storage.
type ObjectStorage struct {
	metadata *objectStoreMetadata
	client   objectStoreClient
	logger   logger.Logger
}

type objectStoreMetadata struct {
	UserOCID                        string `json:"userOCID" mapstructure:"userOCID"`
	BucketName                      string `json:"bucketName" mapstructure:"bucketName"`
	Prefix                          string `json:"prefix" mapstructure:"prefix"`
	Region                          string `json:"region" mapstructure:"region"`
	TenancyOCID                     string `json:"tenancyOCID" mapstructure:"tenancyOCID"`
	FingerPrint                     string `json:"fingerPrint" mapstructure:"fingerPrint"`
	PrivateKey                      string `json:"privateKey" mapstructure:"privateKey"`
	CompartmentOCID                 string `json:"compartmentOCID" mapstructure:"compartmentOCID"`
	Namespace                       string `json:"namespace" mapstructure:"namespace"`
	ConfigFilePath                  string `json:"configFilePath" mapstructure:"configFilePath"`
	ConfigFileProfile               string `json:"configFileProfile" mapstructure:"configFileProfile"`
	InstancePrincipalAuthentication bool   `json:"instancePrincipalAuthentication,string" mapstructure:"instancePrincipalAuthentication"`
	ConfigFileAuthentication        bool   `json:"configFileAuthentication,string" mapstructure:"configFileAuthentication"`
}

// NewOCIObjectStorage returns a new OCI Object Storage binary store.
func NewOCIObjectStorage(log logger.Logger) binarystore.BinaryStore {
	return &ObjectStorage{logger: log}
}

// Init initialises the OCI Object Storage client from component metadata.
func (o *ObjectStorage) Init(ctx context.Context, md binarystore.Metadata) error {
	m, err := parseMetadata(md.Properties)
	if err != nil {
		return err
	}

	client, err := newOCIObjectStoreClient(ctx, m, o.logger)
	if err != nil {
		return fmt.Errorf("failed to initialize OCI Object Storage binary store: %w", err)
	}

	o.metadata = m
	o.client = client
	return nil
}

func parseMetadata(meta map[string]string) (*objectStoreMetadata, error) {
	m := objectStoreMetadata{}
	if err := kitmd.DecodeMetadata(meta, &m); err != nil {
		return nil, err
	}

	if m.ConfigFileAuthentication {
		var err error
		if m.ConfigFilePath, err = getConfigFilePath(m.ConfigFilePath); err != nil {
			return nil, err
		}
	}
	if m.BucketName == "" {
		return nil, fmt.Errorf("missing or empty %s field from metadata", bucketNameKey)
	}
	if m.CompartmentOCID == "" {
		return nil, fmt.Errorf("missing or empty %s field from metadata", compartmentKey)
	}

	externalAuthentication := m.InstancePrincipalAuthentication || m.ConfigFileAuthentication
	if !externalAuthentication {
		if err := getIdentityAuthenticationDetails(m); err != nil {
			return nil, err
		}
	}

	return &m, nil
}

func getConfigFilePath(configFilePath string) (string, error) {
	if strings.HasPrefix(configFilePath, "~/") {
		return "", fmt.Errorf("%s is set to %s which starts with ~/; this is not supported - please provide absolute path to configuration file", configFilePathKey, configFilePath)
	}
	if configFilePath != "" {
		if _, err := os.Stat(configFilePath); err != nil {
			if os.IsNotExist(err) {
				return "", fmt.Errorf("oci configuration file %s does not exist %w", configFilePath, err)
			}
			return "", fmt.Errorf("error %w with reading oci configuration file %s", err, configFilePath)
		}
	}
	return configFilePath, nil
}

func getIdentityAuthenticationDetails(meta objectStoreMetadata) error {
	if meta.Region == "" {
		return fmt.Errorf("missing or empty %s field from metadata", regionKey)
	}
	if meta.UserOCID == "" {
		return fmt.Errorf("missing or empty %s field from metadata", userKey)
	}
	if meta.FingerPrint == "" {
		return fmt.Errorf("missing or empty %s field from metadata", fingerPrintKey)
	}
	if meta.PrivateKey == "" {
		return fmt.Errorf("missing or empty %s field from metadata", privateKeyKey)
	}
	if meta.TenancyOCID == "" {
		return fmt.Errorf("missing or empty %s field from metadata", tenancyKey)
	}
	return nil
}

// Features returns the optional features supported by this component.
func (o *ObjectStorage) Features() []binarystore.Feature {
	return []binarystore.Feature{}
}

// Set uploads binary data to OCI Object Storage.
//
// When req.Overwrite is false, an If-None-Match: * condition is applied so the
// service atomically rejects the upload if the object already exists. When
// req.Overwrite is true, the object is created or replaced.
func (o *ObjectStorage) Set(ctx context.Context, req *binarystore.SetRequest) error {
	if req.FileName == "" {
		return binarystore.ErrMissingFileName
	}

	if err := o.client.putObject(ctx, binarystore.ObjectPath(o.metadata.Prefix, req.FileName), req.Data, req.Overwrite); err != nil {
		// Only a create-only write can fail because the object already
		// exists; when overwriting, a precondition failure signals an
		// unrelated condition that must be surfaced to the caller.
		if !req.Overwrite && isPreconditionFailed(err) {
			return binarystore.ErrFileAlreadyExists
		}
		return fmt.Errorf("error uploading object %q: %w", req.FileName, err)
	}
	return nil
}

// Get downloads binary data from OCI Object Storage as a streaming reader.
func (o *ObjectStorage) Get(ctx context.Context, req *binarystore.GetRequest) (*binarystore.GetResponse, error) {
	if req.FileName == "" {
		return nil, binarystore.ErrMissingFileName
	}

	body, err := o.client.getObject(ctx, binarystore.ObjectPath(o.metadata.Prefix, req.FileName))
	if err != nil {
		if isNotFound(err) {
			return nil, binarystore.ErrFileNotFound
		}
		return nil, fmt.Errorf("error downloading object %q: %w", req.FileName, err)
	}
	return &binarystore.GetResponse{Data: body}, nil
}

// Delete removes an object from OCI Object Storage. If the object does not
// exist, binarystore.ErrFileNotFound is returned.
func (o *ObjectStorage) Delete(ctx context.Context, req *binarystore.DeleteRequest) error {
	if req.FileName == "" {
		return binarystore.ErrMissingFileName
	}

	if err := o.client.deleteObject(ctx, binarystore.ObjectPath(o.metadata.Prefix, req.FileName)); err != nil {
		if isNotFound(err) {
			return binarystore.ErrFileNotFound
		}
		return fmt.Errorf("error deleting object %q: %w", req.FileName, err)
	}
	return nil
}

// GetComponentMetadata returns the metadata schema for this component.
func (o *ObjectStorage) GetComponentMetadata() (metadataInfo contribMetadata.MetadataMap) {
	metadataStruct := objectStoreMetadata{}
	_ = contribMetadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, contribMetadata.BinaryStoreType)
	return
}

// Close releases the resources held by the underlying OCI client.
func (o *ObjectStorage) Close() error {
	if o.client == nil {
		return nil
	}
	return o.client.close()
}
