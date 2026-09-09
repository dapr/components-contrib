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

// Package bucket provides a BinaryStore implementation backed by Google Cloud
// Storage. Each named file maps 1:1 to an object inside the configured bucket.
package bucket

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"reflect"

	"cloud.google.com/go/storage"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"

	"github.com/dapr/components-contrib/binarystore"
	contribMetadata "github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
	kitmd "github.com/dapr/kit/metadata"
)

// GCPBucket implements binarystore.BinaryStore using Google Cloud Storage.
type GCPBucket struct {
	metadata *gcpMetadata
	client   gcsClient
	logger   logger.Logger
}

type gcpMetadata struct {
	// Ignored by metadata parser because included in built-in authentication profile.
	Type                string `json:"type" mapstructure:"type" mdignore:"true"`
	ProjectID           string `json:"project_id" mapstructure:"projectID" mdignore:"true" mapstructurealiases:"project_id"`
	PrivateKeyID        string `json:"private_key_id" mapstructure:"privateKeyID" mdignore:"true" mapstructurealiases:"private_key_id"`
	PrivateKey          string `json:"private_key" mapstructure:"privateKey" mdignore:"true" mapstructurealiases:"private_key"`
	ClientEmail         string `json:"client_email" mapstructure:"clientEmail" mdignore:"true" mapstructurealiases:"client_email"`
	ClientID            string `json:"client_id" mapstructure:"clientID" mdignore:"true" mapstructurealiases:"client_id"`
	AuthURI             string `json:"auth_uri" mapstructure:"authURI" mdignore:"true" mapstructurealiases:"auth_uri"`
	TokenURI            string `json:"token_uri" mapstructure:"tokenURI" mdignore:"true" mapstructurealiases:"token_uri"`
	AuthProviderCertURL string `json:"auth_provider_x509_cert_url" mapstructure:"authProviderX509CertURL" mdignore:"true" mapstructurealiases:"auth_provider_x509_cert_url"`
	ClientCertURL       string `json:"client_x509_cert_url" mapstructure:"clientX509CertURL" mdignore:"true" mapstructurealiases:"client_x509_cert_url"`

	Bucket string `json:"bucket" mapstructure:"bucket"`
	Prefix string `json:"prefix" mapstructure:"prefix"`
}

type gcsClient interface {
	putObject(ctx context.Context, bucket, name string, data io.Reader, overwrite bool) error
	getObject(ctx context.Context, bucket, name string) (io.ReadCloser, error)
	deleteObject(ctx context.Context, bucket, name string) error
	close() error
}

type storageClient struct {
	client *storage.Client
}

// NewGCPBucket returns a new Google Cloud Storage binary store.
func NewGCPBucket(log logger.Logger) binarystore.BinaryStore {
	return &GCPBucket{logger: log}
}

// Init initialises the Google Cloud Storage client from component metadata.
func (g *GCPBucket) Init(ctx context.Context, md binarystore.Metadata) error {
	m, err := parseMetadata(md.Properties)
	if err != nil {
		return err
	}

	client, err := newStorageClient(ctx, m)
	if err != nil {
		return err
	}

	g.metadata = m
	g.client = &storageClient{client: client}
	return nil
}

func parseMetadata(meta map[string]string) (*gcpMetadata, error) {
	m := gcpMetadata{}
	if err := kitmd.DecodeMetadata(meta, &m); err != nil {
		return nil, err
	}
	if m.Bucket == "" {
		return nil, errors.New("missing property `bucket` in metadata")
	}
	return &m, nil
}

func newStorageClient(ctx context.Context, m *gcpMetadata) (*storage.Client, error) {
	if m.PrivateKeyID == "" {
		return storage.NewClient(ctx)
	}

	creds, err := json.Marshal(m)
	if err != nil {
		return nil, err
	}
	return storage.NewClient(ctx, option.WithCredentialsJSON(creds))
}

// Features returns the optional features supported by this component.
func (g *GCPBucket) Features() []binarystore.Feature {
	return []binarystore.Feature{}
}

// Set uploads binary data to Google Cloud Storage.
//
// When req.Overwrite is false, a DoesNotExist generation condition is applied
// so the service atomically rejects the upload if the object already exists.
// When req.Overwrite is true, the object is created or replaced.
func (g *GCPBucket) Set(ctx context.Context, req *binarystore.SetRequest) error {
	if req.FileName == "" {
		return binarystore.ErrMissingFileName
	}

	if err := g.client.putObject(ctx, g.metadata.Bucket, binarystore.ObjectPath(g.metadata.Prefix, req.FileName), req.Data, req.Overwrite); err != nil {
		if isPreconditionFailed(err) {
			return binarystore.ErrFileAlreadyExists
		}
		return fmt.Errorf("error uploading object %q: %w", req.FileName, err)
	}
	return nil
}

// Get downloads binary data from Google Cloud Storage as a streaming reader.
func (g *GCPBucket) Get(ctx context.Context, req *binarystore.GetRequest) (*binarystore.GetResponse, error) {
	if req.FileName == "" {
		return nil, binarystore.ErrMissingFileName
	}

	body, err := g.client.getObject(ctx, g.metadata.Bucket, binarystore.ObjectPath(g.metadata.Prefix, req.FileName))
	if err != nil {
		if isNotFound(err) {
			return nil, binarystore.ErrFileNotFound
		}
		return nil, fmt.Errorf("error downloading object %q: %w", req.FileName, err)
	}
	return &binarystore.GetResponse{Data: body}, nil
}

// Delete removes an object from Google Cloud Storage. If the object does not
// exist, binarystore.ErrFileNotFound is returned.
func (g *GCPBucket) Delete(ctx context.Context, req *binarystore.DeleteRequest) error {
	if req.FileName == "" {
		return binarystore.ErrMissingFileName
	}

	if err := g.client.deleteObject(ctx, g.metadata.Bucket, binarystore.ObjectPath(g.metadata.Prefix, req.FileName)); err != nil {
		if isNotFound(err) {
			return binarystore.ErrFileNotFound
		}
		return fmt.Errorf("error deleting object %q: %w", req.FileName, err)
	}
	return nil
}

// GetComponentMetadata returns the metadata schema for this component.
func (g *GCPBucket) GetComponentMetadata() (metadataInfo contribMetadata.MetadataMap) {
	metadataStruct := gcpMetadata{}
	contribMetadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, contribMetadata.BinaryStoreType)
	return
}

// Close closes the underlying Google Cloud Storage client.
func (g *GCPBucket) Close() error {
	if g.client == nil {
		return nil
	}
	return g.client.close()
}

func (c *storageClient) putObject(ctx context.Context, bucket, name string, data io.Reader, overwrite bool) error {
	obj := c.client.Bucket(bucket).Object(name)
	if !overwrite {
		obj = obj.If(storage.Conditions{DoesNotExist: true})
	}

	writer := obj.NewWriter(ctx)
	if _, err := io.Copy(writer, data); err != nil {
		_ = writer.Close()
		return err
	}
	return writer.Close()
}

func (c *storageClient) getObject(ctx context.Context, bucket, name string) (io.ReadCloser, error) {
	return c.client.Bucket(bucket).Object(name).NewReader(ctx)
}

func (c *storageClient) deleteObject(ctx context.Context, bucket, name string) error {
	return c.client.Bucket(bucket).Object(name).Delete(ctx)
}

func (c *storageClient) close() error {
	return c.client.Close()
}

func isNotFound(err error) bool {
	var apiErr *googleapi.Error
	return errors.As(err, &apiErr) && apiErr.Code == http.StatusNotFound
}

func isPreconditionFailed(err error) bool {
	var apiErr *googleapi.Error
	return errors.As(err, &apiErr) &&
		(apiErr.Code == http.StatusPreconditionFailed || apiErr.Code == http.StatusConflict)
}
