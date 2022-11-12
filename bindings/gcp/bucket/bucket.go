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

package bucket

import (
	"bytes"
	"context"
	b64 "encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"strconv"

	"cloud.google.com/go/storage"
	"github.com/google/uuid"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/components-contrib/internal/utils"
	"github.com/dapr/kit/logger"
)

const (
	objectURLBase        = "https://storage.googleapis.com/%s/%s"
	metadataDecodeBase64 = "decodeBase64"
	metadataEncodeBase64 = "encodeBase64"

	metadataKey = "key"
	maxResults  = 1000

	metadataKeyBC = "name"
)

// GCPStorage allows saving data to GCP bucket storage.
type GCPStorage struct {
	metadata *gcpMetadata
	client   *storage.Client
	logger   logger.Logger
}

type gcpMetadata struct {
	Bucket              string `json:"bucket"`
	Type                string `json:"type"`
	ProjectID           string `json:"project_id"`
	PrivateKeyID        string `json:"private_key_id"`
	PrivateKey          string `json:"private_key"`
	ClientEmail         string `json:"client_email"`
	ClientID            string `json:"client_id"`
	AuthURI             string `json:"auth_uri"`
	TokenURI            string `json:"token_uri"`
	AuthProviderCertURL string `json:"auth_provider_x509_cert_url"`
	ClientCertURL       string `json:"client_x509_cert_url"`
	DecodeBase64        bool   `json:"decodeBase64,string"`
	EncodeBase64        bool   `json:"encodeBase64,string"`
}

type listPayload struct {
	Prefix     string `json:"prefix"`
	MaxResults int32  `json:"maxResults"`
	Delimiter  string `json:"delimiter"`
}

type createResponse struct {
	ObjectURL string `json:"objectURL"`
}

// NewGCPStorage returns a new GCP storage instance.
func NewGCPStorage(logger logger.Logger) bindings.OutputBinding {
	return &GCPStorage{logger: logger}
}

// Init performs connection parsing.
func (g *GCPStorage) Init(metadata bindings.Metadata) error {
	m, b, err := g.parseMetadata(metadata)
	if err != nil {
		return err
	}

	clientOptions := option.WithCredentialsJSON(b)
	ctx := context.Background()
	client, err := storage.NewClient(ctx, clientOptions)
	if err != nil {
		return err
	}

	g.metadata = m
	g.client = client

	return nil
}

func (g *GCPStorage) parseMetadata(metadata bindings.Metadata) (*gcpMetadata, []byte, error) {
	b, err := json.Marshal(metadata.Properties)
	if err != nil {
		return nil, nil, err
	}

	var m gcpMetadata
	err = json.Unmarshal(b, &m)
	if err != nil {
		return nil, nil, err
	}

	return &m, b, nil
}

func (g *GCPStorage) Operations() []bindings.OperationKind {
	return []bindings.OperationKind{
		bindings.CreateOperation,
		bindings.GetOperation,
		bindings.DeleteOperation,
		bindings.ListOperation,
	}
}

func (g *GCPStorage) Invoke(ctx context.Context, req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	req.Metadata = g.handleBackwardCompatibilityForMetadata(req.Metadata)

	switch req.Operation {
	case bindings.CreateOperation:
		return g.create(ctx, req)
	case bindings.GetOperation:
		return g.get(ctx, req)
	case bindings.DeleteOperation:
		return g.delete(ctx, req)
	case bindings.ListOperation:
		return g.list(ctx, req)
	default:
		return nil, fmt.Errorf("unsupported operation %s", req.Operation)
	}
}

func (g *GCPStorage) create(ctx context.Context, req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	var err error
	metadata, err := g.metadata.mergeWithRequestMetadata(req)
	if err != nil {
		return nil, fmt.Errorf("gcp bucket binding error. error merge metadata : %w", err)
	}

	var name string
	if val, ok := req.Metadata[metadataKey]; ok && val != "" {
		name = val
	} else {
		name = uuid.New().String()
		g.logger.Debugf("key not found. generating name %s", name)
	}

	d, err := strconv.Unquote(string(req.Data))
	if err == nil {
		req.Data = []byte(d)
	}

	var r io.Reader = bytes.NewReader(req.Data)
	if metadata.DecodeBase64 {
		r = b64.NewDecoder(b64.StdEncoding, r)
	}

	h := g.client.Bucket(g.metadata.Bucket).Object(name).NewWriter(ctx)
	defer h.Close()
	if _, err = io.Copy(h, r); err != nil {
		return nil, fmt.Errorf("gcp bucket binding error. Uploading: %w", err)
	}

	objectURL, err := url.Parse(fmt.Sprintf(objectURLBase, g.metadata.Bucket, name))
	if err != nil {
		return nil, fmt.Errorf("gcp bucket binding error. error building url response: %w", err)
	}

	resp := createResponse{
		ObjectURL: objectURL.String(),
	}

	b, err := json.Marshal(resp)
	if err != nil {
		return nil, fmt.Errorf("gcp binding error. error marshalling create response: %w", err)
	}

	return &bindings.InvokeResponse{
		Data: b,
	}, nil
}

func (g *GCPStorage) get(ctx context.Context, req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	metadata, err := g.metadata.mergeWithRequestMetadata(req)
	if err != nil {
		return nil, fmt.Errorf("gcp binding error. error merge metadata : %w", err)
	}

	var key string
	if val, ok := req.Metadata[metadataKey]; ok && val != "" {
		key = val
	} else {
		return nil, fmt.Errorf("gcp bucket binding error: can't read key value")
	}

	var rc io.ReadCloser
	rc, err = g.client.Bucket(g.metadata.Bucket).Object(key).NewReader(ctx)
	if err != nil {
		return nil, fmt.Errorf("gcp bucketgcp bucket binding error: error downloading bucket object: %w", err)
	}
	defer rc.Close()

	data, err := io.ReadAll(rc)
	if err != nil {
		return nil, fmt.Errorf("gcp bucketgcp bucket binding error: io.ReadAll: %v", err)
	}

	if metadata.EncodeBase64 {
		encoded := b64.StdEncoding.EncodeToString(data)
		data = []byte(encoded)
	}

	return &bindings.InvokeResponse{
		Data:     data,
		Metadata: nil,
	}, nil
}

func (g *GCPStorage) delete(ctx context.Context, req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	var key string
	if val, ok := req.Metadata[metadataKey]; ok && val != "" {
		key = val
	} else {
		return nil, fmt.Errorf("gcp bucketgcp bucket binding error: can't read key value")
	}

	object := g.client.Bucket(g.metadata.Bucket).Object(key)

	err := object.Delete(ctx)

	return nil, err
}

func (g *GCPStorage) list(ctx context.Context, req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	var payload listPayload
	err := json.Unmarshal(req.Data, &payload)
	if err != nil {
		return nil, err
	}

	if payload.MaxResults == int32(0) {
		payload.MaxResults = maxResults
	}

	input := &storage.Query{
		Prefix:    payload.Prefix,
		Delimiter: payload.Delimiter,
	}

	var result []storage.ObjectAttrs
	it := g.client.Bucket(g.metadata.Bucket).Objects(ctx, input)
	for {
		attrs, errIt := it.Next()
		if errIt == iterator.Done || len(result) == int(payload.MaxResults) {
			break
		}
		result = append(result, *attrs)
	}

	jsonResponse, err := json.Marshal(result)
	if err != nil {
		return nil, fmt.Errorf("gcp bucketgcp bucket binding error. list operation. cannot marshal blobs to json: %w", err)
	}

	return &bindings.InvokeResponse{
		Data: jsonResponse,
	}, nil
}

func (g *GCPStorage) Close() error {
	return g.client.Close()
}

// Helper to merge config and request metadata.
func (metadata gcpMetadata) mergeWithRequestMetadata(req *bindings.InvokeRequest) (gcpMetadata, error) {
	merged := metadata

	if val, ok := req.Metadata[metadataDecodeBase64]; ok && val != "" {
		merged.DecodeBase64 = utils.IsTruthy(val)
	}

	if val, ok := req.Metadata[metadataEncodeBase64]; ok && val != "" {
		merged.EncodeBase64 = utils.IsTruthy(val)
	}

	return merged, nil
}

// Add backward compatibility. 'key' replace 'name'.
func (g *GCPStorage) handleBackwardCompatibilityForMetadata(metadata map[string]string) map[string]string {
	if val, ok := metadata[metadataKeyBC]; ok && val != "" {
		metadata[metadataKey] = val
		delete(metadata, metadataKeyBC)
	}

	return metadata
}
