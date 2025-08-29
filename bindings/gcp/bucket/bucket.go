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
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"reflect"
	"strconv"
	"sync"
	"time"

	"cloud.google.com/go/storage"
	"github.com/google/uuid"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
	kitmd "github.com/dapr/kit/metadata"
	"github.com/dapr/kit/strings"
)

const (
	objectURLBase        = "https://storage.googleapis.com/%s/%s"
	metadataDecodeBase64 = "decodeBase64"
	metadataEncodeBase64 = "encodeBase64"
	metadataSignTTL      = "signTTL"

	metadataContentType = "contentType"
	metadataKey         = "key"
	maxResults          = 1000

	metadataKeyBC    = "name"
	signOperation    = "sign"
	bulkGetOperation = "bulkGet"
	copyOperation    = "copy"
	renameOperation  = "rename"
	moveOperation    = "move"
)

// GCPStorage allows saving data to GCP bucket storage.
type GCPStorage struct {
	metadata *gcpMetadata
	client   *storage.Client
	logger   logger.Logger
}

type gcpMetadata struct {
	// Ignored by metadata parser because included in built-in authentication profile
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
	ContentType         string `json:"contentType,omitempty" mapstructure:"contentType"`

	Bucket       string `json:"bucket" mapstructure:"bucket"`
	DecodeBase64 bool   `json:"decodeBase64,string" mapstructure:"decodeBase64"`
	EncodeBase64 bool   `json:"encodeBase64,string" mapstructure:"encodeBase64"`
	SignTTL      string `json:"signTTL" mapstructure:"signTTL"  mdignore:"true"`
}

type listPayload struct {
	Prefix     string `json:"prefix"`
	MaxResults int32  `json:"maxResults"`
	Delimiter  string `json:"delimiter"`
}

type signResponse struct {
	SignURL string `json:"signURL"`
}

type createResponse struct {
	ObjectURL string `json:"objectURL"`
}

// NewGCPStorage returns a new GCP storage instance.
func NewGCPStorage(logger logger.Logger) bindings.OutputBinding {
	return &GCPStorage{logger: logger}
}

// Init performs connection parsing.
func (g *GCPStorage) Init(ctx context.Context, metadata bindings.Metadata) error {
	m, err := g.parseMetadata(metadata)
	if err != nil {
		return err
	}

	client, err := g.getClient(ctx, m)
	if err != nil {
		return err
	}

	g.metadata = m
	g.client = client

	return nil
}

func (g *GCPStorage) getClient(ctx context.Context, m *gcpMetadata) (*storage.Client, error) {
	var client *storage.Client
	var err error

	if m.Bucket == "" {
		return nil, errors.New("missing property `bucket` in metadata")
	}
	if m.ProjectID == "" {
		return nil, errors.New("missing property `project_id` in metadata")
	}

	// Explicit authentication
	if m.PrivateKeyID != "" {
		var b []byte
		b, err = json.Marshal(m)
		if err != nil {
			return nil, err
		}

		clientOptions := option.WithCredentialsJSON(b)
		client, err = storage.NewClient(ctx, clientOptions)
		if err != nil {
			return nil, err
		}
	} else {
		// Implicit authentication, using GCP Application Default Credentials (ADC)
		// Credentials search order: https://cloud.google.com/docs/authentication/application-default-credentials#order
		client, err = storage.NewClient(ctx)
		if err != nil {
			return nil, err
		}
	}
	return client, nil
}

func (g *GCPStorage) parseMetadata(meta bindings.Metadata) (*gcpMetadata, error) {
	m := gcpMetadata{}
	err := kitmd.DecodeMetadata(meta.Properties, &m)
	if err != nil {
		return nil, err
	}

	return &m, nil
}

func (g *GCPStorage) Operations() []bindings.OperationKind {
	return []bindings.OperationKind{
		bindings.CreateOperation,
		bindings.GetOperation,
		bindings.DeleteOperation,
		bindings.ListOperation,
		signOperation,
		bulkGetOperation,
		copyOperation,
		renameOperation,
		moveOperation,
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
	case signOperation:
		return g.sign(ctx, req)
	case bulkGetOperation:
		return g.bulkGet(ctx, req)
	case copyOperation:
		return g.copy(ctx, req)
	case renameOperation:
		return g.rename(ctx, req)
	case moveOperation:
		return g.move(ctx, req)
	default:
		return nil, fmt.Errorf("unsupported operation %s", req.Operation)
	}
}

func (g *GCPStorage) create(ctx context.Context, req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	var err error
	metadata, err := g.metadata.mergeWithRequestMetadata(req)
	if err != nil {
		return nil, fmt.Errorf("gcp bucket binding error while merging metadata : %w", err)
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

	// Set content type if provided
	if metadata.ContentType != "" {
		h.ContentType = metadata.ContentType
	}

	// Cannot do `defer h.Close()` as Close() will flush the bytes and need to have error handling.
	if _, err = io.Copy(h, r); err != nil {
		cerr := h.Close()
		if cerr != nil {
			return nil, fmt.Errorf("gcp bucket binding error while uploading and closing: %w", err)
		}
		return nil, fmt.Errorf("gcp bucket binding error while uploading: %w", err)
	}

	err = h.Close()
	if err != nil {
		return nil, fmt.Errorf("gcp bucket binding error while flushing: %w", err)
	}

	objectURL, err := url.Parse(fmt.Sprintf(objectURLBase, g.metadata.Bucket, name))
	if err != nil {
		return nil, fmt.Errorf("gcp bucket binding error while building url response: %w", err)
	}

	resp := createResponse{
		ObjectURL: objectURL.String(),
	}

	b, err := json.Marshal(resp)
	if err != nil {
		return nil, fmt.Errorf("gcp bucket binding error while marshalling the create response: %w", err)
	}

	return &bindings.InvokeResponse{
		Data: b,
	}, nil
}

func (g *GCPStorage) get(ctx context.Context, req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	metadata, err := g.metadata.mergeWithRequestMetadata(req)
	if err != nil {
		return nil, fmt.Errorf("gcp binding error while merging metadata : %w", err)
	}

	var key string
	if val, ok := req.Metadata[metadataKey]; ok && val != "" {
		key = val
	} else {
		return nil, errors.New("gcp bucket binding error: can't read key value")
	}

	var rc io.ReadCloser
	rc, err = g.client.Bucket(g.metadata.Bucket).Object(key).NewReader(ctx)
	if err != nil {
		var apiErr *googleapi.Error
		if errors.As(err, &apiErr) && apiErr.Code == http.StatusNotFound {
			return nil, errors.New("object not found")
		}

		return nil, fmt.Errorf("gcp bucketgcp bucket binding error while downloading object: %w", err)
	}
	defer rc.Close()

	data, err := io.ReadAll(rc)
	if err != nil {
		return nil, fmt.Errorf("gcp bucketgcp bucket binding error while reading: %v", err)
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
		return nil, errors.New("gcp bucketgcp bucket binding error: can't read key value")
	}

	object := g.client.Bucket(g.metadata.Bucket).Object(key)

	err := object.Delete(ctx)

	var apiErr *googleapi.Error
	if errors.As(err, &apiErr) && apiErr.Code == http.StatusNotFound {
		return nil, errors.New("object not found")
	}

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
		return nil, fmt.Errorf("gcp bucketgcp bucket binding error while listing: cannot marshal blobs to json: %w", err)
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
		merged.DecodeBase64 = strings.IsTruthy(val)
	}

	if val, ok := req.Metadata[metadataEncodeBase64]; ok && val != "" {
		merged.EncodeBase64 = strings.IsTruthy(val)
	}

	if val, ok := req.Metadata[metadataSignTTL]; ok && val != "" {
		merged.SignTTL = val
	}

	if val, ok := req.Metadata[metadataContentType]; ok && val != "" {
		merged.ContentType = val
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

// GetComponentMetadata returns the metadata of the component.
func (g *GCPStorage) GetComponentMetadata() (metadataInfo metadata.MetadataMap) {
	metadataStruct := gcpMetadata{}
	metadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, metadata.BindingType)
	return
}

func (g *GCPStorage) sign(ctx context.Context, req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	metadata, err := g.metadata.mergeWithRequestMetadata(req)
	if err != nil {
		return nil, fmt.Errorf("gcp binding error while merging metadata : %w", err)
	}

	var key string
	if val, ok := req.Metadata[metadataKey]; ok && val != "" {
		key = val
	} else {
		return nil, errors.New("gcp bucket binding error: can't read key value")
	}

	if metadata.SignTTL == "" {
		return nil, fmt.Errorf("gcp bucket binding error: required metadata '%s' missing", metadataSignTTL)
	}

	signURL, err := g.signObject(metadata.Bucket, key, metadata.SignTTL)
	if err != nil {
		return nil, fmt.Errorf("gcp bucket binding error: %w", err)
	}

	jsonResponse, err := json.Marshal(signResponse{
		SignURL: signURL,
	})
	if err != nil {
		return nil, fmt.Errorf("gcp bucket binding error while marshalling sign response: %w", err)
	}
	return &bindings.InvokeResponse{
		Data: jsonResponse,
	}, nil
}

func (g *GCPStorage) signObject(bucket, object, ttl string) (string, error) {
	d, err := time.ParseDuration(ttl)
	if err != nil {
		return "", fmt.Errorf("gcp bucket binding error while parsing signTTL: %w", err)
	}
	opts := &storage.SignedURLOptions{
		Scheme:  storage.SigningSchemeV4,
		Method:  "GET",
		Expires: time.Now().Add(d),
	}

	u, err := g.client.Bucket(g.metadata.Bucket).SignedURL(object, opts)
	if err != nil {
		return "", fmt.Errorf("Bucket(%q).SignedURL: %w", bucket, err)
	}
	return u, nil
}

type objectData struct {
	Name  string              `json:"name"`
	Data  []byte              `json:"data"`
	Attrs storage.ObjectAttrs `json:"attrs"`
}

func (g *GCPStorage) bulkGet(ctx context.Context, req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	metadata, err := g.metadata.mergeWithRequestMetadata(req)
	if err != nil {
		return nil, fmt.Errorf("gcp binding error while merging metadata : %w", err)
	}

	if g.metadata.Bucket == "" {
		return nil, errors.New("gcp bucket binding error: bucket is required")
	}

	var allObjs []*storage.ObjectAttrs
	it := g.client.Bucket(g.metadata.Bucket).Objects(ctx, nil)
	for {
		var attrs *storage.ObjectAttrs
		attrs, err = it.Next()
		if err == iterator.Done {
			break
		}
		allObjs = append(allObjs, attrs)
	}

	var wg sync.WaitGroup
	wg.Add(len(allObjs))

	objects := make([]objectData, len(allObjs))
	errs := make([]error, len(allObjs))
	for i, obj := range allObjs {
		go func(idx int, object *storage.ObjectAttrs) {
			defer wg.Done()

			rc, gerr := g.client.Bucket(g.metadata.Bucket).Object(object.Name).NewReader(ctx)
			if gerr != nil {
				errs[idx] = err
				return
			}
			defer rc.Close()

			data, gerr := io.ReadAll(rc)
			if gerr != nil {
				errs[idx] = err
				return
			}

			if metadata.EncodeBase64 {
				encoded := b64.StdEncoding.EncodeToString(data)
				data = []byte(encoded)
			}

			objects[idx] = objectData{
				Name:  object.Name,
				Data:  data,
				Attrs: *object,
			}
		}(i, obj)
	}

	wg.Wait()

	if err = errors.Join(errs...); err != nil {
		return nil, fmt.Errorf("gcp bucket binding error while reading objects: %w", err)
	}

	response, err := json.Marshal(objects)
	if err != nil {
		return nil, fmt.Errorf("gcp bucket binding error while marshalling bulk get response: %w", err)
	}

	return &bindings.InvokeResponse{
		Data: response,
	}, nil
}

type movePayload struct {
	DestinationBucket string `json:"destinationBucket"`
}

func (g *GCPStorage) move(ctx context.Context, req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	var key string
	if val, ok := req.Metadata[metadataKey]; ok && val != "" {
		key = val
	} else {
		return nil, errors.New("gcp bucket binding error: can't read key value")
	}

	var payload movePayload
	err := json.Unmarshal(req.Data, &payload)
	if err != nil {
		return nil, errors.New("gcp bucket binding error: invalid move payload")
	}

	if payload.DestinationBucket == "" {
		return nil, errors.New("gcp bucket binding error: required 'destinationBucket' missing")
	}

	src := g.client.Bucket(g.metadata.Bucket).Object(key)
	dst := g.client.Bucket(payload.DestinationBucket).Object(key)
	if _, err := dst.CopierFrom(src).Run(ctx); err != nil {
		return nil, fmt.Errorf("gcp bucket binding error while copying object: %w", err)
	}

	if err := src.Delete(ctx); err != nil {
		return nil, fmt.Errorf("gcp bucket binding error while deleting object: %w", err)
	}

	return &bindings.InvokeResponse{
		Data: []byte(fmt.Sprintf("object %s moved to %s", key, payload.DestinationBucket)),
	}, nil
}

type renamePayload struct {
	NewName string `json:"newName"`
}

func (g *GCPStorage) rename(ctx context.Context, req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	var key string
	if val, ok := req.Metadata[metadataKey]; ok && val != "" {
		key = val
	} else {
		return nil, errors.New("gcp bucket binding error: can't read key value")
	}

	var payload renamePayload
	err := json.Unmarshal(req.Data, &payload)
	if err != nil {
		return nil, errors.New("gcp bucket binding error: invalid rename payload")
	}

	if payload.NewName == "" {
		return nil, errors.New("gcp bucket binding error: required 'newName' missing")
	}

	src := g.client.Bucket(g.metadata.Bucket).Object(key)
	dst := g.client.Bucket(g.metadata.Bucket).Object(payload.NewName)
	if _, err := dst.CopierFrom(src).Run(ctx); err != nil {
		return nil, fmt.Errorf("gcp bucket binding error while copying object: %w", err)
	}

	if err := src.Delete(ctx); err != nil {
		return nil, fmt.Errorf("gcp bucket binding error while deleting object: %w", err)
	}

	return &bindings.InvokeResponse{
		Data: []byte(fmt.Sprintf("object %s renamed to %s", key, payload.NewName)),
	}, nil
}

type copyPayload struct {
	DestinationBucket string `json:"destinationBucket"`
}

func (g *GCPStorage) copy(ctx context.Context, req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	var key string
	if val, ok := req.Metadata[metadataKey]; ok && val != "" {
		key = val
	} else {
		return nil, errors.New("gcp bucket binding error: can't read key value")
	}

	var payload copyPayload
	err := json.Unmarshal(req.Data, &payload)
	if err != nil {
		return nil, errors.New("gcp bucket binding error: invalid copy payload")
	}

	if payload.DestinationBucket == "" {
		return nil, errors.New("gcp bucket binding error: required 'destinationBucket' missing")
	}

	src := g.client.Bucket(g.metadata.Bucket).Object(key)
	dst := g.client.Bucket(payload.DestinationBucket).Object(key)
	if _, err := dst.CopierFrom(src).Run(ctx); err != nil {
		return nil, fmt.Errorf("gcp bucket binding error while copying object: %w", err)
	}

	return &bindings.InvokeResponse{
		Data: []byte(fmt.Sprintf("object %s copied to %s", key, payload.DestinationBucket)),
	}, nil
}
