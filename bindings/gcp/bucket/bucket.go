// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package bucket

import (
	"context"
	b64 "encoding/base64"
	"encoding/json"
	"fmt"
	"strconv"

	"cloud.google.com/go/storage"
	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/kit/logger"
	"github.com/google/uuid"
	"google.golang.org/api/option"
)

const (
	metadataDecodeBase64 = "decodeBase64"
	metadataKey          = "key"
)

// GCPStorage allows saving data to GCP bucket storage
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
}

type createResponse struct {
	Location  string  `json:"Location"`
	VersionID *string `json:"VersionID"`
}

// NewGCPStorage returns a new GCP storage instance
func NewGCPStorage(logger logger.Logger) *GCPStorage {
	return &GCPStorage{logger: logger}
}

// Init performs connection parsing
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
	return []bindings.OperationKind{bindings.CreateOperation}
}

func (g *GCPStorage) Invoke(req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	metadata, err := g.metadata.mergeWithRequestMetadata(req)

	if err != nil {
		return nil, fmt.Errorf("gcp bucket binding error. error merge metadata : %w", err)
	}

	var name string
	if val, ok := req.Metadata[metadataKey]; ok && val != "" {
		name = val
	} else {
		id, err := uuid.NewRandom()
		if err != nil {
			return nil, err
		}
		name = id.String()
	}
	d, err := strconv.Unquote(string(req.Data))
	if err == nil {
		req.Data = []byte(d)
	}

	if metadata.DecodeBase64 {
		decoded, decodeError := b64.StdEncoding.DecodeString(string(req.Data))
		if decodeError != nil {
			return nil, fmt.Errorf("gcp bucket binding error. decode : %w", decodeError)
		}
		req.Data = decoded
	}

	h := g.client.Bucket(g.metadata.Bucket).Object(name).NewWriter(context.Background())
	defer h.Close()
	if _, err := h.Write(req.Data); err != nil {
		return nil, fmt.Errorf("gcp bucket binding error. Uploading: %w", err)
	}

	return nil, nil
}

func (g *GCPStorage) Close() error {
	return g.client.Close()
}

// Helper to merge config and request metadata
func (metadata gcpMetadata) mergeWithRequestMetadata(req *bindings.InvokeRequest) (gcpMetadata, error) {
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
