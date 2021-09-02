// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package bucket

import (
	"context"
	"encoding/json"

	"cloud.google.com/go/storage"
	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/kit/logger"
	"github.com/google/uuid"
	"google.golang.org/api/option"
)

// GCPStorage allows saving data to GCP bucket storage
type GCPStorage struct {
	metadata gcpMetadata
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
}

// NewGCPStorage returns a new GCP storage instance
func NewGCPStorage(logger logger.Logger) *GCPStorage {
	return &GCPStorage{logger: logger}
}

// Init performs connection parsing
func (g *GCPStorage) Init(metadata bindings.Metadata) error {
	b, err := g.parseMetadata(metadata)
	if err != nil {
		return err
	}

	var gm gcpMetadata
	err = json.Unmarshal(b, &gm)
	if err != nil {
		return err
	}
	clientOptions := option.WithCredentialsJSON(b)
	ctx := context.Background()
	client, err := storage.NewClient(ctx, clientOptions)
	if err != nil {
		return err
	}

	g.metadata = gm
	g.client = client

	return nil
}

func (g *GCPStorage) parseMetadata(metadata bindings.Metadata) ([]byte, error) {
	b, err := json.Marshal(metadata.Properties)
	if err != nil {
		return nil, err
	}

	return b, nil
}

func (g *GCPStorage) Operations() []bindings.OperationKind {
	return []bindings.OperationKind{bindings.CreateOperation}
}

func (g *GCPStorage) Invoke(req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	var name string
	if val, ok := req.Metadata["name"]; ok && val != "" {
		name = val
	} else {
		id, err := uuid.NewRandom()
		if err != nil {
			return nil, err
		}
		name = id.String()
	}
	h := g.client.Bucket(g.metadata.Bucket).Object(name).NewWriter(context.Background())
	defer h.Close()
	if _, err := h.Write(req.Data); err != nil {
		return nil, err
	}

	return nil, nil
}

func (g *GCPStorage) Close() error {
	return g.client.Close()
}
