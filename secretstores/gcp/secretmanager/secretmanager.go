package secretmanager

import (
	"context"
	"encoding/json"
	"fmt"

	secretmanager "cloud.google.com/go/secretmanager/apiv1beta1"
	"github.com/dapr/components-contrib/secretstores"
	"github.com/dapr/dapr/pkg/logger"
	"google.golang.org/api/option"
	secretmanagerpb "google.golang.org/genproto/googleapis/cloud/secretmanager/v1beta1"
)

const VersionID = "version_id"

type gcpCredentials struct {
	Type                string `json:"type"`
	ProjectID           string `json:"project_id"`
	PrivateKey          string `json:"private_key"`
	ClientEmail         string `json:"client_email"`
	PrivateKeyID        string `json:"private_key_id"`
	ClientID            string `json:"client_id"`
	AuthURI             string `json:"auth_uri"`
	TokenURI            string `json:"token_uri"`
	AuthProviderCertURL string `json:"auth_provider_x509_cert_url"`
	ClientCertURL       string `json:"client_x509_cert_url"`
}

type secretManagerMetadata struct {
	gcpCredentials
}

// Store contains and GCP secret manager client and project id
type Store struct {
	client    *secretmanager.Client
	ProjectID string

	logger logger.Logger
}

// NewSecreteManager returns new instance of  `SecretManagerStore`
func NewSecreteManager(logger logger.Logger) *Store {
	return &Store{logger: logger}
}

// Init creates a GCP secret manager client
func (s *Store) Init(metadataRaw secretstores.Metadata) error {
	metadata, err := s.parseSecretManagerMetadata(metadataRaw)
	if err != nil {
		return err
	}

	client, err := s.getClient(metadata)
	if err != nil {
		return fmt.Errorf("failed to setup secretmanager client: %s", err)
	}

	s.client = client
	s.ProjectID = metadata.ProjectID

	return nil
}

func (s *Store) getClient(metadata *secretManagerMetadata) (*secretmanager.Client, error) {
	b, _ := json.Marshal(metadata)
	clientOptions := option.WithCredentialsJSON(b)
	ctx := context.Background()

	client, err := secretmanager.NewClient(ctx, clientOptions)
	if err != nil {
		return nil, err
	}

	return client, nil
}

// GetSecret retrieves a secret using a key and returns a map of decrypted string
func (s *Store) GetSecret(req secretstores.GetSecretRequest) (secretstores.GetSecretResponse, error) {
	res := secretstores.GetSecretResponse{Data: nil}

	if s.client == nil {
		return res, fmt.Errorf("client is not initialized")
	}

	if req.Name == "" {
		return res, fmt.Errorf("missing secret name in request")
	}

	versionID := "latest"
	if value, ok := req.Metadata[VersionID]; ok {
		versionID = value
	}

	ctx := context.Background()
	accessRequest := &secretmanagerpb.AccessSecretVersionRequest{
		Name: fmt.Sprintf("projects/%s/secrets/%s/versions/%s", s.ProjectID, req.Name, versionID),
	}
	result, err := s.client.AccessSecretVersion(ctx, accessRequest)
	if err != nil {
		return res, fmt.Errorf("failed to access secret version: %v", err)
	}
	return secretstores.GetSecretResponse{Data: map[string]string{req.Name: string(result.Payload.Data)}}, nil
}

func (s *Store) parseSecretManagerMetadata(metadataRaw secretstores.Metadata) (*secretManagerMetadata, error) {
	b, err := json.Marshal(metadataRaw.Properties)
	if err != nil {
		return nil, err
	}

	var meta secretManagerMetadata
	err = json.Unmarshal(b, &meta)
	if err != nil {
		return nil, err
	}

	if meta.Type == "" {
		return nil, fmt.Errorf("missing property `type` in metadata")
	}
	if meta.ProjectID == "" {
		return nil, fmt.Errorf("missing property `project_id` in metadata")
	}
	if meta.PrivateKey == "" {
		return nil, fmt.Errorf("missing property `private_key` in metadata")
	}
	if meta.ClientEmail == "" {
		return nil, fmt.Errorf("missing property `client_email` in metadata")
	}
	return &meta, nil
}
