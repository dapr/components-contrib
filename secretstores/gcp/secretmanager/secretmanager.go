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

package secretmanager

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"

	secretmanager "cloud.google.com/go/secretmanager/apiv1"
	"cloud.google.com/go/secretmanager/apiv1/secretmanagerpb"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"

	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/secretstores"
	"github.com/dapr/kit/logger"

	"github.com/googleapis/gax-go/v2"
)

const VersionID = "version_id"

type GcpSecretManagerMetadata struct {
	Type                string `mapstructure:"type" json:"type"`
	ProjectID           string `mapstructure:"project_id" json:"project_id"`
	PrivateKey          string `mapstructure:"private_key" json:"private_key"`
	ClientEmail         string `mapstructure:"client_email" json:"client_email"`
	PrivateKeyID        string `mapstructure:"private_key_id" json:"private_key_id"`
	ClientID            string `mapstructure:"client_id" json:"client_id"`
	AuthURI             string `mapstructure:"auth_uri" json:"auth_uri"`
	TokenURI            string `mapstructure:"token_uri" json:"token_uri"`
	AuthProviderCertURL string `mapstructure:"auth_provider_x509_cert_url" json:"auth_provider_x509_cert_url"`
	ClientCertURL       string `mapstructure:"client_x509_cert_url" json:"client_x509_cert_url"`
}

type gcpSecretemanagerClient interface {
	AccessSecretVersion(ctx context.Context, req *secretmanagerpb.AccessSecretVersionRequest, opts ...gax.CallOption) (*secretmanagerpb.AccessSecretVersionResponse, error)
	ListSecrets(ctx context.Context, req *secretmanagerpb.ListSecretsRequest, opts ...gax.CallOption) *secretmanager.SecretIterator
	Close() error
}

var _ secretstores.SecretStore = (*Store)(nil)

// Store contains and GCP secret manager client and project id.
type Store struct {
	client    gcpSecretemanagerClient
	ProjectID string

	logger logger.Logger
}

// NewSecreteManager returns new instance of  `SecretManagerStore`.
func NewSecreteManager(logger logger.Logger) secretstores.SecretStore {
	return &Store{logger: logger}
}

// Init creates a GCP secret manager client.
func (s *Store) Init(ctx context.Context, metadataRaw secretstores.Metadata) error {
	metadata, err := s.parseSecretManagerMetadata(metadataRaw)
	if err != nil {
		return err
	}

	client, err := s.getClient(ctx, metadata)
	if err != nil {
		return fmt.Errorf("failed to setup secretmanager client: %s", err)
	}

	s.client = client
	s.ProjectID = metadata.ProjectID

	return nil
}

func (s *Store) getClient(ctx context.Context, metadata *GcpSecretManagerMetadata) (*secretmanager.Client, error) {
	b, _ := json.Marshal(metadata)
	clientOptions := option.WithCredentialsJSON(b)

	client, err := secretmanager.NewClient(ctx, clientOptions)
	if err != nil {
		return nil, err
	}

	return client, nil
}

// GetSecret retrieves a secret using a key and returns a map of decrypted string.
func (s *Store) GetSecret(ctx context.Context, req secretstores.GetSecretRequest) (secretstores.GetSecretResponse, error) {
	res := secretstores.GetSecretResponse{Data: nil}

	if s.client == nil {
		return res, fmt.Errorf("client is not initialized")
	}

	if req.Name == "" {
		return res, fmt.Errorf("missing secret name in request")
	}
	secretName := fmt.Sprintf("projects/%s/secrets/%s", s.ProjectID, req.Name)

	versionID := "latest"
	if value, ok := req.Metadata[VersionID]; ok {
		versionID = value
	}

	secret, err := s.getSecret(ctx, secretName, versionID)
	if err != nil {
		return res, fmt.Errorf("failed to access secret version: %v", err)
	}

	return secretstores.GetSecretResponse{Data: map[string]string{req.Name: *secret}}, nil
}

// BulkGetSecret retrieves all secrets in the store and returns a map of decrypted string/string values.
func (s *Store) BulkGetSecret(ctx context.Context, req secretstores.BulkGetSecretRequest) (secretstores.BulkGetSecretResponse, error) {
	versionID := "latest"

	response := map[string]map[string]string{}

	if s.client == nil {
		return secretstores.BulkGetSecretResponse{Data: nil}, fmt.Errorf("client is not initialized")
	}

	request := &secretmanagerpb.ListSecretsRequest{
		Parent: fmt.Sprintf("projects/%s", s.ProjectID),
	}
	it := s.client.ListSecrets(ctx, request)

	for {
		resp, err := it.Next()

		if err == iterator.Done {
			break
		}

		if err != nil {
			return secretstores.BulkGetSecretResponse{Data: nil}, fmt.Errorf("failed to list secrets: %v", err)
		}

		name := resp.GetName()
		secret, err := s.getSecret(ctx, name, versionID)
		if err != nil {
			return secretstores.BulkGetSecretResponse{Data: nil}, fmt.Errorf("failed to access secret version: %v", err)
		}
		response[name] = map[string]string{name: *secret}
	}

	return secretstores.BulkGetSecretResponse{Data: response}, nil
}

func (s *Store) getSecret(ctx context.Context, secretName string, versionID string) (*string, error) {
	accessRequest := &secretmanagerpb.AccessSecretVersionRequest{
		Name: fmt.Sprintf("%s/versions/%s", secretName, versionID),
	}
	result, err := s.client.AccessSecretVersion(ctx, accessRequest)
	if err != nil {
		return nil, err
	}

	secret := string(result.Payload.Data)

	return &secret, nil
}

func (s *Store) parseSecretManagerMetadata(metadataRaw secretstores.Metadata) (*GcpSecretManagerMetadata, error) {
	meta := GcpSecretManagerMetadata{}
	metadata.DecodeMetadata(metadataRaw.Properties, &meta)

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

func (s *Store) Close() error {
	return s.client.Close()
}

// Features returns the features available in this secret store.
func (s *Store) Features() []secretstores.Feature {
	return []secretstores.Feature{} // No Feature supported.
}

func (s *Store) GetComponentMetadata() map[string]string {
	metadataStruct := GcpSecretManagerMetadata{}
	metadataInfo := map[string]string{}
	metadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo)
	return metadataInfo
}
