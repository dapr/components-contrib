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

package infisical

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/go-resty/resty/v2"
	apiauth "github.com/infisical/go-sdk/packages/api/auth"
	apisecrets "github.com/infisical/go-sdk/packages/api/secrets"
	"github.com/infisical/go-sdk/packages/models"
	"github.com/infisical/go-sdk/packages/util"

	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/secretstores"
	"github.com/dapr/kit/logger"
	kitmd "github.com/dapr/kit/metadata"
)

const (
	projectIDMetadataKey              = "projectId"
	workspaceIDMetadataKey            = "workspaceId"
	environmentMetadataKey            = "environment"
	apiURLMetadataKey                 = "apiUrl"
	clientIDMetadataKey               = "clientId"
	clientSecretMetadataKey           = "clientSecret"
	accessTokenMetadataKey            = "accessToken"
	secretPathMetadataKey             = "secretPath"
	typeMetadataKey                   = "type"
	includeImportsMetadataKey         = "includeImports"
	expandSecretReferencesMetadataKey = "expandSecretReferences"
)

type secretStoreMetadata struct {
	ProjectID              string `json:"projectId" mapstructure:"projectId"`
	WorkspaceID            string `json:"workspaceId" mapstructure:"workspaceId"`
	Environment            string `json:"environment" mapstructure:"environment"`
	APIURL                 string `json:"apiUrl" mapstructure:"apiUrl"`
	ClientID               string `json:"clientId" mapstructure:"clientId" mdignore:"true"`
	ClientSecret           string `json:"clientSecret" mapstructure:"clientSecret" mdignore:"true"`
	AccessToken            string `json:"accessToken" mapstructure:"accessToken" mdignore:"true"`
	SecretPath             string `json:"secretPath" mapstructure:"secretPath"`
	IncludeImports         bool   `json:"includeImports" mapstructure:"includeImports"`
	ExpandSecretReferences bool   `json:"expandSecretReferences" mapstructure:"expandSecretReferences"`
}

type resolvedRequestMetadata struct {
	projectID              string
	environment            string
	secretPath             string
	secretType             string
	includeImports         bool
	expandSecretReferences bool
}

type infisicalClient interface {
	LoginUniversal(clientID, clientSecret string) error
	SetAccessToken(accessToken string)
	GetSecret(options apisecrets.RetrieveSecretV3RawRequest) (models.Secret, error)
	ListSecrets(options apisecrets.ListSecretsV3RawRequest) ([]models.Secret, error)
}

type sdkClient struct {
	httpClient *resty.Client
}

func newInfisicalClient(_ context.Context, apiURL string) infisicalClient {
	siteURL := strings.TrimSpace(apiURL)
	if siteURL == "" {
		siteURL = "https://app.infisical.com"
	}

	return &sdkClient{
		httpClient: resty.New().SetBaseURL(util.AppendAPIEndpoint(siteURL)).SetHeader("User-Agent", "dapr-components-contrib-infisical"),
	}
}

func (c *sdkClient) LoginUniversal(clientID, clientSecret string) error {
	credential, err := apiauth.CallUniversalAuthLogin(c.httpClient, apiauth.UniversalAuthLoginRequest{
		ClientID:     clientID,
		ClientSecret: clientSecret,
	})
	if err != nil {
		return err
	}
	c.httpClient.SetAuthScheme("Bearer")
	c.httpClient.SetAuthToken(credential.AccessToken)
	return nil
}

func (c *sdkClient) SetAccessToken(accessToken string) {
	c.httpClient.SetAuthScheme("Bearer")
	c.httpClient.SetAuthToken(accessToken)
}

func (c *sdkClient) GetSecret(options apisecrets.RetrieveSecretV3RawRequest) (models.Secret, error) {
	response, err := apisecrets.CallRetrieveSecretV3(c.httpClient, options)
	if err != nil {
		return models.Secret{}, err
	}
	return response.Secret, nil
}

func (c *sdkClient) ListSecrets(options apisecrets.ListSecretsV3RawRequest) ([]models.Secret, error) {
	response, err := apisecrets.CallListSecretsV3(c.httpClient, options)
	if err != nil {
		return nil, err
	}

	secrets := append([]models.Secret(nil), response.Secrets...)
	if options.IncludeImports {
		knownKeys := make(map[string]struct{}, len(secrets))
		for _, secret := range secrets {
			knownKeys[secret.SecretKey] = struct{}{}
		}

		for _, importBlock := range response.Imports {
			for _, secret := range importBlock.Secrets {
				if _, ok := knownKeys[secret.SecretKey]; ok {
					continue
				}
				knownKeys[secret.SecretKey] = struct{}{}
				secrets = append(secrets, secret)
			}
		}
	}

	return secrets, nil
}

var _ secretstores.SecretStore = (*secretStore)(nil)

type secretStore struct {
	logger logger.Logger

	client    infisicalClient
	metadata  secretStoreMetadata
	projectID string

	newClient func(ctx context.Context, apiURL string) infisicalClient
}

// NewInfisicalSecretStore returns a new Infisical secret store.
func NewInfisicalSecretStore(logger logger.Logger) secretstores.SecretStore {
	return &secretStore{logger: logger, newClient: newInfisicalClient}
}

func (s *secretStore) Init(ctx context.Context, meta secretstores.Metadata) error {
	var m secretStoreMetadata
	if err := kitmd.DecodeMetadata(meta.Properties, &m); err != nil {
		return err
	}

	projectID := strings.TrimSpace(m.ProjectID)
	if projectID == "" {
		projectID = strings.TrimSpace(m.WorkspaceID)
	}
	if projectID == "" {
		return fmt.Errorf("missing required metadata: %s (or %s)", projectIDMetadataKey, workspaceIDMetadataKey)
	}
	if strings.TrimSpace(m.Environment) == "" {
		return fmt.Errorf("missing required metadata: %s", environmentMetadataKey)
	}
	if strings.TrimSpace(m.AccessToken) == "" && (strings.TrimSpace(m.ClientID) == "" || strings.TrimSpace(m.ClientSecret) == "") {
		return fmt.Errorf("missing required metadata: either %s, or both %s and %s", accessTokenMetadataKey, clientIDMetadataKey, clientSecretMetadataKey)
	}

	client := s.newClient(ctx, m.APIURL)

	if strings.TrimSpace(m.AccessToken) != "" {
		client.SetAccessToken(m.AccessToken)
	} else {
		if err := client.LoginUniversal(m.ClientID, m.ClientSecret); err != nil {
			return fmt.Errorf("failed to authenticate with Infisical: %w", err)
		}
	}

	s.client = client
	s.metadata = m
	s.projectID = projectID

	return nil
}

// GetSecret retrieves a secret using a key and returns a map of decrypted string/string values.
func (s *secretStore) GetSecret(_ context.Context, req secretstores.GetSecretRequest) (secretstores.GetSecretResponse, error) {
	if s.client == nil {
		return secretstores.GetSecretResponse{}, errors.New("secret store has not been initialized")
	}

	resolved, err := s.resolveMetadata(req.Metadata)
	if err != nil {
		return secretstores.GetSecretResponse{}, err
	}

	secret, err := s.client.GetSecret(apisecrets.RetrieveSecretV3RawRequest{
		SecretKey:      req.Name,
		ProjectID:      resolved.projectID,
		Environment:    resolved.environment,
		SecretPath:     resolved.secretPath,
		Type:           resolved.secretType,
		IncludeImports: resolved.includeImports,
	})
	if err != nil {
		return secretstores.GetSecretResponse{}, fmt.Errorf("failed to get secret %q from Infisical: %w", req.Name, err)
	}

	secretKey := secret.SecretKey
	if secretKey == "" {
		secretKey = req.Name
	}

	return secretstores.GetSecretResponse{
		Data: map[string]string{
			secretKey: secret.SecretValue,
		},
	}, nil
}

// BulkGetSecret retrieves all secrets in the store and returns a map of decrypted string/string values.
func (s *secretStore) BulkGetSecret(_ context.Context, req secretstores.BulkGetSecretRequest) (secretstores.BulkGetSecretResponse, error) {
	if s.client == nil {
		return secretstores.BulkGetSecretResponse{}, errors.New("secret store has not been initialized")
	}

	resolved, err := s.resolveMetadata(req.Metadata)
	if err != nil {
		return secretstores.BulkGetSecretResponse{}, err
	}

	secrets, err := s.client.ListSecrets(apisecrets.ListSecretsV3RawRequest{
		ProjectID:              resolved.projectID,
		Environment:            resolved.environment,
		SecretPath:             resolved.secretPath,
		IncludeImports:         resolved.includeImports,
		ExpandSecretReferences: resolved.expandSecretReferences,
		Recursive:              true,
	})
	if err != nil {
		return secretstores.BulkGetSecretResponse{}, fmt.Errorf("failed to list secrets from Infisical: %w", err)
	}

	data := make(map[string]map[string]string, len(secrets))
	for _, secret := range secrets {
		if secret.SecretKey == "" {
			continue
		}
		data[secret.SecretKey] = map[string]string{secret.SecretKey: secret.SecretValue}
	}

	return secretstores.BulkGetSecretResponse{Data: data}, nil
}

func (s *secretStore) resolveMetadata(requestMetadata map[string]string) (resolvedRequestMetadata, error) {
	resolved := resolvedRequestMetadata{
		projectID:              s.projectID,
		environment:            s.metadata.Environment,
		secretPath:             s.metadata.SecretPath,
		includeImports:         s.metadata.IncludeImports,
		expandSecretReferences: s.metadata.ExpandSecretReferences,
	}

	// Track whether projectId was explicitly set so it takes precedence over workspaceId.
	projectIDSet := false
	for key, value := range requestMetadata {
		switch strings.ToLower(key) {
		case strings.ToLower(projectIDMetadataKey):
			resolved.projectID = strings.TrimSpace(value)
			projectIDSet = true
		case strings.ToLower(workspaceIDMetadataKey):
			if !projectIDSet {
				resolved.projectID = strings.TrimSpace(value)
			}
		case strings.ToLower(environmentMetadataKey):
			resolved.environment = strings.TrimSpace(value)
		case strings.ToLower(secretPathMetadataKey):
			resolved.secretPath = strings.TrimSpace(value)
		case strings.ToLower(typeMetadataKey):
			resolved.secretType = strings.TrimSpace(value)
		case strings.ToLower(includeImportsMetadataKey):
			parsed, err := strconv.ParseBool(strings.TrimSpace(value))
			if err != nil {
				return resolvedRequestMetadata{}, fmt.Errorf("invalid %s metadata value %q: %w", includeImportsMetadataKey, value, err)
			}
			resolved.includeImports = parsed
		case strings.ToLower(expandSecretReferencesMetadataKey):
			parsed, err := strconv.ParseBool(strings.TrimSpace(value))
			if err != nil {
				return resolvedRequestMetadata{}, fmt.Errorf("invalid %s metadata value %q: %w", expandSecretReferencesMetadataKey, value, err)
			}
			resolved.expandSecretReferences = parsed
		}
	}

	if strings.TrimSpace(resolved.projectID) == "" {
		return resolvedRequestMetadata{}, fmt.Errorf("missing required metadata: %s (or %s)", projectIDMetadataKey, workspaceIDMetadataKey)
	}
	if strings.TrimSpace(resolved.environment) == "" {
		return resolvedRequestMetadata{}, fmt.Errorf("missing required metadata: %s", environmentMetadataKey)
	}

	return resolved, nil
}

// Features returns the features available in this secret store.
func (s *secretStore) Features() []secretstores.Feature {
	return []secretstores.Feature{}
}

func (s *secretStore) GetComponentMetadata() (metadataInfo metadata.MetadataMap) {
	metadataStruct := secretStoreMetadata{}
	_ = metadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, metadata.SecretStoreType)
	return
}

func (s *secretStore) Close() error {
	return nil
}
