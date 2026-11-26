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
	"sync"

	infisicalsdk "github.com/infisical/go-sdk"

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
	caPemMetadataKey                  = "caPem"
	clientIDMetadataKey               = "clientId"
	clientSecretMetadataKey           = "clientSecret"
	accessTokenMetadataKey            = "accessToken"
	tokenMetadataKey                  = "token"
	secretPathMetadataKey             = "secretPath"
	typeMetadataKey                   = "type"
	versionIDMetadataKey              = "version_id"
	includeImportsMetadataKey         = "includeImports"
	expandSecretReferencesMetadataKey = "expandSecretReferences"
	recursiveMetadataKey              = "recursive"
)

type secretStoreMetadata struct {
	ProjectID              string `json:"projectId" mapstructure:"projectId" mapstructurealiases:"workspaceId" mdaliases:"workspaceId"`
	Environment            string `json:"environment" mapstructure:"environment"`
	APIURL                 string `json:"apiUrl" mapstructure:"apiUrl"`
	CaPem                  string `json:"caPem" mapstructure:"caPem"`
	ClientID               string `json:"clientId" mapstructure:"clientId"`
	ClientSecret           string `json:"clientSecret" mapstructure:"clientSecret"`
	AccessToken            string `json:"accessToken" mapstructure:"accessToken" mapstructurealiases:"token" mdaliases:"token"`
	SecretPath             string `json:"secretPath" mapstructure:"secretPath"`
	IncludeImports         bool   `json:"includeImports" mapstructure:"includeImports"`
	ExpandSecretReferences bool   `json:"expandSecretReferences" mapstructure:"expandSecretReferences"`
	Recursive              bool   `json:"recursive" mapstructure:"recursive"`
}

type resolvedRequestMetadata struct {
	projectID              string
	environment            string
	secretPath             string
	secretType             string
	version                int
	includeImports         bool
	expandSecretReferences bool
	recursive              bool
}

type infisicalClient interface {
	UniversalAuthLogin(clientID, clientSecret string) error
	SetAccessToken(accessToken string)
	RetrieveSecret(options infisicalsdk.RetrieveSecretOptions) (infisicalsdk.Secret, error)
	ListSecrets(options infisicalsdk.ListSecretsOptions) ([]infisicalsdk.Secret, error)
}

type sdkClient struct {
	client infisicalsdk.InfisicalClientInterface
}

func newInfisicalClient(ctx context.Context, apiURL, caPem string) infisicalClient {
	client := infisicalsdk.NewInfisicalClient(ctx, infisicalsdk.Config{
		SiteUrl:       apiURL,
		CaCertificate: caPem,
		UserAgent:     "dapr-components-contrib-infisical",
		SilentMode:    true,
	})
	return &sdkClient{client: client}
}

func (c *sdkClient) UniversalAuthLogin(clientID, clientSecret string) error {
	_, err := c.client.Auth().UniversalAuthLogin(clientID, clientSecret)
	return err
}

func (c *sdkClient) SetAccessToken(accessToken string) {
	t := strings.TrimSpace(accessToken)
	const bearerPrefix = "bearer "
	if len(t) >= len(bearerPrefix) && strings.EqualFold(t[:len(bearerPrefix)], bearerPrefix) {
		t = strings.TrimSpace(t[len(bearerPrefix):])
	}
	c.client.Auth().SetAccessToken(t)
}

func (c *sdkClient) RetrieveSecret(options infisicalsdk.RetrieveSecretOptions) (infisicalsdk.Secret, error) {
	return c.client.Secrets().Retrieve(options)
}

func (c *sdkClient) ListSecrets(options infisicalsdk.ListSecretsOptions) ([]infisicalsdk.Secret, error) {
	res, err := c.client.Secrets().ListSecrets(options)
	if err != nil {
		return nil, err
	}
	return res.Secrets, nil
}

var _ secretstores.SecretStore = (*secretStore)(nil)

type secretStore struct {
	logger logger.Logger

	mu        sync.RWMutex
	client    infisicalClient
	metadata  secretStoreMetadata
	bgCancel  context.CancelFunc
	newClient func(ctx context.Context, apiURL, caPem string) infisicalClient
}

// NewInfisicalSecretStore returns a new Infisical secret store.
func NewInfisicalSecretStore(logger logger.Logger) secretstores.SecretStore {
	return &secretStore{logger: logger, newClient: newInfisicalClient}
}

// Init authenticates with Infisical and configures the secret store.
func (s *secretStore) Init(_ context.Context, meta secretstores.Metadata) error {
	m := secretStoreMetadata{
		Recursive: true,
	}
	if err := kitmd.DecodeMetadata(meta.Properties, &m); err != nil {
		return err
	}

	m.ProjectID = strings.TrimSpace(m.ProjectID)
	m.Environment = strings.TrimSpace(m.Environment)
	m.APIURL = strings.TrimSpace(m.APIURL)
	m.CaPem = strings.TrimSpace(m.CaPem)
	m.ClientID = strings.TrimSpace(m.ClientID)
	m.ClientSecret = strings.TrimSpace(m.ClientSecret)
	m.AccessToken = strings.TrimSpace(m.AccessToken)
	m.SecretPath = strings.TrimSpace(m.SecretPath)

	if err := validateRequiredMetadata(m.ProjectID, m.Environment); err != nil {
		return err
	}

	hasAccessToken := m.AccessToken != ""
	hasUniversalAuth := m.ClientID != "" || m.ClientSecret != ""
	if hasAccessToken && hasUniversalAuth {
		return fmt.Errorf("conflicting authentication metadata: specify either %s, or %s and %s, not both", accessTokenMetadataKey, clientIDMetadataKey, clientSecretMetadataKey)
	}
	if !hasAccessToken && (m.ClientID == "" || m.ClientSecret == "") {
		return fmt.Errorf("missing required metadata: either %s (or %s), or both %s and %s", accessTokenMetadataKey, tokenMetadataKey, clientIDMetadataKey, clientSecretMetadataKey)
	}

	// Use a component-scoped context because Dapr cancels Init's ctx after initialization,
	// while the SDK's token refresh loop runs for the lifetime of the client.
	bgCtx, bgCancel := context.WithCancel(context.Background())
	client := s.newClient(bgCtx, m.APIURL, m.CaPem)

	if hasAccessToken {
		client.SetAccessToken(m.AccessToken)
	} else {
		if err := client.UniversalAuthLogin(m.ClientID, m.ClientSecret); err != nil {
			bgCancel()
			return fmt.Errorf("failed to authenticate with Infisical: %w", err)
		}
	}

	s.mu.Lock()
	if s.bgCancel != nil {
		s.bgCancel()
	}
	s.client = client
	s.metadata = m
	s.bgCancel = bgCancel
	s.mu.Unlock()

	return nil
}

// GetSecret retrieves a secret using a key and returns a map of decrypted string/string values.
func (s *secretStore) GetSecret(ctx context.Context, req secretstores.GetSecretRequest) (secretstores.GetSecretResponse, error) {
	if err := ctx.Err(); err != nil {
		return secretstores.GetSecretResponse{}, err
	}
	if strings.TrimSpace(req.Name) == "" {
		return secretstores.GetSecretResponse{}, errors.New("secret name cannot be empty")
	}

	client, baseMeta, err := s.getState()
	if err != nil {
		return secretstores.GetSecretResponse{}, err
	}

	resolved, err := resolveMetadata(baseMeta, req.Metadata)
	if err != nil {
		return secretstores.GetSecretResponse{}, err
	}

	secret, err := client.RetrieveSecret(infisicalsdk.RetrieveSecretOptions{
		SecretKey:              req.Name,
		ProjectID:              resolved.projectID,
		Environment:            resolved.environment,
		SecretPath:             resolved.secretPath,
		Type:                   resolved.secretType,
		Version:                resolved.version,
		IncludeImports:         resolved.includeImports,
		ExpandSecretReferences: resolved.expandSecretReferences,
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
func (s *secretStore) BulkGetSecret(ctx context.Context, req secretstores.BulkGetSecretRequest) (secretstores.BulkGetSecretResponse, error) {
	if err := ctx.Err(); err != nil {
		return secretstores.BulkGetSecretResponse{}, err
	}

	client, baseMeta, err := s.getState()
	if err != nil {
		return secretstores.BulkGetSecretResponse{}, err
	}

	resolved, err := resolveMetadata(baseMeta, req.Metadata)
	if err != nil {
		return secretstores.BulkGetSecretResponse{}, err
	}

	secrets, err := client.ListSecrets(infisicalsdk.ListSecretsOptions{
		ProjectID:              resolved.projectID,
		Environment:            resolved.environment,
		SecretPath:             resolved.secretPath,
		IncludeImports:         resolved.includeImports,
		ExpandSecretReferences: resolved.expandSecretReferences,
		Recursive:              resolved.recursive,
	})
	if err != nil {
		return secretstores.BulkGetSecretResponse{}, fmt.Errorf("failed to list secrets from Infisical: %w", err)
	}

	data := make(map[string]map[string]string, len(secrets))
	for _, secret := range secrets {
		if secret.SecretKey == "" {
			continue
		}
		if _, exists := data[secret.SecretKey]; exists {
			s.logger.Debugf("infisical: skipping duplicate secret key %q returned by BulkGetSecret", secret.SecretKey)
			continue
		}
		data[secret.SecretKey] = map[string]string{secret.SecretKey: secret.SecretValue}
	}

	return secretstores.BulkGetSecretResponse{Data: data}, nil
}

func (s *secretStore) getState() (infisicalClient, secretStoreMetadata, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.client == nil {
		return nil, secretStoreMetadata{}, errors.New("secret store has not been initialized")
	}
	return s.client, s.metadata, nil
}

func validateRequiredMetadata(projectID, environment string) error {
	if strings.TrimSpace(projectID) == "" {
		return fmt.Errorf("missing required metadata: %s (or %s)", projectIDMetadataKey, workspaceIDMetadataKey)
	}
	if strings.TrimSpace(environment) == "" {
		return fmt.Errorf("missing required metadata: %s", environmentMetadataKey)
	}
	return nil
}

func parseBoolMetadata(key, value string) (bool, error) {
	parsed, err := strconv.ParseBool(strings.TrimSpace(value))
	if err != nil {
		return false, fmt.Errorf("invalid %s metadata value %q: %w", key, value, err)
	}
	return parsed, nil
}

func resolveMetadata(base secretStoreMetadata, requestMetadata map[string]string) (resolvedRequestMetadata, error) {
	resolved := resolvedRequestMetadata{
		projectID:              base.ProjectID,
		environment:            base.Environment,
		secretPath:             base.SecretPath,
		includeImports:         base.IncludeImports,
		expandSecretReferences: base.ExpandSecretReferences,
		recursive:              base.Recursive,
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
		case strings.ToLower(versionIDMetadataKey):
			parsed, err := strconv.Atoi(strings.TrimSpace(value))
			if err != nil || parsed <= 0 {
				return resolvedRequestMetadata{}, fmt.Errorf("invalid %s metadata value %q: must be a positive integer", versionIDMetadataKey, value)
			}
			resolved.version = parsed
		case strings.ToLower(includeImportsMetadataKey):
			parsed, err := parseBoolMetadata(includeImportsMetadataKey, value)
			if err != nil {
				return resolvedRequestMetadata{}, err
			}
			resolved.includeImports = parsed
		case strings.ToLower(expandSecretReferencesMetadataKey):
			parsed, err := parseBoolMetadata(expandSecretReferencesMetadataKey, value)
			if err != nil {
				return resolvedRequestMetadata{}, err
			}
			resolved.expandSecretReferences = parsed
		case strings.ToLower(recursiveMetadataKey):
			parsed, err := parseBoolMetadata(recursiveMetadataKey, value)
			if err != nil {
				return resolvedRequestMetadata{}, err
			}
			resolved.recursive = parsed
		}
	}

	if err := validateRequiredMetadata(resolved.projectID, resolved.environment); err != nil {
		return resolvedRequestMetadata{}, err
	}

	return resolved, nil
}

// Features returns the features available in this secret store.
func (s *secretStore) Features() []secretstores.Feature {
	return []secretstores.Feature{}
}

// GetComponentMetadata returns the component's metadata specification.
func (s *secretStore) GetComponentMetadata() (metadataInfo metadata.MetadataMap) {
	metadataStruct := secretStoreMetadata{}
	_ = metadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, metadata.SecretStoreType)
	return
}

// Close cancels any background token lifecycle goroutines held by the SDK client.
func (s *secretStore) Close() error {
	s.mu.Lock()
	if s.bgCancel != nil {
		s.bgCancel()
		s.bgCancel = nil
	}
	s.mu.Unlock()
	return nil
}
