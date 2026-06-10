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
	"testing"

	apisecrets "github.com/infisical/go-sdk/packages/api/secrets"
	"github.com/infisical/go-sdk/packages/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	compmd "github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/secretstores"
	"github.com/dapr/kit/logger"
)

type mockInfisicalClient struct {
	loginClientID     string
	loginClientSecret string
	loginErr          error

	accessToken string

	getSecretOptions apisecrets.RetrieveSecretV3RawRequest
	getSecretResult  models.Secret
	getSecretErr     error

	listSecretsOptions apisecrets.ListSecretsV3RawRequest
	listSecretsResult  []models.Secret
	listSecretsErr     error
}

func (m *mockInfisicalClient) LoginUniversal(clientID, clientSecret string) error {
	m.loginClientID = clientID
	m.loginClientSecret = clientSecret
	return m.loginErr
}

func (m *mockInfisicalClient) SetAccessToken(accessToken string) {
	m.accessToken = accessToken
}

func (m *mockInfisicalClient) GetSecret(options apisecrets.RetrieveSecretV3RawRequest) (models.Secret, error) {
	m.getSecretOptions = options
	if m.getSecretErr != nil {
		return models.Secret{}, m.getSecretErr
	}
	return m.getSecretResult, nil
}

func (m *mockInfisicalClient) ListSecrets(options apisecrets.ListSecretsV3RawRequest) ([]models.Secret, error) {
	m.listSecretsOptions = options
	if m.listSecretsErr != nil {
		return nil, m.listSecretsErr
	}
	return m.listSecretsResult, nil
}

func TestInitWithUniversalAuth(t *testing.T) {
	s := NewInfisicalSecretStore(logger.NewLogger("test")).(*secretStore)
	client := &mockInfisicalClient{}

	var gotAPIURL string
	s.newClient = func(_ context.Context, apiURL string) infisicalClient {
		gotAPIURL = apiURL
		return client
	}

	err := s.Init(t.Context(), secretstores.Metadata{Base: compmd.Base{Properties: map[string]string{
		projectIDMetadataKey:    "proj-id",
		environmentMetadataKey:  "dev",
		clientIDMetadataKey:     "client-id",
		clientSecretMetadataKey: "client-secret",
		apiURLMetadataKey:       "https://api.infisical.example",
	}}})

	require.NoError(t, err)
	assert.Equal(t, "https://api.infisical.example", gotAPIURL)
	assert.Equal(t, "client-id", client.loginClientID)
	assert.Equal(t, "client-secret", client.loginClientSecret)
	assert.Equal(t, "proj-id", s.projectID)
	assert.Equal(t, "dev", s.metadata.Environment)
}

func TestInitWithAccessToken(t *testing.T) {
	s := NewInfisicalSecretStore(logger.NewLogger("test")).(*secretStore)
	client := &mockInfisicalClient{}
	s.newClient = func(_ context.Context, _ string) infisicalClient {
		return client
	}

	err := s.Init(t.Context(), secretstores.Metadata{Base: compmd.Base{Properties: map[string]string{
		workspaceIDMetadataKey: "workspace-id",
		environmentMetadataKey: "prod",
		accessTokenMetadataKey: "token-value",
	}}})

	require.NoError(t, err)
	assert.Equal(t, "token-value", client.accessToken)
	assert.Empty(t, client.loginClientID)
	assert.Equal(t, "workspace-id", s.projectID)
}

func TestInitValidationErrors(t *testing.T) {
	tests := []struct {
		name    string
		meta    map[string]string
		errText string
	}{
		{
			name: "missing project",
			meta: map[string]string{
				environmentMetadataKey: "dev",
				accessTokenMetadataKey: "token",
			},
			errText: "missing required metadata: projectId (or workspaceId)",
		},
		{
			name: "missing environment",
			meta: map[string]string{
				projectIDMetadataKey:   "project",
				accessTokenMetadataKey: "token",
			},
			errText: "missing required metadata: environment",
		},
		{
			name: "missing auth",
			meta: map[string]string{
				projectIDMetadataKey:   "project",
				environmentMetadataKey: "dev",
			},
			errText: "missing required metadata: either accessToken, or both clientId and clientSecret",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := NewInfisicalSecretStore(logger.NewLogger("test")).(*secretStore)
			err := s.Init(t.Context(), secretstores.Metadata{Base: compmd.Base{Properties: tt.meta}})
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.errText)
		})
	}
}

func TestInitAuthenticationError(t *testing.T) {
	s := NewInfisicalSecretStore(logger.NewLogger("test")).(*secretStore)
	client := &mockInfisicalClient{loginErr: errors.New("auth failed")}
	s.newClient = func(_ context.Context, _ string) infisicalClient {
		return client
	}

	err := s.Init(t.Context(), secretstores.Metadata{Base: compmd.Base{Properties: map[string]string{
		projectIDMetadataKey:    "proj-id",
		environmentMetadataKey:  "dev",
		clientIDMetadataKey:     "client-id",
		clientSecretMetadataKey: "client-secret",
	}}})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to authenticate with Infisical")
}

func TestGetSecret(t *testing.T) {
	s := NewInfisicalSecretStore(logger.NewLogger("test")).(*secretStore)
	client := &mockInfisicalClient{
		getSecretResult: models.Secret{SecretKey: "api-key", SecretValue: "value-1"},
	}
	s.client = client
	s.projectID = "project-id"
	s.metadata = secretStoreMetadata{
		Environment:            "dev",
		SecretPath:             "/",
		IncludeImports:         true,
		ExpandSecretReferences: false,
	}

	resp, err := s.GetSecret(t.Context(), secretstores.GetSecretRequest{
		Name: "api-key",
		Metadata: map[string]string{
			environmentMetadataKey: "prod",
			typeMetadataKey:        "shared",
		},
	})

	require.NoError(t, err)
	assert.Equal(t, map[string]string{"api-key": "value-1"}, resp.Data)
	assert.Equal(t, "project-id", client.getSecretOptions.ProjectID)
	assert.Equal(t, "prod", client.getSecretOptions.Environment)
	assert.Equal(t, "shared", client.getSecretOptions.Type)
	assert.True(t, client.getSecretOptions.IncludeImports)
}

func TestGetSecretErrors(t *testing.T) {
	t.Run("sdk error", func(t *testing.T) {
		s := NewInfisicalSecretStore(logger.NewLogger("test")).(*secretStore)
		s.client = &mockInfisicalClient{getSecretErr: errors.New("boom")}
		s.projectID = "project-id"
		s.metadata = secretStoreMetadata{Environment: "dev"}

		_, err := s.GetSecret(t.Context(), secretstores.GetSecretRequest{Name: "api-key"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to get secret")
	})

	t.Run("invalid bool override", func(t *testing.T) {
		s := NewInfisicalSecretStore(logger.NewLogger("test")).(*secretStore)
		s.client = &mockInfisicalClient{}
		s.projectID = "project-id"
		s.metadata = secretStoreMetadata{Environment: "dev"}

		_, err := s.GetSecret(t.Context(), secretstores.GetSecretRequest{
			Name: "api-key",
			Metadata: map[string]string{
				includeImportsMetadataKey: "not-a-bool",
			},
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "invalid includeImports metadata value")
	})
}

func TestBulkGetSecret(t *testing.T) {
	s := NewInfisicalSecretStore(logger.NewLogger("test")).(*secretStore)
	client := &mockInfisicalClient{
		listSecretsResult: []models.Secret{
			{SecretKey: "one", SecretValue: "1"},
			{SecretKey: "two", SecretValue: "2"},
		},
	}
	s.client = client
	s.projectID = "project-id"
	s.metadata = secretStoreMetadata{Environment: "dev", SecretPath: "/app", IncludeImports: true, ExpandSecretReferences: true}

	resp, err := s.BulkGetSecret(t.Context(), secretstores.BulkGetSecretRequest{})
	require.NoError(t, err)
	assert.Equal(t, map[string]map[string]string{
		"one": {"one": "1"},
		"two": {"two": "2"},
	}, resp.Data)
	assert.Equal(t, "project-id", client.listSecretsOptions.ProjectID)
	assert.Equal(t, "dev", client.listSecretsOptions.Environment)
	assert.Equal(t, "/app", client.listSecretsOptions.SecretPath)
	assert.True(t, client.listSecretsOptions.Recursive)
	assert.True(t, client.listSecretsOptions.IncludeImports)
	assert.True(t, client.listSecretsOptions.ExpandSecretReferences)
}

func TestBulkGetSecretError(t *testing.T) {
	s := NewInfisicalSecretStore(logger.NewLogger("test")).(*secretStore)
	s.client = &mockInfisicalClient{listSecretsErr: errors.New("boom")}
	s.projectID = "project-id"
	s.metadata = secretStoreMetadata{Environment: "dev"}

	_, err := s.BulkGetSecret(t.Context(), secretstores.BulkGetSecretRequest{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to list secrets from Infisical")
}
