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
	"encoding/json"
	"encoding/pem"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	infisicalsdk "github.com/infisical/go-sdk"
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

	getSecretOptions infisicalsdk.RetrieveSecretOptions
	getSecretResult  infisicalsdk.Secret
	getSecretErr     error

	listSecretsOptions infisicalsdk.ListSecretsOptions
	listSecretsResult  []infisicalsdk.Secret
	listSecretsErr     error
}

func (m *mockInfisicalClient) UniversalAuthLogin(clientID, clientSecret string) error {
	m.loginClientID = clientID
	m.loginClientSecret = clientSecret
	return m.loginErr
}

func (m *mockInfisicalClient) SetAccessToken(accessToken string) {
	m.accessToken = accessToken
}

func (m *mockInfisicalClient) RetrieveSecret(options infisicalsdk.RetrieveSecretOptions) (infisicalsdk.Secret, error) {
	m.getSecretOptions = options
	if m.getSecretErr != nil {
		return infisicalsdk.Secret{}, m.getSecretErr
	}
	return m.getSecretResult, nil
}

func (m *mockInfisicalClient) ListSecrets(options infisicalsdk.ListSecretsOptions) ([]infisicalsdk.Secret, error) {
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
	var gotCaPem string
	var gotCtx context.Context
	s.newClient = func(ctx context.Context, apiURL, caPem string) infisicalClient {
		gotCtx = ctx
		gotAPIURL = apiURL
		gotCaPem = caPem
		return client
	}

	err := s.Init(t.Context(), secretstores.Metadata{Base: compmd.Base{Properties: map[string]string{
		projectIDMetadataKey:    "  proj-id  ",
		environmentMetadataKey:  "  dev ",
		clientIDMetadataKey:     " client-id ",
		clientSecretMetadataKey: " client-secret ",
		apiURLMetadataKey:       " https://api.infisical.example ",
		caPemMetadataKey:        " -----BEGIN CERTIFICATE-----\ntest\n-----END CERTIFICATE----- ",
		secretPathMetadataKey:   " /app ",
	}}})

	require.NoError(t, err)
	assert.Equal(t, "https://api.infisical.example", gotAPIURL)
	assert.Equal(t, "-----BEGIN CERTIFICATE-----\ntest\n-----END CERTIFICATE-----", gotCaPem)
	assert.Equal(t, "client-id", client.loginClientID)
	assert.Equal(t, "client-secret", client.loginClientSecret)
	assert.Equal(t, "proj-id", s.metadata.ProjectID)
	assert.Equal(t, "dev", s.metadata.Environment)
	assert.Equal(t, "/app", s.metadata.SecretPath)
	assert.Equal(t, "-----BEGIN CERTIFICATE-----\ntest\n-----END CERTIFICATE-----", s.metadata.CaPem)
	assert.True(t, s.metadata.Recursive)
	require.NotNil(t, gotCtx)
	require.NoError(t, gotCtx.Err())

	require.NoError(t, s.Close())
	require.ErrorIs(t, gotCtx.Err(), context.Canceled)
}

func TestInitWithAccessTokenAndAliases(t *testing.T) {
	t.Run("accessToken with workspaceId and recursive false", func(t *testing.T) {
		s := NewInfisicalSecretStore(logger.NewLogger("test")).(*secretStore)
		client := &mockInfisicalClient{}
		s.newClient = func(_ context.Context, _, _ string) infisicalClient {
			return client
		}

		err := s.Init(t.Context(), secretstores.Metadata{Base: compmd.Base{Properties: map[string]string{
			workspaceIDMetadataKey: " workspace-id ",
			environmentMetadataKey: " prod ",
			accessTokenMetadataKey: " token-value ",
			recursiveMetadataKey:   "false",
		}}})

		require.NoError(t, err)
		assert.Equal(t, "token-value", client.accessToken)
		assert.Empty(t, client.loginClientID)
		assert.Equal(t, "workspace-id", s.metadata.ProjectID)
		assert.False(t, s.metadata.Recursive)
	})

	t.Run("token alias and projectId takes precedence over workspaceId", func(t *testing.T) {
		s := NewInfisicalSecretStore(logger.NewLogger("test")).(*secretStore)
		client := &mockInfisicalClient{}
		s.newClient = func(_ context.Context, _, _ string) infisicalClient {
			return client
		}

		err := s.Init(t.Context(), secretstores.Metadata{Base: compmd.Base{Properties: map[string]string{
			projectIDMetadataKey:   "proj-primary",
			workspaceIDMetadataKey: "ws-secondary",
			environmentMetadataKey: "prod",
			tokenMetadataKey:       "alias-token",
		}}})

		require.NoError(t, err)
		assert.Equal(t, "proj-primary", s.metadata.ProjectID)
		assert.Equal(t, "alias-token", client.accessToken)
	})
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
			name: "whitespace project",
			meta: map[string]string{
				projectIDMetadataKey:   "   ",
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
			errText: "missing required metadata: either accessToken (or token), or both clientId and clientSecret",
		},
		{
			name: "partial universal auth clientId only",
			meta: map[string]string{
				projectIDMetadataKey:   "project",
				environmentMetadataKey: "dev",
				clientIDMetadataKey:    "client-id",
			},
			errText: "missing required metadata",
		},
		{
			name: "partial universal auth clientSecret only",
			meta: map[string]string{
				projectIDMetadataKey:    "project",
				environmentMetadataKey:  "dev",
				clientSecretMetadataKey: "client-secret",
			},
			errText: "missing required metadata",
		},
		{
			name: "conflicting accessToken and universal auth",
			meta: map[string]string{
				projectIDMetadataKey:    "project",
				environmentMetadataKey:  "dev",
				accessTokenMetadataKey:  "token",
				clientIDMetadataKey:     "client-id",
				clientSecretMetadataKey: "client-secret",
			},
			errText: "conflicting authentication metadata",
		},
		{
			name: "conflicting accessToken and partial universal auth",
			meta: map[string]string{
				projectIDMetadataKey:   "project",
				environmentMetadataKey: "dev",
				accessTokenMetadataKey: "token",
				clientIDMetadataKey:    "client-id",
			},
			errText: "conflicting authentication metadata",
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

func TestInitLifecycleAndAuthError(t *testing.T) {
	t.Run("authentication error cancels background context", func(t *testing.T) {
		s := NewInfisicalSecretStore(logger.NewLogger("test")).(*secretStore)
		client := &mockInfisicalClient{loginErr: errors.New("auth failed")}
		var gotCtx context.Context
		s.newClient = func(ctx context.Context, _, _ string) infisicalClient {
			gotCtx = ctx
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
		require.NotNil(t, gotCtx)
		require.ErrorIs(t, gotCtx.Err(), context.Canceled)
		assert.Nil(t, s.client)
	})

	t.Run("re-init cancels previous context and close is idempotent", func(t *testing.T) {
		s := NewInfisicalSecretStore(logger.NewLogger("test")).(*secretStore)
		require.NoError(t, s.Close())

		var contexts []context.Context
		s.newClient = func(ctx context.Context, _, _ string) infisicalClient {
			contexts = append(contexts, ctx)
			return &mockInfisicalClient{}
		}

		meta := secretstores.Metadata{Base: compmd.Base{Properties: map[string]string{
			projectIDMetadataKey:   "proj-id",
			environmentMetadataKey: "dev",
			accessTokenMetadataKey: "token-1",
		}}}

		require.NoError(t, s.Init(t.Context(), meta))
		require.NoError(t, s.Init(t.Context(), meta))
		require.Len(t, contexts, 2)
		require.ErrorIs(t, contexts[0].Err(), context.Canceled)
		require.NoError(t, contexts[1].Err())

		require.NoError(t, s.Close())
		require.NoError(t, s.Close())
		require.ErrorIs(t, contexts[1].Err(), context.Canceled)
	})
}

func TestGetSecret(t *testing.T) {
	t.Run("overrides, trimming, and projectId precedence", func(t *testing.T) {
		s := NewInfisicalSecretStore(logger.NewLogger("test")).(*secretStore)
		client := &mockInfisicalClient{
			getSecretResult: infisicalsdk.Secret{SecretKey: "api-key", SecretValue: "value-1"},
		}
		s.client = client
		s.metadata = secretStoreMetadata{
			ProjectID:              "default-project",
			Environment:            "dev",
			SecretPath:             "/",
			IncludeImports:         false,
			ExpandSecretReferences: false,
			Recursive:              true,
		}

		for range 10 {
			resp, err := s.GetSecret(t.Context(), secretstores.GetSecretRequest{
				Name: "api-key",
				Metadata: map[string]string{
					projectIDMetadataKey:              "  override-project ",
					workspaceIDMetadataKey:            "ignored-workspace",
					environmentMetadataKey:            " prod ",
					secretPathMetadataKey:             " /services ",
					typeMetadataKey:                   " shared ",
					versionIDMetadataKey:              " 4 ",
					includeImportsMetadataKey:         " true ",
					expandSecretReferencesMetadataKey: " true ",
				},
			})

			require.NoError(t, err)
			assert.Equal(t, map[string]string{"api-key": "value-1"}, resp.Data)
			assert.Equal(t, "override-project", client.getSecretOptions.ProjectID)
			assert.Equal(t, "prod", client.getSecretOptions.Environment)
			assert.Equal(t, "/services", client.getSecretOptions.SecretPath)
			assert.Equal(t, "shared", client.getSecretOptions.Type)
			assert.Equal(t, 4, client.getSecretOptions.Version)
			assert.True(t, client.getSecretOptions.IncludeImports)
			assert.True(t, client.getSecretOptions.ExpandSecretReferences)
		}
	})

	t.Run("workspaceId override when projectId absent and empty SecretKey fallback", func(t *testing.T) {
		s := NewInfisicalSecretStore(logger.NewLogger("test")).(*secretStore)
		client := &mockInfisicalClient{
			getSecretResult: infisicalsdk.Secret{SecretValue: "value-2"},
		}
		s.client = client
		s.metadata = secretStoreMetadata{
			ProjectID:   "default-project",
			Environment: "dev",
		}

		resp, err := s.GetSecret(t.Context(), secretstores.GetSecretRequest{
			Name: "fallback-key",
			Metadata: map[string]string{
				workspaceIDMetadataKey: " ws-override ",
			},
		})

		require.NoError(t, err)
		assert.Equal(t, map[string]string{"fallback-key": "value-2"}, resp.Data)
		assert.Equal(t, "ws-override", client.getSecretOptions.ProjectID)
	})
}

func TestGetSecretErrors(t *testing.T) {
	t.Run("uninitialized store", func(t *testing.T) {
		s := NewInfisicalSecretStore(logger.NewLogger("test"))
		_, err := s.GetSecret(t.Context(), secretstores.GetSecretRequest{Name: "api-key"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "not been initialized")
	})

	t.Run("cancelled context", func(t *testing.T) {
		s := NewInfisicalSecretStore(logger.NewLogger("test")).(*secretStore)
		s.client = &mockInfisicalClient{}
		s.metadata = secretStoreMetadata{ProjectID: "project-id", Environment: "dev"}

		ctx, cancel := context.WithCancel(t.Context())
		cancel()

		_, err := s.GetSecret(ctx, secretstores.GetSecretRequest{Name: "api-key"})
		require.ErrorIs(t, err, context.Canceled)
	})

	t.Run("empty secret name", func(t *testing.T) {
		s := NewInfisicalSecretStore(logger.NewLogger("test")).(*secretStore)
		s.client = &mockInfisicalClient{}
		s.metadata = secretStoreMetadata{ProjectID: "project-id", Environment: "dev"}

		_, err := s.GetSecret(t.Context(), secretstores.GetSecretRequest{Name: "   "})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "secret name cannot be empty")
	})

	t.Run("sdk error", func(t *testing.T) {
		s := NewInfisicalSecretStore(logger.NewLogger("test")).(*secretStore)
		s.client = &mockInfisicalClient{getSecretErr: errors.New("boom")}
		s.metadata = secretStoreMetadata{ProjectID: "project-id", Environment: "dev"}

		_, err := s.GetSecret(t.Context(), secretstores.GetSecretRequest{Name: "api-key"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to get secret")
	})

	t.Run("invalid bool overrides", func(t *testing.T) {
		s := NewInfisicalSecretStore(logger.NewLogger("test")).(*secretStore)
		s.client = &mockInfisicalClient{}
		s.metadata = secretStoreMetadata{ProjectID: "project-id", Environment: "dev"}

		_, err := s.GetSecret(t.Context(), secretstores.GetSecretRequest{
			Name: "api-key",
			Metadata: map[string]string{
				includeImportsMetadataKey: "not-a-bool",
			},
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "invalid includeImports metadata value")

		_, err = s.GetSecret(t.Context(), secretstores.GetSecretRequest{
			Name: "api-key",
			Metadata: map[string]string{
				expandSecretReferencesMetadataKey: "not-a-bool",
			},
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "invalid expandSecretReferences metadata value")
	})

	t.Run("invalid version_id overrides", func(t *testing.T) {
		s := NewInfisicalSecretStore(logger.NewLogger("test")).(*secretStore)
		s.client = &mockInfisicalClient{}
		s.metadata = secretStoreMetadata{ProjectID: "project-id", Environment: "dev"}

		for _, badVersion := range []string{"0", "-1", "abc"} {
			_, err := s.GetSecret(t.Context(), secretstores.GetSecretRequest{
				Name: "api-key",
				Metadata: map[string]string{
					versionIDMetadataKey: badVersion,
				},
			})
			require.Error(t, err)
			assert.Contains(t, err.Error(), "invalid version_id metadata value")
		}
	})
}

func TestBulkGetSecret(t *testing.T) {
	t.Run("success with duplicate and empty key handling", func(t *testing.T) {
		s := NewInfisicalSecretStore(logger.NewLogger("test")).(*secretStore)
		client := &mockInfisicalClient{
			listSecretsResult: []infisicalsdk.Secret{
				{SecretKey: "", SecretValue: "ignored"},
				{SecretKey: "one", SecretValue: "1"},
				{SecretKey: "one", SecretValue: "duplicate"},
				{SecretKey: "two", SecretValue: "2"},
			},
		}
		s.client = client
		s.metadata = secretStoreMetadata{
			ProjectID:              "project-id",
			Environment:            "dev",
			SecretPath:             "/app",
			IncludeImports:         true,
			ExpandSecretReferences: true,
			Recursive:              true,
		}

		resp, err := s.BulkGetSecret(t.Context(), secretstores.BulkGetSecretRequest{
			Metadata: map[string]string{
				recursiveMetadataKey: "false",
			},
		})
		require.NoError(t, err)
		assert.Equal(t, map[string]map[string]string{
			"one": {"one": "1"},
			"two": {"two": "2"},
		}, resp.Data)
		assert.Equal(t, "project-id", client.listSecretsOptions.ProjectID)
		assert.Equal(t, "dev", client.listSecretsOptions.Environment)
		assert.Equal(t, "/app", client.listSecretsOptions.SecretPath)
		assert.False(t, client.listSecretsOptions.Recursive)
		assert.True(t, client.listSecretsOptions.IncludeImports)
		assert.True(t, client.listSecretsOptions.ExpandSecretReferences)
	})
}

func TestBulkGetSecretErrors(t *testing.T) {
	t.Run("uninitialized store", func(t *testing.T) {
		s := NewInfisicalSecretStore(logger.NewLogger("test"))
		_, err := s.BulkGetSecret(t.Context(), secretstores.BulkGetSecretRequest{})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "not been initialized")
	})

	t.Run("cancelled context", func(t *testing.T) {
		s := NewInfisicalSecretStore(logger.NewLogger("test")).(*secretStore)
		s.client = &mockInfisicalClient{}
		s.metadata = secretStoreMetadata{ProjectID: "project-id", Environment: "dev"}

		ctx, cancel := context.WithCancel(t.Context())
		cancel()

		_, err := s.BulkGetSecret(ctx, secretstores.BulkGetSecretRequest{})
		require.ErrorIs(t, err, context.Canceled)
	})

	t.Run("invalid recursive bool override", func(t *testing.T) {
		s := NewInfisicalSecretStore(logger.NewLogger("test")).(*secretStore)
		s.client = &mockInfisicalClient{}
		s.metadata = secretStoreMetadata{ProjectID: "project-id", Environment: "dev"}

		_, err := s.BulkGetSecret(t.Context(), secretstores.BulkGetSecretRequest{
			Metadata: map[string]string{
				recursiveMetadataKey: "invalid",
			},
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "invalid recursive metadata value")
	})

	t.Run("sdk error", func(t *testing.T) {
		s := NewInfisicalSecretStore(logger.NewLogger("test")).(*secretStore)
		s.client = &mockInfisicalClient{listSecretsErr: errors.New("boom")}
		s.metadata = secretStoreMetadata{ProjectID: "project-id", Environment: "dev"}

		_, err := s.BulkGetSecret(t.Context(), secretstores.BulkGetSecretRequest{})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to list secrets from Infisical")
	})
}

func TestGetComponentMetadataAndFeatures(t *testing.T) {
	s := NewInfisicalSecretStore(logger.NewLogger("test")).(*secretStore)
	assert.Empty(t, s.Features())

	md := s.GetComponentMetadata()
	expectedKeys := []string{
		projectIDMetadataKey,
		environmentMetadataKey,
		apiURLMetadataKey,
		caPemMetadataKey,
		clientIDMetadataKey,
		clientSecretMetadataKey,
		accessTokenMetadataKey,
		secretPathMetadataKey,
		includeImportsMetadataKey,
		expandSecretReferencesMetadataKey,
		recursiveMetadataKey,
	}
	for _, key := range expectedKeys {
		field, ok := md[key]
		require.True(t, ok, "expected metadata field %q", key)
		assert.False(t, field.Ignored, "metadata field %q should not be ignored", key)
	}
	assert.Equal(t, []string{workspaceIDMetadataKey}, md[projectIDMetadataKey].Aliases)
	assert.Equal(t, []string{tokenMetadataKey}, md[accessTokenMetadataKey].Aliases)
}

func TestSDKClientEndToEndWithHTTPServer(t *testing.T) {
	t.Run("universal auth, retrieve, and list with imports", func(t *testing.T) {
		var sawLogin bool
		var sawGetAuth string
		var sawListAuth string

		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			switch {
			case r.Method == http.MethodPost && r.URL.Path == "/api/v1/auth/universal-auth/login":
				var body map[string]string
				require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
				assert.Equal(t, "cid", body["clientId"])
				assert.Equal(t, "csec", body["clientSecret"])
				sawLogin = true
				require.NoError(t, json.NewEncoder(w).Encode(map[string]any{
					"accessToken":       "ua-jwt-token",
					"expiresIn":         3600,
					"accessTokenMaxTTL": 7200,
					"tokenType":         "Bearer",
				}))
			case r.Method == http.MethodGet && r.URL.Path == "/api/v3/secrets/raw/api-key":
				sawGetAuth = r.Header.Get("Authorization")
				q := r.URL.Query()
				assert.Equal(t, "proj-1", q.Get("workspaceId"))
				assert.Equal(t, "dev", q.Get("environment"))
				assert.Equal(t, "/app", q.Get("secretPath"))
				assert.Equal(t, "true", q.Get("expandSecretReferences"))
				assert.Equal(t, "true", q.Get("include_imports"))
				assert.Equal(t, "shared", q.Get("type"))
				assert.Equal(t, "3", q.Get("version"))
				require.NoError(t, json.NewEncoder(w).Encode(map[string]any{
					"secret": map[string]any{
						"secretKey":   "api-key",
						"secretValue": "secret-val",
					},
				}))
			case r.Method == http.MethodGet && r.URL.Path == "/api/v3/secrets/raw":
				sawListAuth = r.Header.Get("Authorization")
				q := r.URL.Query()
				assert.Equal(t, "proj-1", q.Get("workspaceId"))
				assert.Equal(t, "dev", q.Get("environment"))
				assert.Equal(t, "/app", q.Get("secretPath"))
				assert.Equal(t, "true", q.Get("expandSecretReferences"))
				assert.Equal(t, "true", q.Get("include_imports"))
				assert.Equal(t, "true", q.Get("recursive"))
				require.NoError(t, json.NewEncoder(w).Encode(map[string]any{
					"secrets": []map[string]any{
						{"secretKey": "api-key", "secretValue": "primary-val"},
					},
					"imports": []map[string]any{
						{
							"secretPath":  "/shared",
							"environment": "dev",
							"secrets": []map[string]any{
								{"secretKey": "api-key", "secretValue": "imported-override"},
								{"secretKey": "db-pass", "secretValue": "imported-pass"},
							},
						},
					},
				}))
			default:
				http.NotFound(w, r)
			}
		}))
		defer srv.Close()

		store := NewInfisicalSecretStore(logger.NewLogger("test"))
		defer func() {
			require.NoError(t, store.Close())
		}()

		err := store.Init(t.Context(), secretstores.Metadata{Base: compmd.Base{Properties: map[string]string{
			projectIDMetadataKey:              "proj-1",
			environmentMetadataKey:            "dev",
			apiURLMetadataKey:                 srv.URL,
			clientIDMetadataKey:               "cid",
			clientSecretMetadataKey:           "csec",
			secretPathMetadataKey:             "/app",
			includeImportsMetadataKey:         "true",
			expandSecretReferencesMetadataKey: "true",
		}}})
		require.NoError(t, err)
		assert.True(t, sawLogin)

		getResp, err := store.GetSecret(t.Context(), secretstores.GetSecretRequest{
			Name: "api-key",
			Metadata: map[string]string{
				typeMetadataKey:      "shared",
				versionIDMetadataKey: "3",
			},
		})
		require.NoError(t, err)
		assert.Equal(t, "Bearer ua-jwt-token", sawGetAuth)
		assert.Equal(t, map[string]string{"api-key": "secret-val"}, getResp.Data)

		bulkResp, err := store.BulkGetSecret(t.Context(), secretstores.BulkGetSecretRequest{})
		require.NoError(t, err)
		assert.Equal(t, "Bearer ua-jwt-token", sawListAuth)
		assert.Equal(t, map[string]map[string]string{
			"api-key": {"api-key": "primary-val"},
			"db-pass": {"db-pass": "imported-pass"},
		}, bulkResp.Data)
	})

	t.Run("access token strips Bearer prefix", func(t *testing.T) {
		var sawAuth string
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			if r.Method == http.MethodGet && r.URL.Path == "/api/v3/secrets/raw/my-secret" {
				sawAuth = r.Header.Get("Authorization")
				require.NoError(t, json.NewEncoder(w).Encode(map[string]any{
					"secret": map[string]any{
						"secretKey":   "my-secret",
						"secretValue": "ok",
					},
				}))
				return
			}
			http.NotFound(w, r)
		}))
		defer srv.Close()

		store := NewInfisicalSecretStore(logger.NewLogger("test"))
		defer func() {
			require.NoError(t, store.Close())
		}()

		err := store.Init(t.Context(), secretstores.Metadata{Base: compmd.Base{Properties: map[string]string{
			projectIDMetadataKey:   "proj-1",
			environmentMetadataKey: "dev",
			apiURLMetadataKey:      srv.URL,
			accessTokenMetadataKey: "Bearer raw-token",
		}}})
		require.NoError(t, err)

		resp, err := store.GetSecret(t.Context(), secretstores.GetSecretRequest{Name: "my-secret"})
		require.NoError(t, err)
		assert.Equal(t, "Bearer raw-token", sawAuth)
		assert.Equal(t, map[string]string{"my-secret": "ok"}, resp.Data)
	})

	t.Run("caPem trusts custom TLS certificate", func(t *testing.T) {
		srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			if r.Method == http.MethodGet && r.URL.Path == "/api/v3/secrets/raw/tls-secret" {
				require.NoError(t, json.NewEncoder(w).Encode(map[string]any{
					"secret": map[string]any{
						"secretKey":   "tls-secret",
						"secretValue": "tls-ok",
					},
				}))
				return
			}
			http.NotFound(w, r)
		}))
		defer srv.Close()

		certPEM := pem.EncodeToMemory(&pem.Block{
			Type:  "CERTIFICATE",
			Bytes: srv.Certificate().Raw,
		})

		store := NewInfisicalSecretStore(logger.NewLogger("test"))
		defer func() {
			require.NoError(t, store.Close())
		}()

		err := store.Init(t.Context(), secretstores.Metadata{Base: compmd.Base{Properties: map[string]string{
			projectIDMetadataKey:   "proj-1",
			environmentMetadataKey: "dev",
			apiURLMetadataKey:      srv.URL,
			caPemMetadataKey:       string(certPEM),
			accessTokenMetadataKey: "raw-token",
		}}})
		require.NoError(t, err)

		resp, err := store.GetSecret(t.Context(), secretstores.GetSecretRequest{Name: "tls-secret"})
		require.NoError(t, err)
		assert.Equal(t, map[string]string{"tls-secret": "tls-ok"}, resp.Data)
	})
}
