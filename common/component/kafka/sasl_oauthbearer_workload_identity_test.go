/*
Copyright 2025 The Dapr Authors
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

package kafka

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWorkloadIdentityTokenSource(t *testing.T) {
	t.Run("successful token exchange", func(t *testing.T) {
		// Set up a mock token endpoint.
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			require.Equal(t, http.MethodPost, r.Method)
			require.NoError(t, r.ParseForm())

			require.Equal(t, "client_credentials", r.FormValue("grant_type"))
			require.Equal(t, "test-client-id", r.FormValue("client_id"))
			require.Equal(t, "urn:ietf:params:oauth:client-assertion-type:jwt-bearer", r.FormValue("client_assertion_type"))
			require.Equal(t, "fake-sa-token", r.FormValue("client_assertion"))
			require.Equal(t, "api://test-client-id/.default", r.FormValue("scope"))

			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]interface{}{
				"access_token": "exchanged-access-token",
				"expires_in":   3600,
			})
		}))
		defer server.Close()

		// Write a fake SA token file.
		tokenFile := filepath.Join(t.TempDir(), "token")
		require.NoError(t, os.WriteFile(tokenFile, []byte("fake-sa-token"), 0o600))

		ts := &OAuthTokenSourceWorkloadIdentity{
			TokenEndpoint: server.URL,
			ClientID:      "test-client-id",
			TokenFilePath: tokenFile,
			Scopes:        []string{"api://test-client-id/.default"},
		}

		token, err := ts.Token()
		require.NoError(t, err)
		require.Equal(t, "exchanged-access-token", token.Token)
	})

	t.Run("caches token on subsequent calls", func(t *testing.T) {
		callCount := 0
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			callCount++
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]interface{}{
				"access_token": "cached-token",
				"expires_in":   3600,
			})
		}))
		defer server.Close()

		tokenFile := filepath.Join(t.TempDir(), "token")
		require.NoError(t, os.WriteFile(tokenFile, []byte("sa-token"), 0o600))

		ts := &OAuthTokenSourceWorkloadIdentity{
			TokenEndpoint: server.URL,
			ClientID:      "test-client",
			TokenFilePath: tokenFile,
			Scopes:        []string{"api://test/.default"},
		}

		token1, err := ts.Token()
		require.NoError(t, err)
		require.Equal(t, "cached-token", token1.Token)

		token2, err := ts.Token()
		require.NoError(t, err)
		require.Equal(t, "cached-token", token2.Token)

		require.Equal(t, 1, callCount, "token endpoint should only be called once")
	})

	t.Run("error when token file does not exist", func(t *testing.T) {
		ts := &OAuthTokenSourceWorkloadIdentity{
			TokenEndpoint: "https://login.microsoftonline.com/tenant/oauth2/v2.0/token",
			ClientID:      "test-client",
			TokenFilePath: "/nonexistent/path/token",
		}

		_, err := ts.Token()
		require.Error(t, err)
		require.Contains(t, err.Error(), "failed to read federated token file")
	})

	t.Run("error when token file is empty", func(t *testing.T) {
		tokenFile := filepath.Join(t.TempDir(), "token")
		require.NoError(t, os.WriteFile(tokenFile, []byte(""), 0o600))

		ts := &OAuthTokenSourceWorkloadIdentity{
			TokenEndpoint: "https://login.microsoftonline.com/tenant/oauth2/v2.0/token",
			ClientID:      "test-client",
			TokenFilePath: tokenFile,
		}

		_, err := ts.Token()
		require.Error(t, err)
		require.Contains(t, err.Error(), "is empty")
	})

	t.Run("error when not configured", func(t *testing.T) {
		ts := &OAuthTokenSourceWorkloadIdentity{}

		_, err := ts.Token()
		require.Error(t, err)
		require.Contains(t, err.Error(), "not fully configured")
	})

	t.Run("error on non-200 response", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusUnauthorized)
		}))
		defer server.Close()

		tokenFile := filepath.Join(t.TempDir(), "token")
		require.NoError(t, os.WriteFile(tokenFile, []byte("sa-token"), 0o600))

		ts := &OAuthTokenSourceWorkloadIdentity{
			TokenEndpoint: server.URL,
			ClientID:      "test-client",
			TokenFilePath: tokenFile,
		}

		_, err := ts.Token()
		require.Error(t, err)
		require.Contains(t, err.Error(), "HTTP 401")
	})

	t.Run("passes extensions to sarama token", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]interface{}{
				"access_token": "token-with-ext",
				"expires_in":   3600,
			})
		}))
		defer server.Close()

		tokenFile := filepath.Join(t.TempDir(), "token")
		require.NoError(t, os.WriteFile(tokenFile, []byte("sa-token"), 0o600))

		ts := &OAuthTokenSourceWorkloadIdentity{
			TokenEndpoint: server.URL,
			ClientID:      "test-client",
			TokenFilePath: tokenFile,
			Extensions: map[string]string{
				"Identity Pool Id": "pool-ABC",
				"Logical Cluster":  "lkc-123",
			},
		}

		token, err := ts.Token()
		require.NoError(t, err)
		require.Equal(t, "token-with-ext", token.Token)
		require.Equal(t, "pool-ABC", token.Extensions["Identity Pool Id"])
		require.Equal(t, "lkc-123", token.Extensions["Logical Cluster"])
	})
}

func TestWorkloadIdentityMetadataDefaults(t *testing.T) {
	t.Run("uses explicit metadata values", func(t *testing.T) {
		meta := KafkaMetadata{
			OidcTokenEndpoint: "https://custom-endpoint/token",
			OidcClientID:      "explicit-client",
			OidcTokenFilePath: "/custom/path/token",
			internalOidcScopes: []string{"custom-scope"},
		}

		ts := meta.getOAuthTokenSourceWorkloadIdentity()
		require.Equal(t, "https://custom-endpoint/token", ts.TokenEndpoint)
		require.Equal(t, "explicit-client", ts.ClientID)
		require.Equal(t, "/custom/path/token", ts.TokenFilePath)
		require.Equal(t, []string{"custom-scope"}, ts.Scopes)
	})

	t.Run("falls back to env vars", func(t *testing.T) {
		t.Setenv("AZURE_CLIENT_ID", "env-client-id")
		t.Setenv("AZURE_TENANT_ID", "env-tenant-id")
		t.Setenv("AZURE_AUTHORITY_HOST", "https://login.microsoftonline.com/")
		t.Setenv("AZURE_FEDERATED_TOKEN_FILE", "/var/run/secrets/azure/tokens/azure-identity-token")

		meta := KafkaMetadata{}

		ts := meta.getOAuthTokenSourceWorkloadIdentity()
		require.Equal(t, "env-client-id", ts.ClientID)
		require.Equal(t, "https://login.microsoftonline.com/env-tenant-id/oauth2/v2.0/token", ts.TokenEndpoint)
		require.Equal(t, "/var/run/secrets/azure/tokens/azure-identity-token", ts.TokenFilePath)
		require.Equal(t, []string{"api://env-client-id/.default"}, ts.Scopes)
	})

	t.Run("auto-derives scope from clientID", func(t *testing.T) {
		meta := KafkaMetadata{
			OidcClientID: "my-app-id",
		}

		ts := meta.getOAuthTokenSourceWorkloadIdentity()
		require.Equal(t, []string{"api://my-app-id/.default"}, ts.Scopes)
	})

	t.Run("defaults token file path when env not set", func(t *testing.T) {
		t.Setenv("AZURE_FEDERATED_TOKEN_FILE", "")

		meta := KafkaMetadata{}

		ts := meta.getOAuthTokenSourceWorkloadIdentity()
		require.Equal(t, defaultAzureFederatedTokenFile, ts.TokenFilePath)
	})
}
