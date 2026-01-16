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

package oauth2

import (
	"context"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/oauth2"
	ccreds "golang.org/x/oauth2/clientcredentials"

	"github.com/dapr/kit/logger"
)

func Test_toConfig(t *testing.T) {
	tests := map[string]struct {
		opts      ClientCredentialsOptions
		expConfig *ccreds.Config
		expErr    bool
	}{
		"no scopes should error": {
			opts: ClientCredentialsOptions{
				TokenURL:     "https://localhost:8080",
				ClientID:     "client-id",
				ClientSecret: "client-secret",
				Audiences:    []string{"audience"},
			},
			expErr: true,
		},
		"bad URL endpoint should error": {
			opts: ClientCredentialsOptions{
				TokenURL:     "&&htp:/f url",
				ClientID:     "client-id",
				ClientSecret: "client-secret",
				Audiences:    []string{"audience"},
				Scopes:       []string{"foo"},
			},
			expErr: true,
		},
		"bad CA certificate should error": {
			opts: ClientCredentialsOptions{
				TokenURL:     "http://localhost:8080",
				ClientID:     "client-id",
				ClientSecret: "client-secret",
				Audiences:    []string{"audience"},
				Scopes:       []string{"foo"},
				CAPEM:        []byte("ca-pem"),
			},
			expErr: true,
		},
		"no audiences should error": {
			opts: ClientCredentialsOptions{
				TokenURL:     "http://localhost:8080",
				ClientID:     "client-id",
				ClientSecret: "client-secret",
				Scopes:       []string{"foo"},
			},
			expErr: true,
		},
		"should default scope": {
			opts: ClientCredentialsOptions{
				TokenURL:     "http://localhost:8080",
				ClientID:     "client-id",
				ClientSecret: "client-secret",
				Audiences:    []string{"audience"},
				Scopes:       []string{"foo", "bar"},
			},
			expConfig: &ccreds.Config{
				ClientID:       "client-id",
				ClientSecret:   "client-secret",
				TokenURL:       "http://localhost:8080",
				Scopes:         []string{"foo", "bar"},
				EndpointParams: url.Values{"audience": []string{"audience"}},
			},
			expErr: false,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			config, _, err := test.opts.toConfig()
			assert.Equalf(t, test.expErr, err != nil, "%v", err)
			assert.Equal(t, test.expConfig, config)
		})
	}
}

func Test_TokenRenewal(t *testing.T) {
	expired := &oauth2.Token{AccessToken: "old-token", Expiry: time.Now().Add(-1 * time.Minute)}
	renewed := &oauth2.Token{AccessToken: "new-token", Expiry: time.Now().Add(1 * time.Hour)}

	c := &ClientCredentials{
		log:          logger.NewLogger("test"),
		currentToken: expired,
		fetchTokenFn: func(ctx context.Context) (*oauth2.Token, error) {
			return renewed, nil
		},
	}

	tok, err := c.Token()
	require.NoError(t, err)
	assert.Equal(t, "new-token", tok)
}

func TestLoadCredentialsFromJSONFile(t *testing.T) {
	t.Run("valid JSON", func(t *testing.T) {
		tmpFile, err := os.CreateTemp(t.TempDir(), "credentials-*.json")
		require.NoError(t, err)
		defer os.Remove(tmpFile.Name())

		content := `{"client_id": "test-id", "client_secret": "test-secret", "issuer_url": "https://oauth.example.com/token"}`
		_, err = tmpFile.WriteString(content)
		require.NoError(t, err)
		require.NoError(t, tmpFile.Close())

		clientID, clientSecret, issuerURL, err := LoadCredentialsFromJSONFile(tmpFile.Name())
		require.NoError(t, err)
		assert.Equal(t, "test-id", clientID)
		assert.Equal(t, "test-secret", clientSecret)
		assert.Equal(t, "https://oauth.example.com/token", issuerURL)
	})

	t.Run("missing required fields", func(t *testing.T) {
		tmpFile, err := os.CreateTemp(t.TempDir(), "credentials-*.json")
		require.NoError(t, err)
		defer os.Remove(tmpFile.Name())

		_, err = tmpFile.WriteString(`{"client_id": "test-id"}`)
		require.NoError(t, err)
		require.NoError(t, tmpFile.Close())

		_, _, _, err = LoadCredentialsFromJSONFile(tmpFile.Name())
		require.Error(t, err)
		assert.Contains(t, err.Error(), "must contain client_id, client_secret, and issuer_url")
	})

	t.Run("invalid JSON", func(t *testing.T) {
		tmpFile, err := os.CreateTemp(t.TempDir(), "credentials-*.json")
		require.NoError(t, err)
		defer os.Remove(tmpFile.Name())

		_, err = tmpFile.WriteString("{ invalid json }")
		require.NoError(t, err)
		require.NoError(t, tmpFile.Close())

		_, _, _, err = LoadCredentialsFromJSONFile(tmpFile.Name())
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to parse JSON")
	})

	t.Run("file not found", func(t *testing.T) {
		_, _, _, err := LoadCredentialsFromJSONFile("/nonexistent/file/path")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "could not read oauth2 credentials from file")
	})
}

func TestClientCredentialsMetadata_ResolveCredentials(t *testing.T) {
	t.Run("oauth2CredentialsFile with metadata override", func(t *testing.T) {
		tmpFile, err := os.CreateTemp(t.TempDir(), "credentials-*.json")
		require.NoError(t, err)
		defer os.Remove(tmpFile.Name())

		_, err = tmpFile.WriteString(`{"client_id": "file-id", "client_secret": "file-secret", "issuer_url": "https://file.com/token"}`)
		require.NoError(t, err)
		require.NoError(t, tmpFile.Close())

		m := ClientCredentialsMetadata{
			ClientID:            "meta-id",
			ClientSecret:        "meta-secret",
			TokenURL:            "https://meta.com/token",
			CredentialsFilePath: tmpFile.Name(),
		}

		err = m.ResolveCredentials()
		require.NoError(t, err)
		assert.Equal(t, "meta-id", m.ClientID)                // metadata overrides
		assert.Equal(t, "meta-secret", m.ClientSecret)        // metadata overrides
		assert.Equal(t, "https://meta.com/token", m.TokenURL) // metadata overrides
	})

	t.Run("oauth2CredentialsFile without metadata", func(t *testing.T) {
		tmpFile, err := os.CreateTemp(t.TempDir(), "credentials-*.json")
		require.NoError(t, err)
		defer os.Remove(tmpFile.Name())

		_, err = tmpFile.WriteString(`{"client_id": "file-id", "client_secret": "file-secret", "issuer_url": "https://file.com/token"}`)
		require.NoError(t, err)
		require.NoError(t, tmpFile.Close())

		m := ClientCredentialsMetadata{CredentialsFilePath: tmpFile.Name()}
		err = m.ResolveCredentials()
		require.NoError(t, err)
		assert.Equal(t, "file-id", m.ClientID)
		assert.Equal(t, "file-secret", m.ClientSecret)
		assert.Equal(t, "https://file.com/token", m.TokenURL)
	})

	t.Run("oauth2ClientSecretPath with metadata override", func(t *testing.T) {
		tmpFile, err := os.CreateTemp(t.TempDir(), "secret-*.txt")
		require.NoError(t, err)
		defer os.Remove(tmpFile.Name())

		_, err = tmpFile.WriteString("file-secret")
		require.NoError(t, err)
		require.NoError(t, tmpFile.Close())

		m := ClientCredentialsMetadata{
			ClientID:         "meta-id",
			ClientSecret:     "meta-secret",
			ClientSecretPath: tmpFile.Name(),
		}

		err = m.ResolveCredentials()
		require.NoError(t, err)
		assert.Equal(t, "meta-id", m.ClientID)
		assert.Equal(t, "meta-secret", m.ClientSecret) // metadata overrides
	})

	t.Run("oauth2ClientSecretPath without metadata", func(t *testing.T) {
		tmpFile, err := os.CreateTemp(t.TempDir(), "secret-*.txt")
		require.NoError(t, err)
		defer os.Remove(tmpFile.Name())

		_, err = tmpFile.WriteString("file-secret")
		require.NoError(t, err)
		require.NoError(t, tmpFile.Close())

		m := ClientCredentialsMetadata{ClientSecretPath: tmpFile.Name()}
		err = m.ResolveCredentials()
		require.NoError(t, err)
		assert.Equal(t, "file-secret", m.ClientSecret)
	})

	t.Run("error both fields set", func(t *testing.T) {
		jsonFile, err := os.CreateTemp(t.TempDir(), "credentials-*.json")
		require.NoError(t, err)
		defer os.Remove(jsonFile.Name())
		_, err = jsonFile.WriteString(`{"client_id": "id", "client_secret": "secret", "issuer_url": "https://example.com"}`)
		require.NoError(t, err)
		require.NoError(t, jsonFile.Close())

		txtFile, err := os.CreateTemp(t.TempDir(), "secret-*.txt")
		require.NoError(t, err)
		defer os.Remove(txtFile.Name())
		_, err = txtFile.WriteString("secret")
		require.NoError(t, err)
		require.NoError(t, txtFile.Close())

		m := ClientCredentialsMetadata{
			CredentialsFilePath: jsonFile.Name(),
			ClientSecretPath:    txtFile.Name(),
		}

		err = m.ResolveCredentials()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "mutually exclusive")
	})
}

func TestClientCredentialsMetadata_ToOptions(t *testing.T) {
	logger := logger.NewLogger("test")
	metadata := ClientCredentialsMetadata{
		TokenURL:     "https://token.example.com",
		TokenCAPEM:   "cert-pem-content",
		ClientID:     "test-client-id",
		ClientSecret: "test-client-secret",
		Scopes:       []string{"scope1", "scope2"},
		Audiences:    []string{"audience1"},
	}

	opts := metadata.ToOptions(logger)

	assert.Equal(t, logger, opts.Logger)
	assert.Equal(t, "https://token.example.com", opts.TokenURL)
	assert.Equal(t, []byte("cert-pem-content"), opts.CAPEM)
	assert.Equal(t, "test-client-id", opts.ClientID)
	assert.Equal(t, "test-client-secret", opts.ClientSecret)
	assert.Equal(t, []string{"scope1", "scope2"}, opts.Scopes)
	assert.Equal(t, []string{"audience1"}, opts.Audiences)
}
