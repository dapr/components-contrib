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

package zeebe

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
)

func TestParseMetadata(t *testing.T) {
	m := bindings.Metadata{Base: metadata.Base{Properties: map[string]string{
		"gatewayAddr":            "172.0.0.1:1234",
		"gatewayKeepAlive":       "5s",
		"caCertificatePath":      "/cert/path",
		"usePlaintextConnection": "true",
		"clientId":               "zeebe-client",
		"clientSecret":           "zeebe-secret",
		"authorizationServerUrl": "https://issuer.example.com/oauth/token",
		"tokenAudience":          "zeebe-api",
		"tokenScope":             "read write",
		"clientConfigPath":       "/tmp/zeebe-cache.yaml",
	}}}
	client := ClientFactoryImpl{logger: logger.NewLogger("test")}
	meta, err := client.parseMetadata(m)
	require.NoError(t, err)
	assert.Equal(t, "172.0.0.1:1234", meta.GatewayAddr)
	assert.Equal(t, 5*time.Second, meta.GatewayKeepAlive)
	assert.Equal(t, "/cert/path", meta.CaCertificatePath)
	assert.True(t, meta.UsePlaintextConnection)
	assert.Equal(t, "zeebe-client", meta.ClientID)
	assert.Equal(t, "zeebe-secret", meta.ClientSecret)
	assert.Equal(t, "https://issuer.example.com/oauth/token", meta.AuthorizationServerURL)
	assert.Equal(t, "zeebe-api", meta.TokenAudience)
	assert.Equal(t, "read write", meta.TokenScope)
	assert.Equal(t, "/tmp/zeebe-cache.yaml", meta.ClientConfigPath)
}

func TestGatewayAddrMetadataIsMandatory(t *testing.T) {
	m := bindings.Metadata{}
	client := ClientFactoryImpl{logger: logger.NewLogger("test")}
	meta, err := client.parseMetadata(m)
	assert.Nil(t, meta)
	require.Error(t, err)
	assert.Equal(t, err, ErrMissingGatewayAddr)
}

func TestParseMetadataDefaultValues(t *testing.T) {
	m := bindings.Metadata{Base: metadata.Base{Properties: map[string]string{"gatewayAddr": "172.0.0.1:1234"}}}
	client := ClientFactoryImpl{logger: logger.NewLogger("test")}
	meta, err := client.parseMetadata(m)
	require.NoError(t, err)
	assert.Equal(t, time.Duration(0), meta.GatewayKeepAlive)
	assert.Equal(t, "", meta.CaCertificatePath)
	assert.False(t, meta.UsePlaintextConnection)
	assert.Equal(t, "", meta.ClientID)
	assert.Equal(t, "", meta.ClientSecret)
	assert.Equal(t, "", meta.AuthorizationServerURL)
	assert.Equal(t, "", meta.TokenAudience)
	assert.Equal(t, "", meta.TokenScope)
	assert.Equal(t, "", meta.ClientConfigPath)
}

func TestNewCredentialsProviderSkipsWhenOAuthNotConfigured(t *testing.T) {
	meta := &ClientMetadata{}

	provider, err := meta.newCredentialsProvider()

	require.NoError(t, err)
	assert.Nil(t, provider)
}

func TestNewCredentialsProviderReturnsErrorOnInvalidOAuthMetadata(t *testing.T) {
	meta := &ClientMetadata{
		AuthorizationServerURL: "https://issuer.example.com/oauth/token",
		TokenAudience:          "zeebe-api",
		ClientID:               "zeebe-client",
	}

	provider, err := meta.newCredentialsProvider()

	assert.Nil(t, provider)
	require.ErrorIs(t, err, ErrInvalidOAuthMetadata)
	assert.Contains(t, err.Error(), "missing: clientSecret")
}

func TestNewCredentialsProviderReturnsErrorWhenOnlyOptionalOAuthFieldsProvided(t *testing.T) {
	meta := &ClientMetadata{
		TokenScope: "scopeA",
	}

	provider, err := meta.newCredentialsProvider()

	assert.Nil(t, provider)
	require.ErrorIs(t, err, ErrInvalidOAuthMetadata)
	errMsg := err.Error()
	assert.Contains(t, errMsg, "missing:")
	assert.Contains(t, errMsg, "clientId")
	assert.Contains(t, errMsg, "clientSecret")
	assert.Contains(t, errMsg, "authorizationServerUrl")
	assert.Contains(t, errMsg, "tokenAudience")
}

func TestNewCredentialsProviderCreatesOAuthProviderWithCustomCachePath(t *testing.T) {
	meta := &ClientMetadata{
		ClientID:               "zeebe-client",
		ClientSecret:           "zeebe-secret",
		AuthorizationServerURL: "https://issuer.example.com/oauth/token",
		TokenAudience:          "zeebe-api",
		TokenScope:             "scopeA",
		ClientConfigPath:       filepath.Join(t.TempDir(), "zeebe-credentials.yaml"),
	}

	provider, err := meta.newCredentialsProvider()

	require.NoError(t, err)
	assert.NotNil(t, provider)
}
