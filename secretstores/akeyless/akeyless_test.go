package akeyless

import (
	"context"
	"testing"

	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/secretstores"
	"github.com/dapr/kit/logger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testToken      = "test-token"
	testGatewayURL = "https://test-gateway.akeyless.io"
)

func TestNewAkeylessSecretStore(t *testing.T) {
	log := logger.NewLogger("test")
	store := NewAkeylessSecretStore(log)
	assert.NotNil(t, store)
}

func TestInit(t *testing.T) {
	tests := []struct {
		name        string
		metadata    secretstores.Metadata
		expectError bool
	}{
		{
			name: "valid metadata with token",
			metadata: secretstores.Metadata{
				Base: metadata.Base{
					Properties: map[string]string{
						"token": testToken,
					},
				},
			},
			expectError: false,
		},
		{
			name: "valid metadata with token and gateway URL",
			metadata: secretstores.Metadata{
				Base: metadata.Base{
					Properties: map[string]string{
						"token":      testToken,
						"gatewayUrl": testGatewayURL,
					},
				},
			},
			expectError: false,
		},
		{
			name: "missing token",
			metadata: secretstores.Metadata{
				Base: metadata.Base{
					Properties: map[string]string{},
				},
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			log := logger.NewLogger("test")
			store := NewAkeylessSecretStore(log).(*akeylessSecretStore)

			err := store.Init(context.Background(), tt.metadata)
			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, store.client)
				assert.Equal(t, "test-token", store.token)
			}
		})
	}
}

func TestGetSecretWithoutInit(t *testing.T) {
	log := logger.NewLogger("test")
	store := NewAkeylessSecretStore(log).(*akeylessSecretStore)

	req := secretstores.GetSecretRequest{
		Name: "test-secret",
	}

	_, err := store.GetSecret(context.Background(), req)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not initialized")
}

func TestBulkGetSecretWithoutInit(t *testing.T) {
	log := logger.NewLogger("test")
	store := NewAkeylessSecretStore(log).(*akeylessSecretStore)

	req := secretstores.BulkGetSecretRequest{}

	_, err := store.BulkGetSecret(context.Background(), req)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not initialized")
}

func TestFeatures(t *testing.T) {
	log := logger.NewLogger("test")
	store := NewAkeylessSecretStore(log)

	features := store.Features()
	assert.Empty(t, features)
}

func TestClose(t *testing.T) {
	log := logger.NewLogger("test")
	store := NewAkeylessSecretStore(log)

	err := store.Close()
	assert.NoError(t, err)
}

func TestParseMetadata(t *testing.T) {
	tests := []struct {
		name        string
		properties  map[string]string
		expectError bool
		expected    *akeylessMetadata
	}{
		{
			name: "valid metadata with token only",
			properties: map[string]string{
				"token": testToken,
			},
			expectError: false,
			expected: &akeylessMetadata{
				Token: testToken,
			},
		},
		{
			name: "valid metadata with token and gateway URL",
			properties: map[string]string{
				"token":      testToken,
				"gatewayUrl": testGatewayURL,
			},
			expectError: false,
			expected: &akeylessMetadata{
				Token:      testToken,
				GatewayURL: testGatewayURL,
			},
		},
		{
			name: "missing token",
			properties: map[string]string{
				"gatewayUrl": testGatewayURL,
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			log := logger.NewLogger("test")
			store := NewAkeylessSecretStore(log).(*akeylessSecretStore)

			meta := secretstores.Metadata{
				Base: metadata.Base{
					Properties: tt.properties,
				},
			}

			result, err := store.parseMetadata(meta)
			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestGetComponentMetadata(t *testing.T) {
	log := logger.NewLogger("test")
	store := NewAkeylessSecretStore(log).(*akeylessSecretStore)

	metadata := store.GetComponentMetadata()
	require.NotNil(t, metadata)

	// Check that the metadata contains the expected fields
	assert.Contains(t, metadata, "gatewayUrl")
	assert.Contains(t, metadata, "token")

	// Check that the metadata fields exist
	tokenField := metadata["token"]
	require.NotNil(t, tokenField)

	gatewayField := metadata["gatewayUrl"]
	require.NotNil(t, gatewayField)
}
