package akeyless

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"

	"github.com/akeylesslabs/akeyless-go/v5"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/secretstores"
	"github.com/dapr/kit/logger"
	kitmd "github.com/dapr/kit/metadata"
)

var _ secretstores.SecretStore = (*akeylessSecretStore)(nil)

// akeylessSecretStore is a secret store implementation for Akeyless.
type akeylessSecretStore struct {
	client *akeyless.APIClient
	token  string
	logger logger.Logger
}

// NewAkeylessSecretStore returns a new Akeyless secret store.
func NewAkeylessSecretStore(logger logger.Logger) secretstores.SecretStore {
	return &akeylessSecretStore{
		logger: logger,
	}
}

// akeylessMetadata contains the metadata for the Akeyless secret store.
type akeylessMetadata struct {
	GatewayURL string `json:"gatewayUrl" mapstructure:"gatewayUrl"`
	Token      string `json:"token" mapstructure:"token"`
}

// Init creates a new Akeyless secret store client.
func (a *akeylessSecretStore) Init(ctx context.Context, meta secretstores.Metadata) error {
	m, err := a.parseMetadata(meta)
	if err != nil {
		return err
	}

	// Set up Akeyless configuration
	config := akeyless.NewConfiguration()
	if m.GatewayURL != "" {
		config.Servers = []akeyless.ServerConfiguration{
			{
				URL: m.GatewayURL,
			},
		}
	}

	// Create the API client
	a.client = akeyless.NewAPIClient(config)
	a.token = m.Token

	return nil
}

// GetSecret retrieves a secret using a key and returns a map of decrypted string/string values.
func (a *akeylessSecretStore) GetSecret(ctx context.Context, req secretstores.GetSecretRequest) (secretstores.GetSecretResponse, error) {
	if a.client == nil {
		return secretstores.GetSecretResponse{}, errors.New("akeyless client not initialized")
	}

	// Create the get secret value request
	getSecretValue := akeyless.NewGetSecretValue([]string{req.Name})
	getSecretValue.SetToken(a.token)

	// Execute the request
	result, _, err := a.client.V2Api.GetSecretValue(ctx).Body(*getSecretValue).Execute()
	if err != nil {
		return secretstores.GetSecretResponse{}, fmt.Errorf("failed to get secret from Akeyless: %w", err)
	}

	// Extract the secret value
	secretValue, exists := result[req.Name]
	if !exists {
		return secretstores.GetSecretResponse{}, fmt.Errorf("secret '%s' not found", req.Name)
	}

	// Convert the secret value to string
	var secretStr string
	if str, ok := secretValue.(string); ok {
		secretStr = str
	} else {
		// If it's not a string, convert it to JSON string
		secretBytes, err := json.Marshal(secretValue)
		if err != nil {
			return secretstores.GetSecretResponse{}, fmt.Errorf("failed to convert secret value to string: %w", err)
		}
		secretStr = string(secretBytes)
	}

	// Return the secret in the expected format
	return secretstores.GetSecretResponse{
		Data: map[string]string{
			req.Name: secretStr,
		},
	}, nil
}

// BulkGetSecret retrieves all secrets in the store and returns a map of decrypted string/string values.
func (a *akeylessSecretStore) BulkGetSecret(ctx context.Context, req secretstores.BulkGetSecretRequest) (secretstores.BulkGetSecretResponse, error) {
	if a.client == nil {
		return secretstores.BulkGetSecretResponse{}, errors.New("akeyless client not initialized")
	}

	// For bulk get, we need to list all secrets first
	listItems := akeyless.NewListItems()
	listItems.SetToken(a.token)

	// Execute the list items request
	itemsList, _, err := a.client.V2Api.ListItems(ctx).Body(*listItems).Execute()
	if err != nil {
		return secretstores.BulkGetSecretResponse{}, fmt.Errorf("failed to list items from Akeyless: %w", err)
	}

	// Get all secret values
	allSecrets := make(map[string]map[string]string)
	for _, item := range itemsList.Items {
		if item.ItemName == nil {
			continue
		}
		secretName := *item.ItemName
		secretResp, err := a.GetSecret(ctx, secretstores.GetSecretRequest{
			Name:     secretName,
			Metadata: req.Metadata,
		})
		if err != nil {
			a.logger.Warnf("Failed to get secret '%s': %v", secretName, err)
			continue
		}
		allSecrets[secretName] = secretResp.Data
	}

	return secretstores.BulkGetSecretResponse{
		Data: allSecrets,
	}, nil
}

// Features returns the features available in this secret store.
func (a *akeylessSecretStore) Features() []secretstores.Feature {
	return []secretstores.Feature{}
}

// GetComponentMetadata returns the component metadata.
func (a *akeylessSecretStore) GetComponentMetadata() (metadataInfo metadata.MetadataMap) {
	metadataStruct := akeylessMetadata{}
	metadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, metadata.SecretStoreType)
	return
}

// Close closes the secret store.
func (a *akeylessSecretStore) Close() error {
	return nil
}

// parseMetadata parses the metadata from the component configuration.
func (a *akeylessSecretStore) parseMetadata(meta secretstores.Metadata) (*akeylessMetadata, error) {
	var m akeylessMetadata
	err := kitmd.DecodeMetadata(meta.Properties, &m)
	if err != nil {
		return nil, err
	}

	if m.Token == "" {
		return nil, errors.New("token is required")
	}

	return &m, nil
}
