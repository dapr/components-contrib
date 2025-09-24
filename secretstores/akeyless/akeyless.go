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
	v2     *akeyless.V2ApiService
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
	JWT        string `json:"jwt" mapstructure:"jwt"`
	AccessID   string `json:"accessId" mapstructure:"accessId"`
	AccessKey  string `json:"accessKey" mapstructure:"accessKey"`
	AccessType string `json:"accessType" mapstructure:"accessType"`
}

// Init creates a new Akeyless secret store client and sets up the Akeyless API client
// with authentication method based on the accessId.
func (a *akeylessSecretStore) Init(ctx context.Context, meta secretstores.Metadata) error {
	a.logger.Info("Initializing Akeyless secret store...")
	a.logger.Info("Parsing metadata...")
	m, err := a.parseMetadata(meta)
	if err != nil {
		return errors.New("failed to parse metadata: " + err.Error())
	}

	err = Authenticate(m, a)
	if err != nil {
		return errors.New("failed to authenticate with Akeyless: " + err.Error())
	}

	return nil
}

// GetSecret retrieves a secret using a key and returns a map of decrypted string/string values.
func (a *akeylessSecretStore) GetSecret(ctx context.Context, req secretstores.GetSecretRequest) (secretstores.GetSecretResponse, error) {
	if a.v2 == nil {
		return secretstores.GetSecretResponse{}, errors.New("akeyless client not initialized")
	}

	// Create the get secret value request
	getSecretValue := akeyless.NewGetSecretValue([]string{req.Name})
	getSecretValue.SetToken(a.token)

	// Execute the request
	result, _, err := a.v2.GetSecretValue(ctx).Body(*getSecretValue).Execute()
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
	if a.v2 == nil {
		return secretstores.BulkGetSecretResponse{}, errors.New("akeyless client not initialized")
	}

	// For bulk get, we need to list all secrets first
	listItems := akeyless.NewListItems()
	listItems.SetToken(a.token)

	// Execute the list items request
	itemsList, _, err := a.v2.ListItems(ctx).Body(*listItems).Execute()
	if err != nil {
		return secretstores.BulkGetSecretResponse{}, fmt.Errorf("failed to list items from Akeyless: %w", err)
	}

	// Create a map to store all secrets for response
	allSecrets := make(map[string]map[string]string)

	// Create a list of item names from all items
	itemsNames := make([]string, 0, len(itemsList.Items))
	for _, item := range itemsList.Items {
		if item.ItemName == nil {
			continue
		}
		itemsNames = append(itemsNames, *item.ItemName)
	}

	// Get all secrets
	getSecretValue := akeyless.NewGetSecretValue(itemsNames)
	getSecretValue.SetToken(a.token)
	secretResp, _, err := a.v2.GetSecretValue(ctx).Body(*getSecretValue).Execute()
	if err != nil {
		return secretstores.BulkGetSecretResponse{}, fmt.Errorf("failed to get secrets from Akeyless: %w", err)
	}

	// Add all secrets to the response
	for name, secret := range secretResp {
		// secret is of type interface{}, need to assert to map[string]interface{} first
		secretMap, ok := secret.(map[string]any)
		if !ok {
			return secretstores.BulkGetSecretResponse{}, fmt.Errorf("unexpected secret type for %s", name)
		}
		// Convert map[string]interface{} to map[string]string
		secretStrMap := make(map[string]string)
		for k, v := range secretMap {
			if v == nil {
				secretStrMap[k] = ""
			} else {
				secretStrMap[k] = fmt.Sprintf("%v", v)
			}
		}
		allSecrets[name] = secretStrMap
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

	a.logger.Debug("Parsing metadata...")
	var m akeylessMetadata
	err := kitmd.DecodeMetadata(meta.Properties, &m)
	if err != nil {
		return nil, err
	}

	// Validate access ID
	if m.AccessID == "" {
		return nil, errors.New("accessId is required")
	}

	if !IsValidAccessIdFormat(m.AccessID) {
		return nil, errors.New("invalid accessId format, expected format is p-([A-Za-z0-9]{14}|[A-Za-z0-9]{12})")
	}

	// Get the authentication method
	a.logger.Debug("extracting access type from accessId...")
	accessTypeChar, err := ExtractAccessTypeChar(m.AccessID)
	if err != nil {
		return nil, errors.New("unable to extract access type character from accessId, expected format is p-([A-Za-z0-9]{14}|[A-Za-z0-9]{12})")
	}

	a.logger.Debug("getting access type display name for character %s...", accessTypeChar)
	accessTypeDisplayName, err := GetAccessTypeDisplayName(accessTypeChar)
	if err != nil {
		return nil, errors.New("unable to get access type display name, expected format is p-([A-Za-z0-9]{14}|[A-Za-z0-9]{12})")
	}
	a.logger.Debug("access type detected: %s", accessTypeDisplayName)

	switch accessTypeDisplayName {
	case AKEYLESS_AUTH_DEFAULT_ACCESS_TYPE:
		if m.AccessKey == "" {
			return nil, errors.New("accessKey is required")
		}
	case AKEYLESS_AUTH_ACCESS_JWT:
		if m.JWT == "" {
			return nil, errors.New("jwt is required")
		}
	}
	m.AccessType = accessTypeDisplayName

	// Set default gateway URL if not specified
	if m.GatewayURL == "" {
		a.logger.Info("Gateway URL is not set, using default value %s...", AKEYLESS_PUBLIC_GATEWAY_URL)
		m.GatewayURL = AKEYLESS_PUBLIC_GATEWAY_URL
	}

	return &m, nil
}
