package akeyless

import (
	"context"
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

	a.logger.Debug("getting secret type for '%s'...", req.Name)
	secretType, err := GetSecretType(req.Name, a)
	if err != nil {
		return secretstores.GetSecretResponse{}, err
	}

	a.logger.Debug("getting secret value for '%s' (type %s)...", req.Name, secretType)

	secretValue, err := GetSingleSecretValue(req.Name, secretType, a)
	if err != nil {
		return secretstores.GetSecretResponse{}, errors.New(err.Error())
	}
	a.logger.Debug("secret '%s' value: %s", req.Name, secretValue[:3]+"[REDACTED]")

	// Return the secret in the expected format
	return GetDaprSingleSecretResponse(req.Name, secretValue)
}

// BulkGetSecret retrieves all secrets in the store and returns a map of decrypted string/string values.
func (a *akeylessSecretStore) BulkGetSecret(ctx context.Context, req secretstores.BulkGetSecretRequest) (secretstores.BulkGetSecretResponse, error) {
	if a.v2 == nil {
		return secretstores.BulkGetSecretResponse{}, errors.New("akeyless client not initialized")
	}

	// For bulk get, we need to list all secrets first
	listItems := akeyless.NewListItems()
	listItems.SetToken(a.token)
	listItems.SetPath("/")
	listItems.SetType([]string{AKEYLESS_SECRET_TYPE_STATIC, AKEYLESS_SECRET_TYPE_DYNAMIC, AKEYLESS_SECRET_TYPE_ROTATED})

	// Execute the list items request
	itemsList, _, err := a.v2.ListItems(ctx).Body(*listItems).Execute()
	if err != nil {
		return secretstores.BulkGetSecretResponse{}, fmt.Errorf("failed to list items from Akeyless: %w", err)
	}
	a.logger.Debug("%d items returned from Akeyless", len(itemsList.Items))

	// Use the new BulkGetSecretResponse function to handle all secret types properly
	// return BulkGetSecretResponse(ctx, itemsList.Items, a)
	return secretstores.BulkGetSecretResponse{}, nil
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
