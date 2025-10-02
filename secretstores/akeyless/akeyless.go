package akeyless

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sync"

	aws "github.com/akeylesslabs/akeyless-go-cloud-id/cloudprovider/aws"
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

	err = a.Authenticate(m)
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
	secretType, err := a.GetSecretType(req.Name)
	if err != nil {
		return secretstores.GetSecretResponse{}, err
	}

	a.logger.Debug("getting secret value for '%s' (type %s)...", req.Name, secretType)

	secretValue, err := a.GetSingleSecretValue(req.Name, secretType)
	if err != nil {
		return secretstores.GetSecretResponse{}, errors.New(err.Error())
	}
	a.logger.Debug("secret '%s' value: %s", req.Name, secretValue[:3]+"[REDACTED]")

	// Return the secret in the expected format
	return GetDaprSingleSecretResponse(req.Name, secretValue)
}

// BulkGetSecret retrieves all secrets in the store and returns a map of decrypted string/string values.
// The method performs the following steps:
// 1. Recursively list all items in Akeyless
// 2. Separate items by type since only static secrets are supported for bulk get
// 3. Get secret values concurrently, each item type in a separate goroutine
func (a *akeylessSecretStore) BulkGetSecret(ctx context.Context, req secretstores.BulkGetSecretRequest) (secretstores.BulkGetSecretResponse, error) {
	if a.v2 == nil {
		return secretstores.BulkGetSecretResponse{}, errors.New("akeyless client not initialized")
	}

	// initialize response
	response := secretstores.BulkGetSecretResponse{
		Data: make(map[string]map[string]string),
	}

	// For bulk get, we need to list all secrets first
	a.logger.Debug("listing items from / path...")
	listItems, err := a.listItemsRecursively("/")
	if err != nil {
		return response, fmt.Errorf("failed to list items from Akeyless: %w", err)
	}

	// if no items returned, return empty response
	if len(listItems) == 0 {
		a.logger.Debug("no items returned from / path")
		return response, nil
	}

	// separate items by type since only static secrets are supported for bulk get
	staticItems, dynamicItems, rotatedItems := a.separateItemsByType(listItems)
	a.logger.Info("%d items returned (static: %d, dynamic: %d, rotated: %d)", len(listItems), len(staticItems), len(dynamicItems), len(rotatedItems))

	// listItems can get quite large, so we don't need all item details, we can use the item names instead
	// and free memory
	listItems = nil
	staticItemNames := GetItemNames(staticItems)
	dynamicItemNames := GetItemNames(dynamicItems)
	rotatedItemNames := GetItemNames(rotatedItems)
	a.logger.Debug("static items: %v", staticItemNames)
	a.logger.Debug("dynamic items: %v", dynamicItemNames)
	a.logger.Debug("rotated items: %v", rotatedItemNames)

	haveStaticItems := len(staticItemNames) > 0
	haveDynamicItems := len(dynamicItemNames) > 0
	haveRotatedItems := len(rotatedItemNames) > 0

	secretResultChannels := make(chan secretResultCollection, boolToInt(haveStaticItems)+boolToInt(haveDynamicItems)+boolToInt(haveRotatedItems))

	mutex := sync.Mutex{}

	// get secret values concurrently, each item type in a separate goroutine
	wg := sync.WaitGroup{}
	if haveStaticItems {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if len(staticItemNames) == 1 {
				staticSecretName := staticItemNames[0]
				value, err := a.GetSingleSecretValue(staticSecretName, AKEYLESS_SECRET_TYPE_STATIC_SECRET_RESPONSE)
				if err != nil {
					secretResultChannels <- secretResultCollection{name: staticSecretName, value: value, err: err}
				} else {
					secretResultChannels <- secretResultCollection{name: staticSecretName, value: value, err: nil}
				}
			} else {
				secretResponse := a.GetBulkStaticSecretValues(staticItemNames)
				if len(secretResponse) > 0 {
					for _, result := range secretResponse {
						secretResultChannels <- result
					}
				}
			}
		}()
	}
	if haveDynamicItems {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for _, item := range dynamicItemNames {
				value, err := a.GetSingleSecretValue(item, AKEYLESS_SECRET_TYPE_DYNAMIC_SECRET_RESPONSE)
				if err != nil {
					secretResultChannels <- secretResultCollection{name: item, value: "", err: err}
				} else {
					secretResultChannels <- secretResultCollection{name: item, value: value, err: nil}
				}
			}
		}()
	}
	if haveRotatedItems {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for _, item := range rotatedItemNames {
				value, err := a.GetSingleSecretValue(item, AKEYLESS_SECRET_TYPE_ROTATED_SECRET_RESPONSE)
				if err != nil {
					secretResultChannels <- secretResultCollection{name: item, value: "", err: err}
				} else {
					secretResultChannels <- secretResultCollection{name: item, value: value, err: nil}
				}
			}
		}()
	}

	// close the channel when all goroutines are done
	go func() {
		wg.Wait()
		close(secretResultChannels)
	}()

	// collect results and populate response
	for result := range secretResultChannels {
		if result.err != nil {
			a.logger.Error("error getting secret '%s': %s. Skipping...", result.name, result.err.Error())
			continue
		}

		// lock the mutex to prevent race conditions
		mutex.Lock()
		response.Data[result.name] = map[string]string{result.name: result.value}
		mutex.Unlock()
	}

	// Use the new BulkGetSecretResponse function to handle all secret types properly
	// return BulkGetSecretResponse(ctx, itemsList.Items, a)
	return response, nil
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

func (a *akeylessSecretStore) GetSecretType(secretName string) (string, error) {
	describeItem := akeyless.NewDescribeItem(secretName)
	describeItem.SetToken(a.token)
	describeItemResp, _, err := a.v2.DescribeItem(context.Background()).Body(*describeItem).Execute()
	if err != nil {
		return "", fmt.Errorf("failed to describe item '%s': %w", secretName, err)
	}

	if describeItemResp.ItemType == nil {
		return "", errors.New("unable to retrieve secret type, missing type in describe item response")
	}

	return *describeItemResp.ItemType, nil
}

// GetSingleSecretValue gets the value of a single secret from Akeyless.
// It returns the value of the secret or an error if the secret is not found.
func (a *akeylessSecretStore) GetSingleSecretValue(secretName string, secretType string) (string, error) {

	var secretValue string
	var err error

	switch secretType {
	case AKEYLESS_SECRET_TYPE_STATIC_SECRET_RESPONSE:
		getSecretValue := akeyless.NewGetSecretValue([]string{secretName})
		getSecretValue.SetToken(a.token)
		secretRespMap, _, apiErr := a.v2.GetSecretValue(context.Background()).Body(*getSecretValue).Execute()
		if apiErr != nil {
			err = fmt.Errorf("failed to get secret '%s' value for static secret from Akeyless API: %w", secretName, apiErr)
			break
		}

		// check if secret key is in response
		value, ok := secretRespMap[secretName]
		if !ok {
			err = fmt.Errorf("failed to get secret '%s' value for static secret from Akeyless API: key not found", secretName)
			break
		}

		// single static secrets can be of type string, or map[string]string
		// if it's a map[string]string, we need to transform it to a string
		secretValue, err = stringifyStaticSecret(value, secretName)
		if err != nil {
			err = fmt.Errorf("failed to stringify static secret '%s': %w", secretName, err)
			break
		}

	case AKEYLESS_SECRET_TYPE_DYNAMIC_SECRET_RESPONSE:
		getDynamicSecretValue := akeyless.NewGetDynamicSecretValue(secretName)
		getDynamicSecretValue.SetToken(a.token)
		secretRespMap, _, apiErr := a.v2.GetDynamicSecretValue(context.Background()).Body(*getDynamicSecretValue).Execute()
		if apiErr != nil {
			err = fmt.Errorf("failed to get dynamic secret '%s' value from Akeyless API: %w", secretName, apiErr)
			break
		}

		// assert type of secretRespMap to DynamicSecretResponse
		var dynamicSecretResp DynamicSecretResponse
		jsonBytes, marshalErr := json.Marshal(secretRespMap)
		if marshalErr != nil {
			err = fmt.Errorf("failed to marshal secret response to JSON: %w", marshalErr)
			break
		}
		if unmarshalErr := json.Unmarshal([]byte(jsonBytes), &dynamicSecretResp); unmarshalErr != nil {
			err = fmt.Errorf("failed to unmarshal secret response to DynamicSecretResponse: %w", unmarshalErr)
			break
		}

		// take only relevant fields (DisplayName and SecretText) from response and marshal it to a JSON string
		dynamicSecretResp.Secret.AppID = ""
		dynamicSecretResp.Secret.EndDateTime = ""
		dynamicSecretResp.Secret.KeyID = ""
		dynamicSecretResp.Secret.TenantID = ""
		jsonBytes, marshalErr = json.Marshal(dynamicSecretResp.Secret)
		if marshalErr != nil {
			err = fmt.Errorf("failed to marshal secret response to JSON: %w", marshalErr)
			break
		}
		secretValue = string(jsonBytes)

	case AKEYLESS_SECRET_TYPE_ROTATED_SECRET_RESPONSE:
		getRotatedSecretValue := akeyless.NewGetRotatedSecretValue(secretName)
		getRotatedSecretValue.SetToken(a.token)
		secretRespMap, _, apiErr := a.v2.GetRotatedSecretValue(context.Background()).Body(*getRotatedSecretValue).Execute()
		if apiErr != nil {
			err = fmt.Errorf("failed to get rotated secret '%s' value from Akeyless API: %w", secretName, apiErr)
			break
		}

		// assert type of secretRespMap to RotatedSecretResponse
		var rotatedSecretResp RotatedSecretResponse
		jsonBytes, marshalErr := json.Marshal(secretRespMap)
		if marshalErr != nil {
			err = fmt.Errorf("failed to marshal secret response to JSON: %w", marshalErr)
			break
		}
		if unmarshalErr := json.Unmarshal([]byte(jsonBytes), &rotatedSecretResp); unmarshalErr != nil {
			err = fmt.Errorf("failed to unmarshal secret response to RotatedSecretResponse: %w", unmarshalErr)
			break
		}

		// take only relevant fields (Username and Password) from response and marshal it to a JSON string
		rotatedSecretResp.Value.ApplicationID = ""
		jsonBytes, marshalErr = json.Marshal(rotatedSecretResp.Value)
		if marshalErr != nil {
			err = fmt.Errorf("failed to marshal secret response to JSON: %w", marshalErr)
			break
		}
		secretValue = string(jsonBytes)
	}

	return secretValue, err
}

// GetBulkStaticSecretValues gets the values of multiple static secrets from Akeyless.
// It returns a map of secret names and their values.
func (a *akeylessSecretStore) GetBulkStaticSecretValues(secretNames []string) []secretResultCollection {

	var secretResponse = make([]secretResultCollection, len(secretNames))

	getSecretsValues := akeyless.NewGetSecretValue(secretNames)
	getSecretsValues.SetToken(a.token)
	secretRespMap, _, apiErr := a.v2.GetSecretValue(context.Background()).Body(*getSecretsValues).Execute()
	if apiErr != nil {
		secretResponse = append(secretResponse, secretResultCollection{name: "", value: "", err: fmt.Errorf("failed to get static secrets' '%s' value from Akeyless API: %w", secretNames, apiErr)})
	} else {
		for secretName, secretValue := range secretRespMap {
			value, err := stringifyStaticSecret(secretValue, secretName)
			secretResponse = append(secretResponse, secretResultCollection{name: secretName, value: value, err: err})
		}
	}

	return secretResponse
}

// listItemsRecursively lists all items in a given path recursively.
// It returns a list of items and an error if the list items request fails.
func (a *akeylessSecretStore) listItemsRecursively(path string) ([]akeyless.Item, error) {
	var allItems []akeyless.Item

	// Create the list items request
	listItems := akeyless.NewListItems()
	listItems.SetToken(a.token)
	listItems.SetPath(path)
	listItems.SetMinimalView(true)
	listItems.SetAutoPagination("enabled")
	listItems.SetType([]string{AKEYLESS_SECRET_TYPE_STATIC, AKEYLESS_SECRET_TYPE_DYNAMIC, AKEYLESS_SECRET_TYPE_ROTATED})

	// Execute the list items request
	a.logger.Debug("listing items from path '%s'...", path)
	itemsList, _, err := a.v2.ListItems(context.Background()).Body(*listItems).Execute()
	if err != nil {
		return nil, err
	}

	// Add items from current path
	if itemsList.Items != nil {
		allItems = append(allItems, itemsList.Items...)
	}

	// Recursively process each subfolder
	if itemsList.Folders != nil {
		for _, folder := range itemsList.Folders {
			subItems, err := a.listItemsRecursively(folder)
			if err != nil {
				return nil, err
			}
			allItems = append(allItems, subItems...)
		}
	}

	return allItems, nil
}

// Authenticate authenticates with Akeyless using the provided metadata.
// It returns an error if the authentication fails.
func (a *akeylessSecretStore) Authenticate(metadata *akeylessMetadata) error {

	a.logger.Debug("Creating authentication request to Akeyless...")
	authRequest := akeyless.NewAuth()
	authRequest.SetAccessId(metadata.AccessID)
	authRequest.SetAccessType(metadata.AccessType)

	// Depending on the access type we set the appropriate authentication method
	switch metadata.AccessType {
	// If access type is AWS IAM we use the cloud ID
	case AKEYLESS_AUTH_ACCESS_IAM:
		a.logger.Debug("getting cloud ID for AWS IAM...")
		id, err := aws.GetCloudId()
		if err != nil {
			return errors.New("unable to get cloud ID")
		}
		authRequest.SetCloudId(id)
	case AKEYLESS_AUTH_ACCESS_JWT:
		a.logger.Debug("setting JWT for authentication...")
		authRequest.SetJwt(metadata.JWT)
	case AKEYLESS_AUTH_DEFAULT_ACCESS_TYPE:
		a.logger.Debug("setting access key for authentication...")
		authRequest.SetAccessKey(metadata.AccessKey)
	}

	// Create Akeyless API client configuration
	a.logger.Debug("creating Akeyless API client configuration...")
	config := akeyless.NewConfiguration()
	config.Servers = []akeyless.ServerConfiguration{
		{
			URL: metadata.GatewayURL,
		},
	}
	config.UserAgent = AKEYLESS_USER_AGENT
	config.AddDefaultHeader("akeylessclienttype", AKEYLESS_USER_AGENT)

	a.v2 = akeyless.NewAPIClient(config).V2Api

	a.logger.Debug("authenticating with Akeyless...")
	out, _, err := a.v2.Auth(context.Background()).Body(*authRequest).Execute()
	if err != nil {
		return fmt.Errorf("failed to authenticate with Akeyless: %w", err)
	}

	a.logger.Debug("setting token %s for authentication...", out.GetToken()[:3]+"[REDACTED]")
	a.logger.Debug("expires at: %s", out.GetExpiration())
	a.token = out.GetToken()

	return nil
}

func (a *akeylessSecretStore) separateItemsByType(items []akeyless.Item) ([]akeyless.Item, []akeyless.Item, []akeyless.Item) {
	staticItems := []akeyless.Item{}
	dynamicItems := []akeyless.Item{}
	rotatedItems := []akeyless.Item{}
	for _, item := range items {
		itemType := *item.ItemType

		switch itemType {
		case AKEYLESS_SECRET_TYPE_STATIC_SECRET_RESPONSE:
			staticItems = append(staticItems, item)
		case AKEYLESS_SECRET_TYPE_DYNAMIC_SECRET_RESPONSE:
			dynamicItems = append(dynamicItems, item)
		case AKEYLESS_SECRET_TYPE_ROTATED_SECRET_RESPONSE:
			rotatedItems = append(rotatedItems, item)
		}
	}
	return staticItems, dynamicItems, rotatedItems
}
