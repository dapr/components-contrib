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

package akeyless

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"reflect"
	"strings"
	"sync"
	"time"

	aws "github.com/akeylesslabs/akeyless-go-cloud-id/cloudprovider/aws"
	akeylesssdk "github.com/akeylesslabs/akeyless-go/v5"

	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/secretstores"
	"github.com/dapr/kit/logger"
	kitmd "github.com/dapr/kit/metadata"
)

var _ secretstores.SecretStore = (*akeylessSecretStore)(nil)

// akeylessSecretStore is a secret store implementation for Akeyless.
type akeylessSecretStore struct {
	v2          *akeylesssdk.V2ApiService
	token       string
	tokenExpiry time.Time
	metadata    *akeylessMetadata
	mu          sync.RWMutex
	logger      logger.Logger
}

// NewAkeylessSecretStore returns a new Akeyless secret store.
func NewAkeylessSecretStore(logger logger.Logger) secretstores.SecretStore {
	return &akeylessSecretStore{
		logger: logger,
	}
}

// akeylessK8sAuth contains Kubernetes authentication settings.
type akeylessK8sAuth struct {
	AuthConfigName      string `json:"authConfigName" mapstructure:"k8sAuthConfigName"`
	GatewayURL          string `json:"gatewayUrl" mapstructure:"k8sGatewayUrl"`
	ServiceAccountToken string `json:"serviceAccountToken" mapstructure:"k8sServiceAccountToken"`
}

// akeylessMetadata contains the metadata for the Akeyless secret store.
type akeylessMetadata struct {
	GatewayURL   string `json:"gatewayUrl" mapstructure:"gatewayUrl"`
	GatewayTLSCa string `json:"gatewayTlsCa" mapstructure:"gatewayTlsCa"`
	AccessID     string `json:"accessId" mapstructure:"accessId"`
	AccessKey    string `json:"accessKey" mapstructure:"accessKey"`
	JWT          string `json:"jwt" mapstructure:"jwt"`
	K8s          akeylessK8sAuth `json:"k8s" mapstructure:",squash"`
}

// Init creates a new Akeyless secret store client and sets up the Akeyless API client
// with authentication method based on the accessId.
func (a *akeylessSecretStore) Init(ctx context.Context, meta secretstores.Metadata) error {
	a.logger.Info("Initializing Akeyless secret store...")
	m, err := a.parseMetadata(meta)
	if err != nil {
		return errors.New("failed to parse metadata: " + err.Error())
	}

	a.metadata = m

	err = a.authenticate(ctx, m)
	if err != nil {
		return errors.New("failed to authenticate with Akeyless: " + err.Error())
	}

	return nil
}

// Authenticate authenticates with Akeyless using the provided metadata.
// It returns an error if the authentication fails.
func (a *akeylessSecretStore) authenticate(ctx context.Context, metadata *akeylessMetadata) error {
	a.logger.Debug("Creating authentication request to Akeyless...")
	authRequest := akeylesssdk.NewAuth()
	authRequest.SetAccessId(metadata.AccessID)

	// Get the authentication method
	a.logger.Debug("extracting access type from accessId...")
	accessTypeChar, err := extractAccessTypeChar(metadata.AccessID)
	if err != nil {
		return errors.New("unable to extract access type character from accessId, expected format is p-([A-Za-z0-9]{14}|[A-Za-z0-9]{12})")
	}

	a.logger.Debugf("getting access type display name for character '%s'...", accessTypeChar)
	accessType, err := getAccessTypeDisplayName(accessTypeChar)
	if err != nil {
		return errors.New("unable to get access type from character '" + accessTypeChar + "': " + err.Error())
	}

	a.logger.Debugf("authenticating using access type '%s'", accessType)

	// Depending on the access type we set the appropriate authentication method
	switch accessType {
	case AuthDefault:
		if metadata.AccessKey == "" {
			return errors.New("accessKey is required for API key authentication")
		}
		authRequest.SetAccessKey(metadata.AccessKey)
	case AuthIAM:
		authRequest.SetAccessType(AuthIAM)
		cloudID, cloudErr := aws.GetCloudId()
		if cloudErr != nil {
			return errors.New("unable to get cloud ID: " + cloudErr.Error())
		}
		authRequest.SetCloudId(cloudID)
	case AuthJWT:
		authRequest.SetAccessType(AuthJWT)
		if metadata.JWT == "" {
			return errors.New("jwt is required for JWT authentication")
		}
		authRequest.SetJwt(metadata.JWT)
	case AuthK8S:
		authRequest.SetAccessType(AuthK8S)
		if k8sErr := setK8SAuthConfiguration(*metadata, authRequest, a); k8sErr != nil {
			return errors.New("failed to set k8s auth configuration: " + k8sErr.Error())
		}
	}

	// Create Akeyless API client configuration
	a.logger.Debug("creating Akeyless API client configuration...")
	config := akeylesssdk.NewConfiguration()
	config.Servers = []akeylesssdk.ServerConfiguration{
		{
			URL: metadata.GatewayURL,
		},
	}
	config.UserAgent = UserAgent
	config.AddDefaultHeader(ClientSource, UserAgent)

	// Configure TLS if gatewayTlsCa is provided
	if metadata.GatewayTLSCa != "" {
		a.logger.Debug("configuring TLS for Akeyless client...")
		tlsConfig, tlsErr := createTLSConfig(metadata.GatewayTLSCa)
		if tlsErr != nil {
			return errors.New("failed to create TLS configuration: " + tlsErr.Error())
		}

		httpClient := &http.Client{
			Timeout: 30 * time.Second,
			Transport: &http.Transport{
				TLSClientConfig: tlsConfig,
			},
		}
		config.HTTPClient = httpClient
	}

	a.v2 = akeylesssdk.NewAPIClient(config).V2Api

	a.logger.Debug("authenticating with Akeyless...")
	out, httpResponse, err := a.v2.Auth(ctx).Body(*authRequest).Execute()
	if httpResponse != nil && httpResponse.Body != nil {
		defer httpResponse.Body.Close()
	}
	if err != nil {
		if httpResponse != nil {
			return fmt.Errorf("failed to authenticate with Akeyless (HTTP status code: %d): %w", httpResponse.StatusCode, err)
		}
		return fmt.Errorf("failed to authenticate with Akeyless: %w", err)
	}
	if httpResponse == nil || httpResponse.StatusCode != http.StatusOK {
		statusCode := 0
		status := "unknown"
		if httpResponse != nil {
			statusCode = httpResponse.StatusCode
			status = httpResponse.Status
		}
		return fmt.Errorf("failed to authenticate with Akeyless (HTTP status code: %d): %s", statusCode, status)
	}
	if out != nil && out.GetToken() == "" {
		return errors.New("authentication failed, no token returned")
	}
	if out != nil && out.GetExpiration() == "" {
		return errors.New("authentication failed, no expiration time returned")
	}

	a.logger.Debugf("authentication successful - token expires at %s", out.GetExpiration())

	// Store token and expiration with mutex protection
	a.mu.Lock()
	a.token = out.GetToken()
	expirationStr := out.GetExpiration()
	a.mu.Unlock()

	// Parse and store expiration time
	if expirationStr != "" {
		expiration, err := parseTokenExpirationDate(expirationStr)
		if err != nil {
			a.logger.Warnf("failed to parse token expiration '%s': %v", expirationStr, err)
		} else {
			a.mu.Lock()
			a.tokenExpiry = expiration
			a.mu.Unlock()
			a.logger.Debugf("token expiration parsed and set successfully: %s", expiration.Format(time.RFC3339))
		}
	}

	return nil
}

// GetSecret retrieves a secret using a key and returns a map of decrypted string/string values.
func (a *akeylessSecretStore) GetSecret(ctx context.Context, req secretstores.GetSecretRequest) (secretstores.GetSecretResponse, error) {
	if a.v2 == nil {
		return secretstores.GetSecretResponse{}, errors.New("akeyless client not initialized")
	}

	a.logger.Debugf("getting secret type for '%s'...", req.Name)
	secretType, err := a.getSecretType(ctx, req.Name)
	if err != nil {
		return secretstores.GetSecretResponse{}, errors.New("failed to get secret type: " + err.Error())
	}

	a.logger.Debugf("getting secret value for '%s' (type %s)...", req.Name, secretType)

	secretValue, err := a.getSingleSecretValue(ctx, req.Name, secretType)
	if err != nil {
		return secretstores.GetSecretResponse{}, errors.New(err.Error())
	}
	a.logger.Debugf("successfully retrieved secret '%s'", req.Name)

	return getDaprSingleSecretResponse(req.Name, secretValue)
}

// BulkGetSecret retrieves all secrets in the store and returns a map of decrypted string/string values.
// The method performs the following steps:
// 1. Recursively list all items in Akeyless
// 2. Filter out inactive/failing secrets
// 3. Separate items by type since only static secrets are supported for bulk get
// 4. Get secret values concurrently, each item type in a separate goroutine
func (a *akeylessSecretStore) BulkGetSecret(ctx context.Context, req secretstores.BulkGetSecretRequest) (secretstores.BulkGetSecretResponse, error) {
	if a.v2 == nil {
		return secretstores.BulkGetSecretResponse{}, errors.New("akeyless client not initialized")
	}

	// initialize response
	response := secretstores.BulkGetSecretResponse{
		Data: make(map[string]map[string]string),
	}

	// get secrets path to retrieve secrets from
	// use root path if not specified
	var secretsPath string
	if value, ok := req.Metadata[MetadataPathKey]; ok {
		// normalize path
		if !strings.HasPrefix(value, "/") {
			secretsPath = "/" + value
		}

		a.logger.Debugf("using path '%s' from metadata...", secretsPath)
	} else {
		a.logger.Debugf("no path found in metadata, using default path '%s'", PathDefault)
		secretsPath = PathDefault
	}

	// get secrets type to retrieve secrets from
	// use all types if not specified
	var requestedTypes []string
	if value, ok := req.Metadata[MetadataSecretsTypeKey]; ok {
		parsedTypes, err := parseSecretTypes(value)
		if err != nil {
			return response, fmt.Errorf("invalid secrets_type metadata: %w", err)
		}
		requestedTypes = parsedTypes
		a.logger.Debugf("using secrets types '%v' from metadata...", requestedTypes)
	} else {
		a.logger.Debugf("no '%s' found in metadata, using all supported secret types '%v'", MetadataSecretsTypeKey, supportedSecretTypes)
		requestedTypes = supportedSecretTypes
	}

	// For bulk get, we need to list all secrets first
	a.logger.Debugf("listing items from '%s' path with types '%v'...", secretsPath, requestedTypes)
	listItems, err := a.listItemsRecursively(ctx, secretsPath, requestedTypes)
	if err != nil {
		return response, fmt.Errorf("failed to list items from Akeyless: %w", err)
	}

	// if no items returned, return empty response
	if len(listItems) == 0 {
		a.logger.Debug("no items returned from / path")
		return response, nil
	}

	// filter out inactive secrets
	a.logger.Debugf("%d items before filtering out inactive secrets", len(listItems))
	listItems = a.filterInactiveSecrets(listItems)
	a.logger.Debugf("%d items remaining after filtering out inactive secrets", len(listItems))

	// separate items by type since only static secrets are supported for bulk get
	staticItemNames, dynamicItemNames, rotatedItemNames := a.separateItemsByType(listItems)
	a.logger.Infof("%d items returned (static: %d, dynamic: %d, rotated: %d)", len(listItems), len(staticItemNames), len(dynamicItemNames), len(rotatedItemNames))

	haveStaticItems := len(staticItemNames) > 0
	haveDynamicItems := len(dynamicItemNames) > 0
	haveRotatedItems := len(rotatedItemNames) > 0

	secretResultChannels := make(chan secretResultCollection, len(listItems))

	// get secret values concurrently, each item type in a separate goroutine
	wg := sync.WaitGroup{}
	if haveStaticItems {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if len(staticItemNames) == 1 {
				staticSecretName := staticItemNames[0]
				value, err := a.getSingleSecretValue(ctx, staticSecretName, StaticSecretResponse)
				if err != nil {
					secretResultChannels <- secretResultCollection{name: staticSecretName, value: "", err: err}
				} else {
					secretResultChannels <- secretResultCollection{name: staticSecretName, value: value, err: nil}
				}
			} else {
				secretResponse := a.getBulkStaticSecretValues(ctx, staticItemNames)
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
				value, err := a.getSingleSecretValue(ctx, item, DynamicSecretResponse)
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
				value, err := a.getSingleSecretValue(ctx, item, RotatedSecretResponse)
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
			a.logger.Errorf("error getting secret '%s': %s. Skipping...", result.name, result.err.Error())
			continue
		}

		response.Data[result.name] = map[string]string{result.name: result.value}
	}

	// Use the new BulkGetSecretResponse function to handle all secret types properly
	// return BulkGetSecretResponse(ctx, itemsList.Items, a)
	return response, nil
}

// Features returns the features available in this secret store.
func (a *akeylessSecretStore) Features() []secretstores.Feature {
	return []secretstores.Feature{}
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

	if !isValidAccessIDFormat(m.AccessID) {
		return nil, errors.New("invalid accessId format, expected format is p-([A-Za-z0-9]{14}|[A-Za-z0-9]{12})")
	}

	// Set default gateway URL if not specified
	if m.GatewayURL == "" {
		a.logger.Infof("Gateway URL is not set, using default value %s...", PublicGatewayURL)
		m.GatewayURL = PublicGatewayURL
	} else {
		_, err = url.ParseRequestURI(m.GatewayURL)
		if err != nil {
			return nil, fmt.Errorf("invalid gateway URL '%s': %w", m.GatewayURL, err)
		}
	}

	// Trim trailing slash from gateway URL
	m.GatewayURL = strings.TrimSuffix(m.GatewayURL, "/")

	return &m, nil
}

func (a *akeylessSecretStore) getSecretType(ctx context.Context, secretName string) (string, error) {
	if err := a.ensureValidToken(ctx); err != nil {
		return "", fmt.Errorf("failed to ensure valid token: %w", err)
	}

	describeItem := akeylesssdk.NewDescribeItem(secretName)

	a.mu.RLock()
	token := a.token
	a.mu.RUnlock()

	describeItem.SetToken(token)

	describeItemResp, httpResponse, err := a.v2.DescribeItem(ctx).Body(*describeItem).Execute()
	if httpResponse != nil && httpResponse.Body != nil {
		defer httpResponse.Body.Close()
	}

	if err != nil {
		return "", fmt.Errorf("failed to describe item '%s': %w", secretName, err)
	}

	if describeItemResp.ItemType == nil {
		return "", errors.New("unable to retrieve secret type, missing type in describe item response")
	}

	return *describeItemResp.ItemType, nil
}

// getSingleSecretValue gets the value of a single secret from Akeyless.
// It returns the value of the secret or an error if the secret is not found.
func (a *akeylessSecretStore) getSingleSecretValue(ctx context.Context, secretName string, secretType string) (string, error) {
	if err := a.ensureValidToken(ctx); err != nil {
		return "", fmt.Errorf("failed to ensure valid token: %w", err)
	}

	var secretValue string
	var err error

	a.mu.RLock()
	token := a.token
	a.mu.RUnlock()

	switch secretType {
	case StaticSecretResponse:
		getSecretValue := akeylesssdk.NewGetSecretValue([]string{secretName})
		getSecretValue.SetToken(token)

		secretRespMap, httpResponse, apiErr := a.v2.GetSecretValue(ctx).Body(*getSecretValue).Execute()
		if httpResponse != nil && httpResponse.Body != nil {
			defer httpResponse.Body.Close()
		}

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

	case DynamicSecretResponse:
		getDynamicSecretValue := akeylesssdk.NewGetDynamicSecretValue(secretName)
		getDynamicSecretValue.SetToken(token)

		secretRespMap, httpResponse, apiErr := a.v2.GetDynamicSecretValue(ctx).Body(*getDynamicSecretValue).Execute()
		if httpResponse != nil && httpResponse.Body != nil {
			defer httpResponse.Body.Close()
		}

		if apiErr != nil {
			err = fmt.Errorf("failed to get dynamic secret '%s' value from Akeyless API: %w", secretName, apiErr)
			break
		}

		// Parse response to extract value and check for errors
		var dynamicSecretResp struct {
			Value string `json:"value"`
			Error string `json:"error"`
		}
		jsonBytes, marshalErr := json.Marshal(secretRespMap)
		if marshalErr != nil {
			err = fmt.Errorf("failed to marshal secret response to JSON: %w", marshalErr)
			break
		}
		if unmarshalErr := json.Unmarshal(jsonBytes, &dynamicSecretResp); unmarshalErr != nil {
			err = fmt.Errorf("failed to unmarshal secret response: %w", unmarshalErr)
			break
		}

		// Check if the response contains an error
		if dynamicSecretResp.Error != "" {
			err = fmt.Errorf("dynamic secret retrieval error: %s", dynamicSecretResp.Error)
			break
		}

		// Return the value field directly (already a JSON string with credentials)
		secretValue = dynamicSecretResp.Value

	case RotatedSecretResponse:
		getRotatedSecretValue := akeylesssdk.NewGetRotatedSecretValue(secretName)
		getRotatedSecretValue.SetToken(token)

		secretRespMap, httpResponse, apiErr := a.v2.GetRotatedSecretValue(ctx).Body(*getRotatedSecretValue).Execute()
		if httpResponse != nil && httpResponse.Body != nil {
			defer httpResponse.Body.Close()
		}

		if apiErr != nil {
			err = fmt.Errorf("failed to get rotated secret '%s' value from Akeyless API: %w", secretName, apiErr)
			break
		}

		// Marshal the entire response value object
		jsonBytes, marshalErr := json.Marshal(secretRespMap)
		if marshalErr != nil {
			err = fmt.Errorf("failed to marshal rotated secret response to JSON: %w", marshalErr)
			break
		}
		secretValue = string(jsonBytes)
	}

	return secretValue, err
}

// getBulkStaticSecretValues gets the values of multiple static secrets from Akeyless.
// It returns a map of secret names and their values.
func (a *akeylessSecretStore) getBulkStaticSecretValues(ctx context.Context, secretNames []string) []secretResultCollection {
	if err := a.ensureValidToken(ctx); err != nil {
		return []secretResultCollection{
			{name: "", value: "", err: fmt.Errorf("failed to ensure valid token: %w", err)},
		}
	}

	var secretResponse []secretResultCollection

	getSecretsValues := akeylesssdk.NewGetSecretValue(secretNames)

	a.mu.RLock()
	token := a.token
	a.mu.RUnlock()

	getSecretsValues.SetToken(token)

	secretRespMap, httpResponse, apiErr := a.v2.GetSecretValue(ctx).Body(*getSecretsValues).Execute()
	if httpResponse != nil && httpResponse.Body != nil {
		defer httpResponse.Body.Close()
	}

	if apiErr != nil {
		secretResponse = append(secretResponse, secretResultCollection{
			name: "", value: "", err: fmt.Errorf("failed to get static secrets' '%s' value from Akeyless API: %w", secretNames, apiErr),
		})
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
func (a *akeylessSecretStore) listItemsRecursively(ctx context.Context, path string, types []string) ([]akeylesssdk.Item, error) {
	if err := a.ensureValidToken(ctx); err != nil {
		return nil, fmt.Errorf("failed to ensure valid token: %w", err)
	}

	var allItems []akeylesssdk.Item

	listItems := akeylesssdk.NewListItems()

	a.mu.RLock()
	token := a.token
	a.mu.RUnlock()

	listItems.SetToken(token)
	listItems.SetPath(path)
	listItems.SetAutoPagination("enabled")
	listItems.SetType(types)

	a.logger.Debugf("listing items from path '%s'...", path)
	itemsList, httpResponse, err := a.v2.ListItems(ctx).Body(*listItems).Execute()
	if httpResponse != nil && httpResponse.Body != nil {
		defer httpResponse.Body.Close()
	}

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
			subItems, err := a.listItemsRecursively(ctx, folder, types)
			if err != nil {
				return nil, err
			}
			allItems = append(allItems, subItems...)
		}
	}

	return allItems, nil
}

func (a *akeylessSecretStore) separateItemsByType(items []akeylesssdk.Item) ([]string, []string, []string) {
	var staticItems []akeylesssdk.Item
	var dynamicItems []akeylesssdk.Item
	var rotatedItems []akeylesssdk.Item
	for _, item := range items {
		itemType := *item.ItemType

		switch itemType {
		case StaticSecretResponse:
			staticItems = append(staticItems, item)
		case DynamicSecretResponse:
			dynamicItems = append(dynamicItems, item)
		case RotatedSecretResponse:
			rotatedItems = append(rotatedItems, item)
		}
	}

	// listItems can get quite large, so we don't need all item details, we can use the item names instead
	// and free memory
	staticItemNames := getItemNames(staticItems)
	dynamicItemNames := getItemNames(dynamicItems)
	rotatedItemNames := getItemNames(rotatedItems)
	a.logger.Debugf("static items: %v", staticItemNames)
	a.logger.Debugf("dynamic items: %v", dynamicItemNames)
	a.logger.Debugf("rotated items: %v", rotatedItemNames)

	return staticItemNames, dynamicItemNames, rotatedItemNames
}

func (a *akeylessSecretStore) filterInactiveSecrets(secrets []akeylesssdk.Item) []akeylesssdk.Item {
	filteredSecrets := []akeylesssdk.Item{}

	for _, secret := range secrets {
		if isSecretActive(secret, a.logger) {
			filteredSecrets = append(filteredSecrets, secret)
		}
	}

	return filteredSecrets
}

// ensureValidToken reuses the cached token while it remains valid. If the token
// expires within TokenRefreshGracePeriod, it refreshes the token before returning.
func (a *akeylessSecretStore) ensureValidToken(ctx context.Context) error {
	a.mu.RLock()
	expiry := a.tokenExpiry
	metadata := a.metadata
	a.mu.RUnlock()

	if expiry.IsZero() {
		a.logger.Debug("token expiration not set, skipping validation")
		return nil
	}

	if time.Now().Before(expiry.Add(-TokenRefreshGracePeriod)) {
		return nil
	}

	a.logger.Debug("token expired or expiring soon, refreshing...")
	return a.authenticate(ctx, metadata)
}

func (a *akeylessSecretStore) GetComponentMetadata() (metadataInfo metadata.MetadataMap) {
	metadataStruct := akeylessMetadata{}
	metadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, metadata.SecretStoreType)
	return
}
