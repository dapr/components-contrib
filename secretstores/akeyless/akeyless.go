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
	"github.com/akeylesslabs/akeyless-go/v5"

	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/secretstores"
	"github.com/dapr/kit/logger"
	kitmd "github.com/dapr/kit/metadata"
)

var _ secretstores.SecretStore = (*akeylessSecretStore)(nil)

// akeylessSecretStore is a secret store implementation for Akeyless.
type akeylessSecretStore struct {
	v2          *akeyless.V2ApiService
	token       string
	tokenExpiry time.Time
	metadata    *akeylessMetadata
	mu          sync.RWMutex
	logger      logger.Logger
	closeCh     chan struct{}
	wg          sync.WaitGroup
}

// NewAkeylessSecretStore returns a new Akeyless secret store.
func NewAkeylessSecretStore(logger logger.Logger) secretstores.SecretStore {
	return &akeylessSecretStore{
		logger: logger,
	}
}

// akeylessMetadata contains the metadata for the Akeyless secret store.
type akeylessMetadata struct {
	GatewayURL             string `json:"gatewayUrl" mapstructure:"gatewayUrl"`
	GatewayTlsCa           string `json:"gatewayTlsCa" mapstructure:"gatewayTlsCa"`
	JWT                    string `json:"jwt" mapstructure:"jwt"`
	AccessID               string `json:"accessId" mapstructure:"accessId"`
	AccessKey              string `json:"accessKey" mapstructure:"accessKey"`
	K8SGatewayURL          string `json:"k8sGatewayUrl" mapstructure:"k8sGatewayUrl"`
	K8SAuthConfigName      string `json:"k8sAuthConfigName" mapstructure:"k8sAuthConfigName"`
	K8sServiceAccountToken string `json:"k8sServiceAccountToken" mapstructure:"k8sServiceAccountToken"`
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
	a.closeCh = make(chan struct{})

	err = a.authenticate(ctx, m)
	if err != nil {
		return errors.New("failed to authenticate with Akeyless: " + err.Error())
	}

	// Start background token refresh routine if we have expiration time
	if !a.tokenExpiry.IsZero() {
		a.startTokenRefreshRoutine(ctx, m)
	}

	return nil
}

// Authenticate authenticates with Akeyless using the provided metadata.
// It returns an error if the authentication fails.
func (a *akeylessSecretStore) authenticate(ctx context.Context, metadata *akeylessMetadata) error {

	a.logger.Debug("Creating authentication request to Akeyless...")
	authRequest := akeyless.NewAuth()
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
	case AUTH_DEFAULT:
		if metadata.AccessKey == "" {
			return errors.New("accessKey is required for API key authentication")
		}
		authRequest.SetAccessKey(metadata.AccessKey)
	case AUTH_IAM:
		authRequest.SetAccessType(AUTH_IAM)
		id, err := aws.GetCloudId()
		if err != nil {
			return errors.New("unable to get cloud ID: " + err.Error())
		}
		authRequest.SetCloudId(id)
	case AUTH_JWT:
		authRequest.SetAccessType(AUTH_JWT)
		if metadata.JWT == "" {
			return errors.New("jwt is required for JWT authentication")
		}
		authRequest.SetJwt(metadata.JWT)
	case AUTH_K8S:
		authRequest.SetAccessType(AUTH_K8S)
		err := setK8SAuthConfiguration(*metadata, authRequest, a)
		if err != nil {
			return errors.New("failed to set k8s auth configuration: " + err.Error())
		}
	}

	// Create Akeyless API client configuration
	a.logger.Debug("creating Akeyless API client configuration...")
	config := akeyless.NewConfiguration()
	config.Servers = []akeyless.ServerConfiguration{
		{
			URL: metadata.GatewayURL,
		},
	}
	config.UserAgent = USER_AGENT
	config.AddDefaultHeader(CLIENT_SOURCE, USER_AGENT)

	// Configure TLS if gatewayTlsCa is provided
	if metadata.GatewayTlsCa != "" {
		a.logger.Debug("configuring TLS for Akeyless client...")
		tlsConfig, err := createTLSConfig(metadata.GatewayTlsCa)
		if err != nil {
			return errors.New("failed to create TLS configuration: " + err.Error())
		}

		httpClient := &http.Client{
			Timeout: 30 * time.Second,
			Transport: &http.Transport{
				TLSClientConfig: tlsConfig,
			},
		}
		config.HTTPClient = httpClient
	}

	a.v2 = akeyless.NewAPIClient(config).V2Api

	a.logger.Debug("authenticating with Akeyless...")
	out, httpResponse, err := a.v2.Auth(ctx).Body(*authRequest).Execute()
	if err != nil {
		if httpResponse != nil {
			return fmt.Errorf("failed to authenticate with Akeyless (HTTP status code: %d): %w", httpResponse.StatusCode, err)
		}
		return fmt.Errorf("failed to authenticate with Akeyless: %w", err)
	}
	if httpResponse == nil || httpResponse.StatusCode != 200 {
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
	if value, ok := req.Metadata[METADATA_PATH_KEY]; ok {

		// normalize path
		if !strings.HasPrefix(value, "/") {
			secretsPath = "/" + value
		}

		a.logger.Debugf("using path '%s' from metadata...", secretsPath)
	} else {
		a.logger.Debugf("no path found in metadata, using default path '%s'", PATH_DEFAULT)
		secretsPath = PATH_DEFAULT
	}

	// get secrets type to retrieve secrets from
	// use all types if not specified
	var requestedTypes []string
	if value, ok := req.Metadata[METADATA_SECRETS_TYPE_KEY]; ok {
		parsedTypes, err := parseSecretTypes(value)
		if err != nil {
			return response, fmt.Errorf("invalid secrets_type metadata: %w", err)
		}
		requestedTypes = parsedTypes
		a.logger.Debugf("using secrets types '%v' from metadata...", requestedTypes)
	} else {
		a.logger.Debugf("no '%s' found in metadata, using all supported secret types '%v'", METADATA_SECRETS_TYPE_KEY, supportedSecretTypes)
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
				value, err := a.getSingleSecretValue(ctx, staticSecretName, STATIC_SECRET_RESPONSE)
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
				value, err := a.getSingleSecretValue(ctx, item, DYNAMIC_SECRET_RESPONSE)
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
				value, err := a.getSingleSecretValue(ctx, item, ROTATED_SECRET_RESPONSE)
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
	if a.closeCh != nil {
		close(a.closeCh)
		a.wg.Wait()
	}
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

	if !isValidAccessIdFormat(m.AccessID) {
		return nil, errors.New("invalid accessId format, expected format is p-([A-Za-z0-9]{14}|[A-Za-z0-9]{12})")
	}

	// Set default gateway URL if not specified
	if m.GatewayURL == "" {
		a.logger.Infof("Gateway URL is not set, using default value %s...", PUBLIC_GATEWAY_URL)
		m.GatewayURL = PUBLIC_GATEWAY_URL
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

	describeItem := akeyless.NewDescribeItem(secretName)

	a.mu.RLock()
	token := a.token
	a.mu.RUnlock()

	describeItem.SetToken(token)
	describeItemResp, httpResponse, err := a.v2.DescribeItem(ctx).Body(*describeItem).Execute()

	if httpResponse != nil && httpResponse.StatusCode == http.StatusUnauthorized {
		a.logger.Debug("received 401 unauthorized, re-authenticating...")
		if err := a.ensureValidToken(ctx); err != nil {
			return "", fmt.Errorf("failed to re-authenticate after 401: %w", err)
		}

		a.mu.RLock()
		token = a.token
		a.mu.RUnlock()

		describeItem.SetToken(token)
		describeItemResp, _, err = a.v2.DescribeItem(ctx).Body(*describeItem).Execute()
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

	// Helper to get current token (with mutex protection)
	getToken := func() string {
		a.mu.RLock()
		defer a.mu.RUnlock()
		return a.token
	}

	retry := func(apiCall func() error, updateToken func(string)) error {
		apiErr := apiCall()
		if apiErr != nil {
			// Check if it's a 401 error by examining the error string or response
			if strings.Contains(apiErr.Error(), "401") || strings.Contains(apiErr.Error(), "Unauthorized") {
				a.logger.Debug("received 401 unauthorized, re-authenticating...")
				if reauthErr := a.ensureValidToken(ctx); reauthErr != nil {
					return fmt.Errorf("failed to re-authenticate after 401: %w", reauthErr)
				}
				// Update token in the request object before retry
				newToken := getToken()
				updateToken(newToken)
				return apiCall()
			}
		}
		return apiErr
	}

	switch secretType {
	case STATIC_SECRET_RESPONSE:
		getSecretValue := akeyless.NewGetSecretValue([]string{secretName})
		getSecretValue.SetToken(token)
		var secretRespMap map[string]interface{}
		var httpResponse *http.Response

		apiErr := retry(func() error {
			var err error
			secretRespMap, httpResponse, err = a.v2.GetSecretValue(ctx).Body(*getSecretValue).Execute()
			if httpResponse != nil && httpResponse.StatusCode == http.StatusUnauthorized {
				return fmt.Errorf("401 Unauthorized")
			}
			return err
		}, func(newToken string) {
			getSecretValue.SetToken(newToken)
		})

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

	case DYNAMIC_SECRET_RESPONSE:
		getDynamicSecretValue := akeyless.NewGetDynamicSecretValue(secretName)
		getDynamicSecretValue.SetToken(token)
		var secretRespMap map[string]interface{}
		var httpResponse *http.Response

		apiErr := retry(func() error {
			var err error
			secretRespMap, httpResponse, err = a.v2.GetDynamicSecretValue(ctx).Body(*getDynamicSecretValue).Execute()
			if httpResponse != nil && httpResponse.StatusCode == http.StatusUnauthorized {
				return fmt.Errorf("401 Unauthorized")
			}
			return err
		}, func(newToken string) {
			getDynamicSecretValue.SetToken(newToken)
		})

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

	case ROTATED_SECRET_RESPONSE:
		getRotatedSecretValue := akeyless.NewGetRotatedSecretValue(secretName)
		getRotatedSecretValue.SetToken(token)
		var secretRespMap map[string]interface{}
		var httpResponse *http.Response

		apiErr := retry(func() error {
			var err error
			secretRespMap, httpResponse, err = a.v2.GetRotatedSecretValue(ctx).Body(*getRotatedSecretValue).Execute()
			if httpResponse != nil && httpResponse.StatusCode == http.StatusUnauthorized {
				return fmt.Errorf("401 Unauthorized")
			}
			return err
		}, func(newToken string) {
			getRotatedSecretValue.SetToken(newToken)
		})

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

	getSecretsValues := akeyless.NewGetSecretValue(secretNames)

	a.mu.RLock()
	token := a.token
	a.mu.RUnlock()

	getSecretsValues.SetToken(token)

	secretRespMap, httpResponse, apiErr := a.v2.GetSecretValue(ctx).Body(*getSecretsValues).Execute()

	// Handle 401 Unauthorized by re-authenticating and retrying once
	if httpResponse != nil && httpResponse.StatusCode == http.StatusUnauthorized {
		a.logger.Debug("received 401 Unauthorized in bulk get, re-authenticating...")
		if err := a.ensureValidToken(ctx); err != nil {
			secretResponse = append(secretResponse, secretResultCollection{
				name: "", value: "", err: fmt.Errorf("failed to re-authenticate after 401: %w", err),
			})
			return secretResponse
		}

		a.mu.RLock()
		token = a.token
		a.mu.RUnlock()

		getSecretsValues.SetToken(token)
		secretRespMap, _, apiErr = a.v2.GetSecretValue(ctx).Body(*getSecretsValues).Execute()
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
func (a *akeylessSecretStore) listItemsRecursively(ctx context.Context, path string, types []string) ([]akeyless.Item, error) {
	if err := a.ensureValidToken(ctx); err != nil {
		return nil, fmt.Errorf("failed to ensure valid token: %w", err)
	}

	var allItems []akeyless.Item

	// Create the list items request
	listItems := akeyless.NewListItems()

	a.mu.RLock()
	token := a.token
	a.mu.RUnlock()

	listItems.SetToken(token)
	listItems.SetPath(path)
	listItems.SetAutoPagination("enabled")
	listItems.SetType(types)

	// Execute the list items request
	a.logger.Debugf("listing items from path '%s'...", path)
	itemsList, httpResponse, err := a.v2.ListItems(ctx).Body(*listItems).Execute()

	// Handle 401 Unauthorized by re-authenticating and retrying once
	// Check this BEFORE checking err, as 401 might come with an error
	if httpResponse != nil && httpResponse.StatusCode == http.StatusUnauthorized {
		a.logger.Debug("received 401 Unauthorized in list items, re-authenticating...")
		if err := a.ensureValidToken(ctx); err != nil {
			return nil, fmt.Errorf("failed to re-authenticate after 401: %w", err)
		}

		a.mu.RLock()
		token = a.token
		a.mu.RUnlock()

		listItems.SetToken(token)
		itemsList, _, err = a.v2.ListItems(ctx).Body(*listItems).Execute()
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

func (a *akeylessSecretStore) separateItemsByType(items []akeyless.Item) ([]string, []string, []string) {
	var staticItems []akeyless.Item
	var dynamicItems []akeyless.Item
	var rotatedItems []akeyless.Item
	for _, item := range items {
		itemType := *item.ItemType

		switch itemType {
		case STATIC_SECRET_RESPONSE:
			staticItems = append(staticItems, item)
		case DYNAMIC_SECRET_RESPONSE:
			dynamicItems = append(dynamicItems, item)
		case ROTATED_SECRET_RESPONSE:
			rotatedItems = append(rotatedItems, item)
		}
	}

	// listItems can get quite large, so we don't need all item details, we can use the item names instead
	// and free memory
	items = nil
	staticItemNames := getItemNames(staticItems)
	dynamicItemNames := getItemNames(dynamicItems)
	rotatedItemNames := getItemNames(rotatedItems)
	a.logger.Debugf("static items: %v", staticItemNames)
	a.logger.Debugf("dynamic items: %v", dynamicItemNames)
	a.logger.Debugf("rotated items: %v", rotatedItemNames)

	return staticItemNames, dynamicItemNames, rotatedItemNames
}

func (a *akeylessSecretStore) filterInactiveSecrets(secrets []akeyless.Item) []akeyless.Item {

	filteredSecrets := []akeyless.Item{}

	for _, secret := range secrets {
		if isSecretActive(secret, a.logger) {
			filteredSecrets = append(filteredSecrets, secret)
		}
	}

	return filteredSecrets
}

// ensureValidToken checks if the token is valid and refreshes it if needed (5 minutes before expiration)
// It returns an error if the token refresh fails.
func (a *akeylessSecretStore) ensureValidToken(ctx context.Context) error {

	a.mu.RLock()
	expiry := a.tokenExpiry
	metadata := a.metadata
	a.mu.RUnlock()

	// If token expiry is zero, we can't validate it, so skip validation
	// This can happen if expiration parsing failed or wasn't provided
	if expiry.IsZero() {
		a.logger.Debug("token expiration not set, skipping validation")
		return nil
	}

	tokenValid := time.Now().Before(expiry.Add(-TOKEN_REFRESH_GRACE_PERIOD))
	if tokenValid {
		return nil
	}

	// Token expired or about to expire, need to refresh/reauthenticate
	a.logger.Debug("token expired or about to expire, reauthenticating...")
	a.mu.Lock()
	defer a.mu.Unlock()

	// Double-check after acquiring lock (another goroutine might have refreshed)
	expiry = a.tokenExpiry
	if expiry.IsZero() || time.Now().Before(expiry.Add(-TOKEN_REFRESH_GRACE_PERIOD)) {
		return nil
	}

	return a.authenticate(ctx, metadata)
}

// startTokenRefreshRoutine starts a bg goroutine that refreshes the token
func (a *akeylessSecretStore) startTokenRefreshRoutine(ctx context.Context, metadata *akeylessMetadata) {
	a.wg.Add(1)
	go func() {
		defer a.wg.Done()
		// Use background context for the refresh routine, not the init context
		refreshCtx := context.Background()

		for {
			// Check if we should stop first, before acquiring any locks
			select {
			case <-a.closeCh:
				a.logger.Debug("token refresh routine stopped")
				return
			default:
			}

			a.mu.RLock()
			expiry := a.tokenExpiry
			a.mu.RUnlock()

			if expiry.IsZero() {
				a.logger.Warn("token expiration is zero, stopping refresh routine...")
				return
			}

			refreshDuration := time.Until(expiry.Add(-TOKEN_REFRESH_GRACE_PERIOD))
			if refreshDuration <= 0 {
				refreshDuration = time.Minute // Refresh immediately if less than 1 minute left
			}

			a.logger.Debugf("next token refresh scheduled in %v", refreshDuration)

			select {
			case <-time.After(refreshDuration):
				a.logger.Debug("refreshing token...")
				if err := a.authenticate(refreshCtx, metadata); err != nil {
					a.logger.Errorf("failed to refresh token: %v", err)
					// Retry after 1 minute on failure
					time.Sleep(time.Minute)
					continue
				}
				a.logger.Debug("token refreshed successfully")
			case <-a.closeCh:
				a.logger.Debug("token refresh routine stopped")
				return
			}
		}
	}()
}

func (a *akeylessSecretStore) GetComponentMetadata() (metadataInfo metadata.MetadataMap) {
	metadataStruct := akeylessMetadata{}
	metadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, metadata.SecretStoreType)
	return
}
