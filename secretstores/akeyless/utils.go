package akeyless

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"

	"encoding/json"

	aws "github.com/akeylesslabs/akeyless-go-cloud-id/cloudprovider/aws"
	"github.com/akeylesslabs/akeyless-go/v5"
	"github.com/dapr/components-contrib/secretstores"
)

// Define constants for the access types. These are equivalent to the TypeScript consts.
const (
	AKEYLESS_AUTH_ACCESS_JWT                     = "jwt"
	AKEYLESS_AUTH_DEFAULT_ACCESS_TYPE            = "access_key"
	AKEYLESS_AUTH_ACCESS_IAM                     = "aws_iam"
	AKEYLESS_PUBLIC_GATEWAY_URL                  = "https://api.akeyless.io"
	AKEYLESS_USER_AGENT                          = "dapr.io/akeyless-secret-store"
	AKEYLESS_SECRET_TYPE_STATIC                  = "static-secret"
	AKEYLESS_SECRET_TYPE_DYNAMIC                 = "dynamic-secret"
	AKEYLESS_SECRET_TYPE_ROTATED                 = "rotated-secret"
	AKEYLESS_SECRET_TYPE_STATIC_SECRET_RESPONSE  = "STATIC_SECRET"
	AKEYLESS_SECRET_TYPE_DYNAMIC_SECRET_RESPONSE = "DYNAMIC_SECRET"
	AKEYLESS_SECRET_TYPE_ROTATED_SECRET_RESPONSE = "ROTATED_SECRET"
)

// AccessTypeCharMap maps single-character access types to their display names.
var AccessTypeCharMap = map[string]string{
	"a": AKEYLESS_AUTH_DEFAULT_ACCESS_TYPE,
	"o": AKEYLESS_AUTH_ACCESS_JWT,
	"w": AKEYLESS_AUTH_ACCESS_IAM,
}

// AccessIdRegex is the compiled regular expression for validating Akeyless Access IDs.
var AccessIdRegex = regexp.MustCompile(`^p-([A-Za-z0-9]{14}|[A-Za-z0-9]{12})$`)

// isValidAccessIdFormat validates the format of an Akeyless Access ID.
// The format is p-([A-Za-z0-9]{14}|[A-Za-z0-9]{12}).
// It returns true if the format is valid, and false otherwise.
func IsValidAccessIdFormat(accessId string) bool {
	return AccessIdRegex.MatchString(accessId)
}

// extractAccessTypeChar extracts the Akeyless Access Type character from a valid Access ID.
// The access type character is the second to last character of the ID part.
// It returns the single-character access type (e.g., 'a', 'o') or an empty string and an error if the format is invalid.
func ExtractAccessTypeChar(accessId string) (string, error) {
	if !IsValidAccessIdFormat(accessId) {
		return "", errors.New("invalid access ID format")
	}
	parts := strings.Split(accessId, "-")
	idPart := parts[1] // Get the part after "p-"
	// The access type char is the second-to-last character
	return string(idPart[len(idPart)-2]), nil
}

// validateAccessTypeChar validates the extracted access type character against a list of allowed types.
// It returns true if the extracted access type is in the allowed list, false otherwise.
func ValidateAccessTypeChar(accessId string, allowedTypes []string) bool {
	typeChar, err := ExtractAccessTypeChar(accessId)
	if err != nil {
		return false // Invalid ID format
	}

	for _, allowedType := range allowedTypes {
		if typeChar == allowedType {
			return true
		}
	}
	return false
}

// getAccessTypeDisplayName gets the full display name of the access type from the character.
// It returns the display name (e.g., 'api_key') or an error if the type character is unknown.
func GetAccessTypeDisplayName(typeChar string) (string, error) {
	if typeChar == "" {
		return "", errors.New("unable to retrieve access type, missing type char")
	}
	displayName, ok := AccessTypeCharMap[typeChar]
	if !ok {
		return "Unknown", errors.New("access type character not found in map")
	}
	return displayName, nil
}

// Authenticate authenticates with Akeyless using the provided metadata.
// It returns an error if the authentication fails.
func Authenticate(metadata *akeylessMetadata, akeylessSecretStore *akeylessSecretStore) error {

	akeylessSecretStore.logger.Debug("Creating authentication request to Akeyless...")
	authRequest := akeyless.NewAuth()
	authRequest.SetAccessId(metadata.AccessID)
	authRequest.SetAccessType(metadata.AccessType)

	// Depending on the access type we set the appropriate authentication method
	switch metadata.AccessType {
	// If access type is AWS IAM we use the cloud ID
	case AKEYLESS_AUTH_ACCESS_IAM:
		akeylessSecretStore.logger.Debug("getting cloud ID for AWS IAM...")
		id, err := aws.GetCloudId()
		if err != nil {
			return errors.New("unable to get cloud ID")
		}
		authRequest.SetCloudId(id)
	case AKEYLESS_AUTH_ACCESS_JWT:
		akeylessSecretStore.logger.Debug("setting JWT for authentication...")
		authRequest.SetJwt(metadata.JWT)
	case AKEYLESS_AUTH_DEFAULT_ACCESS_TYPE:
		akeylessSecretStore.logger.Debug("setting access key for authentication...")
		authRequest.SetAccessKey(metadata.AccessKey)
	}

	// Create Akeyless API client configuration
	akeylessSecretStore.logger.Debug("creating Akeyless API client configuration...")
	config := akeyless.NewConfiguration()
	config.Servers = []akeyless.ServerConfiguration{
		{
			URL: metadata.GatewayURL,
		},
	}
	config.UserAgent = AKEYLESS_USER_AGENT
	config.AddDefaultHeader("akeylessclienttype", AKEYLESS_USER_AGENT)

	akeylessSecretStore.v2 = akeyless.NewAPIClient(config).V2Api

	akeylessSecretStore.logger.Debug("authenticating with Akeyless...")
	out, _, err := akeylessSecretStore.v2.Auth(context.Background()).Body(*authRequest).Execute()
	if err != nil {
		return fmt.Errorf("failed to authenticate with Akeyless: %w", err)
	}

	akeylessSecretStore.logger.Debug("setting token %s for authentication...", out.GetToken()[:3]+"[REDACTED]")
	akeylessSecretStore.logger.Debug("expires at: %s", out.GetExpiration())
	akeylessSecretStore.token = out.GetToken()

	return nil
}

// getSecretType gets the type of the secret from the describe item response.
// It returns the type of the secret (e.g. static, dynamic, rotated) or an error if the type is unknown.
func GetSecretType(secretName string, akeylessSecretStore *akeylessSecretStore) (string, error) {

	describeItem := akeyless.NewDescribeItem(secretName)
	describeItem.SetToken(akeylessSecretStore.token)
	describeItemResp, _, err := akeylessSecretStore.v2.DescribeItem(context.Background()).Body(*describeItem).Execute()
	if err != nil {
		return "", fmt.Errorf("failed to describe item '%s': %w", secretName, err)
	}

	if describeItemResp.ItemType == nil {
		return "", errors.New("unable to retrieve secret type, missing type in describe item response")
	}

	return *describeItemResp.ItemType, nil
}

func GetSingleSecretValue(secretName string, secretType string, akeylessSecretStore *akeylessSecretStore) (string, error) {

	switch secretType {
	case AKEYLESS_SECRET_TYPE_STATIC_SECRET_RESPONSE:
		getSecretValue := akeyless.NewGetSecretValue([]string{secretName})
		getSecretValue.SetToken(akeylessSecretStore.token)
		secretRespMap, _, err := akeylessSecretStore.v2.GetSecretValue(context.Background()).Body(*getSecretValue).Execute()
		if err != nil {
			return "", fmt.Errorf("failed to get secret '%s' value for static secret from Akeyless API: %w", secretName, err)
		}

		// check if secret key is in response
		value, ok := secretRespMap[secretName]
		if !ok {
			return "", fmt.Errorf("failed to get secret '%s' value for static secret from Akeyless API: key not found", secretName)
		}

		// single static secrets can be of type string, or map[string]string
		// if it's a map[string]string, we need to transform it to a string
		switch valueType := value.(type) {
		case string:
			return valueType, nil
		case map[string]string:
			encoded, err := json.Marshal(valueType)
			if err != nil {
				return "", fmt.Errorf("failed to marshal secret response: %w", err)
			}
			return string(encoded), nil
		case interface{}:
			encoded, err := json.Marshal(valueType)
			if err != nil {
				return "", fmt.Errorf("failed to marshal secret response: %w", err)
			}
			return string(encoded), nil
		default:
			return "", fmt.Errorf("failed to assert type of secret response to string for secret '%s'", secretName)
		}

	// TODO implement dynamic secrets
	case AKEYLESS_SECRET_TYPE_DYNAMIC_SECRET_RESPONSE:
		return "", errors.New("dynamic secrets are not supported")
	// TODO implement rotated secrets
	case AKEYLESS_SECRET_TYPE_ROTATED_SECRET_RESPONSE:
		return "", errors.New("rotated secrets are not supported")
	}

	return "", nil
}

// GetSecretValueByType gets the secret value by the type of the secret.
// It returns the secret value or an error if the secret value is not found.
// If secretName is not a string it means that we're getting numerous static secrets
// and we can get them all at once
func GetSecretValueByType(secretName any, secretType string, akeylessSecretStore *akeylessSecretStore) (secretstores.GetSecretResponse, error) {
	var secretResp secretstores.GetSecretResponse
	switch secretType {
	case AKEYLESS_SECRET_TYPE_STATIC_SECRET_RESPONSE:
		var secrets []string
		switch secretName := secretName.(type) {
		case string:
			secrets = []string{secretName}
		case []string:
			secrets = secretName
		}
		getSecretValue := akeyless.NewGetSecretValue(secrets)
		getSecretValue.SetToken(akeylessSecretStore.token)
		secretRespMap, _, err := akeylessSecretStore.v2.GetSecretValue(context.Background()).Body(*getSecretValue).Execute()
		if err != nil {
			return secretstores.GetSecretResponse{}, fmt.Errorf("failed to get secret '%s' value for static secret from Akeyless API: %w", secretName.(string), err)
		}

		transformedResp, err := transformStaticSecretResponse(secretRespMap)
		if err != nil {
			return secretstores.GetSecretResponse{}, fmt.Errorf("failed to transform static secret response for secret '%s': %w", secretName.(string), err)
		}
		secretResp = transformedResp
	// case AKEYLESS_SECRET_TYPE_DYNAMIC_SECRET_RESPONSE:
	// 	getSecretValue := akeyless.NewGetDynamicSecretValue(secretName.(string))
	// 	getSecretValue.SetToken(akeylessSecretStore.token)
	// 	secretRespMap, _, err := akeylessSecretStore.v2.GetDynamicSecretValue(context.Background()).Body(*getSecretValue).Execute()

	// 	if err != nil {
	// 		return secretstores.GetSecretResponse{}, fmt.Errorf("failed to get dynamic secret '%s' value from Akeyless API: %w", secretName.(string), err)
	// 	}
	// 	// Convert secretRespMap (map[string]interface{}) to DynamicSecretResponse using a helper function
	// 	dynamicSecretResp, err := transformDynamicSecretInterface(secretRespMap)
	// 	if err != nil {
	// 		return secretstores.GetSecretResponse{}, fmt.Errorf("failed to transform dynamic secret response for secret '%s': %w", secretName.(string), err)
	// 	}

	// 	transformedResp := transformDynamicSecretResponse(secretName.(string), dynamicSecretResp)
	// 	secretResp = transformedResp
	// case AKEYLESS_SECRET_TYPE_ROTATED_SECRET_RESPONSE:
	// 	getSecretValue := akeyless.NewGetRotatedSecretValue(secretName.(string))
	// 	getSecretValue.SetToken(akeylessSecretStore.token)
	// 	secretRespMap, _, err := akeylessSecretStore.v2.GetRotatedSecretValue(context.Background()).Body(*getSecretValue).Execute()
	// 	if err != nil {
	// 		return secretstores.GetSecretResponse{}, fmt.Errorf("failed to get rotated secret '%s' value from Akeyless API: %w", secretName.(string), err)
	// 	}

	// 	// assert type of secretResp to RotatedSecretResponse
	// 	rotatedSecretResp, ok := secretRespMap.(RotatedSecretResponse)
	// 	if !ok {
	// 		return secretstores.GetSecretResponse{}, fmt.Errorf("failed to assert type of secret response to RotatedSecretResponse: %w", err)
	// 	}

	// 	// transform the response
	// 	transformedResp := transformRotatedSecretResponse(secretName.(string), rotatedSecretResp)
	// 	secretResp = transformedResp
	default:
		return secretstores.GetSecretResponse{}, errors.New("unsupported secret type")
	}
	return secretResp, nil
}

// func transformDynamicSecretInterface(secretRespMap map[string]interface{}) (DynamicSecretResponse, error) {
// 	dynamicSecretResp := DynamicSecretResponse{}
// 	// Attempt to map fields from secretRespMap to dynamicSecretResp
// 	// This assumes DynamicSecretResponse is a struct and secretRespMap is a map[string]interface{}
// 	// Use mapstructure or manual assignment as needed
// 	err := mapstructure.Decode(secretRespMap, &dynamicSecretResp)
// 	if err != nil {
// 		return DynamicSecretResponse{}, fmt.Errorf("failed to decode secret response to DynamicSecretResponse: %w", err)
// 	}
// 	return dynamicSecretResp, nil
// }

// convertSecretToString converts the secret to a string.
// It returns the secret value or an error if the secret value is not found.
// func convertSecretResponseToString(secret any) (string, error) {

// 	// If the secret is a string, return it
// 	if secretStr, ok := secret.(string); ok {
// 		return secretStr, nil
// 	}

// 	// If the secret is a map, marshal it to a JSON string
// 	secretValueBytes, err := json.Marshal(secret)
// 	if err != nil {
// 		return "", fmt.Errorf("failed to marshal secret response: %w", err)
// 	}

// 	// Return the JSON string
// 	return string(secretValueBytes), nil
// }

// result is a helper struct to pass results and errors back from goroutines
// type result struct {
// 	key   string
// 	value *secretstores.GetSecretResponse
// 	error error
// }

// BulkGetSecretResponse takes in a list of `akeyless.Item`,
// splits them into static and non-static subsets
// sends an async request to the appropriate API endpoint for each subset
// and returns them as the corresponding `secretstores.BulkGetSecretResponse`
// func BulkGetSecretResponse(ctx context.Context, allSecrets []akeyless.Item, akeylessSecretStore *akeylessSecretStore) (secretstores.BulkGetSecretResponse, error) {
// 	// Split secrets into static and non-static
// 	staticSecrets, nonStaticSecrets, err := splitSecretsByType(allSecrets)
// 	if err != nil {
// 		return secretstores.BulkGetSecretResponse{}, fmt.Errorf("failed to split secrets by type: %w", err)
// 	}

// 	akeylessSecretStore.logger.Debug("%d static secrets, %d non-static secrets", len(staticSecrets), len(nonStaticSecrets))

// 	// Process static secrets in batch
// 	staticResults, err := processStaticSecrets(staticSecrets, akeylessSecretStore)
// 	if err != nil {
// 		return secretstores.BulkGetSecretResponse{}, fmt.Errorf("failed to process static secrets: %w", err)
// 	}

// 	// Process non-static secrets asynchronously
// 	nonStaticResults, err := processNonStaticSecrets(ctx, nonStaticSecrets, akeylessSecretStore)
// 	if err != nil {
// 		return secretstores.BulkGetSecretResponse{}, fmt.Errorf("failed to process non-static secrets: %w", err)
// 	}

// 	// Combine all results
// 	allResults := make(map[string]map[string]string)
// 	maps.Copy(allResults, staticResults)
// 	maps.Copy(allResults, nonStaticResults)

// 	return secretstores.BulkGetSecretResponse{
// 		Data: allResults,
// 	}, nil
// }

// splitSecretsByType splits secrets into static and non-static (dynamic, rotated) secrets
// func splitSecretsByType(allSecrets []akeyless.Item) ([]string, []akeyless.Item, error) {
// 	var staticSecrets []string
// 	var nonStaticSecrets []akeyless.Item

// 	for _, secret := range allSecrets {
// 		if secret.ItemType == nil || secret.ItemName == nil {
// 			continue
// 		}

// 		if *secret.ItemType == AKEYLESS_SECRET_TYPE_STATIC {
// 			staticSecrets = append(staticSecrets, *secret.ItemName)
// 		} else {
// 			nonStaticSecrets = append(nonStaticSecrets, secret)
// 		}
// 	}

// 	return staticSecrets, nonStaticSecrets, nil
// }

// processStaticSecrets processes all static secrets in a single batch request
// func processStaticSecrets(staticSecrets []string, akeylessSecretStore *akeylessSecretStore) (map[string]map[string]string, error) {
// 	if len(staticSecrets) == 0 {
// 		return make(map[string]map[string]string), nil
// 	}

// 	// Get all static secrets in one batch request
// 	secretValue, err := GetSecretValueByType(staticSecrets, AKEYLESS_SECRET_TYPE_STATIC, akeylessSecretStore)
// 	if err != nil {
// 		return nil, fmt.Errorf("failed to get static secrets: %w", err)
// 	}

// 	// Parse the response - static secrets return a map of secret names to values
// 	var secretsMap map[string]string
// 	if err := json.Unmarshal([]byte(secretValue.(string)), &secretsMap); err != nil {
// 		return nil, fmt.Errorf("failed to parse static secrets response: %w", err)
// 	}

// 	// Convert to the expected format
// 	result := make(map[string]map[string]string)
// 	for secretName, secretValue := range secretsMap {
// 		result[secretName] = map[string]string{
// 			secretName: secretValue,
// 		}
// 	}

// 	return result, nil
// }

// processNonStaticSecrets processes dynamic and rotated secrets asynchronously
// func processNonStaticSecrets(ctx context.Context, nonStaticSecrets []akeyless.Item, akeylessSecretStore *akeylessSecretStore) (map[string]map[string]string, error) {
// 	if len(nonStaticSecrets) == 0 {
// 		return make(map[string]map[string]string), nil
// 	}

// 	// A WaitGroup to wait for all goroutines to finish
// 	var wg sync.WaitGroup

// 	// A buffered channel to receive results from goroutines
// 	resultsChannel := make(chan result, len(nonStaticSecrets))

// 	// Launch a goroutine for each non-static secret
// 	for _, secret := range nonStaticSecrets {
// 		wg.Add(1)

// 		currentSecret := secret

// 		go func() {
// 			defer wg.Done()

// 			// Fetch the secret value
// 			secretValue, err := GetSecretValueByTypeAsync(ctx, *currentSecret.ItemName, *currentSecret.ItemType, akeylessSecretStore)

// 			// Parse the secret value based on its type
// 			parsedData, parseErr := parseSecretResponse(*currentSecret.ItemName, *currentSecret.ItemType, secretValue, err)

// 			resultsChannel <- result{
// 				key:   *currentSecret.ItemName,
// 				value: parsedData,
// 				error: parseErr,
// 			}
// 		}()
// 	}

// 	// Launch separate goroutine to close channel once all workers are done
// 	go func() {
// 		wg.Wait()
// 		close(resultsChannel)
// 	}()

// 	// Collect results
// 	result := make(map[string]map[string]string)
// 	for res := range resultsChannel {
// 		if res.error != nil {
// 			return nil, fmt.Errorf("failed to get secret %s: %w", res.key, res.error)
// 		}
// 		result[res.key] = res.value.Data
// 	}

// 	return result, nil
// }

// parseSecretResponse parses the secret response based on the secret type
// func parseSecretResponse(secretName, secretType, secretValue any, err error) (*secretstores.GetSecretResponse, error) {
// 	if err != nil {
// 		return nil, err
// 	}

// 	var secretData map[string]string

// 	switch secretType {
// 	// TODO: Implement
// 	case AKEYLESS_SECRET_TYPE_DYNAMIC:
// 		var dynamicResp map[string]interface{}
// 		if err := json.Unmarshal([]byte(secretValue), &dynamicResp); err != nil {
// 			return nil, fmt.Errorf("failed to parse dynamic secret response: %w", err)
// 		}

// 	case AKEYLESS_SECRET_TYPE_ROTATED:
// 		// TODO: Implement
// 	default:
// 		// For any other type, return as a simple key-value pair
// 		secretData = map[string]string{
// 			secretName: secretValue,
// 		}
// 	}

// 	return &secretstores.GetSecretResponse{
// 		Data: secretData,
// 	}, nil
// }

// GetSecretValueByTypeAsync gets the secret value by the type of the secret asynchronously
func GetSecretValueByTypeAsync(ctx context.Context, secretName any, secretType string, akeylessSecretStore *akeylessSecretStore) (any, error) {
	return GetSecretValueByType(secretName, secretType, akeylessSecretStore)
}

// transformStaticSecretResponse transforms the static secret response we get from Akeyless API
// into a map ready for Dapr.
// Static secrets can be of type text, json, email/password, key/value
// in case they are JSON/email/password, we need to transform the response into a string ready for Dapr.
func transformStaticSecretResponse(secrets map[string]any) (secretstores.GetSecretResponse, error) {

	secretData := make(map[string]string)

	for path, secret := range secrets {
		switch secretType := secret.(type) {
		case string:
			secretData[path] = secretType
		case map[string]any:
			encoded, err := json.Marshal(secretType)
			if err != nil {
				return secretstores.GetSecretResponse{}, fmt.Errorf("failed to marshal secret response: %w", err)
			}
			secretData[path] = string(encoded)
		}

	}

	return secretstores.GetSecretResponse{
		Data: secretData,
	}, nil
}

type DynamicSecretResponse struct {
	ID           string              `json:"id"`
	Msg          string              `json:"msg"`
	Secret       DynamicSecretSecret `json:"secret"`
	TTLInMinutes string              `json:"ttl_in_minutes"`
}

type DynamicSecretSecret struct {
	AppID       string `json:"appId"`
	DisplayName string `json:"displayName"`
	EndDateTime string `json:"endDateTime"`
	KeyID       string `json:"keyId"`
	SecretText  string `json:"secretText"`
	TenantID    string `json:"tenantId"`
}

// type DynamicSecretTransformedResponse struct {
// 	DynamicSecretName DynamicSecretCredentials `json:"secret_name"`
// }

// type DynamicSecretCredentials struct {
// 	Username string `json:"username"`
// 	Password string `json:"password"`
// }

// transformDynamicSecretResponse transforms the dynamic secret response we get from Akeyless API
// into a map ready for Dapr.
// func transformDynamicSecretResponse(dynamicSecretName string, dynamicResp DynamicSecretResponse) secretstores.GetSecretResponse {
// 	creds := DynamicSecretCredentials{
// 		Username: dynamicResp.Secret.DisplayName,
// 		Password: dynamicResp.Secret.SecretText,
// 	}
// 	credsJSON, err := json.Marshal(creds)
// 	if err != nil {
// 		return secretstores.GetSecretResponse{
// 			Data: map[string]string{},
// 		}
// 	}
// 	return secretstores.GetSecretResponse{
// 		Data: map[string]string{
// 			dynamicSecretName: string(credsJSON),
// 		},
// 	}
// }

// type RotatedSecretResponse struct {
// 	Value RotatedSecretValue `json:"value"`
// }

// type RotatedSecretValue struct {
// 	Username      string `json:"username"`
// 	Password      string `json:"password"`
// 	ApplicationID string `json:"application_id"`
// }

// type RotatedSecretTransformedResponse struct {
// 	RotatedSecretName RotatedSecretCredentials `json:"secret_name"`
// }

// type RotatedSecretCredentials struct {
// 	Username      string `json:"username"`
// 	Password      string `json:"password"`
// 	ApplicationID string `json:"application_id"`
// }

// // transformDynamicSecretResponse transforms the dynamic secret response we get from Akeyless API
// // into a map ready for Dapr.
// func transformRotatedSecretResponse(rotatedSecretName string, rotatedResp RotatedSecretResponse) RotatedSecretTransformedResponse {
// 	return RotatedSecretTransformedResponse{
// 		RotatedSecretName: RotatedSecretCredentials{
// 			Username: rotatedResp.Value.Username,
// 			Password: rotatedResp.Value.Password,
// 		},
// 	}
// }

func GetDaprSingleSecretResponse(secretName string, secretValue string) (secretstores.GetSecretResponse, error) {
	return secretstores.GetSecretResponse{
		Data: map[string]string{
			secretName: secretValue,
		},
	}, nil
}

// func getDaprBulkSecretResponse(secrets map[string]map[string]string) secretstores.BulkGetSecretResponse {
// 	return secretstores.BulkGetSecretResponse{
// 		Data: secrets,
// 	}
// }
