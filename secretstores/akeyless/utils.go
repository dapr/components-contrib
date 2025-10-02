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

// GetSingleSecretValue gets the value of a single secret from Akeyless.
// It returns the value of the secret or an error if the secret is not found.
func GetSingleSecretValue(secretName string, secretType string, akeylessSecretStore *akeylessSecretStore) (string, error) {

	var secretValue string
	var err error

	switch secretType {
	case AKEYLESS_SECRET_TYPE_STATIC_SECRET_RESPONSE:
		getSecretValue := akeyless.NewGetSecretValue([]string{secretName})
		getSecretValue.SetToken(akeylessSecretStore.token)
		secretRespMap, _, apiErr := akeylessSecretStore.v2.GetSecretValue(context.Background()).Body(*getSecretValue).Execute()
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
		switch valueType := value.(type) {
		case string:
			secretValue = valueType
		case map[string]string:
			encoded, marshalErr := json.Marshal(valueType)
			if marshalErr != nil {
				err = fmt.Errorf("failed to marshal secret response: %w", marshalErr)
			} else {
				secretValue = string(encoded)
			}
		case any:
			encoded, marshalErr := json.Marshal(valueType)
			if marshalErr != nil {
				err = fmt.Errorf("failed to marshal secret response: %w", marshalErr)
			} else {
				secretValue = string(encoded)
			}

		default:
			err = fmt.Errorf("failed to assert type of secret response to string for secret '%s'", secretName)
		}

	case AKEYLESS_SECRET_TYPE_DYNAMIC_SECRET_RESPONSE:
		getDynamicSecretValue := akeyless.NewGetDynamicSecretValue(secretName)
		getDynamicSecretValue.SetToken(akeylessSecretStore.token)
		secretRespMap, _, apiErr := akeylessSecretStore.v2.GetDynamicSecretValue(context.Background()).Body(*getDynamicSecretValue).Execute()
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

	// TODO implement rotated secrets
	case AKEYLESS_SECRET_TYPE_ROTATED_SECRET_RESPONSE:
		return "", errors.New("rotated secrets are not supported")
	}

	return secretValue, err
}

type DynamicSecretResponse struct {
	ID           string              `json:"id"`
	Msg          string              `json:"msg"`
	Secret       DynamicSecretSecret `json:"secret"`
	TTLInMinutes string              `json:"ttl_in_minutes"`
}

type DynamicSecretSecret struct {
	AppID       string `json:"appId,omitempty"`
	DisplayName string `json:"displayName"`
	EndDateTime string `json:"endDateTime,omitempty"`
	KeyID       string `json:"keyId,omitempty"`
	SecretText  string `json:"secretText,omitempty"`
	TenantID    string `json:"tenantId,omitempty"`
}

func GetDaprSingleSecretResponse(secretName string, secretValue string) (secretstores.GetSecretResponse, error) {
	return secretstores.GetSecretResponse{
		Data: map[string]string{
			secretName: secretValue,
		},
	}, nil
}
