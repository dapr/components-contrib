package akeyless

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"

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
	SecretText  string `json:"secretText"`
	TenantID    string `json:"tenantId,omitempty"`
}

type RotatedSecretResponse struct {
	Value RotatedSecretValue `json:"value"`
}

type RotatedSecretValue struct {
	Username      string `json:"username"`
	Password      string `json:"password"`
	ApplicationID string `json:"application_id,omitempty"`
}

func GetDaprSingleSecretResponse(secretName string, secretValue string) (secretstores.GetSecretResponse, error) {
	return secretstores.GetSecretResponse{
		Data: map[string]string{
			secretName: secretValue,
		},
	}, nil
}

func GetItemNames(items []akeyless.Item) []string {
	itemNames := []string{}
	for _, item := range items {
		itemNames = append(itemNames, *item.ItemName)
	}
	return itemNames
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

func stringifyStaticSecret(secretValue any, secretName string) (string, error) {
	var err error

	switch valueType := secretValue.(type) {
	case string:
		secretValue = string(valueType)
	case map[string]string:
		encoded, marshalErr := json.Marshal(valueType)
		if marshalErr != nil {
			err = fmt.Errorf("failed to marshal secret response for secret '%s': %w", secretName, marshalErr)
		} else {
			secretValue = string(encoded)
		}
	case any:
		encoded, marshalErr := json.Marshal(valueType)
		if marshalErr != nil {
			err = fmt.Errorf("failed to marshal secret response for secret '%s': %w", secretName, marshalErr)
			break
		} else {
			secretValue = string(encoded)
			break
		}

	default:
		err = fmt.Errorf("failed to assert type of secret response to string for secret '%s'", secretName)
	}

	return string(secretValue.(string)), err
}

type secretResultCollection struct {
	name  string
	value string
	err   error
}
