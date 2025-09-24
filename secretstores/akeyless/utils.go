package akeyless

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"

	aws "github.com/akeylesslabs/akeyless-go-cloud-id/cloudprovider/aws"
	"github.com/akeylesslabs/akeyless-go/v5"
)

// Define constants for the access types. These are equivalent to the TypeScript consts.
const (
	AKEYLESS_AUTH_ACCESS_JWT          = "jwt"
	AKEYLESS_AUTH_DEFAULT_ACCESS_TYPE = "access_key"
	AKEYLESS_AUTH_ACCESS_IAM          = "aws_iam"
	AKEYLESS_PUBLIC_GATEWAY_URL       = "https://api.akeyless.io"
	AKEYLESS_USER_AGENT               = "dapr.io/akeyless-secret-store"
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
		akeylessSecretStore.logger.Debug("Getting cloud ID for AWS IAM...")
		id, err := aws.GetCloudId()
		if err != nil {
			return errors.New("unable to get cloud ID")
		}
		authRequest.SetCloudId(id)
	case AKEYLESS_AUTH_ACCESS_JWT:
		akeylessSecretStore.logger.Debug("Setting JWT for authentication...")
		authRequest.SetJwt(metadata.JWT)
	case AKEYLESS_AUTH_DEFAULT_ACCESS_TYPE:
		akeylessSecretStore.logger.Debug("Setting access key for authentication...")
		authRequest.SetAccessKey(metadata.AccessKey)
	}

	// Create Akeyless API client configuration
	akeylessSecretStore.logger.Debug("Creating Akeyless API client configuration...")
	config := akeyless.NewConfiguration()
	config.Servers = []akeyless.ServerConfiguration{
		{
			URL: metadata.GatewayURL,
		},
	}
	config.UserAgent = AKEYLESS_USER_AGENT
	config.AddDefaultHeader("akeylessclienttype", AKEYLESS_USER_AGENT)

	akeylessSecretStore.v2 = akeyless.NewAPIClient(config).V2Api

	akeylessSecretStore.logger.Debug("Authenticating with Akeyless...")
	out, _, err := akeylessSecretStore.v2.Auth(context.Background()).Body(*authRequest).Execute()
	if err != nil {
		return fmt.Errorf("failed to authenticate with Akeyless: %w", err)
	}

	akeylessSecretStore.logger.Debug("Setting token %s for authentication...", out.GetToken()[:5]+"[REDACTED]")
	akeylessSecretStore.logger.Debug("Expires at: %s", out.GetExpiration())
	akeylessSecretStore.token = out.GetToken()

	return nil
}
