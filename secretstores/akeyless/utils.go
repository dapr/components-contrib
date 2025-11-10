package akeyless

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"regexp"
	"strings"

	"github.com/akeylesslabs/akeyless-go/v5"
	"github.com/dapr/components-contrib/secretstores"
	"github.com/dapr/kit/logger"
)

// Define constants for the access types. These are equivalent to the TypeScript consts.
const (
	AUTH_JWT                = "jwt"
	DEFAULT_AUTH_TYPE       = "access_key"
	AUTH_IAM                = "aws_iam"
	AUTH_K8S                = "k8s"
	PUBLIC_GATEWAY_URL      = "https://api.akeyless.io"
	USER_AGENT              = "dapr.io/akeyless-secret-store"
	STATIC_SECRET_RESPONSE  = "STATIC_SECRET"
	DYNAMIC_SECRET_RESPONSE = "DYNAMIC_SECRET"
	ROTATED_SECRET_RESPONSE = "ROTATED_SECRET"
	CLIENT_SOURCE           = "akeylessclienttype"
)

var SUPPORTED_SECRET_TYPES = []string{"static-secret", "dynamic-secret", "rotated-secret"}

// AccessTypeCharMap maps single-character access types to their display names.
var AccessTypeCharMap = map[string]string{
	"a": DEFAULT_AUTH_TYPE,
	"o": AUTH_JWT,
	"w": AUTH_IAM,
	"k": AUTH_K8S,
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

func isSecretActive(secret akeyless.Item, logger logger.Logger) bool {

	var isActive bool

	// check if secret has isEnabled field
	if secret.IsEnabled == nil {
		logger.Debugf("secret '%s' is missing isEnabled field, skipping...", *secret.ItemName)
		return false
	}

	if !*secret.IsEnabled {
		logger.Debugf("secret '%s' is not enabled, skipping...", *secret.ItemName)
		return false
	}

	switch *secret.ItemType {
	case STATIC_SECRET_RESPONSE:
		logger.Debugf("static secret '%s' is active", *secret.ItemName)
		isActive = true
	case DYNAMIC_SECRET_RESPONSE:
		// Check if ItemGeneralInfo is available, if not, include the secret
		if secret.ItemGeneralInfo != nil &&
			secret.ItemGeneralInfo.DynamicSecretProducerDetails != nil &&
			secret.ItemGeneralInfo.DynamicSecretProducerDetails.ProducerStatus != nil {
			status := *secret.ItemGeneralInfo.DynamicSecretProducerDetails.ProducerStatus
			if status == "ProducerConnected" {
				logger.Debugf("dynamic secret '%s' is active, adding to filtered secrets...", *secret.ItemName)
				isActive = true
			} else {
				logger.Debugf("dynamic secret '%s' producer status is '%s', skipping...", *secret.ItemName, status)
			}
		} else {
			// If detailed info is not available, include the secret
			logger.Debugf("dynamic secret '%s' is missing detailed info. adding to filtered secrets...", *secret.ItemName)
			isActive = true
		}
	case ROTATED_SECRET_RESPONSE:
		// Check if ItemGeneralInfo is available, if not, include the secret
		if secret.ItemGeneralInfo != nil &&
			secret.ItemGeneralInfo.RotatedSecretDetails != nil &&
			secret.ItemGeneralInfo.RotatedSecretDetails.RotatorStatus != nil {
			status := *secret.ItemGeneralInfo.RotatedSecretDetails.RotatorStatus
			if status == "RotationSucceeded" || status == "RotationInitialStatus" {
				isActive = true
			} else {
				logger.Debugf("rotated secret '%s' rotation status is '%s', skipping...", *secret.ItemName, status)
			}
		} else {
			// If detailed info is not available, include the secret
			logger.Debugf("rotated secret '%s' is missing detailed info. adding to filtered secrets...", *secret.ItemName)
			isActive = true
		}
	default:
		logger.Debugf("secret '%s' is of unsupported type '%s', skipping...", *secret.ItemName, *secret.ItemType)
		isActive = false
	}

	return isActive
}

func setK8SAuthConfiguration(metadata akeylessMetadata, authRequest *akeyless.Auth, a *akeylessSecretStore) error {
	if metadata.K8SAuthConfigName == "" {
		return fmt.Errorf("k8s auth config name is required")
	}
	authRequest.SetK8sAuthConfigName(metadata.K8SAuthConfigName)
	if metadata.K8sServiceAccountToken == "" {
		a.logger.Debug("k8s service account token is missing, attempting to read from default service account token file")
		token, err := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/token")
		if err != nil {
			return fmt.Errorf("failed to read default service account token file: %w", err)
		}
		metadata.K8sServiceAccountToken = string(token)
	}
	if metadata.K8SGatewayURL == "" {
		a.logger.Debug("k8s gateway url is missing, using gatewayUrl")
		metadata.K8SGatewayURL = metadata.GatewayURL
	}
	authRequest.SetGatewayUrl(metadata.K8SGatewayURL)
	authRequest.SetK8sServiceAccountToken(metadata.K8sServiceAccountToken)
	return nil
}
