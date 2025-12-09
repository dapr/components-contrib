package akeyless

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/akeylesslabs/akeyless-go/v5"
	"github.com/dapr/components-contrib/secretstores"
	"github.com/dapr/kit/logger"
)

const (
	AUTH_JWT                   = "jwt"
	DEFAULT_AUTH_TYPE          = "access_key"
	AUTH_IAM                   = "aws_iam"
	AUTH_K8S                   = "k8s"
	PUBLIC_GATEWAY_URL         = "https://api.akeyless.io"
	USER_AGENT                 = "dapr.io/akeyless-secret-store"
	STATIC_SECRET_RESPONSE     = "STATIC_SECRET"
	DYNAMIC_SECRET_RESPONSE    = "DYNAMIC_SECRET"
	ROTATED_SECRET_RESPONSE    = "ROTATED_SECRET"
	STATIC_SECRET_TYPE         = "static-secret"
	DYNAMIC_SECRET_TYPE        = "dynamic-secret"
	ROTATED_SECRET_TYPE        = "rotated-secret"
	ALL_SECRET_TYPES           = "all"
	CLIENT_SOURCE              = "akeylessclienttype"
	PATH_DEFAULT               = "/"
	METADATA_PATH_KEY          = "path"
	METADATA_SECRETS_TYPE_KEY  = "secrets_type"
	TOKEN_REFRESH_GRACE_PERIOD = 5 * time.Minute
)

var supportedSecretTypes = []string{STATIC_SECRET_TYPE, DYNAMIC_SECRET_TYPE, ROTATED_SECRET_TYPE}

// AccessTypeCharMap maps single-character access types to their display names.
var accessTypeCharMap = map[string]string{
	"a": DEFAULT_AUTH_TYPE,
	"o": AUTH_JWT,
	"w": AUTH_IAM,
	"k": AUTH_K8S,
}

// AccessIdRegex is the compiled regular expression for validating Akeyless Access IDs.
var accessIdRegex = regexp.MustCompile(`^p-([A-Za-z0-9]{14}|[A-Za-z0-9]{12})$`)

// isValidAccessIdFormat validates the format of an Akeyless Access ID.
// The format is p-([A-Za-z0-9]{14}|[A-Za-z0-9]{12}).
// It returns true if the format is valid, and false otherwise.
func isValidAccessIdFormat(accessId string) bool {
	return accessIdRegex.MatchString(accessId)
}

// extractAccessTypeChar extracts the Akeyless Access Type character from a valid Access ID.
// The access type character is the second to last character of the ID part.
// It returns the single-character access type (e.g., 'a', 'o') or an empty string and an error if the format is invalid.
func extractAccessTypeChar(accessId string) (string, error) {
	if !isValidAccessIdFormat(accessId) {
		return "", errors.New("invalid access ID format")
	}
	parts := strings.Split(accessId, "-")
	idPart := parts[1] // Get the part after "p-"
	// The access type char is the second-to-last character
	return string(idPart[len(idPart)-2]), nil
}

// getAccessTypeDisplayName gets the full display name of the access type from the character.
// It returns the display name (e.g., 'api_key') or an error if the type character is unknown.
func getAccessTypeDisplayName(typeChar string) (string, error) {
	if typeChar == "" {
		return "", errors.New("unable to retrieve access type, missing type char")
	}
	displayName, ok := accessTypeCharMap[typeChar]
	if !ok {
		return "Unknown", errors.New("access type character not found in map")
	}
	return displayName, nil
}

func getDaprSingleSecretResponse(secretName string, secretValue string) (secretstores.GetSecretResponse, error) {
	return secretstores.GetSecretResponse{
		Data: map[string]string{
			secretName: secretValue,
		},
	}, nil
}

func getItemNames(items []akeyless.Item) []string {
	itemNames := []string{}
	for _, item := range items {
		itemNames = append(itemNames, *item.ItemName)
	}
	return itemNames
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

	// base64 encode the token if it's not already encoded
	if _, err := base64.StdEncoding.DecodeString(metadata.K8sServiceAccountToken); err != nil {
		a.logger.Info("k8sServiceAccountToken is not base64 encoded, encoding it...")
		metadata.K8sServiceAccountToken = base64.StdEncoding.EncodeToString([]byte(metadata.K8sServiceAccountToken))
	}
	authRequest.SetK8sServiceAccountToken(metadata.K8sServiceAccountToken)

	if metadata.K8SGatewayURL == "" {
		a.logger.Debug("k8s gateway url is missing, using gatewayUrl")
		metadata.K8SGatewayURL = metadata.GatewayURL
	}
	metadata.K8SGatewayURL = strings.TrimSuffix(metadata.K8SGatewayURL, "/api/v2")
	authRequest.SetGatewayUrl(metadata.K8SGatewayURL)
	return nil
}

// `parseSecretTypes` parses the `secret_types` metadata parameter
// and returns a slice of supported secret types in the format expected
// by the Akeyless `POST /list-items` API.
// It accepts a comma-separated string of secret types and returns a slice of supported secret types.
func parseSecretTypes(secretTypes string) ([]string, error) {
	// Handle "all" or empty string which returns all supported secret types
	if secretTypes == ALL_SECRET_TYPES || secretTypes == "" {
		return supportedSecretTypes, nil
	}

	// Parse comma-separated values
	types := strings.Split(secretTypes, ",")
	if len(types) == 0 {
		return nil, fmt.Errorf("no secret types provided")
	}
	result := make([]string, 0, len(types))

	// Map metadata.secret_types to supportedSecretTypes
	typeMap := map[string]string{
		"static":  STATIC_SECRET_TYPE,
		"dynamic": DYNAMIC_SECRET_TYPE,
		"rotated": ROTATED_SECRET_TYPE,
	}

	for _, t := range types {
		t = strings.ToLower(strings.TrimSpace(t))
		if mappedType, ok := typeMap[t]; ok {
			result = append(result, mappedType)
		} else {
			// Allow direct SDK format
			if t == STATIC_SECRET_TYPE || t == DYNAMIC_SECRET_TYPE || t == ROTATED_SECRET_TYPE {
				result = append(result, t)
			} else {
				return nil, fmt.Errorf("invalid secret type '%s', supported types: static[-secret], dynamic[-secret], rotated[-secret]", t)
			}
		}
	}

	// Dedup
	seen := make(map[string]bool)
	unique := []string{}
	for _, t := range result {
		if !seen[t] {
			seen[t] = true
			unique = append(unique, t)
		}
	}

	return unique, nil
}

func createTLSConfig(gatewayTLSCA string) (*tls.Config, error) {

	// Decode base64 to PEM
	certBytes, err := base64.StdEncoding.DecodeString(gatewayTLSCA)
	if err != nil {
		return nil, fmt.Errorf("failed to decode base64-encoded gateway TLS CA: %w", err)
	}

	// Validate PEM format
	block, _ := pem.Decode(certBytes)
	if block == nil {
		return nil, fmt.Errorf("failed to decode PEM certificate: invalid PEM format")
	}

	// Cereate cert pool and add certificate
	caCertPool := x509.NewCertPool()
	if !caCertPool.AppendCertsFromPEM(certBytes) {
		return nil, errors.New("failed to add certificate to cert pool")
	}

	return &tls.Config{
		MinVersion: tls.VersionTLS12,
		RootCAs:    caCertPool,
	}, nil
}

func parseTokenExpirationDate(expirationStr string) (time.Time, error) {
	// Try multiple formats to handle different expiration date formats
	// Format 1: ISO 8601 format "2025-01-01T00:00:00Z" (used in tests)
	layouts := []string{
		time.RFC3339,                    // "2006-01-02T15:04:05Z07:00"
		time.RFC3339Nano,                // "2006-01-02T15:04:05.999999999Z07:00"
		"2006-01-02T15:04:05Z",          // "2006-01-02T15:04:05Z"
		"2006-01-02 15:04:05 -0700 MST", // "2025-12-09 21:35:00 +0000 UTC" (custom format)
		"2006-01-02 15:04:05 -0700",     // "2025-12-09 21:35:00 +0000" (without MST)
	}

	for _, layout := range layouts {
		parsedTime, err := time.Parse(layout, expirationStr)
		if err == nil {
			return parsedTime, nil
		}
	}

	return time.Time{}, fmt.Errorf("failed to parse token expiration date '%s' with any supported format", expirationStr)
}
