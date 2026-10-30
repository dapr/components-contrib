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
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"regexp"
	"strings"
	"time"

	akeylesssdk "github.com/akeylesslabs/akeyless-go/v5"
)

// accessTypeCharMap maps single-character access types to their display names.
var accessTypeCharMap = map[string]string{
	"a": AuthTypeAccessKey,
	"o": AuthTypeJWT,
	"w": AuthTypeAWSIAM,
	"k": AuthTypeK8S,
}

// accessIDRegex is the compiled regular expression for validating Akeyless Access IDs.
var accessIDRegex = regexp.MustCompile(`^p-([A-Za-z0-9]{14}|[A-Za-z0-9]{12})$`)

func isValidAccessIDFormat(accessID string) bool {
	return accessIDRegex.MatchString(accessID)
}

func extractAccessTypeChar(accessID string) (string, error) {
	if !isValidAccessIDFormat(accessID) {
		return "", errors.New("invalid access ID format")
	}
	parts := strings.Split(accessID, "-")
	idPart := parts[1]
	return string(idPart[len(idPart)-2]), nil
}

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

func setK8SAuthConfiguration(metadata akeylessMetadata, authRequest *akeylesssdk.Auth, a *akeylessSecretStore) error {
	if metadata.K8s.AuthConfigName == "" {
		return errors.New("k8s auth config name is required")
	}
	authRequest.SetK8sAuthConfigName(metadata.K8s.AuthConfigName)
	if metadata.K8s.ServiceAccountToken == "" {
		a.logger.Debug("k8s service account token is missing, attempting to read from default service account token file")
		token, err := os.ReadFile(K8sServiceAccountTokenPath)
		if err != nil {
			return fmt.Errorf("failed to read default service account token file: %w", err)
		}
		metadata.K8s.ServiceAccountToken = string(token)
	}

	if _, err := base64.StdEncoding.DecodeString(metadata.K8s.ServiceAccountToken); err != nil {
		a.logger.Debug("k8sServiceAccountToken is not base64 encoded, encoding it...")
		metadata.K8s.ServiceAccountToken = base64.StdEncoding.EncodeToString([]byte(metadata.K8s.ServiceAccountToken))
	}
	authRequest.SetK8sServiceAccountToken(metadata.K8s.ServiceAccountToken)

	if metadata.K8s.GatewayURL == "" {
		a.logger.Debug("k8s gateway url is missing, using default")
		metadata.K8s.GatewayURL = metadata.GatewayURL
	}
	metadata.K8s.GatewayURL = strings.TrimSuffix(metadata.K8s.GatewayURL, "/api/v2")
	authRequest.SetGatewayUrl(metadata.K8s.GatewayURL)
	return nil
}

func parseTokenExpirationDate(expirationStr string) (time.Time, error) {
	layouts := []string{
		time.RFC3339,
		time.RFC3339Nano,
		"2006-01-02T15:04:05Z",
		"2006-01-02 15:04:05 -0700 MST",
		"2006-01-02 15:04:05 -0700",
	}

	for _, layout := range layouts {
		parsedTime, err := time.Parse(layout, expirationStr)
		if err == nil {
			return parsedTime, nil
		}
	}

	return time.Time{}, fmt.Errorf("failed to parse token expiration date '%s' with any supported format", expirationStr)
}
