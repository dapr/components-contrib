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
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	akeylesssdk "github.com/akeylesslabs/akeyless-go/v5"

	"github.com/dapr/components-contrib/secretstores"
	"github.com/dapr/kit/logger"
)

type secretResultCollection struct {
	name  string
	value string
	err   error
}

func getDaprSingleSecretResponse(secretName string, secretValue string) (secretstores.GetSecretResponse, error) {
	return secretstores.GetSecretResponse{
		Data: map[string]string{
			secretName: secretValue,
		},
	}, nil
}

func getItemNames(items []akeylesssdk.Item) []string {
	itemNames := []string{}
	for _, item := range items {
		itemNames = append(itemNames, *item.ItemName)
	}
	return itemNames
}

func stringifyStaticSecret(secretValue any, secretName string) (string, error) {
	if secretValue == nil {
		return "", fmt.Errorf("nil secret value for secret %q", secretName)
	}
	if s, ok := secretValue.(string); ok {
		return s, nil
	}
	encoded, err := json.Marshal(secretValue)
	if err != nil {
		return "", fmt.Errorf("failed to marshal secret response for secret %q: %w", secretName, err)
	}
	return string(encoded), nil
}

func isSecretActive(secret akeylesssdk.Item, logger logger.Logger) bool {
	if secret.IsEnabled == nil {
		logger.Debugf("secret '%s' is missing isEnabled field, skipping...", *secret.ItemName)
		return false
	}

	if !*secret.IsEnabled {
		logger.Debugf("secret '%s' is not enabled, skipping...", *secret.ItemName)
		return false
	}

	switch SecretResponse(*secret.ItemType) {
	case SecretResponseStatic:
		logger.Debugf("static secret '%s' is active", *secret.ItemName)
		return true
	case SecretResponseDynamic:
		if secret.ItemGeneralInfo != nil &&
			secret.ItemGeneralInfo.DynamicSecretProducerDetails != nil &&
			secret.ItemGeneralInfo.DynamicSecretProducerDetails.ProducerStatus != nil {
			status := *secret.ItemGeneralInfo.DynamicSecretProducerDetails.ProducerStatus
			if status == ProducerStatusConnected {
				logger.Debugf("dynamic secret '%s' is active, adding to filtered secrets...", *secret.ItemName)
				return true
			}
			logger.Debugf("dynamic secret '%s' producer status is '%s', skipping...", *secret.ItemName, status)
			return false
		}
		logger.Debugf("dynamic secret '%s' is missing detailed info. adding to filtered secrets...", *secret.ItemName)
		return true
	case SecretResponseRotated:
		if secret.ItemGeneralInfo != nil &&
			secret.ItemGeneralInfo.RotatedSecretDetails != nil &&
			secret.ItemGeneralInfo.RotatedSecretDetails.RotatorStatus != nil {
			status := *secret.ItemGeneralInfo.RotatedSecretDetails.RotatorStatus
			if status == RotatorStatusSucceeded || status == RotatorStatusInitial {
				return true
			}
			logger.Debugf("rotated secret '%s' rotation status is '%s', skipping...", *secret.ItemName, status)
			return false
		}
		logger.Debugf("rotated secret '%s' is missing detailed info. adding to filtered secrets...", *secret.ItemName)
		return true
	default:
		logger.Debugf("secret '%s' is of unsupported type '%s', skipping...", *secret.ItemName, *secret.ItemType)
		return false
	}
}

func parseSecretTypes(secretTypes string) ([]SecretType, error) {
	if secretTypes == string(SecretTypesAll) || secretTypes == "" {
		return append([]SecretType(nil), supportedSecretTypes...), nil
	}

	types := strings.Split(secretTypes, ",")
	result := make([]SecretType, 0, len(types))

	typeMap := map[string]SecretType{
		"static":  SecretTypeStatic,
		"dynamic": SecretTypeDynamic,
		"rotated": SecretTypeRotated,
	}

	for _, t := range types {
		t = strings.ToLower(strings.TrimSpace(t))
		if mappedType, ok := typeMap[t]; ok {
			result = append(result, mappedType)
			continue
		}
		if t == string(SecretTypeStatic) || t == string(SecretTypeDynamic) || t == string(SecretTypeRotated) {
			result = append(result, SecretType(t))
			continue
		}
		return nil, fmt.Errorf("invalid secret type '%s', supported types: static[-secret], dynamic[-secret], rotated[-secret]", t)
	}

	seen := make(map[SecretType]bool)
	unique := make([]SecretType, 0, len(result))
	for _, t := range result {
		if !seen[t] {
			seen[t] = true
			unique = append(unique, t)
		}
	}

	return unique, nil
}

func secretTypesToStrings(types []SecretType) []string {
	result := make([]string, len(types))
	for i, t := range types {
		result[i] = string(t)
	}
	return result
}

func parseSecretTypesFromMetadata(secretTypes string) ([]string, error) {
	types, err := parseSecretTypes(secretTypes)
	if err != nil {
		return nil, err
	}
	if len(types) == 0 {
		return nil, errors.New("no secret types provided")
	}
	return secretTypesToStrings(types), nil
}
