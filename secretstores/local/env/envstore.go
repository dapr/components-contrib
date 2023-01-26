/*
Copyright 2021 The Dapr Authors
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

package env

import (
	"context"
	"os"
	"reflect"
	"strings"

	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/secretstores"
	"github.com/dapr/kit/logger"
)

var _ secretstores.SecretStore = (*envSecretStore)(nil)

type envSecretStore struct {
	logger logger.Logger
}

// NewEnvSecretStore returns a new env var secret store.
func NewEnvSecretStore(logger logger.Logger) secretstores.SecretStore {
	return &envSecretStore{
		logger: logger,
	}
}

// Init creates a Local secret store.
func (s *envSecretStore) Init(ctx context.Context, metadata secretstores.Metadata) error {
	return nil
}

// GetSecret retrieves a secret from env var using provided key.
func (s *envSecretStore) GetSecret(ctx context.Context, req secretstores.GetSecretRequest) (secretstores.GetSecretResponse, error) {
	var value string
	if s.isKeyAllowed(req.Name) {
		value = os.Getenv(req.Name)
	} else {
		s.logger.Warnf("Access to env var %s is forbidden", req.Name)
	}
	return secretstores.GetSecretResponse{
		Data: map[string]string{
			req.Name: value,
		},
	}, nil
}

// BulkGetSecret retrieves all secrets in the store and returns a map of string/string values.
func (s *envSecretStore) BulkGetSecret(ctx context.Context, req secretstores.BulkGetSecretRequest) (secretstores.BulkGetSecretResponse, error) {
	env := os.Environ()
	r := make(map[string]map[string]string, len(env))

	for _, element := range env {
		envVariable := strings.SplitN(element, "=", 2)
		if s.isKeyAllowed(envVariable[0]) {
			r[envVariable[0]] = map[string]string{envVariable[0]: envVariable[1]}
		}
	}

	return secretstores.BulkGetSecretResponse{
		Data: r,
	}, nil
}

// Features returns the features available in this secret store.
func (s *envSecretStore) Features() []secretstores.Feature {
	return []secretstores.Feature{} // No Feature supported.
}

func (s *envSecretStore) GetComponentMetadata() map[string]string {
	type unusedMetadataStruct struct{}
	metadataStruct := unusedMetadataStruct{}
	metadataInfo := map[string]string{}
	metadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo)
	return metadataInfo
}

func (s *envSecretStore) isKeyAllowed(key string) bool {
	switch key {
	case "APP_API_TOKEN", "DAPR_API_TOKEN",
		"DAPR_TRUST_ANCHORS", "DAPR_CERT_CHAIN", "DAPR_CERT_KEY":
		return false
	default:
		return true
	}
}
