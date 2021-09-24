// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package env

import (
	"os"
	"strings"

	"github.com/dapr/components-contrib/secretstores"
	"github.com/dapr/kit/logger"
)

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
func (s *envSecretStore) Init(metadata secretstores.Metadata) error {
	return nil
}

// GetSecret retrieves a secret from env var using provided key.
func (s *envSecretStore) GetSecret(req secretstores.GetSecretRequest) (secretstores.GetSecretResponse, error) {
	return secretstores.GetSecretResponse{
		Data: map[string]string{
			req.Name: os.Getenv(req.Name),
		},
	}, nil
}

// BulkGetSecret retrieves all secrets in the store and returns a map of decrypted string/string values.
func (s *envSecretStore) BulkGetSecret(req secretstores.BulkGetSecretRequest) (secretstores.BulkGetSecretResponse, error) {
	r := map[string]map[string]string{}

	for _, element := range os.Environ() {
		envVariable := strings.SplitN(element, "=", 2)
		r[envVariable[0]] = map[string]string{envVariable[0]: envVariable[1]}
	}

	return secretstores.BulkGetSecretResponse{
		Data: r,
	}, nil
}
