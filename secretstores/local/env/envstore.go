// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package env

import (
	"os"

	"github.com/dapr/components-contrib/secretstores"
	"github.com/dapr/dapr/pkg/logger"
)

type envSecretStore struct {
	logger logger.Logger
}

// NewEnvSecretStore returns a new env var secret store
func NewEnvSecretStore(logger logger.Logger) secretstores.SecretStore {
	return &envSecretStore{
		logger: logger,
	}
}

// Init creates a Local secret store
func (s *envSecretStore) Init(metadata secretstores.Metadata) error {
	return nil
}

// GetSecret retrieves a secret from env var using provided key
func (s *envSecretStore) GetSecret(req secretstores.GetSecretRequest) (secretstores.GetSecretResponse, error) {
	return secretstores.GetSecretResponse{
		Data: map[string]string{
			req.Name: os.Getenv("req.Name"),
		},
	}, nil
}
