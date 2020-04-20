// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package keyvault

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	kv "github.com/Azure/azure-sdk-for-go/profiles/latest/keyvault/keyvault"
	"github.com/dapr/components-contrib/secretstores"
	"github.com/dapr/dapr/pkg/logger"
)

// Keyvault secret store component metadata properties
const (
	componentSPNCertificate         = "spnCertificate"
	componentSPNCertificateFile     = "spnCertificateFile"
	componentSPNCertificatePassword = "spnCertificatePassword"
	componentSPNClientID            = "spnClientId"
	componentSPNTenantID            = "spnTenantId"
	componentVaultName              = "vaultName"
)

type keyvaultSecretStore struct {
	vaultName   string
	vaultClient kv.BaseClient

	logger logger.Logger
}

// NewAzureKeyvaultSecretStore returns a new Kubernetes secret store
func NewAzureKeyvaultSecretStore(logger logger.Logger) secretstores.SecretStore {
	return &keyvaultSecretStore{
		vaultName:   "",
		vaultClient: kv.New(),
		logger:      logger,
	}
}

// Init creates a Kubernetes client
func (k *keyvaultSecretStore) Init(metadata secretstores.Metadata) error {
	settings := EnvironmentSettings{
		Values: metadata.Properties,
	}

	authorizer, err := settings.GetAuthorizer()
	if err == nil {
		k.vaultClient.Authorizer = authorizer
	}

	k.vaultName = settings.Values[componentVaultName]

	return err
}

// GetSecret retrieves a secret using a key and returns a map of decrypted string/string values
func (k *keyvaultSecretStore) GetSecret(req secretstores.GetSecretRequest) (secretstores.GetSecretResponse, error) {
	secretResp, err := k.vaultClient.GetSecret(context.Background(), k.getVaultURI(), req.Name, "")
	if err != nil {
		return secretstores.GetSecretResponse{}, err
	}

	secretValue := ""
	if secretResp.Value != nil {
		secretValue = *secretResp.Value
	}

	return secretstores.GetSecretResponse{
		Data: map[string]string{
			req.Name: secretValue,
		},
	}, nil
}

// BulkGetSecret retrieves all secrets in the store and returns a map of decrypted string/string values
func (k *keyvaultSecretStore) BulkGetSecret(req secretstores.BulkGetSecretRequest) (secretstores.GetSecretResponse, error) {
	vaultURI := k.getVaultURI()

	maxResults, err := k.getMaxResultsFromMetadata(req.Metadata)
	if err != nil {
		return secretstores.GetSecretResponse{}, err
	}

	secretsResp, err := k.vaultClient.GetSecretsComplete(context.Background(), vaultURI, maxResults)
	if err != nil {
		return secretstores.GetSecretResponse{}, err
	}

	resp := secretstores.GetSecretResponse{
		Data: map[string]string{},
	}

	for secretsResp.NotDone() {
		secretItem := secretsResp.Value()
		secretName := strings.TrimPrefix(*secretItem.ID, vaultURI)

		secretResp, err := k.vaultClient.GetSecret(context.Background(), vaultURI, secretName, "")
		if err != nil {
			return secretstores.GetSecretResponse{}, err
		}

		secretValue := ""
		if secretResp.Value != nil {
			secretValue = *secretResp.Value
		}

		resp.Data[secretName] = secretValue

		secretsResp.NextWithContext(context.Background())
	}

	return resp, nil
}

// getVaultURI returns Azure Key Vault URI
func (k *keyvaultSecretStore) getVaultURI() string {
	return fmt.Sprintf("https://%s.vault.azure.net", k.vaultName)
}

func (k *keyvaultSecretStore) getMaxResultsFromMetadata(metadata map[string]string) (*int32, error) {
	if s, ok := metadata["maxresults"]; ok && s != "" {
		/* #nosec */
		val, err := strconv.Atoi(s)
		if err != nil {
			return nil, err
		}
		converted := int32(val)

		return &converted, nil
	}

	return nil, nil
}
