// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package keyvault

import (
	"context"
	"fmt"

	"github.com/dapr/components-contrib/secretstores"
	"github.com/dapr/dapr/pkg/logger"

	kv "github.com/Azure/azure-sdk-for-go/profiles/latest/keyvault/keyvault"
	"github.com/Azure/go-autorest/autorest"
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

	certFilePath := metadata.Properties[componentSPNCertificateFile]
	certBytes := []byte(metadata.Properties[componentSPNCertificate])
	certPassword := metadata.Properties[componentSPNCertificatePassword]
	clientId := metadata.Properties[componentSPNClientID]
	tenantId := metadata.Properties[componentSPNTenantID]

	var authorizer autorest.Authorizer
	var err error
	if certFilePath != "" && len(certBytes) > 0 && certPassword != "" && clientId != "" && tenantId != "" {
		// SPN configured
		auth := NewClientAuthorizer(
			certFilePath,
			certBytes,
			certPassword,
			clientId,
			tenantId)

		authorizer, err = auth.Authorizer()
		if err != nil {
			return fmt.Errorf("error creating client certificate authorizer: %s", err)
		}
	} else {
		// Try managed identity
		authorizer, err = settings.GetAuthorizer()
		if err != nil {
			return fmt.Errorf("error creating managed identity authorizer: %s", err)
		}
	}
	k.vaultClient.Authorizer = authorizer
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

// getVaultURI returns Azure Key Vault URI
func (k *keyvaultSecretStore) getVaultURI() string {
	return fmt.Sprintf("https://%s.vault.azure.net", k.vaultName)
}
