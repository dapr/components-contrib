// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package keyvault

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	kv "github.com/Azure/azure-sdk-for-go/profiles/latest/keyvault/keyvault"

	azauth "github.com/dapr/components-contrib/authentication/azure"
	"github.com/dapr/components-contrib/secretstores"
	"github.com/dapr/kit/logger"
)

// Keyvault secret store component metadata properties
// This is in addition to what's defined in authentication/azure.
const (
	componentVaultName = "vaultName"
	VersionID          = "version_id"
	secretItemIDPrefix = "/secrets/"
)

type keyvaultSecretStore struct {
	vaultName      string
	vaultClient    kv.BaseClient
	vaultDNSSuffix string

	logger logger.Logger
}

// NewAzureKeyvaultSecretStore returns a new Azure Key Vault secret store.
func NewAzureKeyvaultSecretStore(logger logger.Logger) secretstores.SecretStore {
	return &keyvaultSecretStore{
		vaultName:   "",
		vaultClient: kv.New(),
		logger:      logger,
	}
}

// Init creates a Azure Key Vault client.
func (k *keyvaultSecretStore) Init(metadata secretstores.Metadata) error {
	// Fix for maintaining backwards compatibility with a change introduced in 1.3 that allowed specifying an Azure environment by setting a FQDN for vault name
	// This should be considered deprecated and users should rely the "azureEnvironment" metadata instead, but it's maintained here for backwards-compatibility
	if vaultName, ok := metadata.Properties[componentVaultName]; ok {
		keyVaultSuffixToEnvironment := map[string]string{
			".vault.azure.net":         "AZUREPUBLICCLOUD",
			".vault.azure.cn":          "AZURECHINACLOUD",
			".vault.usgovcloudapi.net": "AZUREUSGOVERNMENTCLOUD",
			".vault.microsoftazure.de": "AZUREGERMANCLOUD",
		}
		for suffix, environment := range keyVaultSuffixToEnvironment {
			if strings.HasSuffix(vaultName, suffix) {
				metadata.Properties["azureEnvironment"] = environment
				vaultName = strings.TrimSuffix(vaultName, suffix)
				if strings.HasPrefix(vaultName, "https://") {
					vaultName = strings.TrimPrefix(vaultName, "https://")
				}
				metadata.Properties[componentVaultName] = vaultName

				break
			}
		}
	}

	// Initialization code
	settings, err := azauth.NewEnvironmentSettings("keyvault", metadata.Properties)
	if err != nil {
		return err
	}

	authorizer, err := settings.GetAuthorizer()
	if err == nil {
		k.vaultClient.Authorizer = authorizer
	}

	k.vaultName = settings.Values[componentVaultName]
	k.vaultDNSSuffix = settings.AzureEnvironment.KeyVaultDNSSuffix

	return err
}

// GetSecret retrieves a secret using a key and returns a map of decrypted string/string values.
func (k *keyvaultSecretStore) GetSecret(req secretstores.GetSecretRequest) (secretstores.GetSecretResponse, error) {
	versionID := ""
	if value, ok := req.Metadata[VersionID]; ok {
		versionID = value
	}

	secretResp, err := k.vaultClient.GetSecret(context.Background(), k.getVaultURI(), req.Name, versionID)
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

// BulkGetSecret retrieves all secrets in the store and returns a map of decrypted string/string values.
func (k *keyvaultSecretStore) BulkGetSecret(req secretstores.BulkGetSecretRequest) (secretstores.BulkGetSecretResponse, error) {
	vaultURI := k.getVaultURI()

	maxResults, err := k.getMaxResultsFromMetadata(req.Metadata)
	if err != nil {
		return secretstores.BulkGetSecretResponse{}, err
	}

	secretsResp, err := k.vaultClient.GetSecretsComplete(context.Background(), vaultURI, maxResults)
	if err != nil {
		return secretstores.BulkGetSecretResponse{}, err
	}

	resp := secretstores.BulkGetSecretResponse{
		Data: map[string]map[string]string{},
	}

	secretIDPrefix := vaultURI + secretItemIDPrefix

	for secretsResp.NotDone() {
		secretEnabled := secretsResp.Value().Attributes.Enabled
		if *secretEnabled {
			secretItem := secretsResp.Value()
			secretName := strings.TrimPrefix(*secretItem.ID, secretIDPrefix)

			secretResp, err := k.vaultClient.GetSecret(context.Background(), vaultURI, secretName, "")
			if err != nil {
				return secretstores.BulkGetSecretResponse{}, err
			}

			secretValue := ""
			if secretResp.Value != nil {
				secretValue = *secretResp.Value
			}

			resp.Data[secretName] = map[string]string{secretName: secretValue}
		}

		secretsResp.NextWithContext(context.Background())
	}

	return resp, nil
}

// getVaultURI returns Azure Key Vault URI.
func (k *keyvaultSecretStore) getVaultURI() string {
	return fmt.Sprintf("https://%s.%s", k.vaultName, k.vaultDNSSuffix)
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
