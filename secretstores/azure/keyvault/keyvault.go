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

package keyvault

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/keyvault/azsecrets"

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
	vaultClient    *azsecrets.Client
	vaultDNSSuffix string

	logger logger.Logger
}

// NewAzureKeyvaultSecretStore returns a new Azure Key Vault secret store.
func NewAzureKeyvaultSecretStore(logger logger.Logger) secretstores.SecretStore {
	return &keyvaultSecretStore{
		vaultName:   "",
		vaultClient: nil,
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

	k.vaultName = settings.Values[componentVaultName]
	k.vaultDNSSuffix = settings.AzureEnvironment.KeyVaultDNSSuffix

	cred, err := settings.GetTokenCredential()
	if err != nil {
		return err
	}
	coreClientOpts := azcore.ClientOptions{
		Telemetry: policy.TelemetryOptions{
			ApplicationID: "dapr-" + logger.DaprVersion,
		},
	}
	k.vaultClient, err = azsecrets.NewClient(k.getVaultURI(), cred, &azsecrets.ClientOptions{
		ClientOptions: coreClientOpts,
	})
	if err != nil {
		return err
	}

	return nil
}

// GetSecret retrieves a secret using a key and returns a map of decrypted string/string values.
func (k *keyvaultSecretStore) GetSecret(req secretstores.GetSecretRequest) (secretstores.GetSecretResponse, error) {
	opts := &azsecrets.GetSecretOptions{}
	if value, ok := req.Metadata[VersionID]; ok {
		opts.Version = value
	}

	secretResp, err := k.vaultClient.GetSecret(context.TODO(), req.Name, opts)
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
	maxResults, err := k.getMaxResultsFromMetadata(req.Metadata)
	if err != nil {
		return secretstores.BulkGetSecretResponse{}, err
	}

	resp := secretstores.BulkGetSecretResponse{
		Data: map[string]map[string]string{},
	}

	secretIDPrefix := k.getVaultURI() + secretItemIDPrefix

	pager := k.vaultClient.ListPropertiesOfSecrets(nil)

out:
	for pager.More() {
		pr, err := pager.NextPage(context.TODO())
		if err != nil {
			return secretstores.BulkGetSecretResponse{}, err
		}

		for _, secret := range pr.Secrets {
			if secret.Properties == nil || secret.Properties.Enabled == nil || !*secret.Properties.Enabled {
				continue
			}

			secretName := strings.TrimPrefix(*secret.ID, secretIDPrefix)
			secretResp, err := k.vaultClient.GetSecret(context.TODO(), secretName, nil)
			if err != nil {
				return secretstores.BulkGetSecretResponse{}, err
			}

			secretValue := ""
			if secretResp.Value != nil {
				secretValue = *secretResp.Value
			}

			resp.Data[secretName] = map[string]string{secretName: secretValue}
		}

		if maxResults != nil && *maxResults > 0 && len(resp.Data) >= int(*maxResults) {
			break out
		}
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

// getVaultURI returns Azure Key Vault URI.
func (k *keyvaultSecretStore) Ping() error {
	return nil
}
