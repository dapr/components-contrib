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
	"reflect"
	"strconv"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/security/keyvault/azsecrets"

	azauth "github.com/dapr/components-contrib/common/authentication/azure"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/secretstores"
	"github.com/dapr/kit/logger"
	kitmd "github.com/dapr/kit/metadata"
)

// Keyvault secret store component metadata properties
// This is in addition to what's defined in authentication/azure.
const (
	VersionID          = "version_id"
	secretItemIDPrefix = "/secrets/"
)

var _ secretstores.SecretStore = (*keyvaultSecretStore)(nil)

type keyvaultSecretStore struct {
	vaultName      string
	vaultClient    *azsecrets.Client
	vaultDNSSuffix string

	logger logger.Logger
}

type KeyvaultMetadata struct {
	VaultName string
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
func (k *keyvaultSecretStore) Init(_ context.Context, meta secretstores.Metadata) error {
	m := KeyvaultMetadata{}
	if err := kitmd.DecodeMetadata(meta.Properties, &m); err != nil {
		return err
	}
	// Fix for maintaining backwards compatibility with a change introduced in 1.3 that allowed specifying an Azure environment by setting a FQDN for vault name
	// This should be considered deprecated and users should rely the "azureEnvironment" metadata instead, but it's maintained here for backwards-compatibility
	if m.VaultName != "" {
		keyVaultSuffixToEnvironment := map[string]string{
			".vault.azure.net":         "AzurePublicCloud",
			".vault.azure.cn":          "AzureChinaCloud",
			".vault.usgovcloudapi.net": "AzureUSGovernmentCloud",
		}
		for suffix, environment := range keyVaultSuffixToEnvironment {
			if strings.HasSuffix(m.VaultName, suffix) {
				meta.Properties["azureEnvironment"] = environment
				m.VaultName = strings.TrimPrefix(strings.TrimSuffix(m.VaultName, suffix), "https://")
				k.vaultName = m.VaultName
				break
			}
		}
	}

	// Initialization code
	settings, err := azauth.NewEnvironmentSettings(meta.Properties)
	if err != nil {
		return err
	}

	k.vaultName = m.VaultName
	k.vaultDNSSuffix = settings.EndpointSuffix(azauth.ServiceAzureKeyVault)

	cred, err := settings.GetTokenCredential()
	if err != nil {
		return err
	}
	coreClientOpts := azcore.ClientOptions{
		Telemetry: policy.TelemetryOptions{
			ApplicationID: "dapr-" + logger.DaprVersion,
		},
	}
	client, clientErr := azsecrets.NewClient(k.getVaultURI(), cred, &azsecrets.ClientOptions{
		ClientOptions: coreClientOpts,
	})
	k.vaultClient = client
	return clientErr
}

// GetSecret retrieves a secret using a key and returns a map of decrypted string/string values.
func (k *keyvaultSecretStore) GetSecret(ctx context.Context, req secretstores.GetSecretRequest) (secretstores.GetSecretResponse, error) {
	version := "" // empty string means latest version
	if val, ok := req.Metadata[VersionID]; ok {
		version = val
	}

	secretResp, err := k.vaultClient.GetSecret(ctx, req.Name, version, nil)
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
func (k *keyvaultSecretStore) BulkGetSecret(ctx context.Context, req secretstores.BulkGetSecretRequest) (secretstores.BulkGetSecretResponse, error) {
	maxResults, err := k.getMaxResultsFromMetadata(req.Metadata)
	if err != nil {
		return secretstores.BulkGetSecretResponse{}, err
	}

	resp := secretstores.BulkGetSecretResponse{
		Data: map[string]map[string]string{},
	}

	secretIDPrefix := k.getVaultURI() + secretItemIDPrefix

	pager := k.vaultClient.NewListSecretPropertiesPager(nil)

out:
	for pager.More() {
		pr, err := pager.NextPage(ctx)
		if err != nil {
			return secretstores.BulkGetSecretResponse{}, err
		}

		for _, secret := range pr.Value {
			if secret.Attributes == nil || secret.Attributes.Enabled == nil || !*secret.Attributes.Enabled {
				continue
			}

			secretName := strings.TrimPrefix(secret.ID.Name(), secretIDPrefix)
			secretResp, err := k.vaultClient.GetSecret(ctx, secretName, "", nil) // empty string means latest version
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
		val, err := strconv.Atoi(s)
		if err != nil {
			return nil, err
		}
		converted := int32(val) //nolint:gosec

		return &converted, nil
	}

	return nil, nil
}

// Features returns the features available in this secret store.
func (k *keyvaultSecretStore) Features() []secretstores.Feature {
	return []secretstores.Feature{} // No Feature supported.
}

func (k *keyvaultSecretStore) GetComponentMetadata() (metadataInfo metadata.MetadataMap) {
	metadataStruct := KeyvaultMetadata{}
	metadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, metadata.SecretStoreType)
	return
}

func (k *keyvaultSecretStore) Close() error {
	return nil
}
