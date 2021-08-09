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
	"github.com/dapr/components-contrib/secretstores"
	"github.com/dapr/kit/logger"
)

// Keyvault secret store component metadata properties
const (
	componentSPNCertificate         = "spnCertificate"
	componentSPNCertificateFile     = "spnCertificateFile"
	componentSPNCertificatePassword = "spnCertificatePassword"
	componentSPNClientID            = "spnClientId"
	componentSPNTenantID            = "spnTenantId"
	componentVaultName              = "vaultName"
	VersionID                       = "version_id"
	secretItemIDPrefix              = "/secrets/"

	// AzureCloud urls refer to https://docs.microsoft.com/en-us/azure/key-vault/general/about-keys-secrets-certificates#dns-suffixes-for-base-url
	AzureCloud       = ".vault.azure.net"
	AzureChinaCloud  = ".vault.azure.cn"
	AzureUSGov       = ".vault.usgovcloudapi.net"
	AzureGermanCloud = ".vault.microsoftazure.de"
	https            = "https://"
)

type keyvaultSecretStore struct {
	vaultName   string
	vaultClient kv.BaseClient

	logger logger.Logger
}

// NewAzureKeyvaultSecretStore returns a new Kubernetes secret store
func New(logger logger.Logger) secretstores.SecretStore {
	return NewAzureKeyvaultSecretStore(logger)
}

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

// BulkGetSecret retrieves all secrets in the store and returns a map of decrypted string/string values
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

// getVaultURI returns Azure Key Vault URI
func (k *keyvaultSecretStore) getVaultURI() string {
	for _, suffix := range []string{AzureCloud, AzureChinaCloud, AzureGermanCloud, AzureUSGov} {
		if strings.HasSuffix(k.vaultName, suffix) {
			if strings.HasPrefix(k.vaultName, https) {
				return k.vaultName
			}

			return fmt.Sprintf("%s%s", https, k.vaultName)
		}
	}

	return fmt.Sprintf("%s%s%s", https, k.vaultName, AzureCloud)
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
