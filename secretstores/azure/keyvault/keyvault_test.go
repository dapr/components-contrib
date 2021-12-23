// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------
package keyvault

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/dapr/components-contrib/secretstores"
	"github.com/dapr/kit/logger"
)

func TestInit(t *testing.T) {
	m := secretstores.Metadata{}
	s := NewAzureKeyvaultSecretStore(logger.NewLogger("test"))
	t.Run("Init with valid metadata", func(t *testing.T) {
		m.Properties = map[string]string{
			"vaultName":         "foo",
			"azureTenantId":     "00000000-0000-0000-0000-000000000000",
			"azureClientId":     "00000000-0000-0000-0000-000000000000",
			"azureClientSecret": "passw0rd",
		}
		err := s.Init(m)
		assert.Nil(t, err)
		kv, ok := s.(*keyvaultSecretStore)
		assert.True(t, ok)
		assert.Equal(t, kv.vaultName, "foo")
		assert.Equal(t, kv.vaultDNSSuffix, "vault.azure.net")
		assert.NotNil(t, kv.vaultClient)
	})
	t.Run("Init with valid metadata and Azure environment", func(t *testing.T) {
		m.Properties = map[string]string{
			"vaultName":         "foo",
			"azureTenantId":     "00000000-0000-0000-0000-000000000000",
			"azureClientId":     "00000000-0000-0000-0000-000000000000",
			"azureClientSecret": "passw0rd",
			"azureEnvironment":  "AZURECHINACLOUD",
		}
		err := s.Init(m)
		assert.Nil(t, err)
		kv, ok := s.(*keyvaultSecretStore)
		assert.True(t, ok)
		assert.Equal(t, kv.vaultName, "foo")
		assert.Equal(t, kv.vaultDNSSuffix, "vault.azure.cn")
		assert.NotNil(t, kv.vaultClient)
	})
	t.Run("Init with Azure environment as part of vaultName FQDN (1) - legacy", func(t *testing.T) {
		m.Properties = map[string]string{
			"vaultName":         "foo.vault.azure.cn",
			"azureTenantId":     "00000000-0000-0000-0000-000000000000",
			"azureClientId":     "00000000-0000-0000-0000-000000000000",
			"azureClientSecret": "passw0rd",
		}
		err := s.Init(m)
		assert.Nil(t, err)
		kv, ok := s.(*keyvaultSecretStore)
		assert.True(t, ok)
		assert.Equal(t, kv.vaultName, "foo")
		assert.Equal(t, kv.vaultDNSSuffix, "vault.azure.cn")
		assert.NotNil(t, kv.vaultClient)
	})
	t.Run("Init with Azure environment as part of vaultName FQDN (2) - legacy", func(t *testing.T) {
		m.Properties = map[string]string{
			"vaultName":         "https://foo.vault.usgovcloudapi.net",
			"azureTenantId":     "00000000-0000-0000-0000-000000000000",
			"azureClientId":     "00000000-0000-0000-0000-000000000000",
			"azureClientSecret": "passw0rd",
		}
		err := s.Init(m)
		assert.Nil(t, err)
		kv, ok := s.(*keyvaultSecretStore)
		assert.True(t, ok)
		assert.Equal(t, kv.vaultName, "foo")
		assert.Equal(t, kv.vaultDNSSuffix, "vault.usgovcloudapi.net")
		assert.NotNil(t, kv.vaultClient)
	})
}
