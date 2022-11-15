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

func TestGetFeatures(t *testing.T) {
	s := NewAzureKeyvaultSecretStore(logger.NewLogger("test"))
	// Yes, we are skipping initialization as feature retrieval doesn't depend on it.
	t.Run("no features are advertised", func(t *testing.T) {
		f := s.Features()
		assert.Empty(t, f)
	})
}
