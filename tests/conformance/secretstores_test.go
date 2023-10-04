//go:build conftests
// +build conftests

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

package conformance

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/secretstores"
	ss_azure "github.com/dapr/components-contrib/secretstores/azure/keyvault"
	ss_hashicorp_vault "github.com/dapr/components-contrib/secretstores/hashicorp/vault"
	ss_kubernetes "github.com/dapr/components-contrib/secretstores/kubernetes"
	ss_local_env "github.com/dapr/components-contrib/secretstores/local/env"
	ss_local_file "github.com/dapr/components-contrib/secretstores/local/file"
	conf_secret "github.com/dapr/components-contrib/tests/conformance/secretstores"
)

func TestSecretStoreConformance(t *testing.T) {
	const configPath = "../config/secretstores/"
	tc, err := NewTestConfiguration(filepath.Join(configPath, "tests.yml"))
	require.NoError(t, err)
	require.NotNil(t, tc)

	tc.TestFn = func(comp *TestComponent) func(t *testing.T) {
		return func(t *testing.T) {
			ParseConfigurationMap(t, comp.Config)

			componentConfigPath := convertComponentNameToPath(comp.Component, comp.Profile)
			props, err := loadComponentsAndProperties(t, filepath.Join(configPath, componentConfigPath))
			require.NoErrorf(t, err, "error running conformance test for component %s", comp.Component)

			store := loadSecretStore(comp.Component)
			require.NotNilf(t, store, "error running conformance test for component %s", comp.Component)

			storeConfig := conf_secret.NewTestConfig(comp.Component, comp.Operations)

			conf_secret.ConformanceTests(t, props, store, storeConfig)
		}
	}

	tc.Run(t)
}

func loadSecretStore(name string) secretstores.SecretStore {
	switch name {
	case "azure.keyvault.certificate":
		return ss_azure.NewAzureKeyvaultSecretStore(testLogger)
	case "azure.keyvault.serviceprincipal":
		return ss_azure.NewAzureKeyvaultSecretStore(testLogger)
	case "kubernetes":
		return ss_kubernetes.NewKubernetesSecretStore(testLogger)
	case "local.env":
		return ss_local_env.NewEnvSecretStore(testLogger)
	case "local.file":
		return ss_local_file.NewLocalSecretStore(testLogger)
	case "hashicorp.vault":
		return ss_hashicorp_vault.NewHashiCorpVaultSecretStore(testLogger)
	default:
		return nil
	}
}
