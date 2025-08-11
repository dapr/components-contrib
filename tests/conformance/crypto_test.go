//go:build conftests
// +build conftests

/*
Copyright 2023 The Dapr Authors
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

	contribCrypto "github.com/dapr/components-contrib/crypto"
	cr_azurekeyvault "github.com/dapr/components-contrib/crypto/azure/keyvault"
	cr_jwks "github.com/dapr/components-contrib/crypto/jwks"
	cr_localstorage "github.com/dapr/components-contrib/crypto/localstorage"
	conf_crypto "github.com/dapr/components-contrib/tests/conformance/crypto"
)

func TestCryptoConformance(t *testing.T) {
	const configPath = "../config/crypto/"
	tc, err := NewTestConfiguration(filepath.Join(configPath, "tests.yml"))
	require.NoError(t, err)
	require.NotNil(t, tc)

	tc.TestFn = func(comp *TestComponent) func(t *testing.T) {
		return func(t *testing.T) {
			ParseConfigurationMap(t, comp.Config)

			componentConfigPath := convertComponentNameToPath(comp.Component, comp.Profile)
			props, err := loadComponentsAndProperties(t, filepath.Join(configPath, componentConfigPath))
			require.NoErrorf(t, err, "error running conformance test for component %s", comp.Component)

			component := loadCryptoProvider(comp.Component)
			require.NotNil(t, component, "error running conformance test for component %s", comp.Component)

			cryptoConfig, err := conf_crypto.NewTestConfig(comp.Component, comp.Operations, comp.Config)
			require.NoErrorf(t, err, "error running conformance test for component %s", comp.Component)

			conf_crypto.ConformanceTests(t, props, component, cryptoConfig)
		}
	}

	tc.Run(t)
}

func loadCryptoProvider(name string) contribCrypto.SubtleCrypto {
	switch name {
	case "azure.keyvault":
		return cr_azurekeyvault.NewAzureKeyvaultCrypto(testLogger)
	case "localstorage":
		return cr_localstorage.NewLocalStorageCrypto(testLogger)
	case "jwks":
		return cr_jwks.NewJWKSCrypto(testLogger)
	default:
		return nil
	}
}
