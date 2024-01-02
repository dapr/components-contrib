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

package vault_test

import (
	"context"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/dapr/components-contrib/secretstores"
	"github.com/dapr/components-contrib/tests/certification/embedded"
	"github.com/dapr/components-contrib/tests/certification/flow"
	"github.com/dapr/components-contrib/tests/certification/flow/dockercompose"
	"github.com/dapr/components-contrib/tests/certification/flow/network"
	"github.com/dapr/components-contrib/tests/certification/flow/sidecar"
)

func TestBasicSecretRetrieval(t *testing.T) {
	const (
		secretStoreComponentPath = "./components/default"
		secretStoreName          = "my-hashicorp-vault" // as set in the component YAML
	)

	currentGrpcPort, currentHttpPort := GetCurrentGRPCAndHTTPPort(t)

	// This test reuses the HashiCorp Vault's conformance test resources created using
	// .github/infrastructure/docker-compose-hashicorp-vault.yml,
	// so it reuses the tests/conformance/secretstores/secretstores.go test secrets.
	testGetKnownSecret := testKeyValuesInSecret(currentGrpcPort, secretStoreName,
		"secondsecret", map[string]string{
			"secondsecret": "efgh",
		})

	testGetMissingSecret := testSecretIsNotFound(currentGrpcPort, secretStoreName, "this_secret_is_not_there")

	flow.New(t, "Test component is up and we can retrieve some secrets").
		Step(dockercompose.Run(dockerComposeProjectName, defaultDockerComposeClusterYAML)).
		Step("Waiting for component to start...", flow.Sleep(5*time.Second)).
		Step(sidecar.Run(sidecarName,
			embedded.WithoutApp(),
			embedded.WithResourcesPath(secretStoreComponentPath),
			embedded.WithDaprGRPCPort(strconv.Itoa(currentGrpcPort)),
			embedded.WithDaprHTTPPort(strconv.Itoa(currentHttpPort)),
			componentRuntimeOptions(),
		)).
		Step("Waiting for component to load...", flow.Sleep(5*time.Second)).
		Step("Verify component is registered", testComponentFound(secretStoreName, currentGrpcPort)).
		Step("Verify no errors regarding component initialization", AssertNoInitializationErrorsForComponent(secretStoreComponentPath)).
		Step("Run basic secret retrieval test", testGetKnownSecret).
		Step("Test retrieval of secret that does not exist", testGetMissingSecret).
		Step("Interrupt network for 1 minute",
			network.InterruptNetwork(networkInstabilityTime, nil, nil, servicePortToInterrupt)).
		Step("Wait for component to recover", flow.Sleep(waitAfterInstabilityTime)).
		Step("Run basic test again to verify reconnection occurred", testGetKnownSecret).
		Step("Stop HashiCorp Vault server", dockercompose.Stop(dockerComposeProjectName, defaultDockerComposeClusterYAML)).
		Run()
}

func TestMultipleKVRetrieval(t *testing.T) {
	const (
		secretStoreComponentPath = "./components/default"
		secretStoreName          = "my-hashicorp-vault" // as set in the component YAML
	)

	currentGrpcPort, currentHttpPort := GetCurrentGRPCAndHTTPPort(t)

	flow.New(t, "Test retrieving multiple key values from a secret").
		Step(dockercompose.Run(dockerComposeProjectName, defaultDockerComposeClusterYAML)).
		Step("Waiting for component to start...", flow.Sleep(5*time.Second)).
		Step(sidecar.Run(sidecarName,
			embedded.WithoutApp(),
			embedded.WithResourcesPath(secretStoreComponentPath),
			embedded.WithDaprGRPCPort(strconv.Itoa(currentGrpcPort)),
			embedded.WithDaprHTTPPort(strconv.Itoa(currentHttpPort)),
			componentRuntimeOptions(),
		)).
		Step("Waiting for component to load...", flow.Sleep(5*time.Second)).
		Step("Verify component is registered", testComponentFound(secretStoreName, currentGrpcPort)).
		Step("Verify no errors regarding component initialization", AssertNoInitializationErrorsForComponent(secretStoreComponentPath)).
		Step("Verify component has support for multiple key-values under the same secret",
			testComponentHasFeature(currentGrpcPort, secretStoreName, secretstores.FeatureMultipleKeyValuesPerSecret)).
		Step("Test retrieval of a secret with multiple key-values",
			testKeyValuesInSecret(currentGrpcPort, secretStoreName, "multiplekeyvaluessecret", map[string]string{
				"first":  "1",
				"second": "2",
				"third":  "3",
			})).
		Step("Test secret registered under a non-default vaultKVPrefix cannot be found",
			testSecretIsNotFound(currentGrpcPort, secretStoreName, "secretUnderAlternativePrefix")).
		Step("Test secret registered with no prefix cannot be found", testSecretIsNotFound(currentGrpcPort, secretStoreName, "secretWithNoPrefix")).
		Step("Stop HashiCorp Vault server", dockercompose.Stop(dockerComposeProjectName, defaultDockerComposeClusterYAML)).
		Run()
}

func TestVaultKVPrefix(t *testing.T) {
	const (
		secretStoreComponentPath = "./components/vaultKVPrefix"
		secretStoreName          = "my-hashicorp-vault" // as set in the component YAML
	)

	currentGrpcPort, currentHttpPort := GetCurrentGRPCAndHTTPPort(t)

	flow.New(t, "Test setting a non-default vaultKVPrefix value").
		Step(dockercompose.Run(dockerComposeProjectName, defaultDockerComposeClusterYAML)).
		Step("Waiting for component to start...", flow.Sleep(5*time.Second)).
		Step(sidecar.Run(sidecarName,
			embedded.WithoutApp(),
			embedded.WithResourcesPath(secretStoreComponentPath),
			embedded.WithDaprGRPCPort(strconv.Itoa(currentGrpcPort)),
			embedded.WithDaprHTTPPort(strconv.Itoa(currentHttpPort)),
			componentRuntimeOptions(),
		)).
		Step("Waiting for component to load...", flow.Sleep(5*time.Second)).
		Step("Verify component is registered", testComponentFound(secretStoreName, currentGrpcPort)).
		Step("Verify no errors regarding component initialization", AssertNoInitializationErrorsForComponent(secretStoreComponentPath)).
		Step("Verify component has support for multiple key-values under the same secret",
			testComponentHasFeature(currentGrpcPort, secretStoreName, secretstores.FeatureMultipleKeyValuesPerSecret)).
		Step("Test retrieval of a secret under a non-default vaultKVPrefix",
			testKeyValuesInSecret(currentGrpcPort, secretStoreName, "secretUnderAlternativePrefix", map[string]string{
				"altPrefixKey": "altPrefixValue",
			})).
		Step("Test secret registered with no prefix cannot be found", testSecretIsNotFound(currentGrpcPort, secretStoreName, "secretWithNoPrefix")).
		Step("Stop HashiCorp Vault server", dockercompose.Stop(dockerComposeProjectName, defaultDockerComposeClusterYAML)).
		Run()
}

func TestVaultKVUsePrefixFalse(t *testing.T) {
	const (
		secretStoreComponentPath = "./components/vaultKVUsePrefixFalse"
		secretStoreName          = "my-hashicorp-vault" // as set in the component YAML
	)

	currentGrpcPort, currentHttpPort := GetCurrentGRPCAndHTTPPort(t)

	flow.New(t, "Test using an empty vaultKVPrefix value").
		Step(dockercompose.Run(dockerComposeProjectName, defaultDockerComposeClusterYAML)).
		Step("Waiting for component to start...", flow.Sleep(5*time.Second)).
		Step(sidecar.Run(sidecarName,
			embedded.WithoutApp(),
			embedded.WithResourcesPath(secretStoreComponentPath),
			embedded.WithDaprGRPCPort(strconv.Itoa(currentGrpcPort)),
			embedded.WithDaprHTTPPort(strconv.Itoa(currentHttpPort)),
			componentRuntimeOptions(),
		)).
		Step("Waiting for component to load...", flow.Sleep(5*time.Second)).
		Step("Verify component is registered", testComponentFound(secretStoreName, currentGrpcPort)).
		Step("Verify no errors regarding component initialization", AssertNoInitializationErrorsForComponent(secretStoreComponentPath)).
		Step("Verify component has support for multiple key-values under the same secret",
			testComponentHasFeature(currentGrpcPort, secretStoreName, secretstores.FeatureMultipleKeyValuesPerSecret)).
		Step("Test retrieval of a secret registered with no prefix and assuming vaultKVUsePrefix=false",
			testKeyValuesInSecret(currentGrpcPort, secretStoreName, "secretWithNoPrefix", map[string]string{
				"noPrefixKey": "noProblem",
			})).
		Step("Test secret registered under the default vaultKVPrefix cannot be found",
			testSecretIsNotFound(currentGrpcPort, secretStoreName, "multiplekeyvaluessecret")).
		Step("Test secret registered under a non-default vaultKVPrefix cannot be found",
			testSecretIsNotFound(currentGrpcPort, secretStoreName, "secretUnderAlternativePrefix")).
		Step("Stop HashiCorp Vault server", dockercompose.Stop(dockerComposeProjectName, defaultDockerComposeClusterYAML)).
		Run()
}

func TestVaultValueTypeText(t *testing.T) {
	const (
		secretStoreComponentPath = "./components/vaultValueTypeText"
		secretStoreName          = "my-hashicorp-vault" // as set in the component YAML
	)

	currentGrpcPort, currentHttpPort := GetCurrentGRPCAndHTTPPort(t)

	flow.New(t, "Test setting vaultValueType=text should cause it to behave with single-value semantics").
		Step(dockercompose.Run(dockerComposeProjectName, defaultDockerComposeClusterYAML)).
		Step("Waiting for component to start...", flow.Sleep(5*time.Second)).
		Step(sidecar.Run(sidecarName,
			embedded.WithoutApp(),
			embedded.WithResourcesPath(secretStoreComponentPath),
			embedded.WithDaprGRPCPort(strconv.Itoa(currentGrpcPort)),
			embedded.WithDaprHTTPPort(strconv.Itoa(currentHttpPort)),
			componentRuntimeOptions(),
		)).
		Step("Waiting for component to load...", flow.Sleep(5*time.Second)).
		Step("Verify component is registered", testComponentFound(secretStoreName, currentGrpcPort)).
		Step("Verify no errors regarding component initialization", AssertNoInitializationErrorsForComponent(secretStoreComponentPath)).
		Step("Verify component DOES NOT support  multiple key-values under the same secret",
			testComponentDoesNotHaveFeature(currentGrpcPort, secretStoreName, secretstores.FeatureMultipleKeyValuesPerSecret)).
		Step("Test secret store presents name/value semantics for secrets",
			// result has a single key with tha same name as the secret and a JSON-like content
			testKeyValuesInSecret(currentGrpcPort, secretStoreName, "secondsecret", map[string]string{
				"secondsecret": "{\"secondsecret\":\"efgh\"}",
			})).
		Step("Test secret registered under a non-default vaultKVPrefix cannot be found",
			testSecretIsNotFound(currentGrpcPort, secretStoreName, "secretUnderAlternativePrefix")).
		Step("Test secret registered with no prefix cannot be found", testSecretIsNotFound(currentGrpcPort, secretStoreName, "secretWithNoPrefix")).
		Step("Stop HashiCorp Vault server", dockercompose.Stop(dockerComposeProjectName, defaultDockerComposeClusterYAML)).
		Run()
}

func TestVaultAddr(t *testing.T) {
	fs := NewFlowSettings(t)
	fs.secretStoreComponentPathBase = "./components/vaultAddr/"
	fs.componentNamePrefix = "my-hashicorp-vault-TestVaultAddr-"

	createInitSucceedsButComponentFailsFlow(fs,
		"Verify initialization success but use failure when vaultAddr does not point to a valid vault server address",
		"wrongAddress",
		false)

	createPositiveTestFlow(fs,
		"Verify success when vaultAddr is missing and skipVerify is true and vault is using a self-signed certificate",
		"missing",
		true)

	createPositiveTestFlow(fs,
		"Verify success when vaultAddr points to a non-standard port",
		"nonStdPort",
		true)

	createInitSucceedsButComponentFailsFlow(fs,
		"Verify initialization success but use failure when vaultAddr is missing and skipVerify is true and vault is using its own self-signed certificate",
		"missingSkipVerifyFalse",
		true)
}

func TestEnginePathCustomSecretsPath(t *testing.T) {
	const (
		secretStoreComponentPathBase = "./components/enginePath/"
		componentNamePrefix          = "my-hashicorp-vault-TestEnginePath-"
		componentSuffix              = "customSecretsPath"
		componentName                = componentNamePrefix + componentSuffix
	)

	currentGrpcPort, currentHttpPort := GetCurrentGRPCAndHTTPPort(t)

	componentPath := filepath.Join(secretStoreComponentPathBase, componentSuffix)
	dockerComposeClusterYAML := filepath.Join(componentPath, "docker-compose-hashicorp-vault.yml")

	flow.New(t, "Verify success when we set enginePath to a non-std value").
		Step(dockercompose.Run(dockerComposeProjectName, dockerComposeClusterYAML)).
		Step("Waiting for component to start...", flow.Sleep(5*time.Second)).
		Step(sidecar.Run(sidecarName,
			embedded.WithoutApp(),
			embedded.WithResourcesPath(componentPath),
			embedded.WithDaprGRPCPort(strconv.Itoa(currentGrpcPort)),
			embedded.WithDaprHTTPPort(strconv.Itoa(currentHttpPort)),
			// Dapr log-level debug?
			componentRuntimeOptions(),
		)).
		Step("Waiting for component to load...", flow.Sleep(5*time.Second)).
		Step("Verify component is registered", testComponentFound(componentName, currentGrpcPort)).
		Step("Verify no errors regarding component initialization", AssertNoInitializationErrorsForComponent(componentPath)).
		Step("Verify that the custom path has secrets under it", testGetBulkSecretsWorksAndFoundKeys(currentGrpcPort, componentName)).
		Step("Verify that the custom path-specific secret is found", testKeyValuesInSecret(currentGrpcPort, componentName,
			"secretUnderCustomPath", map[string]string{
				"the":  "trick",
				"was":  "the",
				"path": "parameter",
			})).
		Step("Stop HashiCorp Vault server", dockercompose.Stop(dockerComposeProjectName, dockerComposeClusterYAML)).
		Run()
}

func TestEnginePathSecrets(t *testing.T) {
	fs := NewFlowSettings(t)
	fs.secretStoreComponentPathBase = "./components/enginePath/"
	fs.componentNamePrefix = "my-hashicorp-vault-TestEnginePath-"

	createPositiveTestFlow(fs,
		"Verify success when vaultEngine explicitly uses the secrets engine",
		"secret", false)
}

func TestCaFamilyOfFields(t *testing.T) {
	fs := NewFlowSettings(t)
	fs.secretStoreComponentPathBase = "./components/caFamily/"
	fs.componentNamePrefix = "my-hashicorp-vault-TestCaFamilyOfFields-"

	// Generate certificates and caPem/hashicorp-vault.yml
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()
	makeCmd := exec.CommandContext(ctx, "make",
		// Change to components/caFamily directory so files are generated relative to that directory
		"-C", fs.secretStoreComponentPathBase,
	)

	if out, err := makeCmd.CombinedOutput(); err != nil {
		t.Logf("Make exited with error %s", out)
		t.Fatal(err)
	}

	createPositiveTestFlow(fs,
		"Verify success when using a caCert to talk to vault with tlsServerName and enforceVerify",
		"caCert", true)

	createPositiveTestFlow(fs,
		"Verify success when using a caPath to talk to vault with tlsServerName and enforceVerify",
		"caCert", true)

	createPositiveTestFlow(fs,
		"Verify success when using a caPem to talk to vault with tlsServerName and enforceVerify",
		"caPem", true)

	createInitSucceedsButComponentFailsFlow(fs,
		"Verify successful initialization but secret retrieval failure when `caPem` is set to a valid server certificate (baseline) but `tlsServerName` does not match the server name",
		"badTlsServerName", true)

	createInitSucceedsButComponentFailsFlow(fs,
		"Verify successful initialization but secret retrieval failure when `caPem` is set to an invalid server certificate (flag under test) despite `tlsServerName` matching the server name ",
		"badCaCert", true)

	createPositiveTestFlow(fs,
		"Verify success when using a caPem is invalid but skipVerify is on",
		"badCaCertAndSkipVerify", true)
}

func TestVersioning(t *testing.T) {
	const (
		componentPath = "./components/versioning/"
		componentName = "my-hashicorp-vault-TestVersioning"
	)
	dockerComposeClusterYAML := filepath.Join(componentPath, "docker-compose-hashicorp-vault.yml")

	currentGrpcPort, currentHttpPort := GetCurrentGRPCAndHTTPPort(t)

	flow.New(t, "Verify success on retrieval of a past version of a secret").
		Step(dockercompose.Run(dockerComposeProjectName, dockerComposeClusterYAML)).
		Step("Waiting for component to start...", flow.Sleep(5*time.Second)).
		Step(sidecar.Run(sidecarName,
			embedded.WithoutApp(),
			embedded.WithResourcesPath(componentPath),
			embedded.WithDaprGRPCPort(strconv.Itoa(currentGrpcPort)),
			embedded.WithDaprHTTPPort(strconv.Itoa(currentHttpPort)),
			// Dapr log-level debug?
			componentRuntimeOptions(),
		)).
		Step("Waiting for component to load...", flow.Sleep(5*time.Second)).
		Step("Verify component is registered", testComponentFound(componentName, currentGrpcPort)).
		Step("Verify no errors regarding component initialization", AssertNoInitializationErrorsForComponent(componentPath)).
		Step("Verify that we can list secrets", testGetBulkSecretsWorksAndFoundKeys(currentGrpcPort, componentName)).
		Step("Verify that the latest version of the secret is there", testKeyValuesInSecret(currentGrpcPort, componentName,
			"secretUnderTest", map[string]string{
				"versionedKey": "latestValue",
			})).
		Step("Verify that a past version of the secret is there", testKeyValuesInSecret(currentGrpcPort, componentName,
			"secretUnderTest", map[string]string{
				"versionedKey": "secondVersion",
			}, "2")).
		Step("Stop HashiCorp Vault server", dockercompose.Stop(dockerComposeProjectName, dockerComposeClusterYAML)).
		Run()
}
