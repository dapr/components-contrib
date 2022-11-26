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
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dapr/components-contrib/secretstores"
	"github.com/dapr/components-contrib/secretstores/hashicorp/vault"
	"github.com/dapr/components-contrib/tests/certification/embedded"
	"github.com/dapr/components-contrib/tests/certification/flow"
	"github.com/dapr/components-contrib/tests/certification/flow/dockercompose"
	"github.com/dapr/components-contrib/tests/certification/flow/network"
	"github.com/dapr/components-contrib/tests/certification/flow/sidecar"
	secretstores_loader "github.com/dapr/dapr/pkg/components/secretstores"
	"github.com/dapr/dapr/pkg/runtime"
	dapr_testing "github.com/dapr/dapr/pkg/testing"
	"github.com/dapr/go-sdk/client"
	"github.com/dapr/kit/logger"
	"github.com/stretchr/testify/assert"

	"github.com/golang/protobuf/ptypes/empty"
)

const (
	sidecarName                     = "hashicorp-vault-sidecar"
	defaultDockerComposeClusterYAML = "../../../../../.github/infrastructure/docker-compose-hashicorp-vault.yml"
	dockerComposeProjectName        = "hashicorp-vault"
	// secretStoreName          = "my-hashicorp-vault" // as set in the component YAML

	networkInstabilityTime   = 1 * time.Minute
	waitAfterInstabilityTime = networkInstabilityTime / 4
	servicePortToInterrupt   = "8200"
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
			embedded.WithComponentsPath(secretStoreComponentPath),
			embedded.WithDaprGRPCPort(currentGrpcPort),
			embedded.WithDaprHTTPPort(currentHttpPort),
			componentRuntimeOptions(),
		)).
		Step("Waiting for component to load...", flow.Sleep(5*time.Second)).
		Step("Verify component is registered", testComponentFound(secretStoreName, currentGrpcPort)).
		Step("Verify no errors regarding component initialization", assertNoInitializationErrorsForComponent(secretStoreComponentPath)).
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
			embedded.WithComponentsPath(secretStoreComponentPath),
			embedded.WithDaprGRPCPort(currentGrpcPort),
			embedded.WithDaprHTTPPort(currentHttpPort),
			componentRuntimeOptions(),
		)).
		Step("Waiting for component to load...", flow.Sleep(5*time.Second)).
		Step("Verify component is registered", testComponentFound(secretStoreName, currentGrpcPort)).
		Step("Verify no errors regarding component initialization", assertNoInitializationErrorsForComponent(secretStoreComponentPath)).
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
			embedded.WithComponentsPath(secretStoreComponentPath),
			embedded.WithDaprGRPCPort(currentGrpcPort),
			embedded.WithDaprHTTPPort(currentHttpPort),
			componentRuntimeOptions(),
		)).
		Step("Waiting for component to load...", flow.Sleep(5*time.Second)).
		Step("Verify component is registered", testComponentFound(secretStoreName, currentGrpcPort)).
		Step("Verify no errors regarding component initialization", assertNoInitializationErrorsForComponent(secretStoreComponentPath)).
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
			embedded.WithComponentsPath(secretStoreComponentPath),
			embedded.WithDaprGRPCPort(currentGrpcPort),
			embedded.WithDaprHTTPPort(currentHttpPort),
			componentRuntimeOptions(),
		)).
		Step("Waiting for component to load...", flow.Sleep(5*time.Second)).
		Step("Verify component is registered", testComponentFound(secretStoreName, currentGrpcPort)).
		Step("Verify no errors regarding component initialization", assertNoInitializationErrorsForComponent(secretStoreComponentPath)).
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
			embedded.WithComponentsPath(secretStoreComponentPath),
			embedded.WithDaprGRPCPort(currentGrpcPort),
			embedded.WithDaprHTTPPort(currentHttpPort),
			componentRuntimeOptions(),
		)).
		Step("Waiting for component to load...", flow.Sleep(5*time.Second)).
		Step("Verify component is registered", testComponentFound(secretStoreName, currentGrpcPort)).
		Step("Verify no errors regarding component initialization", assertNoInitializationErrorsForComponent(secretStoreComponentPath)).
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

func TestTokenAndTokenMountPath(t *testing.T) {
	fs := NewFlowSettings(t)
	fs.secretStoreComponentPathBase = "./components/vaultTokenAndTokenMountPath/"
	fs.componentNamePrefix = "my-hashicorp-vault-TestTokenAndTokenMountPath-"

	createNegativeTestFlow(fs,
		"Verify component initialization failure when BOTH vaultToken and vaultTokenMountPath are present",
		"both",
		"token mount path and token both set")

	createNegativeTestFlow(fs,
		"Verify component initialization failure when NEITHER vaultToken nor vaultTokenMountPath are present",
		"neither",
		"token mount path and token not set")

	createNegativeTestFlow(fs,
		"Verify component initialization failure when vaultTokenPath points to a non-existing file",
		"tokenMountPathPointsToBrokenPath",
		"couldn't read vault token from mount path")

	createInitSucceedsButComponentFailsFlow(fs,
		"Verify failure when vaultToken value does not match our servers's value",
		"badVaultToken",
		false)

	createPositiveTestFlow(fs,
		"Verify success when vaultTokenPath points to an existing file matching the configured secret we have for our secret seeder",
		"tokenMountPathHappyCase",
		false)
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
			embedded.WithComponentsPath(componentPath),
			embedded.WithDaprGRPCPort(currentGrpcPort),
			embedded.WithDaprHTTPPort(currentHttpPort),
			// Dapr log-level debug?
			componentRuntimeOptions(),
		)).
		Step("Waiting for component to load...", flow.Sleep(5*time.Second)).
		Step("✅Verify component is registered", testComponentFound(componentName, currentGrpcPort)).
		Step("Verify no errors regarding component initialization", assertNoInitializationErrorsForComponent(componentPath)).
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
			embedded.WithComponentsPath(componentPath),
			embedded.WithDaprGRPCPort(currentGrpcPort),
			embedded.WithDaprHTTPPort(currentHttpPort),
			// Dapr log-level debug?
			componentRuntimeOptions(),
		)).
		Step("Waiting for component to load...", flow.Sleep(5*time.Second)).
		Step("✅Verify component is registered", testComponentFound(componentName, currentGrpcPort)).
		Step("Verify no errors regarding component initialization", assertNoInitializationErrorsForComponent(componentPath)).
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

//
// Aux. functions for testing key presence
//

func testKeyValuesInSecret(currentGrpcPort int, secretStoreName string, secretName string, keyValueMap map[string]string, maybeVersionID ...string) flow.Runnable {
	return func(ctx flow.Context) error {
		daprClient, err := client.NewClientWithPort(fmt.Sprint(currentGrpcPort))
		if err != nil {
			panic(err)
		}
		defer daprClient.Close()

		metadata := map[string]string{}
		if len(maybeVersionID) > 0 {
			metadata["version_id"] = maybeVersionID[0]
		}

		res, err := daprClient.GetSecret(ctx, secretStoreName, secretName, metadata)
		assert.NoError(ctx.T, err)
		assert.NotNil(ctx.T, res)

		for key, valueExpected := range keyValueMap {
			valueInSecret, exists := res[key]
			assert.True(ctx.T, exists, "expected key not found in key")
			assert.Equal(ctx.T, valueExpected, valueInSecret)
		}
		return nil
	}
}

func testSecretIsNotFound(currentGrpcPort int, secretStoreName string, secretName string) flow.Runnable {
	return func(ctx flow.Context) error {
		daprClient, err := client.NewClientWithPort(fmt.Sprint(currentGrpcPort))
		if err != nil {
			panic(err)
		}
		defer daprClient.Close()

		emptyOpt := map[string]string{}

		_, err = daprClient.GetSecret(ctx, secretStoreName, secretName, emptyOpt)
		assert.Error(ctx.T, err)

		return nil
	}
}

func testDefaultSecretIsFound(currentGrpcPort int, secretStoreName string) flow.Runnable {
	return testKeyValuesInSecret(currentGrpcPort, secretStoreName, "multiplekeyvaluessecret", map[string]string{
		"first":  "1",
		"second": "2",
		"third":  "3",
	})
}

func testComponentIsNotWorking(targetComponentName string, currentGrpcPort int) flow.Runnable {
	return testSecretIsNotFound(currentGrpcPort, targetComponentName, "multiplekeyvaluessecret")
}

func testGetBulkSecretsWorksAndFoundKeys(currentGrpcPort int, secretStoreName string) flow.Runnable {
	return func(ctx flow.Context) error {
		client, err := client.NewClientWithPort(fmt.Sprint(currentGrpcPort))
		if err != nil {
			panic(err)
		}
		defer client.Close()

		emptyOpt := map[string]string{}

		res, err := client.GetBulkSecret(ctx, secretStoreName, emptyOpt)
		assert.NoError(ctx.T, err)
		assert.NotNil(ctx.T, res)
		assert.NotEmpty(ctx.T, res)

		for k, v := range res {
			ctx.Logf("💡 %s", k)
			for i, j := range v {
				ctx.Logf("💡\t %s : %s", i, j)
			}
		}

		return nil
	}
}

//
// Helper methods for checking component registration and availability of its features
//

func testComponentFound(targetComponentName string, currentGrpcPort int) flow.Runnable {
	return func(ctx flow.Context) error {
		componentFound, _ := getComponentCapabilities(ctx, currentGrpcPort, targetComponentName)
		assert.True(ctx.T, componentFound, "Component was expected to be found but it was missing.")
		return nil
	}
}

// Due to https://github.com/dapr/dapr/issues/5487 we cannot perform negative tests
// for the component presence against the metadata registry.
// Instead, we turned testComponentNotFound into a simpler negative test that ensures a good key cannot be found
func testComponentNotFound(targetComponentName string, currentGrpcPort int) flow.Runnable {
	// TODO(tmacam) once https://github.com/dapr/dapr/issues/5487 is fixed, uncomment the code bellow
	return testSecretIsNotFound(currentGrpcPort, targetComponentName, "multiplekeyvaluessecret")

	//return func(ctx flow.Context) error {
	//	// Find the component
	//	componentFound, _ := getComponentCapabilities(ctx, currentGrpcPort, targetComponentName)
	//	assert.False(ctx.T, componentFound, "Component was expected to be missing but it was found.")
	//	return nil
	//}
}

func testComponentDoesNotHaveFeature(currentGrpcPort int, targetComponentName string, targetCapability secretstores.Feature) flow.Runnable {
	return testComponentAndFeaturePresence(currentGrpcPort, targetComponentName, targetCapability, false)
}

func testComponentHasFeature(currentGrpcPort int, targetComponentName string, targetCapability secretstores.Feature) flow.Runnable {
	return testComponentAndFeaturePresence(currentGrpcPort, targetComponentName, targetCapability, true)
}

func testComponentAndFeaturePresence(currentGrpcPort int, targetComponentName string, targetCapability secretstores.Feature, expectedToBeFound bool) flow.Runnable {
	return func(ctx flow.Context) error {
		componentFound, capabilities := getComponentCapabilities(ctx, currentGrpcPort, targetComponentName)

		assert.True(ctx.T, componentFound, "Component was expected to be found but it was missing.")

		targetCapabilityAsString := string(targetCapability)
		// Find capability
		capabilityFound := false
		for _, cap := range capabilities {
			if cap == targetCapabilityAsString {
				capabilityFound = true
				break
			}
		}
		assert.Equal(ctx.T, expectedToBeFound, capabilityFound)

		return nil
	}
}

func getComponentCapabilities(ctx flow.Context, currentGrpcPort int, targetComponentName string) (found bool, capabilities []string) {
	daprClient, err := client.NewClientWithPort(fmt.Sprint(currentGrpcPort))
	if err != nil {
		panic(err)
	}
	defer daprClient.Close()

	clientCtx := context.Background()

	resp, err := daprClient.GrpcClient().GetMetadata(clientCtx, &empty.Empty{})
	assert.NoError(ctx.T, err)
	assert.NotNil(ctx.T, resp)
	assert.NotNil(ctx.T, resp.GetRegisteredComponents())

	// Find the component
	for _, component := range resp.GetRegisteredComponents() {
		if component.GetName() == targetComponentName {
			ctx.Logf("🩺 component found=%s", component)
			return true, component.GetCapabilities()
		}
	}
	return false, []string{}
}

//
// Flow and test setup helpers
//

func componentRuntimeOptions() []runtime.Option {
	log := logger.NewLogger("dapr.components")

	secretStoreRegistry := secretstores_loader.NewRegistry()
	secretStoreRegistry.Logger = log
	secretStoreRegistry.RegisterComponent(vault.NewHashiCorpVaultSecretStore, "hashicorp.vault")

	return []runtime.Option{
		runtime.WithSecretStores(secretStoreRegistry),
	}
}

func GetCurrentGRPCAndHTTPPort(t *testing.T) (int, int) {
	ports, err := dapr_testing.GetFreePorts(2)
	assert.NoError(t, err)

	currentGrpcPort := ports[0]
	currentHttpPort := ports[1]

	return currentGrpcPort, currentHttpPort
}

//
// Helper functions for asserting error messages during component initialization
//
// These can be exported to their own module.
// Do notice that they have side effects: using more than one in a single
// flow will cause only the latest to work. Perhaps this functionality
// (dapr.runtime log capture) could be baked into flows themselves?
//
// Also: this is not thread-safe nor concurrent safe: only one test
// can be run at a time to ensure deterministic capture of dapr.runtime output.

type initErrorChecker func(ctx flow.Context, errorLine string) error

func captureLogsAndCheckInitErrors(checker initErrorChecker) flow.Runnable {
	// Setup log capture
	logCaptor := &bytes.Buffer{}
	runtimeLogger := logger.NewLogger("dapr.runtime")
	runtimeLogger.SetOutput(io.MultiWriter(os.Stdout, logCaptor))

	// Stop log capture, reset buffer just for good mesure
	cleanup := func() {
		logCaptor.Reset()
		runtimeLogger.SetOutput(os.Stdout)
	}

	grepInitErrorFromLogs := func() (string, error) {
		errorMarker := []byte("INIT_COMPONENT_FAILURE")
		scanner := bufio.NewScanner(logCaptor)
		for scanner.Scan() {
			if err := scanner.Err(); err != nil {
				return "", err
			}
			if bytes.Contains(scanner.Bytes(), errorMarker) {
				return scanner.Text(), nil
			}
		}
		return "", scanner.Err()
	}

	// Wraps the our initErrorChecker with cleanup and error-grepping logic so we only care about the
	// log error
	return func(ctx flow.Context) error {
		defer cleanup()

		errorLine, err := grepInitErrorFromLogs()
		if err != nil {
			return err
		}
		ctx.Logf("👀 errorLine: %s", errorLine)

		return checker(ctx, errorLine)
	}
}

func assertNoInitializationErrorsForComponent(componentName string) flow.Runnable {
	checker := func(ctx flow.Context, errorLine string) error {
		componentFailedToInitialize := strings.Contains(errorLine, componentName)
		assert.False(ctx.T, componentFailedToInitialize,
			"Found component name mentioned in an component initialization error message: %s", errorLine)

		return nil
	}

	return captureLogsAndCheckInitErrors(checker)
}

func assertInitializationFailedWithErrorsForComponent(componentName string, additionalSubStringsToMatch ...string) flow.Runnable {
	checker := func(ctx flow.Context, errorLine string) error {
		assert.NotEmpty(ctx.T, errorLine, "Expected a component initialization error message but none found")
		assert.Contains(ctx.T, errorLine, componentName,
			"Expected to find component '%s' mentioned in error message but found none: %s", componentName, errorLine)

		for _, subString := range additionalSubStringsToMatch {
			assert.Contains(ctx.T, errorLine, subString,
				"Expected to find '%s' mentioned in error message but found none: %s", componentName, errorLine)
		}

		return nil
	}

	return captureLogsAndCheckInitErrors(checker)
}

//
// Helper functions for common tests for happy case, init-but-does-not-work and fails-initialization tests
// These test re-use the same seed secrets. They aim to check how certain flags break or keep vault working
// instead of verifying some specific tha change key retrieval behavior.
//
// Those tests require a `<base dir>/<specific flow>` structure.

type commonFlowSettings struct {
	t                            *testing.T
	currentGrpcPort              int
	currentHttpPort              int
	secretStoreComponentPathBase string
	componentNamePrefix          string
}

func NewFlowSettings(t *testing.T) *commonFlowSettings {
	res := commonFlowSettings{}
	res.t = t
	res.currentGrpcPort, res.currentHttpPort = GetCurrentGRPCAndHTTPPort(t)
	return &res
}

func createPositiveTestFlow(fs *commonFlowSettings, flowDescription string, componentSuffix string, useCustomDockerCompose bool) {
	componentPath := filepath.Join(fs.secretStoreComponentPathBase, componentSuffix)
	componentName := fs.componentNamePrefix + componentSuffix

	dockerComposeClusterYAML := defaultDockerComposeClusterYAML
	if useCustomDockerCompose {
		dockerComposeClusterYAML = filepath.Join(componentPath, "docker-compose-hashicorp-vault.yml")
	}

	flow.New(fs.t, flowDescription).
		Step(dockercompose.Run(dockerComposeProjectName, dockerComposeClusterYAML)).
		Step("Waiting for component to start...", flow.Sleep(5*time.Second)).
		Step(sidecar.Run(sidecarName,
			embedded.WithoutApp(),
			embedded.WithComponentsPath(componentPath),
			embedded.WithDaprGRPCPort(fs.currentGrpcPort),
			embedded.WithDaprHTTPPort(fs.currentHttpPort),
			componentRuntimeOptions(),
		)).
		Step("Waiting for component to load...", flow.Sleep(5*time.Second)).
		Step("Verify component is registered", testComponentFound(componentName, fs.currentGrpcPort)).
		Step("Verify no errors regarding component initialization", assertNoInitializationErrorsForComponent(componentPath)).
		Step("Test that the default secret is found", testDefaultSecretIsFound(fs.currentGrpcPort, componentName)).
		Step("Stop HashiCorp Vault server", dockercompose.Stop(dockerComposeProjectName, dockerComposeClusterYAML)).
		Run()
}

func createInitSucceedsButComponentFailsFlow(fs *commonFlowSettings, flowDescription string, componentSuffix string, useCustomDockerCompose bool, initErrorCodes ...string) {
	componentPath := filepath.Join(fs.secretStoreComponentPathBase, componentSuffix)
	componentName := fs.componentNamePrefix + componentSuffix

	dockerComposeClusterYAML := defaultDockerComposeClusterYAML
	if useCustomDockerCompose {
		dockerComposeClusterYAML = filepath.Join(componentPath, "docker-compose-hashicorp-vault.yml")
	}

	flow.New(fs.t, flowDescription).
		Step(dockercompose.Run(dockerComposeProjectName, dockerComposeClusterYAML)).
		Step("Waiting for component to start...", flow.Sleep(5*time.Second)).
		Step(sidecar.Run(sidecarName,
			embedded.WithoutApp(),
			embedded.WithComponentsPath(componentPath),
			embedded.WithDaprGRPCPort(fs.currentGrpcPort),
			embedded.WithDaprHTTPPort(fs.currentHttpPort),
			componentRuntimeOptions(),
		)).
		Step("Waiting for component to load...", flow.Sleep(5*time.Second)).
		Step("✅Verify component is registered", testComponentFound(componentName, fs.currentGrpcPort)).
		Step("Verify no errors regarding component initialization", assertNoInitializationErrorsForComponent(componentPath)).
		Step("🛑Verify component does not work", testComponentIsNotWorking(componentName, fs.currentGrpcPort)).
		Step("Stop HashiCorp Vault server", dockercompose.Stop(dockerComposeProjectName, dockerComposeClusterYAML)).
		Run()
}

func createNegativeTestFlow(fs *commonFlowSettings, flowDescription string, componentSuffix string, initErrorCodes ...string) {
	componentPath := filepath.Join(fs.secretStoreComponentPathBase, componentSuffix)
	componentName := fs.componentNamePrefix + componentSuffix
	dockerComposeClusterYAML := defaultDockerComposeClusterYAML

	flow.New(fs.t, flowDescription).
		Step(dockercompose.Run(dockerComposeProjectName, dockerComposeClusterYAML)).
		Step("Waiting for component to start...", flow.Sleep(5*time.Second)).
		Step(sidecar.Run(sidecarName,
			embedded.WithoutApp(),
			embedded.WithComponentsPath(componentPath),
			embedded.WithDaprGRPCPort(fs.currentGrpcPort),
			embedded.WithDaprHTTPPort(fs.currentHttpPort),
			componentRuntimeOptions(),
		)).
		Step("Waiting for component to load...", flow.Sleep(5*time.Second)).
		// TODO(tmacam) FIX https://github.com/dapr/dapr/issues/5487
		Step("🛑Verify component is NOT registered", testComponentNotFound(componentName, fs.currentGrpcPort)).
		Step("Verify initialization error reported for component", assertInitializationFailedWithErrorsForComponent(componentName, initErrorCodes...)).
		Step("🐞😱 Bug dependant behavior - test component is actually registered", testComponentFound(componentName, fs.currentGrpcPort)).
		Step("Stop HashiCorp Vault server", dockercompose.Stop(dockerComposeProjectName, dockerComposeClusterYAML)).
		Run()
}
