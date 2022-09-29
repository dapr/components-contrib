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
	"fmt"
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
	sidecarName              = "hashicorp-vault-sidecar"
	dockerComposeClusterYAML = "../../../../../.github/infrastructure/docker-compose-hashicorp-vault.yml"
	dockerComposeProjectName = "hashicorp-vault"
	secretStoreComponentPath = "./components/default"
	secretStoreName          = "my-hashicorp-vault" // as set in the component YAML

	networkInstabilityTime   = 1 * time.Minute
	waitAfterInstabilityTime = networkInstabilityTime / 4
	servicePortToInterrupt   = "8200"
)

func TestBasicSecretRetrieval(t *testing.T) {
	ports, err := dapr_testing.GetFreePorts(2)
	assert.NoError(t, err)

	currentGrpcPort := ports[0]
	currentHttpPort := ports[1]

	testGetKnownSecret := func(ctx flow.Context) error {
		client, err := client.NewClientWithPort(fmt.Sprint(currentGrpcPort))
		if err != nil {
			panic(err)
		}
		defer client.Close()

		emptyOpt := map[string]string{}

		// This test reuses the HashiCorp Vault's conformance test resources created using
		// .github/infrastructure/docker-compose-hashicorp-vault.yml,
		// so it reuses the tests/conformance/secretstores/secretstores.go test secrets.
		res, err := client.GetSecret(ctx, secretStoreName, "secondsecret", emptyOpt)
		assert.NoError(t, err)
		assert.Equal(t, "efgh", res["secondsecret"])
		return nil
	}

	testGetMissingSecret := func(ctx flow.Context) error {
		client, err := client.NewClientWithPort(fmt.Sprint(currentGrpcPort))
		if err != nil {
			panic(err)
		}
		defer client.Close()

		emptyOpt := map[string]string{}

		_, getErr := client.GetSecret(ctx, secretStoreName, "this_secret_is_not_there", emptyOpt)
		assert.Error(t, getErr)

		return nil
	}

	flow.New(t, "Test component is up and we can retrieve some secrets").
		Step(dockercompose.Run(dockerComposeProjectName, dockerComposeClusterYAML)).
		Step("Waiting for component to start...", flow.Sleep(5*time.Second)).
		Step(sidecar.Run(sidecarName,
			embedded.WithoutApp(),
			embedded.WithComponentsPath(secretStoreComponentPath),
			embedded.WithDaprGRPCPort(currentGrpcPort),
			embedded.WithDaprHTTPPort(currentHttpPort),
			componentRuntimeOptions(),
		)).
		Step("Waiting for component to load...", flow.Sleep(5*time.Second)).
		Step("Verify component is registered", testComponentFound(t, secretStoreName, currentGrpcPort)).
		Step("Run basic secret retrieval test", testGetKnownSecret).
		Step("Test retrieval of secret that does not exist", testGetMissingSecret).
		Step("Interrupt network for 1 minute",
			network.InterruptNetwork(networkInstabilityTime, nil, nil, servicePortToInterrupt)).
		Step("Wait for component to recover", flow.Sleep(waitAfterInstabilityTime)).
		Step("Run basic test again to verify reconnection occurred", testGetKnownSecret).
		Step("Stop HashiCorp Vault server", dockercompose.Stop(dockerComposeProjectName, dockerComposeClusterYAML)).
		Run()
}

func TestMultipleKVRetrieval(t *testing.T) {
	ports, err := dapr_testing.GetFreePorts(2)
	assert.NoError(t, err)

	currentGrpcPort := ports[0]
	currentHttpPort := ports[1]

	testGetMultipleKeyValuesFromSecret := func(ctx flow.Context) error {
		client, err := client.NewClientWithPort(fmt.Sprint(currentGrpcPort))
		if err != nil {
			panic(err)
		}
		defer client.Close()

		emptyOpt := map[string]string{}

		// This test reuses the HashiCorp Vault's conformance test resources created using
		// .github/infrastructure/docker-compose-hashicorp-vault.yml,
		// so it reuses the tests/conformance/secretstores/secretstores.go test secrets.
		res, err := client.GetSecret(ctx, secretStoreName, "multiplekeyvaluessecret", emptyOpt)
		assert.NoError(t, err)
		assert.NotNil(t, res)
		assert.Equal(t, "1", res["first"])
		assert.Equal(t, "2", res["second"])
		assert.Equal(t, "3", res["third"])
		return nil
	}

	flow.New(t, "Test retrieving multiple key values from a secret").
		Step(dockercompose.Run(dockerComposeProjectName, dockerComposeClusterYAML)).
		Step("Waiting for component to start...", flow.Sleep(5*time.Second)).
		Step(sidecar.Run(sidecarName,
			embedded.WithoutApp(),
			embedded.WithComponentsPath(secretStoreComponentPath),
			embedded.WithDaprGRPCPort(currentGrpcPort),
			embedded.WithDaprHTTPPort(currentHttpPort),
			componentRuntimeOptions(),
		)).
		Step("Waiting for component to load...", flow.Sleep(5*time.Second)).
		Step("Verify component is registered", testComponentFound(t, secretStoreName, currentGrpcPort)).
		Step("Verify component has support for multiple key-values under the same secret",
			testComponentHasFeature(t, secretStoreName, string(secretstores.FeatureMultipleKeyValuesPerSecret), currentGrpcPort)).
		Step("Test retrieval of a secret with multiple key-values", testGetMultipleKeyValuesFromSecret).
		Step("Stop HashiCorp Vault server", dockercompose.Stop(dockerComposeProjectName, dockerComposeClusterYAML)).
		Run()
}

func testComponentFound(t *testing.T, targetComponentName string, currentGrpcPort int) flow.Runnable {
	return func(ctx flow.Context) error {
		client, err := client.NewClientWithPort(fmt.Sprint(currentGrpcPort))
		if err != nil {
			panic(err)
		}
		defer client.Close()

		clientCtx := context.Background()

		resp, err := client.GrpcClient().GetMetadata(clientCtx, &empty.Empty{})
		assert.NoError(t, err)
		assert.NotNil(t, resp)
		assert.NotNil(t, resp.GetRegisteredComponents())

		// Find the component
		componentFound := false
		for _, component := range resp.GetRegisteredComponents() {
			if component.GetName() == targetComponentName {
				componentFound = true
				break
			}
		}
		assert.True(t, componentFound)

		return nil
	}
}

func testComponentHasFeature(t *testing.T, targetComponentName string, targetCapability string, currentGrpcPort int) flow.Runnable {
	return func(ctx flow.Context) error {
		client, err := client.NewClientWithPort(fmt.Sprint(currentGrpcPort))
		if err != nil {
			panic(err)
		}
		defer client.Close()

		clientCtx := context.Background()

		resp, err := client.GrpcClient().GetMetadata(clientCtx, &empty.Empty{})
		assert.NoError(t, err)
		assert.NotNil(t, resp)
		assert.NotNil(t, resp.GetRegisteredComponents())

		// Find the component
		var capabilities []string = []string{}
		for _, component := range resp.GetRegisteredComponents() {
			if component.GetName() == targetComponentName {
				capabilities = component.GetCapabilities()
				break
			}
		}
		assert.NotEmpty(t, capabilities)

		// Find capability
		capabilityFound := false
		for _, cap := range capabilities {
			if cap == targetCapability {
				capabilityFound = true
				break
			}
		}
		assert.True(t, capabilityFound)

		return nil
	}
}

func componentRuntimeOptions() []runtime.Option {
	log := logger.NewLogger("dapr.components")

	secretStoreRegistry := secretstores_loader.NewRegistry()
	secretStoreRegistry.Logger = log
	secretStoreRegistry.RegisterComponent(vault.NewHashiCorpVaultSecretStore, "hashicorp.vault")

	return []runtime.Option{
		runtime.WithSecretStores(secretStoreRegistry),
	}
}
