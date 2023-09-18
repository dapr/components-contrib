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
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/dapr/components-contrib/secretstores/hashicorp/vault"
	"github.com/dapr/components-contrib/tests/certification/embedded"
	"github.com/dapr/components-contrib/tests/certification/flow"
	"github.com/dapr/components-contrib/tests/certification/flow/dockercompose"
	"github.com/dapr/components-contrib/tests/certification/flow/sidecar"
	secretstores_loader "github.com/dapr/dapr/pkg/components/secretstores"
	"github.com/dapr/dapr/pkg/runtime"
	"github.com/dapr/dapr/pkg/runtime/registry"
	dapr_testing "github.com/dapr/dapr/pkg/testing"
	"github.com/dapr/kit/logger"
	"github.com/stretchr/testify/assert"
)

const (
	defaultDockerComposeClusterYAML = "../../../../../.github/infrastructure/docker-compose-hashicorp-vault.yml"
	sidecarName                     = "hashicorp-vault-sidecar"
	dockerComposeProjectName        = "hashicorp-vault"
	// secretStoreName          = "my-hashicorp-vault" // as set in the component YAML

	networkInstabilityTime   = 1 * time.Minute
	waitAfterInstabilityTime = networkInstabilityTime / 4
	servicePortToInterrupt   = "8200"
)

//
// Flow and test setup helpers
//

func componentRuntimeOptions() embedded.Option {
	log := logger.NewLogger("dapr.components")

	secretStoreRegistry := secretstores_loader.NewRegistry()
	secretStoreRegistry.Logger = log
	secretStoreRegistry.RegisterComponent(vault.NewHashiCorpVaultSecretStore, "hashicorp.vault")

	return func(cfg *runtime.Config) {
		cfg.Registry = registry.NewOptions().WithSecretStores(secretStoreRegistry)
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
			embedded.WithResourcesPath(componentPath),
			embedded.WithDaprGRPCPort(strconv.Itoa(fs.currentGrpcPort)),
			embedded.WithDaprHTTPPort(strconv.Itoa(fs.currentHttpPort)),
			componentRuntimeOptions(),
		)).
		Step("Waiting for component to load...", flow.Sleep(5*time.Second)).
		Step("Verify component is registered", testComponentFound(componentName, fs.currentGrpcPort)).
		Step("Verify no errors regarding component initialization", AssertNoInitializationErrorsForComponent(componentPath)).
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
			embedded.WithResourcesPath(componentPath),
			embedded.WithDaprGRPCPort(strconv.Itoa(fs.currentGrpcPort)),
			embedded.WithDaprHTTPPort(strconv.Itoa(fs.currentHttpPort)),
			componentRuntimeOptions(),
		)).
		Step("Waiting for component to load...", flow.Sleep(5*time.Second)).
		Step("Verify component is registered", testComponentFound(componentName, fs.currentGrpcPort)).
		Step("Verify no errors regarding component initialization", AssertNoInitializationErrorsForComponent(componentPath)).
		Step("Verify component does not work", testComponentIsNotWorking(componentName, fs.currentGrpcPort)).
		Step("Stop HashiCorp Vault server", dockercompose.Stop(dockerComposeProjectName, dockerComposeClusterYAML)).
		Run()
}
