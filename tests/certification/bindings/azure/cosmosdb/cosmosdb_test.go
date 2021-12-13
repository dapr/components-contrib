// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package keyvault_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/dapr/components-contrib/bindings"
	cosmosdbbinding "github.com/dapr/components-contrib/bindings/azure/cosmosdb"
	"github.com/dapr/components-contrib/secretstores"
	secretstore_env "github.com/dapr/components-contrib/secretstores/local/env"
	bindings_loader "github.com/dapr/dapr/pkg/components/bindings"
	secretstores_loader "github.com/dapr/dapr/pkg/components/secretstores"
	"github.com/dapr/dapr/pkg/runtime"
	dapr_testing "github.com/dapr/dapr/pkg/testing"
	"github.com/dapr/kit/logger"

	"github.com/dapr/components-contrib/tests/certification/embedded"
	"github.com/dapr/components-contrib/tests/certification/flow"
	"github.com/dapr/components-contrib/tests/certification/flow/sidecar"
)

const (
	sidecarName = "cosmosdb-sidecar"
)

func TestKeyVault(t *testing.T) {
	ports, err := dapr_testing.GetFreePorts(2)
	assert.NoError(t, err)

	currentGrpcPort := ports[0]
	currentHttpPort := ports[1]

	log := logger.NewLogger("dapr.components")

	flow.New(t, "cosmosdb binding authentication using service principal").
		Step(sidecar.Run(sidecarName,
			embedded.WithoutApp(),
			embedded.WithComponentsPath("./components/serviceprincipal"),
			embedded.WithDaprGRPCPort(currentGrpcPort),
			embedded.WithDaprHTTPPort(currentHttpPort),
			runtime.WithSecretStores(
				secretstores_loader.New("local.env", func() secretstores.SecretStore {
					return secretstore_env.NewEnvSecretStore(log)
				}),
			),
			runtime.WithOutputBindings(
				bindings_loader.NewOutput("azure.cosmosdb", func() bindings.OutputBinding {
					return cosmosdbbinding.NewCosmosDB(log)
				}),
			))).
		Run()

	ports, err = dapr_testing.GetFreePorts(2)
	assert.NoError(t, err)

	currentGrpcPort = ports[0]
	currentHttpPort = ports[1]

	flow.New(t, "cosmosdb binding authentication using master key").
		Step(sidecar.Run(sidecarName,
			embedded.WithoutApp(),
			embedded.WithComponentsPath("./components/masterkey"),
			embedded.WithDaprGRPCPort(currentGrpcPort),
			embedded.WithDaprHTTPPort(currentHttpPort),
			runtime.WithSecretStores(
				secretstores_loader.New("local.env", func() secretstores.SecretStore {
					return secretstore_env.NewEnvSecretStore(log)
				}),
			),
			runtime.WithOutputBindings(
				bindings_loader.NewOutput("azure.cosmosdb", func() bindings.OutputBinding {
					return cosmosdbbinding.NewCosmosDB(log)
				}),
			))).
		Run()
}
