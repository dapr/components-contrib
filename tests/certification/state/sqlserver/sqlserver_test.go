// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package sqlserver_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	// State.
	"github.com/dapr/components-contrib/state"
	state_sqlserver "github.com/dapr/components-contrib/state/sqlserver"
	state_loader "github.com/dapr/dapr/pkg/components/state"

	// Secret stores.
	"github.com/dapr/components-contrib/secretstores"
	secretstore_env "github.com/dapr/components-contrib/secretstores/local/env"
	secretstores_loader "github.com/dapr/dapr/pkg/components/secretstores"

	// Dapr runtime and Go-SDK
	"github.com/dapr/dapr/pkg/runtime"
	dapr_testing "github.com/dapr/dapr/pkg/testing"
	"github.com/dapr/kit/logger"

	// Certification testing runnables
	"github.com/dapr/components-contrib/tests/certification/embedded"
	"github.com/dapr/components-contrib/tests/certification/flow"
	"github.com/dapr/components-contrib/tests/certification/flow/dockercompose"
	"github.com/dapr/components-contrib/tests/certification/flow/network"
	"github.com/dapr/components-contrib/tests/certification/flow/sidecar"
)

const (
	sidecarName       = "dapr-1"
	dockerComposeYAML = "docker-compose.yml"
	stateStoreName    = "dapr-state-store"
)

func TestSqlServer(t *testing.T) {
	log := logger.NewLogger("dapr.components")
	ports, err := dapr_testing.GetFreePorts(2)
	assert.NoError(t, err)

	currentGrpcPort := ports[0]
	currentHttpPort := ports[1]

	basicTest := func(ctx flow.Context) error {
		client := sidecar.GetClient(ctx, sidecarName)

		// save state with the key certification1, default options: strong, last-write
		err := client.SaveState(ctx, stateStoreName, "certificationkey1", []byte("certificationdata"))
		assert.NoError(t, err)

		// get state for key certificationkey1
		item, err := client.GetState(ctx, stateStoreName, "certificationkey1")
		assert.NoError(t, err)
		assert.Equal(t, "certificationdata", string(item.Value))

		// delete state for key certificationkey1
		err = client.DeleteState(ctx, stateStoreName, "certificationkey1")
		assert.NoError(t, err)

		return nil
	}

	flow.New(t, "sqlserver certification").
		// Run SQL Server using Docker Compose.
		Step(dockercompose.Run("Spinning up SQL Server via Docker Compose", dockerComposeYAML)).

		// Run the Dapr sidecar with the Kafka component.
		Step(sidecar.Run(sidecarName,
			embedded.WithoutApp(),
			embedded.WithDaprGRPCPort(currentGrpcPort),
			embedded.WithDaprHTTPPort(currentHttpPort),
			embedded.WithComponentsPath("components/azure"),
			runtime.WithSecretStores(
				secretstores_loader.New("local.env", func() secretstores.SecretStore {
					return secretstore_env.NewEnvSecretStore(log)
				})),
			runtime.WithStates(
				state_loader.New("sqlserver", func() state.Store {
					return state_sqlserver.NewSQLServerStateStore(log)
				}),
			))).
		Step("Run basic test", basicTest).

		// Errors will occurring here.
		Step("interrupt network",
			network.InterruptNetwork(30*time.Second, nil, nil, "1433")).
		//
		// Component should recover at this point.
		Step("wait", flow.Sleep(30*time.Second)).
		Step("Run basic test again to verify reconnection occurred", basicTest, sidecar.Stop(sidecarName)).
		Run()
}
