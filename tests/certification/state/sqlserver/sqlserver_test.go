// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package sqlserver_test

import (
	"fmt"
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

	// mssql "github.com/denisenkom/go-mssqldb"

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
	"github.com/dapr/go-sdk/client"
)

const (
	sidecarName             = "dapr-1"
	dockerComposeYAML       = "docker-compose.yml"
	stateStoreName          = "dapr-state-store"
	certificationTestPrefix = "stable-certification-"
)

func TestSqlServer(t *testing.T) {
	log := logger.NewLogger("dapr.components")
	ports, err := dapr_testing.GetFreePorts(2)
	assert.NoError(t, err)

	currentGrpcPort := ports[0]
	currentHttpPort := ports[1]

	basicTest := func(ctx flow.Context) error {
		client, err := client.NewClientWithPort(fmt.Sprint(currentGrpcPort))
		if err != nil {
			panic(err)
		}
		defer client.Close()

		// save state with the key certification1, default options: strong, last-write
		err = client.SaveState(ctx, stateStoreName, certificationTestPrefix+"key1", []byte("certificationdata"))
		assert.NoError(t, err)

		// get state for key certificationkey1
		item, err := client.GetState(ctx, stateStoreName, certificationTestPrefix+"key1")
		assert.NoError(t, err)
		assert.Equal(t, "certificationdata", string(item.Value))

		// delete state for key certificationkey1
		err = client.DeleteState(ctx, stateStoreName, certificationTestPrefix+"key1")
		assert.NoError(t, err)

		return nil
	}

	flow.New(t, "SQLServer certification using SQL Server Docker").
		// Run SQL Server using Docker Compose.
		Step(dockercompose.Run("sqlserver", dockerComposeYAML)).
		// This step only applies to spinning up the docker container for the first time
		Step("Wait for Docker", flow.Sleep(time.Second*10)).

		// Run the Dapr sidecar with the Kafka component.
		Step(sidecar.Run(sidecarName,
			embedded.WithoutApp(),
			embedded.WithDaprGRPCPort(currentGrpcPort),
			embedded.WithDaprHTTPPort(currentHttpPort),
			embedded.WithComponentsPath("components/docker/default"),
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
		// Introduce network interruption
		Step("interrupt network",
			network.InterruptNetwork(40*time.Second, nil, nil, "1433", "1434")).

		// Component should recover at this point.
		Step("wait", flow.Sleep(10*time.Second)).
		Step("Run basic test again to verify reconnection occurred", basicTest, sidecar.Stop(sidecarName)).
		Step("Stopping SQL Server Docker container", dockercompose.Stop("sqlserver", dockerComposeYAML)).
		Run()

	ports, err = dapr_testing.GetFreePorts(2)
	assert.NoError(t, err)

	currentGrpcPort = ports[0]
	currentHttpPort = ports[1]

	flow.New(t, "SQL Server certification using Azure SQL").
		// Run the Dapr sidecar with the SQL Server component.
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
			network.InterruptNetwork(40*time.Second, nil, nil, "1433", "1434")).

		// Component should recover at this point.
		Step("wait", flow.Sleep(10*time.Second)).
		Step("Run basic test again to verify reconnection occurred", basicTest, sidecar.Stop(sidecarName)).
		Run()
}
