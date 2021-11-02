// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package sqlserver_test

import (
	"database/sql"
	"encoding/json"
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

	// Dapr runtime and Go-SDK
	"github.com/dapr/dapr/pkg/runtime"
	dapr_testing "github.com/dapr/dapr/pkg/testing"
	"github.com/dapr/kit/logger"

	// Certification testing runnables
	"github.com/dapr/components-contrib/tests/certification/embedded"
	"github.com/dapr/components-contrib/tests/certification/flow"

	"github.com/dapr/components-contrib/tests/certification/flow/dockercompose"
	"github.com/dapr/components-contrib/tests/certification/flow/network"
	"github.com/dapr/components-contrib/tests/certification/flow/retry"
	"github.com/dapr/components-contrib/tests/certification/flow/sidecar"
	"github.com/dapr/go-sdk/client"
)

const (
	sidecarNamePrefix       = "sqlserver-sidecar-"
	dockerComposeYAML       = "docker-compose.yml"
	stateStoreName          = "dapr-state-store"
	certificationTestPrefix = "stable-certification-"
	dockerConnectionString  = "server=localhost;user id=sa;password=Pass@Word1;port=1433;"
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

		// save state with the key certificationkey1, default options: strong, last-write
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

	verifyIndexedPopertiesTest := func(ctx flow.Context) error {
		// verify indices were created by Dapr
		db, err := sql.Open("sqlserver", fmt.Sprintf("%sdatabase=certificationtest;", dockerConnectionString))
		assert.NoError(t, err)
		defer db.Close()

		rows, err := db.Query("sp_helpindex '[customschema].[mystates]'")
		assert.NoError(t, err)

		indexFoundCount := 0
		for rows.Next() {
			var indexedField, otherdata1, otherdata2 string
			err = rows.Scan(&indexedField, &otherdata1, &otherdata2)
			assert.NoError(t, err)

			expectedIndices := []string{"IX_customerid", "IX_transactionid", "PK_mystates"}
			for _, item := range expectedIndices {
				if item == indexedField {
					indexFoundCount++
					break
				}
			}
		}

		assert.Equal(t, 3, indexFoundCount)

		// write JSON data to the table (which will automatically be indexed in separate columns)
		client, err := client.NewClientWithPort(fmt.Sprint(currentGrpcPort))
		if err != nil {
			panic(err)
		}
		defer client.Close()

		order := struct {
			ID          int    `json:"id"`
			Customer    string `json:"customer"`
			Description string `json:"description"`
		}{123456, "John Doe", "something"}

		data, err := json.Marshal(order)
		assert.NoError(t, err)

		// save state with the key certificationkey1, default options: strong, last-write
		err = client.SaveState(ctx, stateStoreName, certificationTestPrefix+"key1", data)
		assert.NoError(t, err)

		// get state for key certificationkey1
		item, err := client.GetState(ctx, stateStoreName, certificationTestPrefix+"key1")
		assert.NoError(t, err)
		assert.Equal(t, string(data), string(item.Value))

		// delete state for key certificationkey1
		err = client.DeleteState(ctx, stateStoreName, certificationTestPrefix+"key1")
		assert.NoError(t, err)

		return nil
	}

	createCustomSchema := func(ctx flow.Context) error {
		db, err := sql.Open("sqlserver", dockerConnectionString)
		assert.NoError(t, err)
		_, err = db.Exec("CREATE SCHEMA customschema;")
		assert.NoError(t, err)
		db.Close()
		return nil
	}

	checkSqlServerAvailability := func(ctx flow.Context) error {
		db, err := sql.Open("sqlserver", dockerConnectionString)
		if err != nil {
			return err
		}
		_, err = db.Exec("SELECT * FROM INFORMATION_SCHEMA.TABLES;")
		if err != nil {
			return err
		}
		return nil
	}

	flow.New(t, "SQLServer certification using SQL Server Docker").
		// Run SQL Server using Docker Compose.
		Step(dockercompose.Run("sqlserver", dockerComposeYAML)).
		Step("wait for SQL Server readiness", retry.Do(time.Second*3, 10, checkSqlServerAvailability)).

		// Run the Dapr sidecar with the SQL Server component.
		Step(sidecar.Run(sidecarNamePrefix+"dockerDefault",
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
		Step("Run basic test again to verify reconnection occurred", basicTest, sidecar.Stop(sidecarNamePrefix+"dockerDefault")).
		Step("Stopping SQL Server Docker container", dockercompose.Stop("sqlserver", dockerComposeYAML)).
		Run()

	ports, err = dapr_testing.GetFreePorts(2)
	assert.NoError(t, err)

	currentGrpcPort = ports[0]
	currentHttpPort = ports[1]

	flow.New(t, "Using existing custom schema with indexed data").
		// Run SQL Server using Docker Compose.
		Step(dockercompose.Run("sqlserver", dockerComposeYAML)).
		Step("wait for SQL Server readiness", retry.Do(time.Second*3, 10, checkSqlServerAvailability)).
		Step("Creating schema", createCustomSchema).

		// Run the Dapr sidecar with the SQL Server component.
		Step(sidecar.Run(sidecarNamePrefix+"dockerCustomSchema",
			embedded.WithoutApp(),
			embedded.WithDaprGRPCPort(currentGrpcPort),
			embedded.WithDaprHTTPPort(currentHttpPort),
			embedded.WithComponentsPath("components/docker/customschemawithindex"),
			runtime.WithStates(
				state_loader.New("sqlserver", func() state.Store {
					return state_sqlserver.NewSQLServerStateStore(log)
				}),
			))).
		Step("Run indexed properties verfication test", verifyIndexedPopertiesTest, sidecar.Stop(sidecarNamePrefix+"dockerCustomSchema")).
		Step("Stopping SQL Server Docker container", dockercompose.Stop("sqlserver", dockerComposeYAML)).
		Run()

	ports, err = dapr_testing.GetFreePorts(2)
	assert.NoError(t, err)

	currentGrpcPort = ports[0]
	currentHttpPort = ports[1]

	flow.New(t, "SQL Server certification using Azure SQL").
		// Run the Dapr sidecar with the SQL Server component.
		Step(sidecar.Run(sidecarNamePrefix+"azure",
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
		Step("interrupt network",
			network.InterruptNetwork(40*time.Second, nil, nil, "1433", "1434")).

		// Component should recover at this point.
		Step("wait", flow.Sleep(10*time.Second)).
		Step("Run basic test again to verify reconnection occurred", basicTest, sidecar.Stop(sidecarNamePrefix+"azure")).
		Run()
}
