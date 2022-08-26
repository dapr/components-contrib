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

package sqlserver_test

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	// State.

	state_sqlserver "github.com/dapr/components-contrib/state/sqlserver"
	state_loader "github.com/dapr/dapr/pkg/components/state"
	"github.com/dapr/kit/logger"

	// Secret stores.

	secretstore_env "github.com/dapr/components-contrib/secretstores/local/env"
	secretstores_loader "github.com/dapr/dapr/pkg/components/secretstores"

	// Dapr runtime and Go-SDK
	"github.com/dapr/dapr/pkg/runtime"
	dapr_testing "github.com/dapr/dapr/pkg/testing"
	"github.com/dapr/go-sdk/client"

	// Certification testing runnables
	"github.com/dapr/components-contrib/tests/certification/embedded"
	"github.com/dapr/components-contrib/tests/certification/flow"

	"github.com/dapr/components-contrib/tests/certification/flow/dockercompose"
	"github.com/dapr/components-contrib/tests/certification/flow/network"
	"github.com/dapr/components-contrib/tests/certification/flow/retry"
	"github.com/dapr/components-contrib/tests/certification/flow/sidecar"
)

const (
	sidecarNamePrefix       = "sqlserver-sidecar-"
	dockerComposeYAML       = "docker-compose.yml"
	stateStoreName          = "dapr-state-store"
	certificationTestPrefix = "stable-certification-"
	dockerConnectionString  = "server=localhost;user id=sa;password=Pass@Word1;port=1433;"
)

func TestSqlServer(t *testing.T) {
	ports, err := dapr_testing.GetFreePorts(2)
	assert.NoError(t, err)

	currentGrpcPort := ports[0]
	currentHTTPPort := ports[1]

	basicTest := func(ctx flow.Context) error {
		client, err := client.NewClientWithPort(fmt.Sprint(currentGrpcPort))
		if err != nil {
			panic(err)
		}
		defer client.Close()

		// save state, default options: strong, last-write
		err = client.SaveState(ctx, stateStoreName, certificationTestPrefix+"key1", []byte("certificationdata"), nil)
		assert.NoError(t, err)

		// get state
		item, err := client.GetState(ctx, stateStoreName, certificationTestPrefix+"key1", nil)
		assert.NoError(t, err)
		assert.Equal(t, "certificationdata", string(item.Value))

		// delete state
		err = client.DeleteState(ctx, stateStoreName, certificationTestPrefix+"key1", nil)
		assert.NoError(t, err)

		return nil
	}

	// this test function heavily depends on the values defined in ./components/docker/customschemawithindex
	verifyIndexedPopertiesTest := func(ctx flow.Context) error {
		// verify indices were created by Dapr as specified in the component metadata
		db, err := sql.Open("sqlserver", fmt.Sprintf("%sdatabase=certificationtest;", dockerConnectionString))
		assert.NoError(t, err)
		defer db.Close()

		rows, err := db.Query("sp_helpindex '[customschema].[mystates]'")
		assert.NoError(t, err)
		assert.NoError(t, rows.Err())
		defer rows.Close()

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

		// write JSON data to the state store (which will automatically be indexed in separate columns)
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
		err = client.SaveState(ctx, stateStoreName, certificationTestPrefix+"key1", data, nil)
		assert.NoError(t, err)

		// get state for key certificationkey1
		item, err := client.GetState(ctx, stateStoreName, certificationTestPrefix+"key1", nil)
		assert.NoError(t, err)
		assert.Equal(t, string(data), string(item.Value))

		// check that Dapr wrote the indexed properties to separate columns
		rows, err = db.Query("SELECT TOP 1 transactionid, customerid FROM [customschema].[mystates];")
		assert.NoError(t, err)
		assert.NoError(t, rows.Err())
		defer rows.Close()
		if rows.Next() {
			var transactionID int
			var customerID string
			err = rows.Scan(&transactionID, &customerID)
			assert.NoError(t, err)
			assert.Equal(t, transactionID, order.ID)
			assert.Equal(t, customerID, order.Customer)
		} else {
			assert.Fail(t, "no rows returned")
		}

		// delete state for key certificationkey1
		err = client.DeleteState(ctx, stateStoreName, certificationTestPrefix+"key1", nil)
		assert.NoError(t, err)

		return nil
	}

	// helper function for testing the use of an existing custom schema
	createCustomSchema := func(ctx flow.Context) error {
		db, err := sql.Open("sqlserver", dockerConnectionString)
		assert.NoError(t, err)
		_, err = db.Exec("CREATE SCHEMA customschema;")
		assert.NoError(t, err)
		db.Close()
		return nil
	}

	// helper function to insure the SQL Server Docker Container is truly ready
	checkSQLServerAvailability := func(ctx flow.Context) error {
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

	// checks the state store component is not vulnerable to SQL injection
	verifySQLInjectionTest := func(ctx flow.Context) error {
		client, err := client.NewClientWithPort(fmt.Sprint(currentGrpcPort))
		if err != nil {
			panic(err)
		}
		defer client.Close()

		// common SQL injection techniques for SQL Server
		sqlInjectionAttempts := []string{
			"; DROP states--",
			"dapr' OR '1'='1",
		}

		for _, sqlInjectionAttempt := range sqlInjectionAttempts {
			// save state with sqlInjectionAttempt's value as key, default options: strong, last-write
			err = client.SaveState(ctx, stateStoreName, sqlInjectionAttempt, []byte(sqlInjectionAttempt), nil)
			assert.NoError(t, err)

			// get state for key sqlInjectionAttempt's value
			item, err := client.GetState(ctx, stateStoreName, sqlInjectionAttempt, nil)
			assert.NoError(t, err)
			assert.Equal(t, sqlInjectionAttempt, string(item.Value))

			// delete state for key sqlInjectionAttempt's value
			err = client.DeleteState(ctx, stateStoreName, sqlInjectionAttempt, nil)
			assert.NoError(t, err)
		}

		return nil
	}

	flow.New(t, "SQLServer certification using SQL Server Docker").
		// Run SQL Server using Docker Compose.
		Step(dockercompose.Run("sqlserver", dockerComposeYAML)).
		Step("wait for SQL Server readiness", retry.Do(time.Second*3, 10, checkSQLServerAvailability)).

		// Run the Dapr sidecar with the SQL Server component.
		Step(sidecar.Run(sidecarNamePrefix+"dockerDefault",
			embedded.WithoutApp(),
			embedded.WithDaprGRPCPort(currentGrpcPort),
			embedded.WithDaprHTTPPort(currentHTTPPort),
			embedded.WithComponentsPath("components/docker/default"),
			componentRuntimeOptions(),
		)).
		Step("Run basic test", basicTest).
		// Introduce network interruption of 15 seconds
		// Note: the connection timeout is set to 5 seconds via the component metadata connection string.
		Step("interrupt network",
			network.InterruptNetwork(15*time.Second, nil, nil, "1433", "1434")).

		// Component should recover at this point.
		Step("wait", flow.Sleep(5*time.Second)).
		Step("Run basic test again to verify reconnection occurred", basicTest).
		Step("Run SQL injection test", verifySQLInjectionTest, sidecar.Stop(sidecarNamePrefix+"dockerDefault")).
		Step("Stopping SQL Server Docker container", dockercompose.Stop("sqlserver", dockerComposeYAML)).
		Run()

	ports, err = dapr_testing.GetFreePorts(2)
	assert.NoError(t, err)

	currentGrpcPort = ports[0]
	currentHTTPPort = ports[1]

	flow.New(t, "Using existing custom schema with indexed data").
		// Run SQL Server using Docker Compose.
		Step(dockercompose.Run("sqlserver", dockerComposeYAML)).
		Step("wait for SQL Server readiness", retry.Do(time.Second*3, 10, checkSQLServerAvailability)).
		Step("Creating schema", createCustomSchema).

		// Run the Dapr sidecar with the SQL Server component.
		Step(sidecar.Run(sidecarNamePrefix+"dockerCustomSchema",
			embedded.WithoutApp(),
			embedded.WithDaprGRPCPort(currentGrpcPort),
			embedded.WithDaprHTTPPort(currentHTTPPort),
			embedded.WithComponentsPath("components/docker/customschemawithindex"),
			componentRuntimeOptions(),
		)).
		Step("Run indexed properties verification test", verifyIndexedPopertiesTest, sidecar.Stop(sidecarNamePrefix+"dockerCustomSchema")).
		Step("Stopping SQL Server Docker container", dockercompose.Stop("sqlserver", dockerComposeYAML)).
		Run()

	ports, err = dapr_testing.GetFreePorts(2)
	assert.NoError(t, err)

	currentGrpcPort = ports[0]
	currentHTTPPort = ports[1]

	flow.New(t, "SQL Server certification using Azure SQL").
		// Run the Dapr sidecar with the SQL Server component.
		Step(sidecar.Run(sidecarNamePrefix+"azure",
			embedded.WithoutApp(),
			embedded.WithDaprGRPCPort(currentGrpcPort),
			embedded.WithDaprHTTPPort(currentHTTPPort),
			embedded.WithComponentsPath("components/azure"),
			componentRuntimeOptions(),
		)).
		Step("Run basic test", basicTest).
		Step("interrupt network",
			network.InterruptNetwork(40*time.Second, nil, nil, "1433", "1434")).

		// Component should recover at this point.
		Step("wait", flow.Sleep(10*time.Second)).
		Step("Run basic test again to verify reconnection occurred", basicTest).
		Step("Run SQL injection test", verifySQLInjectionTest, sidecar.Stop(sidecarNamePrefix+"azure")).
		Run()
}

func componentRuntimeOptions() []runtime.Option {
	log := logger.NewLogger("dapr.components")

	stateRegistry := state_loader.NewRegistry()
	stateRegistry.Logger = log
	stateRegistry.RegisterComponent(state_sqlserver.NewSQLServerStateStore, "sqlserver")

	secretstoreRegistry := secretstores_loader.NewRegistry()
	secretstoreRegistry.Logger = log
	secretstoreRegistry.RegisterComponent(secretstore_env.NewEnvSecretStore, "local.env")

	return []runtime.Option{
		runtime.WithStates(stateRegistry),
		runtime.WithSecretStores(secretstoreRegistry),
	}
}
