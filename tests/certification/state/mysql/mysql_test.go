/*
Copyright 2022 The Dapr Authors
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

package main

import (
	"database/sql"
	"errors"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/state"
	stateMysql "github.com/dapr/components-contrib/state/mysql"
	"github.com/dapr/components-contrib/tests/certification/embedded"
	"github.com/dapr/components-contrib/tests/certification/flow"
	"github.com/dapr/components-contrib/tests/certification/flow/dockercompose"
	"github.com/dapr/components-contrib/tests/certification/flow/sidecar"
	stateLoader "github.com/dapr/dapr/pkg/components/state"
	"github.com/dapr/dapr/pkg/runtime"
	daprTesting "github.com/dapr/dapr/pkg/testing"
	daprClient "github.com/dapr/go-sdk/client"
	"github.com/dapr/kit/logger"
)

const (
	sidecarNamePrefix       = "mysql-sidecar-"
	dockerComposeYAML       = "docker-compose.yaml"
	certificationTestPrefix = "stable-certification-"
	timeout                 = 5 * time.Second

	defaultSchemaName = "dapr_state_store"
	defaultTableName  = "state"

	mysqlConnString   = "root:root@tcp(localhost:3306)/?allowNativePasswords=true"
	mariadbConnString = "root:root@tcp(localhost:3307)/"
)

func TestMySQL(t *testing.T) {
	log := logger.NewLogger("dapr.components")

	ports, err := daprTesting.GetFreePorts(1)
	require.NoError(t, err)
	currentGrpcPort := ports[0]

	registeredComponents := [2]*stateMysql.MySQL{}
	stateRegistry := stateLoader.NewRegistry()
	stateRegistry.Logger = log
	n := atomic.Int32{}
	stateRegistry.RegisterComponent(func(_ logger.Logger) state.Store {
		component := stateMysql.NewMySQLStateStore(log).(*stateMysql.MySQL)
		registeredComponents[n.Add(1)-1] = component
		return component
	}, "mysql")

	basicTest := func(stateStoreName string) func(ctx flow.Context) error {
		return func(ctx flow.Context) error {
			client, err := daprClient.NewClientWithPort(strconv.Itoa(currentGrpcPort))
			if err != nil {
				panic(err)
			}
			defer client.Close()

			// save state
			err = client.SaveState(ctx, stateStoreName, certificationTestPrefix+"key1", []byte("mysqlcert1"), nil)
			require.NoError(t, err)

			// get state
			item, err := client.GetState(ctx, stateStoreName, certificationTestPrefix+"key1", nil)
			require.NoError(t, err)
			assert.Equal(t, "mysqlcert1", string(item.Value))

			// update state
			errUpdate := client.SaveState(ctx, stateStoreName, certificationTestPrefix+"key1", []byte("mysqlqlcert2"), nil)
			require.NoError(t, errUpdate)
			item, errUpdatedGet := client.GetState(ctx, stateStoreName, certificationTestPrefix+"key1", nil)
			require.NoError(t, errUpdatedGet)
			assert.Equal(t, "mysqlqlcert2", string(item.Value))

			// delete state
			err = client.DeleteState(ctx, stateStoreName, certificationTestPrefix+"key1", nil)
			require.NoError(t, err)

			return nil
		}
	}

	eTagTest := func(stateStoreName string) func(ctx flow.Context) error {
		return func(ctx flow.Context) error {
			client, err := daprClient.NewClientWithPort(strconv.Itoa(currentGrpcPort))
			if err != nil {
				panic(err)
			}
			defer client.Close()

			err = client.SaveState(ctx, stateStoreName, "k", []byte("v1"), nil)
			require.NoError(t, err)

			resp1, err := client.GetState(ctx, stateStoreName, "k", nil)
			require.NoError(t, err)

			err = client.SaveStateWithETag(ctx, stateStoreName, "k", []byte("v2"), resp1.Etag, nil)
			require.NoError(t, err)

			resp2, err := client.GetState(ctx, stateStoreName, "k", nil)
			require.NoError(t, err)

			err = client.SaveStateWithETag(ctx, stateStoreName, "k", []byte("v3"), "900invalid", nil)
			require.Error(t, err)

			resp3, err := client.GetState(ctx, stateStoreName, "k", nil)
			require.NoError(t, err)
			assert.Equal(t, resp2.Etag, resp3.Etag)
			assert.Equal(t, "v2", string(resp3.Value))

			return nil
		}
	}

	transactionsTest := func(stateStoreName string) func(ctx flow.Context) error {
		return func(ctx flow.Context) error {
			client, err := daprClient.NewClientWithPort(strconv.Itoa(currentGrpcPort))
			if err != nil {
				panic(err)
			}
			defer client.Close()

			err = client.ExecuteStateTransaction(ctx, stateStoreName, nil, []*daprClient.StateOperation{
				{
					Type: daprClient.StateOperationTypeUpsert,
					Item: &daprClient.SetStateItem{
						Key:   "reqKey1",
						Value: []byte("reqVal1"),
						Metadata: map[string]string{
							"ttlInSeconds": "-1",
						},
					},
				},
				{
					Type: daprClient.StateOperationTypeUpsert,
					Item: &daprClient.SetStateItem{
						Key:   "reqKey2",
						Value: []byte("reqVal2"),
						Metadata: map[string]string{
							"ttlInSeconds": "222",
						},
					},
				},
				{
					Type: daprClient.StateOperationTypeUpsert,
					Item: &daprClient.SetStateItem{
						Key:   "reqKey3",
						Value: []byte("reqVal3"),
					},
				},
				{
					Type: daprClient.StateOperationTypeUpsert,
					Item: &daprClient.SetStateItem{
						Key:   "reqKey1",
						Value: []byte("reqVal101"),
						Metadata: map[string]string{
							"ttlInSeconds": "50",
						},
					},
				},
				{
					Type: daprClient.StateOperationTypeUpsert,
					Item: &daprClient.SetStateItem{
						Key:   "reqKey3",
						Value: []byte("reqVal103"),
						Metadata: map[string]string{
							"ttlInSeconds": "50",
						},
					},
				},
			})
			require.NoError(t, err)

			resp1, err := client.GetState(ctx, stateStoreName, "reqKey1", nil)
			require.NoError(t, err)
			assert.Equal(t, "reqVal101", string(resp1.Value))

			resp3, err := client.GetState(ctx, stateStoreName, "reqKey3", nil)
			require.NoError(t, err)
			assert.Equal(t, "reqVal103", string(resp3.Value))
			return nil
		}
	}

	testGetAfterDBRestart := func(stateStoreName string) func(ctx flow.Context) error {
		return func(ctx flow.Context) error {
			client, err := daprClient.NewClientWithPort(strconv.Itoa(currentGrpcPort))
			if err != nil {
				panic(err)
			}
			defer client.Close()

			// save state
			_, err = client.GetState(ctx, stateStoreName, certificationTestPrefix+"key1", nil)
			assert.NoError(t, err)

			return nil
		}
	}

	// checks the state store component is not vulnerable to SQL injection
	verifySQLInjectionTest := func(stateStoreName string) func(ctx flow.Context) error {
		return func(ctx flow.Context) error {
			client, err := daprClient.NewClientWithPort(strconv.Itoa(currentGrpcPort))
			if err != nil {
				panic(err)
			}
			defer client.Close()

			// common SQL injection techniques for MySQL
			sqlInjectionAttempts := []string{
				"DROP TABLE dapr_user",
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
	}

	// checks that pings fail
	pingFail := func(idx int) func(ctx flow.Context) error {
		return func(ctx flow.Context) (err error) {
			component := registeredComponents[idx]

			// Should fail
			err = component.Ping()
			require.Error(t, err)
			assert.Equal(t, "driver: bad connection", err.Error())

			return nil
		}
	}

	// checks that operations time out
	// Currently disabled because the comcast library can't block traffic to a Docker container
	/*timeoutTest := func(parentCtx flow.Context) (err error) {
		ctx, cancel := context.WithCancel(parentCtx)
		defer cancel()

		go network.InterruptNetworkWithContext(ctx, 30*time.Second, nil, nil, "3306", "3307")
		time.Sleep(5 * time.Second)

		for idx := 0; idx < 2; idx++ {
			log.Infof("Testing timeout for component %d", idx)

			component := registeredComponents[idx]

			start := time.Now()
			// Should fail
			err = component.Ping()
			assert.Error(t, err)
			assert.Truef(t, errors.Is(err, context.DeadlineExceeded), "expected context.DeadlineExceeded but got %v", err)
			assert.GreaterOrEqual(t, time.Since(start), timeout)
		}

		return nil
	}*/

	// checks that the connection is closed when the component is closed
	closeTest := func(idx int) func(ctx flow.Context) error {
		return func(ctx flow.Context) (err error) {
			component := registeredComponents[idx]

			// Check connection is active
			err = component.Ping()
			require.NoError(t, err)

			// Close the component
			err = component.Close()
			require.NoError(t, err)

			// Ensure the connection is closed
			err = component.Ping()
			require.Error(t, err)
			assert.Truef(t, errors.Is(err, sql.ErrConnDone), "expected sql.ErrConnDone but got %v", err)

			return nil
		}
	}

	// checks that metadata options schemaName and tableName behave correctly
	metadataTest := func(connString string, schemaName string, tableName string) func(ctx flow.Context) error {
		return func(ctx flow.Context) (err error) {
			properties := map[string]string{
				"connectionString": connString,
			}

			// Check if schemaName and tableName are set to custom values
			if schemaName != "" {
				properties["schemaName"] = schemaName
			} else {
				schemaName = defaultSchemaName
			}
			if tableName != "" {
				properties["tableName"] = tableName
			} else {
				tableName = defaultTableName
			}

			// Init the component
			component := stateMysql.NewMySQLStateStore(log).(*stateMysql.MySQL)
			component.Init(state.Metadata{
				Base: metadata.Base{
					Properties: properties,
				},
			})

			// Check connection is active
			err = component.Ping()
			require.NoError(t, err)

			var exists int
			conn := component.GetConnection()
			require.NotNil(t, conn)

			// Check that the database exists
			query := `SELECT EXISTS (
				SELECT SCHEMA_NAME FROM information_schema.schemata WHERE SCHEMA_NAME = ?
			) AS 'exists'`
			err = conn.QueryRow(query, schemaName).Scan(&exists)
			require.NoError(t, err)
			assert.Equal(t, 1, exists)

			// Check that the table exists
			query = `SELECT EXISTS (
				SELECT TABLE_NAME FROM information_schema.tables WHERE TABLE_NAME = ?
			) AS 'exists'`
			err = conn.QueryRow(query, tableName).Scan(&exists)
			require.NoError(t, err)
			assert.Equal(t, 1, exists)

			// Close the component
			err = component.Close()
			require.NoError(t, err)

			return nil
		}
	}

	flow.New(t, "Run tests").
		Step(dockercompose.Run("db", dockerComposeYAML)).
		Step("Wait for databases to start", flow.Sleep(30*time.Second)).
		Step(sidecar.Run(sidecarNamePrefix+"dockerDefault",
			embedded.WithoutApp(),
			embedded.WithDaprGRPCPort(currentGrpcPort),
			embedded.WithComponentsPath("components/docker/default"),
			runtime.WithStates(stateRegistry),
		)).
		// Test flow on mysql and mariadb
		Step("Run CRUD test on mysql", basicTest("mysql")).
		Step("Run CRUD test on mariadb", basicTest("mariadb")).
		Step("Run eTag test on mysql", eTagTest("mysql")).
		Step("Run eTag test on mariadb", eTagTest("mariadb")).
		Step("Run transactions test", transactionsTest("mysql")).
		Step("Run transactions test", transactionsTest("mariadb")).
		Step("Run SQL injection test on mysql", verifySQLInjectionTest("mysql")).
		Step("Run SQL injection test on mariadb", verifySQLInjectionTest("mariadb")).
		//Step("Interrupt network and simulate timeouts", timeoutTest).
		Step("Stop mysql", dockercompose.Stop("db", dockerComposeYAML, "mysql")).
		Step("Stop mariadb", dockercompose.Stop("db", dockerComposeYAML, "mariadb")).
		Step("Wait for databases to stop", flow.Sleep(10*time.Second)).
		// We don't know exactly which database is which (since init order isn't deterministic), so we'll just test both
		Step("Close database connection 1", pingFail(0)).
		Step("Close database connection 2", pingFail(1)).
		Step("Start mysql", dockercompose.Start("db", dockerComposeYAML, "mysql")).
		Step("Start mariadb", dockercompose.Start("db", dockerComposeYAML, "mariadb")).
		Step("Wait for databases to start", flow.Sleep(10*time.Second)).
		Step("Run connection test on mysql", testGetAfterDBRestart("mysql")).
		Step("Run connection test on mariadb", testGetAfterDBRestart("mariadb")).
		// Test closing the connection
		// We don't know exactly which database is which (since init order isn't deterministic), so we'll just close both
		Step("Close database connection 1", closeTest(0)).
		Step("Close database connection 2", closeTest(1)).
		// Metadata
		Step("Default schemaName and tableName on mysql", metadataTest(mysqlConnString, "", "")).
		Step("Custom schemaName and tableName on mysql", metadataTest(mysqlConnString, "mydaprdb", "mytable")).
		Step("Default schemaName and tableName on mariadb", metadataTest(mariadbConnString, "", "")).
		Step("Custom schemaName and tableName on mariadb", metadataTest(mariadbConnString, "mydaprdb", "mytable")).
		// Run tests
		Run()
}
