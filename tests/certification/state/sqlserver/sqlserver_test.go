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
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	// State.
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/state"
	state_sqlserver "github.com/dapr/components-contrib/state/sqlserver"
	state_loader "github.com/dapr/dapr/pkg/components/state"
	"github.com/dapr/kit/logger"

	// Secret stores.
	secretstore_env "github.com/dapr/components-contrib/secretstores/local/env"
	secretstores_loader "github.com/dapr/dapr/pkg/components/secretstores"

	// Dapr runtime and Go-SDK

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
		ctx.T.Run("basic test", func(t *testing.T) {
			client, err := client.NewClientWithPort(strconv.Itoa(currentGrpcPort))
			if err != nil {
				panic(err)
			}
			defer client.Close()

			// save state, default options: strong, last-write
			err = client.SaveState(ctx, stateStoreName, certificationTestPrefix+"key1", []byte("certificationdata"), nil)
			require.NoError(t, err)

			// get state
			item, err := client.GetState(ctx, stateStoreName, certificationTestPrefix+"key1", nil)
			require.NoError(t, err)
			assert.Equal(t, "certificationdata", string(item.Value))

			// delete state
			err = client.DeleteState(ctx, stateStoreName, certificationTestPrefix+"key1", nil)
			require.NoError(t, err)
		})
		return nil
	}

	basicTTLTest := func(ctx flow.Context) error {
		ctx.T.Run("basic TTL test", func(t *testing.T) {
			client, err := client.NewClientWithPort(strconv.Itoa(currentGrpcPort))
			if err != nil {
				panic(err)
			}
			defer client.Close()

			err = client.SaveState(ctx, stateStoreName, certificationTestPrefix+"key2", []byte("certificationdata"), map[string]string{
				"ttlInSeconds": "86400",
			})
			require.NoError(t, err)

			// get state
			item, err := client.GetState(ctx, stateStoreName, certificationTestPrefix+"key2", nil)
			require.NoError(t, err)
			assert.Equal(t, "certificationdata", string(item.Value))
			assert.Contains(t, item.Metadata, "ttlExpireTime")
			expireTime, err := time.Parse(time.RFC3339, item.Metadata["ttlExpireTime"])
			_ = assert.NoError(t, err) &&
				assert.InDelta(t, time.Now().Add(24*time.Hour).Unix(), expireTime.Unix(), 10)

			err = client.SaveState(ctx, stateStoreName, certificationTestPrefix+"key2", []byte("certificationdata"), map[string]string{
				"ttlInSeconds": "1",
			})
			require.NoError(t, err)

			time.Sleep(2 * time.Second)

			item, err = client.GetState(ctx, stateStoreName, certificationTestPrefix+"key2", nil)
			require.NoError(t, err)
			assert.Nil(t, item.Value)
			assert.Nil(t, item.Metadata)
		})

		return nil
	}

	// this test function heavily depends on the values defined in ./components/docker/customschemawithindex
	verifyIndexedPopertiesTest := func(ctx flow.Context) error {
		// verify indices were created by Dapr as specified in the component metadata
		db, err := sql.Open("mssql", fmt.Sprintf("%sdatabase=certificationtest;", dockerConnectionString))
		require.NoError(ctx.T, err)
		defer db.Close()

		rows, err := db.Query("sp_helpindex '[customschema].[mystates]'")
		require.NoError(ctx.T, err)
		assert.NoError(ctx.T, rows.Err())
		defer rows.Close()

		indexFoundCount := 0
		for rows.Next() {
			var indexedField, otherdata1, otherdata2 string
			err = rows.Scan(&indexedField, &otherdata1, &otherdata2)
			assert.NoError(ctx.T, err)

			expectedIndices := []string{"IX_customerid", "IX_transactionid", "PK_mystates"}
			for _, item := range expectedIndices {
				if item == indexedField {
					indexFoundCount++
					break
				}
			}
		}
		assert.Equal(ctx.T, 3, indexFoundCount)

		// write JSON data to the state store (which will automatically be indexed in separate columns)
		client, err := client.NewClientWithPort(strconv.Itoa(currentGrpcPort))
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
		assert.NoError(ctx.T, err)

		// save state with the key certificationkey1, default options: strong, last-write
		err = client.SaveState(ctx, stateStoreName, certificationTestPrefix+"key1", data, nil)
		require.NoError(ctx.T, err)

		// get state for key certificationkey1
		item, err := client.GetState(ctx, stateStoreName, certificationTestPrefix+"key1", nil)
		assert.NoError(ctx.T, err)
		assert.Equal(ctx.T, string(data), string(item.Value))

		// check that Dapr wrote the indexed properties to separate columns
		rows, err = db.Query("SELECT TOP 1 transactionid, customerid FROM [customschema].[mystates];")
		assert.NoError(ctx.T, err)
		assert.NoError(ctx.T, rows.Err())
		defer rows.Close()
		if rows.Next() {
			var transactionID int
			var customerID string
			err = rows.Scan(&transactionID, &customerID)
			assert.NoError(ctx.T, err)
			assert.Equal(ctx.T, transactionID, order.ID)
			assert.Equal(ctx.T, customerID, order.Customer)
		} else {
			assert.Fail(ctx.T, "no rows returned")
		}

		// delete state for key certificationkey1
		err = client.DeleteState(ctx, stateStoreName, certificationTestPrefix+"key1", nil)
		assert.NoError(ctx.T, err)

		return nil
	}

	// helper function for testing the use of an existing custom schema
	createCustomSchema := func(ctx flow.Context) error {
		db, err := sql.Open("mssql", dockerConnectionString)
		assert.NoError(ctx.T, err)
		_, err = db.Exec("CREATE SCHEMA customschema;")
		assert.NoError(ctx.T, err)
		db.Close()
		return nil
	}

	// helper function to insure the SQL Server Docker Container is truly ready
	checkSQLServerAvailability := func(ctx flow.Context) error {
		db, err := sql.Open("mssql", dockerConnectionString)
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
		client, err := client.NewClientWithPort(strconv.Itoa(currentGrpcPort))
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
			assert.NoError(ctx.T, err)

			// get state for key sqlInjectionAttempt's value
			item, err := client.GetState(ctx, stateStoreName, sqlInjectionAttempt, nil)
			assert.NoError(ctx.T, err)
			assert.Equal(ctx.T, sqlInjectionAttempt, string(item.Value))

			// delete state for key sqlInjectionAttempt's value
			err = client.DeleteState(ctx, stateStoreName, sqlInjectionAttempt, nil)
			assert.NoError(ctx.T, err)
		}

		return nil
	}

	// Validates TTLs and garbage collections
	ttlTest := func(connString string) func(ctx flow.Context) error {
		return func(ctx flow.Context) error {
			log := logger.NewLogger("dapr.components")
			md := state.Metadata{
				Base: metadata.Base{
					Name: "ttltest",
					Properties: map[string]string{
						"connectionString":  connString,
						"databaseName":      "certificationtest",
						"tableName":         "ttltest",
						"metadataTableName": "ttltest_metadata",
						"schema":            "ttlschema",
					},
				},
			}

			ctx.T.Run("parse cleanupIntervalInSeconds", func(t *testing.T) {
				t.Run("default value", func(t *testing.T) {
					// Default value is 1 hr
					md.Properties["cleanupIntervalInSeconds"] = ""
					storeObj := state_sqlserver.New(log).(*state_sqlserver.SQLServer)

					err := storeObj.Init(context.Background(), md)
					require.NoError(t, err, "failed to init")
					defer storeObj.Close()

					cleanupInterval := storeObj.GetCleanupInterval()
					require.NotNil(t, cleanupInterval)
					assert.Equal(t, time.Duration(1*time.Hour), *cleanupInterval)
				})

				t.Run("positive value", func(t *testing.T) {
					// A positive value is interpreted in seconds
					md.Properties["cleanupIntervalInSeconds"] = "10"
					storeObj := state_sqlserver.New(log).(*state_sqlserver.SQLServer)

					err := storeObj.Init(context.Background(), md)
					require.NoError(t, err, "failed to init")
					defer storeObj.Close()

					cleanupInterval := storeObj.GetCleanupInterval()
					require.NotNil(t, cleanupInterval)
					assert.Equal(t, time.Duration(10*time.Second), *cleanupInterval)
				})

				t.Run("disabled", func(t *testing.T) {
					// A value of <=0 means that the cleanup is disabled
					md.Properties["cleanupIntervalInSeconds"] = "0"
					storeObj := state_sqlserver.New(log).(*state_sqlserver.SQLServer)

					err := storeObj.Init(context.Background(), md)
					require.NoError(t, err, "failed to init")
					defer storeObj.Close()

					cleanupInterval := storeObj.GetCleanupInterval()
					assert.Nil(t, cleanupInterval)
				})
			})

			ctx.T.Run("cleanup", func(t *testing.T) {
				dbClient, err := sql.Open("mssql", connString)
				require.NoError(t, err)

				t.Run("automatically delete expiredate records", func(t *testing.T) {
					// Run every second
					md.Properties["cleanupIntervalInSeconds"] = "1"

					storeObj := state_sqlserver.New(log).(*state_sqlserver.SQLServer)
					err := storeObj.Init(context.Background(), md)
					require.NoError(t, err, "failed to init")
					defer storeObj.Close()

					// Seed the database with some records
					err = clearTable(ctx, dbClient, "ttlschema", "ttltest")
					require.NoError(t, err, "failed to clear table")
					err = populateTTLRecords(ctx, dbClient, "ttlschema", "ttltest")
					require.NoError(t, err, "failed to seed records")

					cleanupInterval := storeObj.GetCleanupInterval()
					assert.NotNil(t, cleanupInterval)
					assert.Equal(t, time.Duration(time.Second), *cleanupInterval)

					// Wait up to 3 seconds then verify we have only 10 rows left
					var count int
					assert.Eventually(t, func() bool {
						count, err = countRowsInTable(ctx, dbClient, "ttlschema", "ttltest")
						require.NoError(t, err, "failed to run query to count rows")
						return count == 10
					}, 3*time.Second, 10*time.Millisecond, "expected 10 rows, got %d", count)

					// The "last-cleanup" value should be <= 1 second (+ a bit of buffer)
					lastCleanup, err := loadLastCleanupInterval(ctx, dbClient, "ttlschema", "ttltest_metadata")
					require.NoError(t, err, "failed to load value for 'last-cleanup'")
					assert.LessOrEqual(t, lastCleanup, int64(1200))

					// Wait 6 more seconds and verify there are no more rows left
					assert.Eventually(t, func() bool {
						count, err = countRowsInTable(ctx, dbClient, "ttlschema", "ttltest")
						require.NoError(t, err, "failed to run query to count rows")
						return count == 0
					}, 6*time.Second, 10*time.Millisecond, "expected 0 rows, got %d", count)

					// The "last-cleanup" value should be <= 1 second (+ a bit of buffer)
					lastCleanup, err = loadLastCleanupInterval(ctx, dbClient, "ttlschema", "ttltest_metadata")
					require.NoError(t, err, "failed to load value for 'last-cleanup'")
					assert.LessOrEqual(t, lastCleanup, int64(1200))
				})

				t.Run("cleanup concurrency", func(t *testing.T) {
					// Set to run every hour
					// (we'll manually trigger more frequent iterations)
					md.Properties["cleanupIntervalInSeconds"] = "3600"

					storeObj := state_sqlserver.New(log).(*state_sqlserver.SQLServer)
					err := storeObj.Init(context.Background(), md)
					require.NoError(t, err, "failed to init")
					defer storeObj.Close()

					cleanupInterval := storeObj.GetCleanupInterval()
					assert.NotNil(t, cleanupInterval)
					assert.Equal(t, time.Hour, *cleanupInterval)

					// Seed the database with some records
					err = clearTable(ctx, dbClient, "ttlschema", "ttltest")
					require.NoError(t, err, "failed to clear table")
					err = populateTTLRecords(ctx, dbClient, "ttlschema", "ttltest")
					require.NoError(t, err, "failed to seed records")

					// Validate that 20 records are present
					count, err := countRowsInTable(ctx, dbClient, "ttlschema", "ttltest")
					require.NoError(t, err, "failed to run query to count rows")
					assert.Equal(t, 20, count)

					// Set last-cleanup to 1s ago
					_, err = dbClient.ExecContext(ctx, `UPDATE [ttlschema].[ttltest_metadata] SET [Value] = CONVERT(nvarchar(MAX), DATEADD(second, -1, GETDATE()), 21) WHERE [Key] = 'last-cleanup'`)
					require.NoError(t, err, "failed to set last-cleanup")

					// The "last-cleanup" value
					lastCleanup, err := loadLastCleanupInterval(ctx, dbClient, "ttlschema", "ttltest_metadata")
					require.NoError(t, err, "failed to load value for 'last-cleanup'")
					assert.LessOrEqual(t, lastCleanup, int64(1200))
					lastCleanupValueOrig, err := getValueFromMetadataTable(ctx, dbClient, "ttlschema", "ttltest_metadata", "'last-cleanup'")
					require.NoError(t, err, "failed to load absolute value for 'last-cleanup'")
					require.NotEmpty(t, lastCleanupValueOrig)

					// Trigger the background cleanup, which should do nothing because the last cleanup was < 3600s
					require.NoError(t, storeObj.CleanupExpired(), "CleanupExpired returned an error")

					// Validate that 20 records are still present
					count, err = countRowsInTable(ctx, dbClient, "ttlschema", "ttltest")
					require.NoError(t, err, "failed to run query to count rows")
					assert.Equal(t, 20, count)

					// The "last-cleanup" value should not have been changed
					lastCleanupValue, err := getValueFromMetadataTable(ctx, dbClient, "ttlschema", "ttltest_metadata", "'last-cleanup'")
					require.NoError(t, err, "failed to load absolute value for 'last-cleanup'")
					assert.Equal(t, lastCleanupValueOrig, lastCleanupValue)
				})
			})

			return nil
		}
	}

	t.Run("SQLServer certification using SQL Server Docker", func(t *testing.T) {
		flow.New(t, "SQLServer certification using SQL Server Docker").
			// Run SQL Server using Docker Compose.
			Step(dockercompose.Run("sqlserver", dockerComposeYAML)).
			Step("wait for SQL Server readiness", retry.Do(time.Second*3, 10, checkSQLServerAvailability)).

			// Run the Dapr sidecar with the SQL Server component.
			Step(sidecar.Run(sidecarNamePrefix+"dockerDefault",
				append(componentRuntimeOptions(),
					embedded.WithoutApp(),
					embedded.WithDaprGRPCPort(strconv.Itoa(currentGrpcPort)),
					embedded.WithDaprHTTPPort(strconv.Itoa(currentHTTPPort)),
					embedded.WithResourcesPath("components/docker/default"),
					embedded.WithProfilingEnabled(false),
				)...,
			)).
			Step("Run basic test", basicTest).
			Step("Run basic TTL test", basicTTLTest).
			// Introduce network interruption of 10 seconds
			// Note: the connection timeout is set to 5 seconds via the component metadata connection string.
			Step("interrupt network",
				network.InterruptNetwork(10*time.Second, nil, nil, "1433", "1434")).

			// Component should recover at this point.
			Step("wait", flow.Sleep(5*time.Second)).
			Step("Run basic test again to verify reconnection occurred", basicTest).
			Step("Run SQL injection test", verifySQLInjectionTest, sidecar.Stop(sidecarNamePrefix+"dockerDefault")).
			Step("run TTL test", ttlTest(dockerConnectionString+"database=certificationtest;")).
			Step("Stopping SQL Server Docker container", dockercompose.Stop("sqlserver", dockerComposeYAML)).
			Run()
	})

	ports, err = dapr_testing.GetFreePorts(2)
	assert.NoError(t, err)

	currentGrpcPort = ports[0]
	currentHTTPPort = ports[1]

	t.Run("Using existing custom schema with indexed data", func(t *testing.T) {
		flow.New(t, "Using existing custom schema with indexed data").
			// Run SQL Server using Docker Compose.
			Step(dockercompose.Run("sqlserver", dockerComposeYAML)).
			Step("wait for SQL Server readiness", retry.Do(time.Second*3, 10, checkSQLServerAvailability)).
			Step("Creating schema", createCustomSchema).

			// Run the Dapr sidecar with the SQL Server component.
			Step(sidecar.Run(sidecarNamePrefix+"dockerCustomSchema",
				append(componentRuntimeOptions(),
					embedded.WithoutApp(),
					embedded.WithDaprGRPCPort(strconv.Itoa(currentGrpcPort)),
					embedded.WithDaprHTTPPort(strconv.Itoa(currentHTTPPort)),
					embedded.WithResourcesPath("components/docker/customschemawithindex"),
					embedded.WithProfilingEnabled(false),
				)...,
			)).
			Step("Run indexed properties verification test", verifyIndexedPopertiesTest, sidecar.Stop(sidecarNamePrefix+"dockerCustomSchema")).
			Step("Stopping SQL Server Docker container", dockercompose.Stop("sqlserver", dockerComposeYAML)).
			Run()
	})

	ports, err = dapr_testing.GetFreePorts(2)
	assert.NoError(t, err)

	currentGrpcPort = ports[0]
	currentHTTPPort = ports[1]

	t.Run("SQL Server certification using Azure SQL", func(t *testing.T) {
		flow.New(t, "SQL Server certification using Azure SQL").
			// Run the Dapr sidecar with the SQL Server component.
			Step(sidecar.Run(sidecarNamePrefix+"azure",
				append(componentRuntimeOptions(),
					embedded.WithoutApp(),
					embedded.WithDaprGRPCPort(strconv.Itoa(currentGrpcPort)),
					embedded.WithDaprHTTPPort(strconv.Itoa(currentHTTPPort)),
					embedded.WithResourcesPath("components/azure"),
					embedded.WithProfilingEnabled(false),
				)...,
			)).
			Step("Run basic test", basicTest).
			Step("Run basic TTL test", basicTTLTest).
			Step("interrupt network",
				network.InterruptNetwork(15*time.Second, nil, nil, "1433", "1434")).

			// Component should recover at this point.
			Step("wait", flow.Sleep(10*time.Second)).
			Step("Run basic test again to verify reconnection occurred", basicTest).
			Step("Run SQL injection test", verifySQLInjectionTest, sidecar.Stop(sidecarNamePrefix+"azure")).
			Step("run TTL test", ttlTest(os.Getenv("AzureSqlServerConnectionString")+"database=stablecertification;")).
			Run()
	})
}

func componentRuntimeOptions() []embedded.Option {
	log := logger.NewLogger("dapr.components")

	stateRegistry := state_loader.NewRegistry()
	stateRegistry.Logger = log
	stateRegistry.RegisterComponent(state_sqlserver.New, "sqlserver")

	secretstoreRegistry := secretstores_loader.NewRegistry()
	secretstoreRegistry.Logger = log
	secretstoreRegistry.RegisterComponent(secretstore_env.NewEnvSecretStore, "local.env")

	return []embedded.Option{
		embedded.WithStates(stateRegistry),
		embedded.WithSecretStores(secretstoreRegistry),
	}
}

func countRowsInTable(ctx context.Context, dbClient *sql.DB, schema, table string) (count int, err error) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	err = dbClient.QueryRowContext(ctx, fmt.Sprintf("SELECT COUNT(*) FROM [%s].[%s]", schema, table)).Scan(&count)
	return
}

func clearTable(ctx context.Context, dbClient *sql.DB, schema, table string) error {
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	_, err := dbClient.ExecContext(ctx, fmt.Sprintf("DELETE FROM [%s].[%s]", schema, table))
	return err
}

func loadLastCleanupInterval(ctx context.Context, dbClient *sql.DB, schema, table string) (lastCleanup int64, err error) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	err = dbClient.
		QueryRowContext(ctx,
			fmt.Sprintf("SELECT DATEDIFF(MILLISECOND, CAST([Value] AS DATETIME2), GETDATE()) FROM [%s].[%s] WHERE [Key] = 'last-cleanup'", schema, table),
		).
		Scan(&lastCleanup)
	return
}

func populateTTLRecords(ctx context.Context, dbClient *sql.DB, schema, table string) error {
	// Insert 10 records that have expired, and 10 that will expire in 4
	// seconds.
	rows := make([][]any, 20)
	for i := 0; i < 10; i++ {
		rows[i] = []any{
			fmt.Sprintf("'expired_%d'", i),
			json.RawMessage(fmt.Sprintf("'value_%d'", i)),
			"DATEADD(MINUTE, -1, GETDATE())",
		}
	}
	for i := 0; i < 10; i++ {
		rows[i+10] = []any{
			fmt.Sprintf("'notexpired_%d'", i),
			json.RawMessage(fmt.Sprintf(`'value_%d'`, i)),
			"DATEADD(SECOND, 4, GETDATE())",
		}
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	for _, row := range rows {
		_, err := dbClient.ExecContext(ctx, fmt.Sprintf(
			"INSERT INTO [%[1]s].[%[2]s] ([Key], [Data], [ExpireDate]) VALUES (%[3]s, %[4]s, %[5]s)",
			schema, table, row[0], row[1], row[2]),
		)
		if err != nil {
			return err
		}
	}
	return nil
}

func getValueFromMetadataTable(ctx context.Context, dbClient *sql.DB, schema, table, key string) (value string, err error) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	err = dbClient.
		QueryRowContext(ctx, fmt.Sprintf("SELECT [Value] FROM [%[1]s].[%[2]s] WHERE [Key] = %[3]s", schema, table, key)).
		Scan(&value)
	return
}
