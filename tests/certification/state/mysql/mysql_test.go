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
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
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

	defaultSchemaName        = "dapr_state_store"
	defaultTableName         = "state"
	defaultMetadataTableName = "dapr_metadata"

	mysqlConnString   = "root:root@tcp(localhost:3306)/?allowNativePasswords=true"
	mariadbConnString = "root:root@tcp(localhost:3307)/"

	keyConnectionString = "connectionString"
	keyCleanupInterval  = "cleanupInterval"
	keyTableName        = "tableName"
	keyMetadatTableName = "metadataTableName"
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
			err = component.Ping(context.Background())
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
			err = component.Ping(context.Background())
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
			err = component.Ping(context.Background())
			require.NoError(t, err)

			// Close the component
			err = component.Close()
			require.NoError(t, err)

			// Ensure the connection is closed
			err = component.Ping(context.Background())
			require.Error(t, err)
			assert.Truef(t, errors.Is(err, sql.ErrConnDone), "expected sql.ErrConnDone but got %v", err)

			return nil
		}
	}

	// checks that metadata options schemaName and tableName behave correctly
	metadataTest := func(connString, schemaName, tableName, metadataTableName string) func(ctx flow.Context) error {
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
			if metadataTableName != "" {
				properties["metadataTableName"] = metadataTableName
			} else {
				metadataTableName = defaultMetadataTableName
			}

			// Init the component
			component := stateMysql.NewMySQLStateStore(log).(*stateMysql.MySQL)
			component.Init(context.Background(), state.Metadata{
				Base: metadata.Base{
					Properties: properties,
				},
			})

			// Check connection is active
			err = component.Ping(context.Background())
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
				SELECT TABLE_NAME FROM information_schema.tables WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
			) AS 'exists'`
			err = conn.QueryRow(query, schemaName, tableName).Scan(&exists)
			require.NoError(t, err)
			assert.Equal(t, 1, exists)

			// Check that the expiredate column exists
			query = `SELECT count(*) AS 'exists' FROM information_schema.columns 
				WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND COLUMN_NAME = ?`
			err = conn.QueryRow(query, schemaName, tableName, "expiredate").Scan(&exists)
			require.NoError(t, err)
			assert.Equal(t, 1, exists)

			// Check that the metadata table exists
			query = `SELECT count(*) AS 'exists' FROM information_schema.tables 
				WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?`
			err = conn.QueryRow(query, schemaName, tableName).Scan(&exists)
			require.NoError(t, err)
			assert.Equal(t, 1, exists)

			// Close the component
			err = component.Close()
			require.NoError(t, err)

			return nil
		}
	}

	// Validates TTLs and garbage collections
	ttlTest := func(connString string) func(ctx flow.Context) error {
		return func(ctx flow.Context) (err error) {
			md := state.Metadata{
				Base: metadata.Base{
					Name: "ttltest",
					Properties: map[string]string{
						keyConnectionString: connString,
						keyTableName:        "ttl_state",
						keyMetadatTableName: "ttl_metadata",
					},
				},
			}

			t.Run("parse cleanupInterval", func(t *testing.T) {
				t.Run("default value", func(t *testing.T) {
					// Default value is 1 hr
					md.Properties[keyCleanupInterval] = ""
					storeObj := stateMysql.NewMySQLStateStore(log).(*stateMysql.MySQL)

					err := storeObj.Init(ctx, md)
					require.NoError(t, err, "failed to init")
					defer storeObj.Close()

					cleanupInterval := storeObj.CleanupInterval()
					_ = assert.NotNil(t, cleanupInterval) &&
						assert.Equal(t, time.Duration(1*time.Hour), *cleanupInterval)
				})

				t.Run("positive value", func(t *testing.T) {
					md.Properties[keyCleanupInterval] = "10s"
					storeObj := stateMysql.NewMySQLStateStore(log).(*stateMysql.MySQL)

					err := storeObj.Init(ctx, md)
					require.NoError(t, err, "failed to init")
					defer storeObj.Close()

					cleanupInterval := storeObj.CleanupInterval()
					_ = assert.NotNil(t, cleanupInterval) &&
						assert.Equal(t, time.Duration(10*time.Second), *cleanupInterval)
				})

				t.Run("disabled", func(t *testing.T) {
					// A value of <=0 means that the cleanup is disabled
					md.Properties[keyCleanupInterval] = "0"
					storeObj := stateMysql.NewMySQLStateStore(log).(*stateMysql.MySQL)

					err := storeObj.Init(ctx, md)
					require.NoError(t, err, "failed to init")
					defer storeObj.Close()

					cleanupInterval := storeObj.CleanupInterval()
					_ = assert.Nil(t, cleanupInterval)
				})

			})

			t.Run("cleanup", func(t *testing.T) {
				md := state.Metadata{
					Base: metadata.Base{
						Name: "ttltest",
						Properties: map[string]string{
							keyConnectionString: connString,
							keyTableName:        "ttl_state",
							keyMetadatTableName: "ttl_metadata",
						},
					},
				}

				t.Run("automatically delete expired records", func(t *testing.T) {
					// Run every second
					md.Properties[keyCleanupInterval] = "1s"

					storeObj := stateMysql.NewMySQLStateStore(log).(*stateMysql.MySQL)
					err := storeObj.Init(ctx, md)
					require.NoError(t, err, "failed to init")
					defer storeObj.Close()

					conn := storeObj.GetConnection()
					// Seed the database with some records
					err = populateTTLRecords(ctx, conn)
					require.NoError(t, err, "failed to seed records")

					// Wait 2 seconds then verify we have only 10 rows left
					time.Sleep(2 * time.Second)
					count, err := countRowsInTable(ctx, conn, "ttl_state")
					require.NoError(t, err, "failed to run query to count rows")
					assert.Equal(t, 10, count)

					// The "last-cleanup" value should be <= 2 seconds (+ a bit of buffer)
					lastCleanup, err := loadLastCleanupInterval(ctx, conn, "ttl_metadata")
					require.NoError(t, err, "failed to load value for 'last-cleanup'")
					assert.LessOrEqual(t, lastCleanup, 2)

					// Wait 6 more seconds and verify there are no more rows left
					time.Sleep(6 * time.Second)
					count, err = countRowsInTable(ctx, conn, "ttl_state")
					require.NoError(t, err, "failed to run query to count rows")
					assert.Equal(t, 0, count)

					// The "last-cleanup" value should be <= 2 seconds (+ a bit of buffer)
					lastCleanup, err = loadLastCleanupInterval(ctx, conn, "ttl_metadata")
					require.NoError(t, err, "failed to load value for 'last-cleanup'")
					assert.LessOrEqual(t, lastCleanup, 2)
				})

				t.Run("cleanup concurrency", func(t *testing.T) {
					// Set to run every hour
					// (we'll manually trigger more frequent iterations)
					md.Properties[keyCleanupInterval] = "1h"

					storeObj := stateMysql.NewMySQLStateStore(log).(*stateMysql.MySQL)
					err := storeObj.Init(ctx, md)
					require.NoError(t, err, "failed to init")
					defer storeObj.Close()

					conn := storeObj.GetConnection()

					// Seed the database with some records
					err = populateTTLRecords(ctx, conn)
					require.NoError(t, err, "failed to seed records")

					// Validate that 20 records are present
					count, err := countRowsInTable(ctx, conn, "ttl_state")
					require.NoError(t, err, "failed to run query to count rows")
					assert.Equal(t, 20, count)

					// Set last-cleanup to 1s ago (less than 3600s)
					err = setValueInMetadataTable(ctx, conn, "ttl_metadata", "'last-cleanup'", "CURRENT_TIMESTAMP - INTERVAL 1 SECOND")
					require.NoError(t, err, "failed to set last-cleanup")

					// The "last-cleanup" value should be ~2 seconds (+ a bit of buffer)
					lastCleanup, err := loadLastCleanupInterval(ctx, conn, "ttl_metadata")
					require.NoError(t, err, "failed to load value for 'last-cleanup'")
					assert.LessOrEqual(t, lastCleanup, 2)
					lastCleanupValueOrig, err := getValueFromMetadataTable(ctx, conn, "ttl_metadata", "last-cleanup")
					require.NoError(t, err, "failed to load absolute value for 'last-cleanup'")
					require.NotEmpty(t, lastCleanupValueOrig)

					// Trigger the background cleanup, which should do nothing because the last cleanup was < 3600s
					err = storeObj.CleanupExpired()
					require.NoError(t, err, "CleanupExpired returned an error")

					// Validate that 20 records are still present
					count, err = countRowsInTable(ctx, conn, "ttl_state")
					require.NoError(t, err, "failed to run query to count rows")
					assert.Equal(t, 20, count)

					// The "last-cleanup" value should not have been changed
					lastCleanupValue, err := getValueFromMetadataTable(ctx, conn, "ttl_metadata", "last-cleanup")
					require.NoError(t, err, "failed to load absolute value for 'last-cleanup'")
					assert.Equal(t, lastCleanupValueOrig, lastCleanupValue)
				})
			})

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
		Step("Run TTL test on mysql", ttlTest(mysqlConnString)).
		Step("Run TTL test on mariadb", ttlTest(mariadbConnString)).
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
		Step("Default schemaName, tableName and metadataTableName on mysql", metadataTest(mysqlConnString, "", "", "")).
		Step("Custom schemaName, tableName and metadataTableName on mysql", metadataTest(mysqlConnString, "mydaprdb", "mytable", "metadatatable")).
		Step("Default schemaName, tableName and metadataTableName on mariadb", metadataTest(mariadbConnString, "", "", "")).
		Step("Custom schemaName, tableName and metadataTableName on mariadb", metadataTest(mariadbConnString, "mydaprdb", "mytable", "metadatatable")).
		// Run tests
		Run()
}

func populateTTLRecords(ctx context.Context, dbClient *sql.DB) error {
	// Insert 10 records that have expired, and 10 that will expire in 4 seconds
	exp := "DATE_SUB(CURRENT_TIMESTAMP, INTERVAL 1 MINUTE)"
	rows := make([][]any, 20)
	for i := 0; i < 10; i++ {
		rows[i] = []any{
			fmt.Sprintf("expired_%d", i),
			json.RawMessage(fmt.Sprintf(`"value_%d"`, i)),
			false,
			exp,
		}
	}
	exp = "DATE_ADD(CURRENT_TIMESTAMP, INTERVAL 4 second)"
	for i := 0; i < 10; i++ {
		rows[i+10] = []any{
			fmt.Sprintf("notexpired_%d", i),
			json.RawMessage(fmt.Sprintf(`"value_%d"`, i)),
			false,
			exp,
		}
	}
	queryCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	for _, row := range rows {
		query := fmt.Sprintf("INSERT INTO ttl_state (id, value, isbinary, eTag, expiredate) VALUES (?, ?, ?, '', %s)", row[3])
		_, err := dbClient.ExecContext(queryCtx, query, row[0], row[1], row[2])
		if err != nil {
			return err
		}
	}

	return nil
}

func countRowsInTable(ctx context.Context, dbClient *sql.DB, table string) (count int, err error) {
	queryCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	err = dbClient.QueryRowContext(queryCtx, "SELECT COUNT(id) FROM "+table).Scan(&count)

	cancel()
	return
}

func loadLastCleanupInterval(ctx context.Context, dbClient *sql.DB, table string) (lastCleanup int, err error) {
	queryCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	var lastCleanupf float64
	err = dbClient.
		QueryRowContext(queryCtx,
			fmt.Sprintf("SELECT UNIX_TIMESTAMP(CURRENT_TIMESTAMP) - UNIX_TIMESTAMP(value) AS lastCleanupf FROM %s WHERE id = 'last-cleanup'", table),
		).
		Scan(&lastCleanupf)
	lastCleanup = int(lastCleanupf)
	cancel()
	if errors.Is(err, sql.ErrNoRows) {
		err = nil
	}
	return
}

func setValueInMetadataTable(ctx context.Context, dbClient *sql.DB, table, id, value string) error {
	queryCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	_, err := dbClient.ExecContext(queryCtx,
		//nolint:gosec
		fmt.Sprintf(`INSERT INTO %[1]s (id, value) VALUES (%[2]s, %[3]s) ON DUPLICATE KEY UPDATE
		value = %[3]s`, table, id, value),
	)
	cancel()
	return err
}

func getValueFromMetadataTable(ctx context.Context, dbClient *sql.DB, table, id string) (value string, err error) {
	queryCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	err = dbClient.
		QueryRowContext(queryCtx, fmt.Sprintf("SELECT value FROM %s WHERE id = ?", table), id).
		Scan(&value)
	cancel()
	if errors.Is(err, sql.ErrNoRows) {
		err = nil
	}
	return
}
