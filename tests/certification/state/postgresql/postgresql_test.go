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
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	pgx "github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/tests/certification/embedded"
	"github.com/dapr/components-contrib/tests/certification/flow"
	"github.com/dapr/components-contrib/tests/certification/flow/dockercompose"
	"github.com/dapr/components-contrib/tests/certification/flow/sidecar"

	"github.com/dapr/components-contrib/state"
	state_postgres "github.com/dapr/components-contrib/state/postgresql"
	state_loader "github.com/dapr/dapr/pkg/components/state"
	"github.com/dapr/dapr/pkg/runtime"
	dapr_testing "github.com/dapr/dapr/pkg/testing"
	"github.com/dapr/go-sdk/client"
	"github.com/dapr/kit/logger"
)

const (
	sidecarNamePrefix       = "postgresql-sidecar-"
	dockerComposeYAML       = "docker-compose.yml"
	stateStoreName          = "statestore"
	certificationTestPrefix = "stable-certification-"
	connStringValue         = "postgres://postgres:example@localhost:5432/dapr_test"
	keyConnectionString     = "connectionString"
	keyCleanupInterval      = "cleanupIntervalInSeconds"
)

func TestPostgreSQL(t *testing.T) {
	log := logger.NewLogger("dapr.components")

	stateStore := state_postgres.NewPostgreSQLStateStore(log).(*state_postgres.PostgreSQL)
	ports, err := dapr_testing.GetFreePorts(2)
	assert.NoError(t, err)

	stateRegistry := state_loader.NewRegistry()
	stateRegistry.Logger = log
	stateRegistry.RegisterComponent(func(l logger.Logger) state.Store {
		return stateStore
	}, "postgresql")

	currentGrpcPort := ports[0]

	// Update this constant if you add more migrations
	const migrationLevel = "2"

	// Tests the "Init" method and the database migrations
	// It also tests the metadata properties "tableName" and "metadataTableName"
	initTest := func(ctx flow.Context) error {
		// Acquire a DB client as the "postgres" (ie. "root") user which we'll use to validate migrations and other changes in state
		dbClient, err := connectDB()
		require.NoError(t, err, "failed to create a connection to the database")

		md := state.Metadata{
			Base: metadata.Base{
				Name: "inittest",
				Properties: map[string]string{
					keyConnectionString: connStringValue,
					keyCleanupInterval:  "-1",
				},
			},
		}

		t.Run("initial state clean", func(t *testing.T) {
			storeObj := state_postgres.NewPostgreSQLStateStore(log).(*state_postgres.PostgreSQL)
			md.Properties["tableName"] = "clean_state"
			md.Properties["metadataTableName"] = "clean_metadata"

			// Init and perform the migrations
			err := storeObj.Init(md)
			require.NoError(t, err, "failed to init")

			// We should have the tables correctly created
			err = tableExists(dbClient, "public", "clean_state")
			assert.NoError(t, err, "state table does not exist")
			err = tableExists(dbClient, "public", "clean_metadata")
			assert.NoError(t, err, "metadata table does not exist")

			// Ensure migration level is correct
			level, err := getMigrationLevel(dbClient, "clean_metadata")
			assert.NoError(t, err, "failed to get migration level")
			assert.Equal(t, migrationLevel, level, "migration level mismatch: found '%s' but expected '%s'", level, migrationLevel)

			err = storeObj.Close()
			require.NoError(t, err, "failed to close component")
		})

		t.Run("initial state clean, with explicit schema name", func(t *testing.T) {
			storeObj := state_postgres.NewPostgreSQLStateStore(log).(*state_postgres.PostgreSQL)
			md.Properties["tableName"] = "public.clean2_state"
			md.Properties["metadataTableName"] = "public.clean2_metadata"

			// Init and perform the migrations
			err := storeObj.Init(md)
			require.NoError(t, err, "failed to init")

			// We should have the tables correctly created
			err = tableExists(dbClient, "public", "clean2_state")
			assert.NoError(t, err, "state table does not exist")
			err = tableExists(dbClient, "public", "clean2_metadata")
			assert.NoError(t, err, "metadata table does not exist")

			// Ensure migration level is correct
			level, err := getMigrationLevel(dbClient, "clean2_metadata")
			assert.NoError(t, err, "failed to get migration level")
			assert.Equal(t, migrationLevel, level, "migration level mismatch: found '%s' but expected '%s'", level, migrationLevel)

			err = storeObj.Close()
			require.NoError(t, err, "failed to close component")
		})

		t.Run("all migrations performed", func(t *testing.T) {
			// Re-use "clean_state" and "clean_metadata"
			storeObj := state_postgres.NewPostgreSQLStateStore(log).(*state_postgres.PostgreSQL)
			md.Properties["tableName"] = "clean_state"
			md.Properties["metadataTableName"] = "clean_metadata"

			// Should already have migration level 2
			level, err := getMigrationLevel(dbClient, "clean_metadata")
			assert.NoError(t, err, "failed to get migration level")
			assert.Equal(t, migrationLevel, level, "migration level mismatch: found '%s' but expected '%s'", level, migrationLevel)

			// Init and perform the migrations
			err = storeObj.Init(md)
			require.NoError(t, err, "failed to init")

			// Ensure migration level is correct
			level, err = getMigrationLevel(dbClient, "clean_metadata")
			assert.NoError(t, err, "failed to get migration level")
			assert.Equal(t, migrationLevel, level, "migration level mismatch: found '%s' but expected '%s'", level, migrationLevel)

			err = storeObj.Close()
			require.NoError(t, err, "failed to close component")
		})

		t.Run("migrate from implied level 1", func(t *testing.T) {
			// Before we added the metadata table, the "implied" level 1 had only the state table
			// Create that table to simulate the old state and validate the migration
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			_, err := dbClient.Exec(
				ctx,
				`CREATE TABLE pre_state (
					key text NOT NULL PRIMARY KEY,
					value jsonb NOT NULL,
					isbinary boolean NOT NULL,
					insertdate TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
					updatedate TIMESTAMP WITH TIME ZONE NULL
				)`,
			)
			require.NoError(t, err, "failed to create initial migration state")

			storeObj := state_postgres.NewPostgreSQLStateStore(log).(*state_postgres.PostgreSQL)
			md.Properties["tableName"] = "pre_state"
			md.Properties["metadataTableName"] = "pre_metadata"

			// Init and perform the migrations
			err = storeObj.Init(md)
			require.NoError(t, err, "failed to init")

			// We should have the metadata table created
			err = tableExists(dbClient, "public", "pre_metadata")
			assert.NoError(t, err, "metadata table does not exist")

			// Ensure migration level is correct
			level, err := getMigrationLevel(dbClient, "pre_metadata")
			assert.NoError(t, err, "failed to get migration level")
			assert.Equal(t, migrationLevel, level, "migration level mismatch: found '%s' but expected '%s'", level, migrationLevel)

			// Ensure the expiredate column has been added
			var colExists bool
			ctx, cancel = context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			err = dbClient.
				QueryRow(ctx,
					`SELECT EXISTS (
						SELECT 1
						FROM information_schema.columns
						WHERE
							table_schema = 'public'
							AND table_name = 'pre_state'
							AND column_name = 'expiredate'
					)`,
				).
				Scan(&colExists)
			assert.True(t, colExists, "column expiredate not found in updated table")

			err = storeObj.Close()
			require.NoError(t, err, "failed to close component")
		})

		t.Run("initialize components concurrently", func(t *testing.T) {
			// Initializes 3 components concurrently using the same table names, and ensure that they perform migrations without conflicts and race conditions
			md.Properties["tableName"] = "mystate"
			md.Properties["metadataTableName"] = "mymetadata"

			errs := make(chan error, 3)
			hasLogs := atomic.Int32{}
			for i := 0; i < 3; i++ {
				go func(i int) {
					buf := &bytes.Buffer{}
					l := logger.NewLogger("multi-init-" + strconv.Itoa(i))
					l.SetOutput(io.MultiWriter(buf, os.Stdout))

					// Init and perform the migrations
					storeObj := state_postgres.NewPostgreSQLStateStore(l).(*state_postgres.PostgreSQL)
					err := storeObj.Init(md)
					if err != nil {
						errs <- fmt.Errorf("%d failed to init: %w", i, err)
						return
					}

					// One and only one of the loggers should have any message
					if buf.Len() > 0 {
						hasLogs.Add(1)
					}

					// Close the component right away
					err = storeObj.Close()
					if err != nil {
						errs <- fmt.Errorf("%d failed to close: %w", i, err)
						return
					}

					errs <- nil
				}(i)
			}

			failed := false
			for i := 0; i < 3; i++ {
				select {
				case err := <-errs:
					failed = failed || !assert.NoError(t, err)
				case <-time.After(time.Minute):
					t.Fatal("timed out waiting for components to initialize")
				}
			}
			if failed {
				// Short-circuit
				t.FailNow()
			}

			// Exactly one component should have written logs (which means generated any activity during migrations)
			assert.Equal(t, int32(1), hasLogs.Load(), "expected 1 component to log anything to indicate migration activity, but got %d", hasLogs.Load())

			// We should have the tables correctly created
			err = tableExists(dbClient, "public", "mystate")
			assert.NoError(t, err, "state table does not exist")
			err = tableExists(dbClient, "public", "mymetadata")
			assert.NoError(t, err, "metadata table does not exist")

			// Ensure migration level is correct
			level, err := getMigrationLevel(dbClient, "mymetadata")
			assert.NoError(t, err, "failed to get migration level")
			assert.Equal(t, migrationLevel, level, "migration level mismatch: found '%s' but expected '%s'", level, migrationLevel)
		})

		return nil
	}

	basicTest := func(ctx flow.Context) error {
		client, err := client.NewClientWithPort(strconv.Itoa(currentGrpcPort))
		if err != nil {
			panic(err)
		}
		defer client.Close()

		// save state
		err = client.SaveState(ctx, stateStoreName, certificationTestPrefix+"key1", []byte("postgresqlcert1"), nil)
		require.NoError(t, err)

		// get state
		item, err := client.GetState(ctx, stateStoreName, certificationTestPrefix+"key1", nil)
		require.NoError(t, err)
		assert.Equal(t, "postgresqlcert1", string(item.Value))

		// update state
		errUpdate := client.SaveState(ctx, stateStoreName, certificationTestPrefix+"key1", []byte("postgresqlcert2"), nil)
		require.NoError(t, errUpdate)
		item, errUpdatedGet := client.GetState(ctx, stateStoreName, certificationTestPrefix+"key1", nil)
		require.NoError(t, errUpdatedGet)
		assert.Equal(t, "postgresqlcert2", string(item.Value))

		// delete state
		err = client.DeleteState(ctx, stateStoreName, certificationTestPrefix+"key1", nil)
		require.NoError(t, err)

		return nil
	}

	eTagTest := func(ctx flow.Context) error {
		etag900invalid := "900invalid"

		err1 := stateStore.Set(&state.SetRequest{
			Key:   "k",
			Value: "v1",
		})
		require.NoError(t, err1)
		resp1, err2 := stateStore.Get(&state.GetRequest{
			Key: "k",
		})

		require.NoError(t, err2)
		err3 := stateStore.Set(&state.SetRequest{
			Key:   "k",
			Value: "v2",
			ETag:  resp1.ETag,
		})
		require.NoError(t, err3)

		resp11, err12 := stateStore.Get(&state.GetRequest{
			Key: "k",
		})
		expectedEtag := *resp11.ETag
		require.NoError(t, err12)

		err4 := stateStore.Set(&state.SetRequest{
			Key:   "k",
			Value: "v3",
			ETag:  &etag900invalid,
		})
		require.Error(t, err4)

		resp, err := stateStore.Get(&state.GetRequest{
			Key: "k",
		})

		require.NoError(t, err)
		assert.Equal(t, expectedEtag, *resp.ETag)
		assert.Equal(t, "\"v2\"", string(resp.Data))

		return nil
	}

	transactionsTest := func(ctx flow.Context) error {
		err := stateStore.Multi(&state.TransactionalStateRequest{
			Operations: []state.TransactionalStateOperation{
				{
					Operation: state.Upsert,
					Request: state.SetRequest{
						Key:   "reqKey1",
						Value: "reqVal1",
						Metadata: map[string]string{
							"ttlInSeconds": "-1",
						},
					},
				},
				{
					Operation: state.Upsert,
					Request: state.SetRequest{
						Key:   "reqKey2",
						Value: "reqVal2",
						Metadata: map[string]string{
							"ttlInSeconds": "222",
						},
					},
				},
				{
					Operation: state.Upsert,
					Request: state.SetRequest{
						Key:   "reqKey3",
						Value: "reqVal3",
					},
				},
				{
					Operation: state.Upsert,
					Request: state.SetRequest{
						Key:   "reqKey1",
						Value: "reqVal101",
						Metadata: map[string]string{
							"ttlInSeconds": "50",
						},
					},
				},
				{
					Operation: state.Upsert,
					Request: state.SetRequest{
						Key:   "reqKey3",
						Value: "reqVal103",
						Metadata: map[string]string{
							"ttlInSeconds": "50",
						},
					},
				},
			},
		})
		require.NoError(t, err)

		resp1, err := stateStore.Get(&state.GetRequest{
			Key: "reqKey1",
		})
		assert.Equal(t, "\"reqVal101\"", string(resp1.Data))

		resp3, err := stateStore.Get(&state.GetRequest{
			Key: "reqKey3",
		})
		assert.Equal(t, "\"reqVal103\"", string(resp3.Data))
		return nil
	}

	testGetAfterPostgresRestart := func(ctx flow.Context) error {
		client, err := client.NewClientWithPort(fmt.Sprint(currentGrpcPort))
		if err != nil {
			panic(err)
		}
		defer client.Close()

		// save state
		_, err = client.GetState(ctx, stateStoreName, certificationTestPrefix+"key1", nil)
		require.NoError(t, err)

		return nil
	}

	// checks the state store component is not vulnerable to SQL injection
	verifySQLInjectionTest := func(ctx flow.Context) error {
		client, err := client.NewClientWithPort(fmt.Sprint(currentGrpcPort))
		if err != nil {
			panic(err)
		}
		defer client.Close()

		// common SQL injection techniques for PostgreSQL
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

	flow.New(t, "Run tests").
		Step(dockercompose.Run("db", dockerComposeYAML)).
		Step("wait for component to start", flow.Sleep(10*time.Second)).
		Step("Run Init test", initTest).
		Step(sidecar.Run(sidecarNamePrefix+"dockerDefault",
			embedded.WithoutApp(),
			embedded.WithDaprGRPCPort(currentGrpcPort),
			embedded.WithComponentsPath("components/docker/default"),
			runtime.WithStates(stateRegistry),
		)).
		Step("Run CRUD test", basicTest).
		Step("Run eTag test", eTagTest).
		Step("Run transactions test", transactionsTest).
		Step("Run SQL injection test", verifySQLInjectionTest).
		Step("stop postgresql", dockercompose.Stop("db", dockerComposeYAML, "db")).
		Step("wait for component to stop", flow.Sleep(10*time.Second)).
		Step("start postgresql", dockercompose.Start("db", dockerComposeYAML, "db")).
		Step("wait for component to start", flow.Sleep(10*time.Second)).
		Step("Run connection test", testGetAfterPostgresRestart).
		Run()
}

func connectDB() (*pgx.Conn, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	dbClient, err := pgx.Connect(ctx, connStringValue)
	cancel()
	return dbClient, err
}

func tableExists(dbClient *pgx.Conn, schema string, table string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	var scanTable, scanSchema string
	err := dbClient.QueryRow(
		ctx,
		"SELECT table_name, table_schema FROM information_schema.tables WHERE table_name = $1 AND table_schema = $2",
		table, schema,
	).Scan(&scanTable, &scanSchema)
	if err != nil {
		return fmt.Errorf("error querying for table: %w", err)
	}
	if table != scanTable || schema != scanSchema {
		return fmt.Errorf("found table '%s.%s' does not match", scanSchema, scanTable)
	}
	return nil
}

func getMigrationLevel(dbClient *pgx.Conn, metadataTable string) (level string, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	err = dbClient.
		QueryRow(ctx, fmt.Sprintf(`SELECT value FROM %s WHERE key = 'migrations'`, metadataTable)).
		Scan(&level)
	if err != nil && errors.Is(err, sql.ErrNoRows) {
		err = nil
		level = ""
	}
	return level, err
}
