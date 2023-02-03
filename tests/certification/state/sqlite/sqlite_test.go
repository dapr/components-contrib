/*
Copyright 2023 The Dapr Authors
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
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/tests/certification/embedded"
	"github.com/dapr/components-contrib/tests/certification/flow"
	"github.com/dapr/components-contrib/tests/certification/flow/sidecar"

	"github.com/dapr/components-contrib/state"
	state_sqlite "github.com/dapr/components-contrib/state/sqlite"
	state_loader "github.com/dapr/dapr/pkg/components/state"
	"github.com/dapr/dapr/pkg/runtime"
	"github.com/dapr/go-sdk/client"
	"github.com/dapr/kit/logger"
)

const (
	stateStoreName          = "statestore"
	certificationTestPrefix = "stable-certification-"
	portOffset              = 2
	readonlyDBPath          = "artifacts/readonly.db"

	keyConnectionString  = "connectionString"
	keyTableName         = "tableName"
	keyMetadataTableName = "metadataTableName"
	keyCleanupInterval   = "cleanupInterval"
)

func TestSQLite(t *testing.T) {
	log := logger.NewLogger("dapr.components")

	stateStore := state_sqlite.NewSQLiteStateStore(log).(*state_sqlite.SQLiteStore)

	stateRegistry := state_loader.NewRegistry()
	stateRegistry.Logger = log
	stateRegistry.RegisterComponent(func(l logger.Logger) state.Store {
		return stateStore
	}, "sqlite")

	// Compute the hash of the read-only DB
	readonlyDBHash, err := hashFile(readonlyDBPath)
	require.NoError(t, err)

	// Basic test validating CRUD operations
	basicTest := func(port int) func(ctx flow.Context) error {
		return func(ctx flow.Context) error {
			ctx.T.Run("basic test", func(t *testing.T) {
				client, err := client.NewClientWithPort(strconv.Itoa(port))
				require.NoError(t, err)
				defer client.Close()

				// save state
				err = client.SaveState(ctx, stateStoreName, "key1", []byte("la nebbia agli irti colli piovigginando sale"), nil)
				require.NoError(t, err)

				// get state
				item, err := client.GetState(ctx, stateStoreName, "key1", nil)
				require.NoError(t, err)
				assert.Equal(t, "la nebbia agli irti colli piovigginando sale", string(item.Value))

				// update state
				errUpdate := client.SaveState(ctx, stateStoreName, "key1", []byte("e sotto il maestrale urla e biancheggia il mar"), nil)
				require.NoError(t, errUpdate)
				item, errUpdatedGet := client.GetState(ctx, stateStoreName, "key1", nil)
				require.NoError(t, errUpdatedGet)
				assert.Equal(t, "e sotto il maestrale urla e biancheggia il mar", string(item.Value))

				// delete state
				err = client.DeleteState(ctx, stateStoreName, "key1", nil)
				require.NoError(t, err)
			})

			return nil
		}
	}

	// checks the state store component is not vulnerable to SQL injection
	verifySQLInjectionTest := func(port int) func(ctx flow.Context) error {
		return func(ctx flow.Context) error {
			ctx.T.Run("sql injection test", func(t *testing.T) {
				client, err := client.NewClientWithPort(strconv.Itoa(port))
				require.NoError(t, err)
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
			})

			return nil
		}
	}

	// Checks that the read-only database cannot be written to
	readonlyTest := func(port int) func(ctx flow.Context) error {
		return func(ctx flow.Context) error {
			ctx.T.Run("read-only test", func(t *testing.T) {
				client, err := client.NewClientWithPort(strconv.Itoa(port))
				require.NoError(t, err)
				defer client.Close()

				// Retrieving state should work
				item, err := client.GetState(ctx, stateStoreName, "my_string", nil)
				require.NoError(t, err)
				assert.Equal(t, `"hello world"`, string(item.Value))

				// Saving state should fail
				err = client.SaveState(ctx, stateStoreName, "my_string", []byte("updated!"), nil)
				require.Error(t, err)
				assert.ErrorContains(t, err, "attempt to write a readonly database")

				// Value should not be updated
				item, err = client.GetState(ctx, stateStoreName, "my_string", nil)
				require.NoError(t, err)
				assert.Equal(t, `"hello world"`, string(item.Value))

				// Deleting state should fail
				err = client.DeleteState(ctx, stateStoreName, "my_string", nil)
				require.Error(t, err)
				assert.ErrorContains(t, err, "attempt to write a readonly database")
			})

			return nil
		}
	}

	// Checks the hash of the readonly DB (after the sidecar has been stopped) to confirm it wasn't modified
	readonlyConfirmTest := func(ctx flow.Context) error {
		ctx.T.Run("confirm read-only test", func(t *testing.T) {
			newHash, err := hashFile(readonlyDBPath)
			require.NoError(t, err)

			assert.Equal(t, readonlyDBHash, newHash, "read-only datbaase has been modified on disk")
		})

		return nil
	}

	// Validates TTLs and garbage collections
	ttlTest := func(ctx flow.Context) error {
		md := state.Metadata{
			Base: metadata.Base{
				Name: "ttltest",
				Properties: map[string]string{
					keyConnectionString: "file::memory:",
					keyTableName:        "ttl_state",
				},
			},
		}

		ctx.T.Run("parse cleanupIntervalInSeconds", func(t *testing.T) {
			t.Run("default value", func(t *testing.T) {
				// Default value is disabled
				md.Properties[keyCleanupInterval] = ""
				storeObj := state_sqlite.NewSQLiteStateStore(log).(*state_sqlite.SQLiteStore)

				err := storeObj.Init(md)
				require.NoError(t, err, "failed to init")
				defer storeObj.Close()

				dbAccess := storeObj.GetDBAccess()
				require.NotNil(t, dbAccess)

				cleanupInterval := dbAccess.GetCleanupInterval()
				assert.Equal(t, time.Duration(0), cleanupInterval)
			})

			t.Run("positive value", func(t *testing.T) {
				md.Properties[keyCleanupInterval] = "10s"
				storeObj := state_sqlite.NewSQLiteStateStore(log).(*state_sqlite.SQLiteStore)

				err := storeObj.Init(md)
				require.NoError(t, err, "failed to init")
				defer storeObj.Close()

				dbAccess := storeObj.GetDBAccess()
				require.NotNil(t, dbAccess)

				cleanupInterval := dbAccess.GetCleanupInterval()
				assert.Equal(t, time.Duration(10*time.Second), cleanupInterval)
			})

			t.Run("disabled", func(t *testing.T) {
				// A value of 0 means that the cleanup is disabled
				md.Properties[keyCleanupInterval] = "0"
				storeObj := state_sqlite.NewSQLiteStateStore(log).(*state_sqlite.SQLiteStore)

				err := storeObj.Init(md)
				require.NoError(t, err, "failed to init")
				defer storeObj.Close()

				dbAccess := storeObj.GetDBAccess()
				require.NotNil(t, dbAccess)

				cleanupInterval := dbAccess.GetCleanupInterval()
				assert.Equal(t, time.Duration(0), cleanupInterval)
			})

		})

		ctx.T.Run("cleanup", func(t *testing.T) {
			md := state.Metadata{
				Base: metadata.Base{
					Name: "ttltest",
					Properties: map[string]string{
						keyConnectionString:  "file::memory:",
						keyTableName:         "ttl_state",
						keyMetadataTableName: "ttl_metadata",
					},
				},
			}

			t.Run("automatically delete expired records", func(t *testing.T) {
				// Run every second
				md.Properties[keyCleanupInterval] = "1s"

				storeObj := state_sqlite.NewSQLiteStateStore(log).(*state_sqlite.SQLiteStore)
				err := storeObj.Init(md)
				require.NoError(t, err, "failed to init")
				defer storeObj.Close()
				dbClient := storeObj.GetDBAccess().GetConnection()

				// Seed the database with some records
				err = populateTTLRecords(ctx, dbClient)
				require.NoError(t, err, "failed to seed records")

				// Wait 2 seconds then verify we have only 10 rows left
				time.Sleep(2 * time.Second)
				count, err := countRowsInTable(ctx, dbClient, "ttl_state")
				assert.NoError(t, err, "failed to run query to count rows")
				assert.Equal(t, 10, count)

				// The "last-cleanup" value should be <= 1 second (+ a bit of buffer)
				lastCleanup, err := loadLastCleanupInterval(ctx, dbClient, "ttl_metadata")
				require.NoError(t, err, "failed to load value for 'last-cleanup'")
				assert.LessOrEqual(t, lastCleanup, int64(1200))

				// Wait 6 more seconds and verify there are no more rows left
				time.Sleep(6 * time.Second)
				count, err = countRowsInTable(ctx, dbClient, "ttl_state")
				require.NoError(t, err, "failed to run query to count rows")
				assert.Equal(t, 0, count)

				// The "last-cleanup" value should be <= 1 second (+ a bit of buffer)
				lastCleanup, err = loadLastCleanupInterval(ctx, dbClient, "ttl_metadata")
				require.NoError(t, err, "failed to load value for 'last-cleanup'")
				assert.LessOrEqual(t, lastCleanup, int64(1200))
			})

			t.Run("cleanup concurrency", func(t *testing.T) {
				// Set to run every hour
				// (we'll manually trigger more frequent iterations)
				md.Properties[keyCleanupInterval] = "1h"

				storeObj := state_sqlite.NewSQLiteStateStore(log).(*state_sqlite.SQLiteStore)
				err := storeObj.Init(md)
				require.NoError(t, err, "failed to init")
				defer storeObj.Close()

				dbAccess := storeObj.GetDBAccess()
				require.NotNil(t, dbAccess)

				dbClient := dbAccess.GetConnection()

				// Seed the database with some records
				err = populateTTLRecords(ctx, dbClient)
				require.NoError(t, err, "failed to seed records")

				// Validate that 20 records are present
				count, err := countRowsInTable(ctx, dbClient, "ttl_state")
				require.NoError(t, err, "failed to run query to count rows")
				assert.Equal(t, 20, count)

				// Set last-cleanup to 1s ago (less than 3600s)
				err = setValueInMetadataTable(ctx, dbClient, "ttl_metadata", "'last-cleanup'", "datetime('now', '-1 second')")
				require.NoError(t, err, "failed to set last-cleanup")

				// The "last-cleanup" value should be ~1 second (+ a bit of buffer)
				lastCleanup, err := loadLastCleanupInterval(ctx, dbClient, "ttl_metadata")
				require.NoError(t, err, "failed to load value for 'last-cleanup'")
				assert.LessOrEqual(t, lastCleanup, int64(1200))
				lastCleanupValueOrig, err := getValueFromMetadataTable(ctx, dbClient, "ttl_metadata", "last-cleanup")
				require.NoError(t, err, "failed to load absolute value for 'last-cleanup'")
				require.NotEmpty(t, lastCleanupValueOrig)

				// Trigger the background cleanup, which should do nothing because the last cleanup was < 3600s
				err = dbAccess.CleanupExpired()
				require.NoError(t, err, "CleanupExpired returned an error")

				// Validate that 20 records are still present
				count, err = countRowsInTable(ctx, dbClient, "ttl_state")
				require.NoError(t, err, "failed to run query to count rows")
				assert.Equal(t, 20, count)

				// The "last-cleanup" value should not have been changed
				lastCleanupValue, err := getValueFromMetadataTable(ctx, dbClient, "ttl_metadata", "last-cleanup")
				require.NoError(t, err, "failed to load absolute value for 'last-cleanup'")
				assert.Equal(t, lastCleanupValueOrig, lastCleanupValue)
			})
		})

		return nil
	}

	flow.New(t, "Run tests").
		// Start the sidecar with the in-memory database
		Step(sidecar.Run("sqlite-memory",
			embedded.WithoutApp(),
			embedded.WithDaprGRPCPort(runtime.DefaultDaprAPIGRPCPort),
			embedded.WithDaprHTTPPort(runtime.DefaultDaprHTTPPort),
			embedded.WithProfilePort(runtime.DefaultProfilePort),
			embedded.WithComponentsPath("resources/memory"),
			runtime.WithStates(stateRegistry),
		)).

		// Run some basic certification tests with the in-memory database
		Step("run basic test", basicTest(runtime.DefaultDaprAPIGRPCPort)).
		Step("run SQL injection test", verifySQLInjectionTest(runtime.DefaultDaprAPIGRPCPort)).

		// Start the sidecar with a read-only database
		Step(sidecar.Run("sqlite-readonly",
			embedded.WithoutApp(),
			embedded.WithDaprGRPCPort(runtime.DefaultDaprAPIGRPCPort+portOffset),
			embedded.WithDaprHTTPPort(runtime.DefaultDaprHTTPPort+portOffset),
			embedded.WithProfilePort(runtime.DefaultProfilePort+portOffset),
			embedded.WithComponentsPath("resources/readonly"),
			runtime.WithStates(stateRegistry),
		)).
		Step("run read-only test", readonlyTest(runtime.DefaultDaprAPIGRPCPort+portOffset)).
		Step("stop sqlite-readonly sidecar", sidecar.Stop("sqlite-readonly")).
		Step("confirm read-only test", readonlyConfirmTest).

		// Run TTL tests
		Step("run TTL test", ttlTest).

		// Start tests
		Run()
}

func populateTTLRecords(ctx context.Context, dbClient *sql.DB) error {
	// Insert 10 records that have expired, and 10 that will expire in 4 seconds
	// Note this uses fmt.Sprintf and not parametrized queries-on purpose, so we can pass multiple rows in the same INSERT query
	// Normally this would be a very bad idea, just don't do it outside of tests (and maybe not even in tests like I'm doing right now)...
	rows := make([]string, 20)
	for i := 0; i < 10; i++ {
		rows[i] = fmt.Sprintf(`("expired_%d", '"value_%d"', false, "etag", datetime('now', '-1 minute'))`, i, i)
	}
	for i := 0; i < 10; i++ {
		rows[i+10] = fmt.Sprintf(`("notexpired_%d", '"value_%d"', false, "etag", datetime('now', '+4 seconds'))`, i, i)
	}
	queryCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	q := "INSERT INTO ttl_state (key, value, is_binary, etag, expiration_time) VALUES " + strings.Join(rows, ", ")
	res, err := dbClient.ExecContext(queryCtx, q)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n != 20 {
		return fmt.Errorf("expected to insert 20 rows, but only got %d", n)
	}
	return nil
}

func countRowsInTable(ctx context.Context, dbClient *sql.DB, table string) (count int, err error) {
	queryCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	err = dbClient.QueryRowContext(queryCtx, "SELECT COUNT(key) FROM "+table).Scan(&count)
	cancel()
	return
}

func loadLastCleanupInterval(ctx context.Context, dbClient *sql.DB, table string) (lastCleanup int64, err error) {
	queryCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	err = dbClient.
		QueryRowContext(queryCtx,
			fmt.Sprintf("SELECT unixepoch(CURRENT_TIMESTAMP) - unixepoch(value) FROM %s WHERE key = 'last-cleanup'", table),
		).
		Scan(&lastCleanup)
	cancel()
	if errors.Is(err, sql.ErrNoRows) {
		err = nil
	}
	return
}

// Note this uses fmt.Sprintf and not parametrized queries-on purpose, so we can pass SQLite functions.
// Normally this would be a very bad idea, just don't do it... (do as I say don't do as I do :) ).
func setValueInMetadataTable(ctx context.Context, dbClient *sql.DB, table, key, value string) error {
	queryCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	_, err := dbClient.ExecContext(queryCtx,
		//nolint:gosec
		fmt.Sprintf(`REPLACE INTO %s (key, value) VALUES (%s, %s)`, table, key, value),
	)
	cancel()
	return err
}

func getValueFromMetadataTable(ctx context.Context, dbClient *sql.DB, table, key string) (value string, err error) {
	queryCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	err = dbClient.
		QueryRowContext(queryCtx, fmt.Sprintf("SELECT value FROM %s WHERE key = ?", table), key).
		Scan(&value)
	cancel()
	if errors.Is(err, sql.ErrNoRows) {
		err = nil
	}
	return
}

// Calculates the SHA-256 hash of a file
func hashFile(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()

	h := sha256.New()
	_, err = io.Copy(h, f)
	if err != nil {
		return "", err
	}

	return hex.EncodeToString(h.Sum(nil)), nil
}
