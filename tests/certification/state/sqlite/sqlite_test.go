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
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	// Blank import for the underlying SQLite Driver.
	_ "modernc.org/sqlite"

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
	keyBusyTimeout       = "busyTimeout"

	// Update this constant if you add more migrations
	migrationLevel = "1"
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
				delete(md.Properties, keyCleanupInterval)
				storeObj := state_sqlite.NewSQLiteStateStore(log).(*state_sqlite.SQLiteStore)

				err := storeObj.Init(context.Background(), md)
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

				err := storeObj.Init(context.Background(), md)
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

				err := storeObj.Init(context.Background(), md)
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
				err := storeObj.Init(context.Background(), md)
				require.NoError(t, err, "failed to init")
				defer storeObj.Close()

				dbClient := storeObj.GetDBAccess().GetConnection()
				require.NotNil(t, dbClient)

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
				err := storeObj.Init(context.Background(), md)
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

	// Tests the "Init" method and the database migrations
	// It also tests the metadata properties "tableName" and "metadataTableName"
	initTest := func(ctx flow.Context) error {
		ctx.T.Run("init and migrations", func(t *testing.T) {
			// Create a temporary database and create a connection to it
			tmpDir := t.TempDir()
			dbPath := filepath.Join(tmpDir, "init.db")
			dbClient, err := sql.Open("sqlite", "file:"+dbPath+"?_pragma=busy_timeout%282000%29&_pragma=journal_mode%28WAL%29&_txlock=immediate")
			require.NoError(t, err)

			md := state.Metadata{
				Base: metadata.Base{
					Name: "inittest",
					Properties: map[string]string{
						keyConnectionString: dbPath,
					},
				},
			}

			t.Run("initial state clean", func(t *testing.T) {
				storeObj := state_sqlite.NewSQLiteStateStore(log).(*state_sqlite.SQLiteStore)
				md.Properties[keyTableName] = "clean_state"
				md.Properties[keyMetadataTableName] = "clean_metadata"

				// Init and perform the migrations
				err := storeObj.Init(context.Background(), md)
				require.NoError(t, err, "failed to init")
				defer storeObj.Close()

				// We should have the tables correctly created
				err = tableExists(dbClient, "clean_state")
				assert.NoError(t, err, "state table does not exist")
				err = tableExists(dbClient, "clean_metadata")
				assert.NoError(t, err, "metadata table does not exist")

				// Ensure migration level is correct
				level, err := getMigrationLevel(dbClient, "clean_metadata")
				assert.NoError(t, err, "failed to get migration level")
				assert.Equal(t, migrationLevel, level, "migration level mismatch: found '%s' but expected '%s'", level, migrationLevel)
			})

			t.Run("all migrations performed", func(t *testing.T) {
				// Re-use "clean_state" and "clean_metadata"
				storeObj := state_sqlite.NewSQLiteStateStore(log).(*state_sqlite.SQLiteStore)
				md.Properties[keyTableName] = "clean_state"
				md.Properties[keyMetadataTableName] = "clean_metadata"

				// Should already have migration level 2
				level, err := getMigrationLevel(dbClient, "clean_metadata")
				assert.NoError(t, err, "failed to get migration level")
				assert.Equal(t, migrationLevel, level, "migration level mismatch: found '%s' but expected '%s'", level, migrationLevel)

				// Init and perform the migrations
				err = storeObj.Init(context.Background(), md)
				require.NoError(t, err, "failed to init")
				defer storeObj.Close()

				// Ensure migration level is correct
				level, err = getMigrationLevel(dbClient, "clean_metadata")
				assert.NoError(t, err, "failed to get migration level")
				assert.Equal(t, migrationLevel, level, "migration level mismatch: found '%s' but expected '%s'", level, migrationLevel)
			})

			t.Run("migrate from implied level 1", func(t *testing.T) {
				// Before we added the metadata table, the "implied" level 1 had only the state table
				// Create that table to simulate the old state and validate the migration
				ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
				defer cancel()
				_, err := dbClient.ExecContext(
					ctx,
					`CREATE TABLE pre_state (
						key TEXT NOT NULL PRIMARY KEY,
						value TEXT NOT NULL,
						is_binary BOOLEAN NOT NULL,
						etag TEXT NOT NULL,
						expiration_time TIMESTAMP DEFAULT NULL,
						update_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
					)`,
				)
				require.NoError(t, err, "failed to create initial migration state")

				storeObj := state_sqlite.NewSQLiteStateStore(log).(*state_sqlite.SQLiteStore)
				md.Properties[keyTableName] = "pre_state"
				md.Properties[keyMetadataTableName] = "pre_metadata"

				// Init and perform the migrations
				err = storeObj.Init(context.Background(), md)
				require.NoError(t, err, "failed to init")
				defer storeObj.Close()

				// We should have the metadata table created
				err = tableExists(dbClient, "pre_metadata")
				assert.NoError(t, err, "metadata table does not exist")

				// Ensure migration level is correct
				level, err := getMigrationLevel(dbClient, "pre_metadata")
				assert.NoError(t, err, "failed to get migration level")
				assert.Equal(t, migrationLevel, level, "migration level mismatch: found '%s' but expected '%s'", level, migrationLevel)
			})

			t.Run("initialize components concurrently", func(t *testing.T) {
				// Initializes 3 components concurrently using the same table names, and ensure that they perform migrations without conflicts and race conditions
				md.Properties[keyTableName] = "mystate"
				md.Properties[keyMetadataTableName] = "mymetadata"

				errs := make(chan error, 3)
				hasLogs := atomic.Int32{}
				for i := 0; i < 3; i++ {
					go func(i int) {
						buf := &bytes.Buffer{}
						l := logger.NewLogger("multi-init-" + strconv.Itoa(i))
						l.SetOutput(io.MultiWriter(buf, os.Stdout))

						// Init and perform the migrations
						storeObj := state_sqlite.NewSQLiteStateStore(l).(*state_sqlite.SQLiteStore)
						err := storeObj.Init(context.Background(), md)
						if err != nil {
							errs <- fmt.Errorf("%d failed to init: %w", i, err)
							return
						}

						// One and only one of the loggers should have any message indicating "Performing migration 1"
						if strings.Contains(buf.String(), "Performing migration 1") {
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
				err = tableExists(dbClient, "mystate")
				assert.NoError(t, err, "state table does not exist")
				err = tableExists(dbClient, "mymetadata")
				assert.NoError(t, err, "metadata table does not exist")

				// Ensure migration level is correct
				level, err := getMigrationLevel(dbClient, "mymetadata")
				assert.NoError(t, err, "failed to get migration level")
				assert.Equal(t, migrationLevel, level, "migration level mismatch: found '%s' but expected '%s'", level, migrationLevel)
			})
		})

		return nil
	}

	// Tests many concurrent operations to ensure the database can operate successfully
	concurrencyTest := func(ctx flow.Context) error {
		ctx.T.Run("concurrency", func(t *testing.T) {
			// Create a temporary database
			tmpDir := t.TempDir()
			dbPath := filepath.Join(tmpDir, "init.db")

			md := state.Metadata{
				Base: metadata.Base{
					Name: "inittest",
					Properties: map[string]string{
						keyConnectionString: dbPath,
						// Connect with a higher busy timeout
						keyBusyTimeout: "10s",
					},
				},
			}

			// Maintain all store objects
			var storeObjs []*state_sqlite.SQLiteStore
			newStoreObj := func() error {
				storeObj := state_sqlite.NewSQLiteStateStore(log).(*state_sqlite.SQLiteStore)
				err := storeObj.Init(context.Background(), md)
				if err != nil {
					return err
				}
				storeObjs = append(storeObjs, storeObj)
				return nil
			}
			defer func() {
				for _, storeObj := range storeObjs {
					_ = storeObj.Close()
				}
			}()

			testMultipleKeys := func(t *testing.T) {
				const parallel = 10
				const runs = 30

				ctx := context.Background()

				wg := sync.WaitGroup{}
				wg.Add(parallel)
				for i := 0; i < parallel; i++ {
					go func(i int) {
						defer wg.Done()

						var (
							res *state.GetResponse
							err error
						)

						storeObj := storeObjs[i%len(storeObjs)]

						key := fmt.Sprintf("multiple_%d", i)
						for j := 0; j < runs; j++ {
							// Save state
							err = storeObj.Set(ctx, &state.SetRequest{
								Key:   key,
								Value: j,
							})
							assert.NoError(t, err)

							// Retrieve state
							res, err = storeObj.Get(ctx, &state.GetRequest{
								Key: key,
							})
							assert.NoError(t, err)
							assert.Equal(t, strconv.Itoa(j), string(res.Data))
						}
					}(i)
				}

				wg.Wait()
			}

			testSameKey := func(t *testing.T) {
				const parallel = 10
				const runs = 30

				ctx := context.Background()

				// We have as many counters as we have number of parallel workers. The final value will be one of them.
				// This is because we have a race condition between the time we call `save := counter.Add(1)` and when we set the value, causing tests to be flaky since the value stored in the database may not be the one stored in counter.
				// This is not a bug in the SQLite component, as it's working as intended; the race condition is on the tests themselves.
				// The real solution to the race condition (and what one should do in a real-world app) would be to wrap the call to increment counter and the store operation in a mutex. However, that would make it so only one query is hitting the database at the same time, while here we're precisely testing how the component handles multiple writes at the same time.
				wg := sync.WaitGroup{}
				wg.Add(parallel)
				counters := [parallel]atomic.Int32{}
				key := "same"
				for i := 0; i < parallel; i++ {
					go func(i int) {
						defer wg.Done()

						var err error

						storeObj := storeObjs[i%len(storeObjs)]

						for j := 0; j < runs; j++ {
							save := counters[i].Add(1)
							// Save state
							err = storeObj.Set(ctx, &state.SetRequest{
								Key:   key,
								Value: int(save),
							})
							assert.NoError(t, err)
						}
					}(i)
				}

				wg.Wait()

				// Retrieve state
				res, err := storeObjs[0].Get(ctx, &state.GetRequest{
					Key: key,
				})
				assert.NoError(t, err)

				expect := [parallel]string{}
				for i := 0; i < parallel; i++ {
					expect[i] = strconv.Itoa(int(counters[i].Load()))
				}
				assert.Contains(t, expect, string(res.Data))
			}

			// Init one store object
			require.NoError(t, newStoreObj(), "failed to init")

			t.Run("same connection, multiple keys", testMultipleKeys)
			t.Run("same connection, same key", testSameKey)

			// Init two more store objects
			require.NoError(t, newStoreObj(), "failed to init")
			require.NoError(t, newStoreObj(), "failed to init")

			t.Run("multiple connections, multiple keys", testMultipleKeys)
			t.Run("multiple connections, same key", testSameKey)
		})
		return nil
	}

	// This makes it possible to comment-out individual tests
	_ = basicTest
	_ = verifySQLInjectionTest
	_ = readonlyTest
	_ = ttlTest
	_ = initTest
	_ = concurrencyTest

	flow.New(t, "Run tests").
		// Start the sidecar with the in-memory database
		Step(sidecar.Run("sqlite-memory",
			embedded.WithoutApp(),
			embedded.WithDaprGRPCPort(strconv.Itoa(runtime.DefaultDaprAPIGRPCPort)),
			embedded.WithDaprHTTPPort(strconv.Itoa(runtime.DefaultDaprHTTPPort)),
			embedded.WithProfilingEnabled(false),
			embedded.WithResourcesPath("resources/memory"),
			embedded.WithStates(stateRegistry),
		)).

		// Run some basic certification tests with the in-memory database
		Step("run basic test", basicTest(runtime.DefaultDaprAPIGRPCPort)).
		Step("run SQL injection test", verifySQLInjectionTest(runtime.DefaultDaprAPIGRPCPort)).
		Step("stop app", sidecar.Stop("sqlite-memory")).

		// Start the sidecar with a read-only database
		Step(sidecar.Run("sqlite-readonly",
			embedded.WithoutApp(),
			embedded.WithDaprGRPCPort(strconv.Itoa(runtime.DefaultDaprAPIGRPCPort+portOffset)),
			embedded.WithDaprHTTPPort(strconv.Itoa(runtime.DefaultDaprHTTPPort+portOffset)),
			embedded.WithProfilingEnabled(false),
			embedded.WithResourcesPath("resources/readonly"),
			embedded.WithStates(stateRegistry),
		)).
		Step("run read-only test", readonlyTest(runtime.DefaultDaprAPIGRPCPort+portOffset)).
		Step("stop sqlite-readonly sidecar", sidecar.Stop("sqlite-readonly")).
		Step("confirm read-only test", readonlyConfirmTest).

		// Run TTL tests
		Step("run TTL test", ttlTest).

		// Run init and migrations tests
		Step("run init and migrations test", initTest).

		// Concurrency test
		Step("run concurrency test", concurrencyTest).

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

func tableExists(dbClient *sql.DB, tableName string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	var exists string
	// Returns 1 or 0 as a string if the table exists or not.
	const q = `SELECT EXISTS (
		SELECT name FROM sqlite_master WHERE type='table' AND name = ?
	) AS 'exists'`
	err := dbClient.QueryRowContext(ctx, q, tableName).
		Scan(&exists)
	if err != nil {
		return err
	}
	if exists != "1" {
		return errors.New("table not found")
	}
	return nil
}

func getMigrationLevel(dbClient *sql.DB, metadataTable string) (level string, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	err = dbClient.
		QueryRowContext(ctx, fmt.Sprintf(`SELECT value FROM %s WHERE key = 'migrations'`, metadataTable)).
		Scan(&level)
	if err != nil && errors.Is(err, sql.ErrNoRows) {
		err = nil
		level = ""
	}
	return level, err
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
