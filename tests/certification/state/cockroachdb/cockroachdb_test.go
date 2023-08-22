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

package cockroachdb_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	// Blank import for the underlying PostgreSQL driver.
	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/dapr/components-contrib/internal/component/postgresql"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/state"
	state_cockroach "github.com/dapr/components-contrib/state/cockroachdb"
	"github.com/dapr/components-contrib/tests/certification/embedded"
	"github.com/dapr/components-contrib/tests/certification/flow"
	"github.com/dapr/components-contrib/tests/certification/flow/dockercompose"
	"github.com/dapr/components-contrib/tests/certification/flow/sidecar"
	state_loader "github.com/dapr/dapr/pkg/components/state"
	dapr_testing "github.com/dapr/dapr/pkg/testing"
	goclient "github.com/dapr/go-sdk/client"
	"github.com/dapr/kit/logger"
)

const (
	sidecarNamePrefix       = "cockroach-sidecar-"
	dockerComposeYAML       = "docker-compose.yml"
	stateStoreName          = "statestore"
	certificationTestPrefix = "stable-certification-"
	stateStoreNoConfigError = "error saving state: rpc error: code = FailedPrecondition desc = state store is not configured"
)

func TestCockroach(t *testing.T) {
	log := logger.NewLogger("dapr.components")

	stateStore := state_cockroach.New(log).(*postgresql.PostgreSQL)
	ports, err := dapr_testing.GetFreePorts(3)
	assert.NoError(t, err)

	stateRegistry := state_loader.NewRegistry()
	stateRegistry.Logger = log
	stateRegistry.RegisterComponent(func(l logger.Logger) state.Store {
		return stateStore
	}, "cockroachdb")

	currentGrpcPort := ports[0]
	currentHTTPPort := ports[1]

	// Generate a unique value for the key being inserted to ensure no conflicts occur
	keyOne := uuid.New()
	keyOneString := strings.Replace(keyOne.String(), "-", "", -1)

	var dbClient *sql.DB
	connectStep := func(ctx flow.Context) error {
		connCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()

		// Continue re-trying until the context times out, so we can wait for the DB to be up
		for {
			dbClient, err = sql.Open("pgx", "host=localhost user=root port=26257 connect_timeout=10 database=dapr_test")
			if err == nil || connCtx.Err() != nil {
				break
			}
			time.Sleep(750 * time.Millisecond)
		}
		return err
	}

	basicTest := func(ctx flow.Context) error {
		client, err := goclient.NewClientWithPort(fmt.Sprint(currentGrpcPort))
		if err != nil {
			panic(err)
		}
		defer client.Close()

		err = client.SaveState(ctx, stateStoreName, certificationTestPrefix+"key1", []byte("certificationdata"), nil)
		assert.NoError(t, err)

		// get state
		item, err := client.GetState(ctx, stateStoreName, certificationTestPrefix+"key1", nil)
		assert.NoError(t, err)
		assert.Equal(t, "certificationdata", string(item.Value))

		errUpdate := client.SaveState(ctx, stateStoreName, certificationTestPrefix+"key1", []byte("cockroachCertUpdate"), nil)
		assert.NoError(t, errUpdate)
		item, errUpdatedGet := client.GetState(ctx, stateStoreName, certificationTestPrefix+"key1", nil)
		assert.NoError(t, errUpdatedGet)
		assert.Equal(t, "cockroachCertUpdate", string(item.Value))

		// delete state
		err = client.DeleteState(ctx, stateStoreName, certificationTestPrefix+"key1", nil)
		assert.NoError(t, err)

		// get state
		item, errUpdatedGet = client.GetState(ctx, stateStoreName, certificationTestPrefix+"key1", nil)
		assert.NoError(t, errUpdatedGet)
		assert.Equal(t, []byte(nil), item.Value)
		return nil
	}

	// Check if TCP port is actually open
	testGetAfterCockroachdbRestart := func(ctx flow.Context) error {
		client, err := goclient.NewClientWithPort(fmt.Sprint(currentGrpcPort))
		if err != nil {
			panic(err)
		}
		defer client.Close()

		resp, err := stateStore.Get(context.Background(), &state.GetRequest{
			Key: keyOneString,
		})
		assert.NoError(t, err)
		assert.Equal(t, "2", *resp.ETag) // 2 is returned since the previous etag value of "1" was incremented by 1 when the update occurred
		assert.Equal(t, "\"Overwrite Success\"", string(resp.Data))

		return nil
	}

	// ETag test
	eTagTest := func(ctx flow.Context) error {
		etag1 := "1"
		etag100 := "100"

		// Setting with nil etag will insert an item with an etag value of 1 unless there is a conflict
		err := stateStore.Set(context.Background(), &state.SetRequest{
			Key:   keyOneString,
			Value: "v1",
		})
		assert.NoError(t, err)

		// Setting with an etag wil do an update, not an insert so an error is expected since the etag of 100 is not present
		err = stateStore.Set(context.Background(), &state.SetRequest{
			Key:   keyOneString,
			Value: "v3",
			ETag:  &etag100,
		})
		assert.Equal(t, state.NewETagError(state.ETagMismatch, nil), err)

		resp, err := stateStore.Get(context.Background(), &state.GetRequest{
			Key: keyOneString,
		})
		assert.NoError(t, err)
		assert.Equal(t, etag1, *resp.ETag)           // 1 is returned since the default when the new data is written is a value of 1
		assert.Equal(t, "\"v1\"", string(resp.Data)) // v1 is returned since it was the only item successfully inserted with the key of keyOneString

		// This will update the value stored in key K with "Overwrite Success" since the previously created etag has a value of 1
		// It will also increment the etag stored by a value of 1
		err = stateStore.Set(context.Background(), &state.SetRequest{
			Key:   keyOneString,
			Value: "Overwrite Success",
			ETag:  &etag1,
		})
		assert.NoError(t, err)

		resp, err = stateStore.Get(context.Background(), &state.GetRequest{
			Key: keyOneString,
		})
		assert.NoError(t, err)
		assert.Equal(t, "2", *resp.ETag) // 2 is returned since the previous etag value of "1" was incremented by 1 when the update occurred
		assert.Equal(t, "\"Overwrite Success\"", string(resp.Data))

		return nil
	}

	// Transaction related test - also for Multi
	transactionsTest := func(ctx flow.Context) error {
		// Set state to allow for a delete operation inside the multi list
		err = stateStore.Set(context.Background(), &state.SetRequest{Key: certificationTestPrefix + "key1", Value: []byte("certificationdata")})
		assert.NoError(t, err)

		// get state
		item, errUpdatedGet := stateStore.Get(context.Background(), &state.GetRequest{Key: certificationTestPrefix + "key1"})
		assert.NoError(t, errUpdatedGet)
		assert.Equal(t, []byte("certificationdata"), item.Data)

		err = stateStore.Multi(context.Background(), &state.TransactionalStateRequest{
			Operations: []state.TransactionalStateOperation{
				state.SetRequest{
					Key:   "reqKey1",
					Value: "reqVal1",
					Metadata: map[string]string{
						"ttlInSeconds": "-1",
					},
				},
				state.SetRequest{
					Key:   "reqKey2",
					Value: "reqVal2",
					Metadata: map[string]string{
						"ttlInSeconds": "222",
					},
				},
				state.SetRequest{
					Key:   "reqKey3",
					Value: "reqVal3",
				},
				state.SetRequest{
					Key:   "reqKey1",
					Value: "reqVal101",
					Metadata: map[string]string{
						"ttlInSeconds": "50",
					},
				},
				state.SetRequest{
					Key:   "reqKey3",
					Value: "reqVal103",
					Metadata: map[string]string{
						"ttlInSeconds": "50",
					},
				},
				state.DeleteRequest{
					Key:      certificationTestPrefix + "key1",
					Metadata: map[string]string{},
				},
			},
		})
		assert.Equal(t, nil, err)

		// get state
		item, errUpdatedGet = stateStore.Get(context.Background(), &state.GetRequest{Key: certificationTestPrefix + "key1"})
		assert.NoError(t, errUpdatedGet)
		assert.Equal(t, []byte(nil), item.Data)

		resp1, err := stateStore.Get(context.Background(), &state.GetRequest{
			Key: "reqKey1",
		})
		assert.NoError(t, err)
		assert.Equal(t, "2", *resp1.ETag)
		assert.Equal(t, "\"reqVal101\"", string(resp1.Data))

		resp3, err := stateStore.Get(context.Background(), &state.GetRequest{
			Key: "reqKey3",
		})
		assert.NoError(t, err)
		assert.Equal(t, "2", *resp3.ETag)
		assert.Equal(t, "\"reqVal103\"", string(resp3.Data))
		require.Contains(t, resp3.Metadata, "ttlExpireTime")
		expireTime, err := time.Parse(time.RFC3339, resp3.Metadata["ttlExpireTime"])
		assert.InDelta(t, time.Now().Add(50*time.Second).Unix(), expireTime.Unix(), 5)
		return nil
	}

	// Validates TTLs and garbage collections
	ttlTest := func(ctx flow.Context) error {
		md := state.Metadata{
			Base: metadata.Base{
				Name: "ttltest",
				Properties: map[string]string{
					"connectionString":  "host=localhost user=root port=26257 connect_timeout=10 database=dapr_test",
					"tableName":         "ttl_state",
					"metadataTableName": "ttl_metadata",
				},
			},
		}

		t.Run("parse cleanupIntervalInSeconds", func(t *testing.T) {
			t.Run("default value", func(t *testing.T) {
				// Default value is 1 hr
				md.Properties["cleanupIntervalInSeconds"] = ""
				storeObj := state_cockroach.New(log).(*postgresql.PostgreSQL)

				err := storeObj.Init(context.Background(), md)
				require.NoError(t, err, "failed to init")
				defer storeObj.Close()

				dbAccess := storeObj.GetDBAccess().(*postgresql.PostgresDBAccess)
				require.NotNil(t, dbAccess)

				cleanupInterval := dbAccess.GetCleanupInterval()
				require.NotNil(t, cleanupInterval)
				assert.Equal(t, time.Duration(1*time.Hour), *cleanupInterval)
			})

			t.Run("positive value", func(t *testing.T) {
				// A positive value is interpreted in seconds
				md.Properties["cleanupIntervalInSeconds"] = "10"
				storeObj := state_cockroach.New(log).(*postgresql.PostgreSQL)

				err := storeObj.Init(context.Background(), md)
				require.NoError(t, err, "failed to init")
				defer storeObj.Close()

				dbAccess := storeObj.GetDBAccess().(*postgresql.PostgresDBAccess)
				require.NotNil(t, dbAccess)

				cleanupInterval := dbAccess.GetCleanupInterval()
				require.NotNil(t, cleanupInterval)
				assert.Equal(t, time.Duration(10*time.Second), *cleanupInterval)
			})

			t.Run("disabled", func(t *testing.T) {
				// A value of <=0 means that the cleanup is disabled
				md.Properties["cleanupIntervalInSeconds"] = "0"
				storeObj := state_cockroach.New(log).(*postgresql.PostgreSQL)

				err := storeObj.Init(context.Background(), md)
				require.NoError(t, err, "failed to init")
				defer storeObj.Close()

				dbAccess := storeObj.GetDBAccess().(*postgresql.PostgresDBAccess)
				require.NotNil(t, dbAccess)

				cleanupInterval := dbAccess.GetCleanupInterval()
				_ = assert.Nil(t, cleanupInterval)
			})
		})

		t.Run("cleanup", func(t *testing.T) {
			md := state.Metadata{
				Base: metadata.Base{
					Name: "ttltest",
					Properties: map[string]string{
						"connectionString":  "host=localhost user=root port=26257 connect_timeout=10 database=dapr_test",
						"tableName":         "ttl_state",
						"metadataTableName": "ttl_metadata",
					},
				},
			}

			t.Run("automatically delete expiredate records", func(t *testing.T) {
				// Run every second
				md.Properties["cleanupIntervalInSeconds"] = "1"

				storeObj := state_cockroach.New(log).(*postgresql.PostgreSQL)
				err := storeObj.Init(context.Background(), md)
				require.NoError(t, err, "failed to init")
				defer storeObj.Close()

				// Seed the database with some records
				err = populateTTLRecords(ctx, dbClient)
				require.NoError(t, err, "failed to seed records")

				// Wait 3 seconds then verify we have only 10 rows left
				time.Sleep(3 * time.Second)
				count, err := countRowsInTable(ctx, dbClient, "ttl_state")
				require.NoError(t, err, "failed to run query to count rows")
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
				md.Properties["cleanupIntervalInSeconds"] = "3600"

				storeObj := state_cockroach.New(log).(*postgresql.PostgreSQL)
				err := storeObj.Init(context.Background(), md)
				require.NoError(t, err, "failed to init")
				defer storeObj.Close()

				// Seed the database with some records
				err = populateTTLRecords(ctx, dbClient)
				require.NoError(t, err, "failed to seed records")

				// Validate that 20 records are present
				count, err := countRowsInTable(ctx, dbClient, "ttl_state")
				require.NoError(t, err, "failed to run query to count rows")
				assert.Equal(t, 20, count)

				// Set last-cleanup to 1s ago (less than 3600s)
				_, err = dbClient.ExecContext(ctx,
					fmt.Sprintf(`INSERT INTO ttl_metadata (key, value) VALUES ('last-cleanup', %[1]s) ON CONFLICT (key) DO UPDATE SET value = %[1]s`, "(CURRENT_TIMESTAMP - interval '1 second')::STRING"),
				)
				require.NoError(t, err, "failed to set last-cleanup")

				// The "last-cleanup" value should be ~1 second (+ a bit of buffer)
				lastCleanup, err := loadLastCleanupInterval(ctx, dbClient, "ttl_metadata")
				require.NoError(t, err, "failed to load value for 'last-cleanup'")
				assert.LessOrEqual(t, lastCleanup, int64(1200))
				lastCleanupValueOrig, err := getValueFromMetadataTable(ctx, dbClient, "ttl_metadata", "last-cleanup")
				require.NoError(t, err, "failed to load absolute value for 'last-cleanup'")
				require.NotEmpty(t, lastCleanupValueOrig)

				// Trigger the background cleanup, which should do nothing because the last cleanup was < 3600s
				dbAccess := storeObj.GetDBAccess().(*postgresql.PostgresDBAccess)
				require.NotNil(t, dbAccess)
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

	flow.New(t, "Connecting cockroachdb And Verifying majority of the tests here").
		Step(dockercompose.Run("cockroachdb", dockerComposeYAML)).
		Step("Waiting for cockroachdb readiness", flow.Sleep(30*time.Second)).
		Step(sidecar.Run(sidecarNamePrefix+"dockerDefault",
			embedded.WithoutApp(),
			embedded.WithDaprGRPCPort(strconv.Itoa(currentGrpcPort)),
			embedded.WithDaprHTTPPort(strconv.Itoa(currentHTTPPort)),
			embedded.WithComponentsPath("components/standard"),
			embedded.WithStates(stateRegistry),
		)).
		Step("connect to the database", connectStep).
		Step("Run basic test", basicTest).
		Step("Run eTag test", eTagTest).
		Step("Run transactions test", transactionsTest).
		Step("run TTL test", ttlTest).
		Step("Stop cockroachdb server", dockercompose.Stop("cockroachdb", dockerComposeYAML, "cockroachdb")).
		Step("Sleep after dockercompose stop", flow.Sleep(10*time.Second)).
		Step("Start cockroachdb server", dockercompose.Start("cockroachdb", dockerComposeYAML, "cockroachdb")).
		Step("wait for component to start", flow.Sleep(10*time.Second)).
		Step("Get Values Saved Earlier And Not Expired, after cockroachdb restart", testGetAfterCockroachdbRestart).
		Run()
}

func loadLastCleanupInterval(ctx context.Context, dbClient *sql.DB, table string) (lastCleanup int64, err error) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	err = dbClient.
		QueryRowContext(ctx,
			fmt.Sprintf("SELECT (EXTRACT('epoch' FROM CURRENT_TIMESTAMP - value::timestamp with time zone) * 1000)::bigint FROM %s WHERE key = 'last-cleanup'", table),
		).
		Scan(&lastCleanup)
	return
}

func countRowsInTable(ctx context.Context, dbClient *sql.DB, table string) (count int, err error) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	err = dbClient.QueryRowContext(ctx, "SELECT COUNT(key) FROM "+table).Scan(&count)
	return
}

func populateTTLRecords(ctx context.Context, dbClient *sql.DB) error {
	// Insert 10 records that have expired, and 10 that will expire in 4
	// seconds.
	rows := make([][]any, 20)
	for i := 0; i < 10; i++ {
		rows[i] = []any{
			fmt.Sprintf("expired_%d", i),
			json.RawMessage(fmt.Sprintf(`"value_%d"`, i)),
			false,
			"CURRENT_TIMESTAMP - INTERVAL '1 minutes'",
		}
	}
	for i := 0; i < 10; i++ {
		rows[i+10] = []any{
			fmt.Sprintf("notexpired_%d", i),
			json.RawMessage(fmt.Sprintf(`"value_%d"`, i)),
			false,
			"CURRENT_TIMESTAMP + INTERVAL '4 seconds'",
		}
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	for _, row := range rows {
		query := fmt.Sprintf("INSERT INTO ttl_state (key, value, isbinary, expiredate) VALUES ($1, $2, $3, %s)", row[3])
		_, err := dbClient.ExecContext(ctx, query, row[0], row[1], row[2])
		if err != nil {
			return err
		}
	}
	return nil
}

func getValueFromMetadataTable(ctx context.Context, dbClient *sql.DB, table, key string) (value string, err error) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	err = dbClient.
		QueryRowContext(ctx, fmt.Sprintf("SELECT value FROM %s WHERE key = $1", table), key).
		Scan(&value)
	cancel()
	return
}
