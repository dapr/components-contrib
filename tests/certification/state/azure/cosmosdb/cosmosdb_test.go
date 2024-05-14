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

package cosmosDBStorage_test

import (
	"os"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/metadata"
	secretstore_env "github.com/dapr/components-contrib/secretstores/local/env"
	"github.com/dapr/components-contrib/state"
	cosmosdb "github.com/dapr/components-contrib/state/azure/cosmosdb"
	"github.com/dapr/components-contrib/tests/certification/embedded"
	"github.com/dapr/components-contrib/tests/certification/flow"
	"github.com/dapr/components-contrib/tests/certification/flow/sidecar"
	secretstores_loader "github.com/dapr/dapr/pkg/components/secretstores"
	state_loader "github.com/dapr/dapr/pkg/components/state"
	dapr_testing "github.com/dapr/dapr/pkg/testing"
	daprClient "github.com/dapr/go-sdk/client"
	"github.com/dapr/kit/logger"
)

var log = logger.NewLogger("dapr.components")

const (
	sidecarNamePrefix       = "cosmosdb-sidecar-"
	certificationTestPrefix = "stable-certification-"
)

func TestAzureCosmosDBStorage(t *testing.T) {
	sidecarName := sidecarNamePrefix + uuid.NewString()

	ports, err := dapr_testing.GetFreePorts(2)
	require.NoError(t, err)

	currentGrpcPort := ports[0]
	currentHTTPPort := ports[1]

	basicTest := func(statestore string) flow.Runnable {
		return func(ctx flow.Context) error {
			client, err := daprClient.NewClientWithPort(strconv.Itoa(currentGrpcPort))
			if err != nil {
				panic(err)
			}
			defer client.Close()

			stateKey := certificationTestPrefix + "key"
			stateValue := "certificationdata"

			// save state, default options: strong, last-write
			err = client.SaveState(ctx, statestore, stateKey, []byte(stateValue), nil)
			require.NoError(t, err)

			// get state
			item, err := client.GetState(ctx, statestore, stateKey, nil)
			require.NoError(t, err)
			assert.Equal(t, stateValue, string(item.Value))

			// delete state
			err = client.DeleteState(ctx, statestore, stateKey, nil)
			require.NoError(t, err)

			return nil
		}
	}

	transactionsTest := func(statestore string) func(ctx flow.Context) error {
		return func(ctx flow.Context) error {
			client, err := daprClient.NewClientWithPort(strconv.Itoa(currentGrpcPort))
			if err != nil {
				panic(err)
			}
			defer client.Close()

			err = client.ExecuteStateTransaction(ctx, statestore, nil, []*daprClient.StateOperation{
				{
					Type: daprClient.StateOperationTypeUpsert,
					Item: &daprClient.SetStateItem{
						Key:   "reqKey1",
						Value: []byte("reqVal1"),
						Etag: &daprClient.ETag{
							Value: "test",
						},
						Metadata: map[string]string{
							"ttlInSeconds": "60",
							"partitionKey": "txpartition",
						},
					},
				},
			})
			require.NoError(t, err)

			return nil
		}
	}

	partitionTest := func(statestore string) flow.Runnable {
		return func(ctx flow.Context) error {
			client, err := daprClient.NewClientWithPort(strconv.Itoa(currentGrpcPort))
			if err != nil {
				panic(err)
			}
			defer client.Close()

			stateKey := certificationTestPrefix + "key"
			stateValue := "certificationdata"

			// The default value for partition key is <App ID>||<state key>
			meta1 := map[string]string{
				"partitionKey": sidecarName + "||" + stateKey,
			}

			// Specifying custom partition key
			meta2 := map[string]string{
				"partitionKey": "mypartition",
			}

			test := func(setMeta, getMeta map[string]string, expectedValue string, expectedErr bool) {
				// save state, default options: strong, last-write
				err = client.SaveState(ctx, statestore, stateKey, []byte(stateValue), setMeta)
				require.NoError(t, err)

				// get state
				item, err := client.GetState(ctx, statestore, stateKey, getMeta)
				if expectedErr {
					require.Error(t, err)
					return
				}
				require.NoError(t, err)
				assert.Equal(t, expectedValue, string(item.Value))

				// delete state
				err = client.DeleteState(ctx, statestore, stateKey, setMeta)
				require.NoError(t, err)
			}

			// Test	with no partition key
			test(nil, meta1, stateValue, false)

			// Test with specific partition key
			test(meta2, meta2, stateValue, false)

			// Test with incorrect partition key
			test(meta2, meta1, "", true)

			return nil
		}
	}

	// Special test for bulk operations designed to validate that BulkGet/BulkSet/BulkDelete work especially across partitions
	bulkTest := func(statestore string) func(ctx flow.Context) error {
		return func(ctx flow.Context) error {
			// Instantiate a component directly so we get access to its APIs and can test the ones we want specifically
			store := cosmosdb.NewCosmosDBStateStore(log)
			err := store.Init(ctx, state.Metadata{
				Base: metadata.Base{Properties: map[string]string{
					"url":        os.Getenv("AzureCosmosDBUrl"),
					"masterKey":  os.Getenv("AzureCosmosDBMasterKey"),
					"database":   os.Getenv("AzureCosmosDB"),
					"collection": os.Getenv("AzureCosmosDBCollection"),
				}},
			})
			if err != nil {
				panic(err)
			}

			runTest := func(testKey string, setMetadata map[string]string) func(t *testing.T) {
				return func(t *testing.T) {
					t.Run("save", func(t *testing.T) {
						reqs := []state.SetRequest{}
						for i := 1; i <= 5; i++ {
							key := sidecarName + "||" + testKey + "||" + strconv.Itoa(i)
							reqs = append(reqs, state.SetRequest{
								Key:      key,
								Value:    key,
								Metadata: setMetadata,
							})
						}
						err := store.BulkSet(ctx, reqs, state.BulkStoreOpts{})
						require.NoError(t, err)
					})

					t.Run("get", func(t *testing.T) {
						expectKeys := []string{}
						reqs := []state.GetRequest{}
						// Only from 2 to 6 (which doesn't exist so should be empty)
						for i := 2; i <= 6; i++ {
							key := sidecarName + "||" + testKey + "||" + strconv.Itoa(i)
							expectKeys = append(expectKeys, key)
							reqs = append(reqs, state.GetRequest{
								Key:      key,
								Metadata: setMetadata,
							})
						}
						res, err := store.BulkGet(ctx, reqs, state.BulkGetOpts{})
						require.NoError(t, err)
						require.Len(t, res, 5)

						foundKeys := []string{}
						for i := 0; i < len(res); i++ {
							if strings.HasSuffix(res[i].Key, "6") {
								assert.Empty(t, res[i].Data)
							} else {
								assert.Equalf(t, `"`+res[i].Key+`"`, string(res[i].Data), "value for key %s is not valid", res[i].Key)
							}
							foundKeys = append(foundKeys, res[i].Key)
						}

						// Sort the keys before checking for equality
						slices.Sort(expectKeys)
						slices.Sort(foundKeys)
						assert.Equal(t, expectKeys, foundKeys)
					})

					t.Run("delete", func(t *testing.T) {
						// Delete only from 3 to 5
						// Then retrieve from 1 to 5
						deleteReqs := []state.DeleteRequest{}
						getReqs := []state.GetRequest{}
						expectKeys := []string{}
						for i := 1; i <= 5; i++ {
							key := sidecarName + "||" + testKey + "||" + strconv.Itoa(i)
							if i >= 3 {
								deleteReqs = append(deleteReqs, state.DeleteRequest{
									Key:      key,
									Metadata: setMetadata,
								})
							}
							getReqs = append(getReqs, state.GetRequest{
								Key:      key,
								Metadata: setMetadata,
							})
							expectKeys = append(expectKeys, key)
						}

						// Delete
						err := store.BulkDelete(ctx, deleteReqs, state.BulkStoreOpts{})
						require.NoError(t, err)

						// Retrieve
						res, err := store.BulkGet(ctx, getReqs, state.BulkGetOpts{})
						require.NoError(t, err)
						require.Len(t, res, 5)

						foundKeys := []string{}
						for i := 0; i < len(res); i++ {
							key := res[i].Key
							keyNum, err := strconv.Atoi(key[len(key)-1:])
							require.NoErrorf(t, err, "failed to get number from key %s", key)
							if keyNum >= 3 {
								assert.Empty(t, res[i].Data)
							} else {
								assert.Equalf(t, `"`+res[i].Key+`"`, string(res[i].Data), "value for key %s is not valid", res[i].Key)
							}
							foundKeys = append(foundKeys, res[i].Key)
						}

						// Sort the keys before checking for equality
						slices.Sort(expectKeys)
						slices.Sort(foundKeys)
						assert.Equal(t, expectKeys, foundKeys)
					})
				}
			}

			// If no partition key is passed, then the component uses the key name as partition key, so they're all unique
			ctx.T.Run("no partition key specified", runTest("bulk-nopk", nil))

			ctx.T.Run("same partition key", runTest("bulk-pk", map[string]string{
				"partitionKey": sidecarName,
			}))

			return nil
		}
	}

	flow.New(t, "Test master key auth").
		// Run the Dapr sidecar with azure CosmosDB storage.
		Step(sidecar.Run(sidecarName,
			append(componentRuntimeOptions(),
				embedded.WithoutApp(),
				embedded.WithDaprGRPCPort(strconv.Itoa(currentGrpcPort)),
				embedded.WithDaprHTTPPort(strconv.Itoa(currentHTTPPort)),
				embedded.WithComponentsPath("./components/basictest"),
			)...,
		)).
		Step("Run basic test with master key", basicTest("statestore-basic")).
		Step("Run transaction test with etag present", transactionsTest("statestore-basic")).
		Step("Run basic test with multiple parition keys", partitionTest("statestore-basic")).
		Step("Run tests for bulk operations", bulkTest("statestore-basic")).
		Run()

	flow.New(t, "Test AAD authentication").
		// Run the Dapr sidecar with azure CosmosDB storage.
		Step(sidecar.Run(sidecarNamePrefix,
			append(componentRuntimeOptions(),
				embedded.WithoutApp(),
				embedded.WithDaprGRPCPort(strconv.Itoa(currentGrpcPort)),
				embedded.WithDaprHTTPPort(strconv.Itoa(currentHTTPPort)),
				embedded.WithComponentsPath("./components/aadtest"),
			)...,
		)).
		Step("Run basic test with Azure AD Authentication", basicTest("statestore-aad")).
		Run()
}

func componentRuntimeOptions() []embedded.Option {
	stateRegistry := state_loader.NewRegistry()
	stateRegistry.Logger = log
	stateRegistry.RegisterComponent(cosmosdb.NewCosmosDBStateStore, "azure.cosmosdb")

	secretstoreRegistry := secretstores_loader.NewRegistry()
	secretstoreRegistry.Logger = log
	secretstoreRegistry.RegisterComponent(secretstore_env.NewEnvSecretStore, "local.env")

	return []embedded.Option{
		embedded.WithStates(stateRegistry),
		embedded.WithSecretStores(secretstoreRegistry),
	}
}
