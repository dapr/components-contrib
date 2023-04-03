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
	"strconv"
	"testing"

	cosmosdb "github.com/dapr/components-contrib/state/azure/cosmosdb"
	"github.com/dapr/components-contrib/tests/certification/embedded"
	"github.com/dapr/components-contrib/tests/certification/flow"
	"github.com/dapr/go-sdk/client"
	"github.com/google/uuid"

	secretstore_env "github.com/dapr/components-contrib/secretstores/local/env"
	"github.com/dapr/components-contrib/tests/certification/flow/sidecar"
	secretstores_loader "github.com/dapr/dapr/pkg/components/secretstores"
	state_loader "github.com/dapr/dapr/pkg/components/state"
	"github.com/dapr/dapr/pkg/runtime"
	dapr_testing "github.com/dapr/dapr/pkg/testing"
	daprClient "github.com/dapr/go-sdk/client"
	"github.com/dapr/kit/logger"
	"github.com/stretchr/testify/assert"
)

const (
	sidecarNamePrefix       = "cosmosdb-sidecar-"
	certificationTestPrefix = "stable-certification-"
)

func TestAzureCosmosDBStorage(t *testing.T) {
	sidecarName := sidecarNamePrefix + uuid.NewString()

	ports, err := dapr_testing.GetFreePorts(2)
	assert.NoError(t, err)

	currentGrpcPort := ports[0]
	currentHTTPPort := ports[1]

	basicTest := func(statestore string) flow.Runnable {
		return func(ctx flow.Context) error {
			client, err := client.NewClientWithPort(strconv.Itoa(currentGrpcPort))
			if err != nil {
				panic(err)
			}
			defer client.Close()

			stateKey := certificationTestPrefix + "key"
			stateValue := "certificationdata"

			// save state, default options: strong, last-write
			err = client.SaveState(ctx, statestore, stateKey, []byte(stateValue), nil)
			assert.NoError(t, err)

			// get state
			item, err := client.GetState(ctx, statestore, stateKey, nil)
			assert.NoError(t, err)
			assert.Equal(t, stateValue, string(item.Value))

			// delete state
			err = client.DeleteState(ctx, statestore, stateKey, nil)
			assert.NoError(t, err)

			return nil
		}
	}

	transactionsTest := func(statestore string) func(ctx flow.Context) error {
		return func(ctx flow.Context) error {
			client, err := client.NewClientWithPort(strconv.Itoa(currentGrpcPort))
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
							"ttlInSeconds": "-1",
						},
					},
				},
			})
			assert.NoError(t, err)

			return nil
		}
	}

	partitionTest := func(statestore string) flow.Runnable {
		return func(ctx flow.Context) error {
			client, err := client.NewClientWithPort(strconv.Itoa(currentGrpcPort))
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

			test := func(setMeta, getMeta map[string]string, expectedValue string) {
				// save state, default options: strong, last-write
				err = client.SaveState(ctx, statestore, stateKey, []byte(stateValue), setMeta)
				assert.NoError(t, err)

				// get state
				item, err := client.GetState(ctx, statestore, stateKey, getMeta)
				assert.NoError(t, err)
				assert.Equal(t, expectedValue, string(item.Value))

				// delete state
				err = client.DeleteState(ctx, statestore, stateKey, setMeta)
				assert.NoError(t, err)
			}

			// Test	with no partition key
			test(nil, meta1, stateValue)

			// Test with specific partition key
			test(meta2, meta2, stateValue)

			// Test with incorrect partition key
			test(meta2, meta1, "")

			return nil
		}
	}

	flow.New(t, "Test basic operations").
		// Run the Dapr sidecar with azure CosmosDB storage.
		Step(sidecar.Run(sidecarName,
			embedded.WithoutApp(),
			embedded.WithDaprGRPCPort(currentGrpcPort),
			embedded.WithDaprHTTPPort(currentHTTPPort),
			embedded.WithComponentsPath("./components/basictest"),
			componentRuntimeOptions())).
		Step("Run basic test with master key", basicTest("statestore-basic")).
		Run()

	flow.New(t, "Test transaction operations").
		// Run the Dapr sidecar with azure CosmosDB storage.
		Step(sidecar.Run(sidecarName,
			embedded.WithoutApp(),
			embedded.WithDaprGRPCPort(currentGrpcPort),
			embedded.WithDaprHTTPPort(currentHTTPPort),
			embedded.WithComponentsPath("./components/basictest"),
			componentRuntimeOptions())).
		Step("Run transaction test with etag present", transactionsTest("statestore-basic")).
		Run()

	flow.New(t, "Test basic operations with different partition keys").
		// Run the Dapr sidecar with azure CosmosDB storage.
		Step(sidecar.Run(sidecarName,
			embedded.WithoutApp(),
			embedded.WithDaprGRPCPort(currentGrpcPort),
			embedded.WithDaprHTTPPort(currentHTTPPort),
			embedded.WithComponentsPath("./components/basictest"),
			componentRuntimeOptions())).
		Step("Run basic test with multiple parition keys", partitionTest("statestore-basic")).
		Run()

	/*
		flow.New(t, "Test AAD authentication").
			// Run the Dapr sidecar with azure CosmosDB storage.
			Step(sidecar.Run(sidecarNamePrefix,
				embedded.WithoutApp(),
				embedded.WithDaprGRPCPort(currentGrpcPort),
				embedded.WithDaprHTTPPort(currentHTTPPort),
				embedded.WithComponentsPath("./components/aadtest"),
				componentRuntimeOptions())).
			Step("Run basic test with Azure AD Authentication", basicTest("statestore-aad")).
			Run()
	*/
}

func componentRuntimeOptions() []runtime.Option {
	log := logger.NewLogger("dapr.components")

	stateRegistry := state_loader.NewRegistry()
	stateRegistry.Logger = log
	stateRegistry.RegisterComponent(cosmosdb.NewCosmosDBStateStore, "azure.cosmosdb")

	secretstoreRegistry := secretstores_loader.NewRegistry()
	secretstoreRegistry.Logger = log
	secretstoreRegistry.RegisterComponent(secretstore_env.NewEnvSecretStore, "local.env")

	return []runtime.Option{
		runtime.WithStates(stateRegistry),
		runtime.WithSecretStores(secretstoreRegistry),
	}
}
