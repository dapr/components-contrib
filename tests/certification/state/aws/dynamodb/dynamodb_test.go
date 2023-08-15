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

package dynamoDBStorage_test

import (
	"fmt"
	"strconv"
	"testing"
	"time"

	dynamodb "github.com/dapr/components-contrib/state/aws/dynamodb"
	"github.com/dapr/components-contrib/tests/certification/embedded"
	"github.com/dapr/components-contrib/tests/certification/flow"
	"github.com/dapr/go-sdk/client"

	secretstore_env "github.com/dapr/components-contrib/secretstores/local/env"
	"github.com/dapr/components-contrib/tests/certification/flow/sidecar"
	secretstores_loader "github.com/dapr/dapr/pkg/components/secretstores"
	state_loader "github.com/dapr/dapr/pkg/components/state"
	dapr_testing "github.com/dapr/dapr/pkg/testing"
	"github.com/dapr/kit/logger"
	"github.com/stretchr/testify/assert"
)

const (
	sidecarNamePrefix = "dynamodb-sidecar-"
	key               = "key"
)

func TestAWSDynamoDBStorage(t *testing.T) {
	ports, err := dapr_testing.GetFreePorts(2)
	assert.NoError(t, err)

	currentGrpcPort := ports[0]
	currentHTTPPort := ports[1]

	basicTest := func(statestore string) flow.Runnable {
		return func(ctx flow.Context) error {
			client, err := client.NewClientWithPort(fmt.Sprint(currentGrpcPort))
			if err != nil {
				panic(err)
			}
			defer client.Close()

			stateKey := key
			stateValue := "certificationdata"

			// save state, default options: strong, last-write
			err = client.SaveState(ctx, statestore, stateKey, []byte(stateValue), nil)
			assert.NoError(t, err)

			// get state
			item, err := client.GetState(ctx, statestore, stateKey, nil)
			assert.NoError(t, err)
			assert.NotNil(t, item)
			assert.Equal(t, stateValue, string(item.Value))
			assert.NotContains(t, item.Metadata, "ttlExpireTime")

			// delete state
			err = client.DeleteState(ctx, statestore, stateKey, nil)
			assert.NoError(t, err)

			return nil
		}
	}

	ttlTest := func(statestore string) flow.Runnable {
		return func(ctx flow.Context) error {
			client, err := client.NewClientWithPort(fmt.Sprint(currentGrpcPort))
			if err != nil {
				panic(err)
			}
			defer client.Close()

			stateKey := key
			stateValue := "certificationdata"

			metaTTL := map[string]string{
				"ttlInSeconds": "300", // 5 minutes TTL
			}

			metaExpiredTTL := map[string]string{
				"ttlInSeconds": "-1",
			}

			test := func(metaTTL map[string]string, expectedValue string) {
				err = client.SaveState(ctx, statestore, stateKey, []byte(stateValue), metaTTL)
				assert.NoError(t, err)

				item, err := client.GetState(ctx, statestore, stateKey, nil)
				assert.NoError(t, err)
				assert.Equal(t, expectedValue, string(item.Value))

				if len(expectedValue) > 0 {
					assert.Contains(t, item.Metadata, "ttlExpireTime")
					expireTime, err := time.Parse(time.RFC3339, item.Metadata["ttlExpireTime"])
					_ = assert.NoError(t, err) && assert.InDelta(t, time.Now().Add(5*time.Minute).Unix(), expireTime.Unix(), 10)
				}

				err = client.DeleteState(ctx, statestore, stateKey, nil)
				assert.NoError(t, err)
			}

			// Test	with TTL long enough for value to exist
			test(metaTTL, stateValue)

			// Test with expired TTL; value must not exist
			test(metaExpiredTTL, "")

			return nil
		}
	}

	transactionsTest := func(statestore string) func(ctx flow.Context) error {
		return func(ctx flow.Context) error {
			cl, err := client.NewClientWithPort(strconv.Itoa(currentGrpcPort))
			if err != nil {
				return err
			}
			defer cl.Close()

			ktx1 := "reqKeyTx1"
			ktx2 := "reqKeyTx2"
			kdel := "reqKey2"

			err = cl.SaveState(ctx, statestore, kdel, []byte(kdel), nil)
			assert.NoError(t, err)

			err = cl.ExecuteStateTransaction(ctx, statestore, nil, []*client.StateOperation{
				{
					Type: client.StateOperationTypeUpsert,
					Item: &client.SetStateItem{
						Key:   ktx1,
						Value: []byte("reqValTx1"),
						Etag: &client.ETag{
							Value: "test",
						},
						Metadata: map[string]string{},
					},
				},
				{
					Type: client.StateOperationTypeDelete,
					Item: &client.SetStateItem{
						Key:      kdel,
						Metadata: map[string]string{},
					},
				},
				{
					Type: client.StateOperationTypeUpsert,
					Item: &client.SetStateItem{
						Key:   ktx2,
						Value: []byte("reqValTx2"),
						Etag: &client.ETag{
							Value: "test",
						},
						Metadata: map[string]string{},
					},
				},
			})
			assert.NoError(t, err)

			err = cl.DeleteState(ctx, statestore, ktx1, nil)
			assert.NoError(t, err)

			err = cl.DeleteState(ctx, statestore, ktx2, nil)
			assert.NoError(t, err)

			return nil
		}
	}

	flow.New(t, "Test basic operations").
		// Run the Dapr sidecar with AWS DynamoDB storage.
		Step(sidecar.Run(sidecarNamePrefix,
			append(componentRuntimeOptions(),
				embedded.WithoutApp(),
				embedded.WithDaprGRPCPort(strconv.Itoa(currentGrpcPort)),
				embedded.WithDaprHTTPPort(strconv.Itoa(currentHTTPPort)),
				embedded.WithComponentsPath("./components/basictest"),
			)...,
		)).
		Step("Run basic test with master key", basicTest("statestore-basic")).
		Run()

	flow.New(t, "Test TTL").
		// Run the Dapr sidecar with AWS DynamoDB storage.
		Step(sidecar.Run(sidecarNamePrefix,
			append(componentRuntimeOptions(),
				embedded.WithoutApp(),
				embedded.WithDaprGRPCPort(strconv.Itoa(currentGrpcPort)),
				embedded.WithDaprHTTPPort(strconv.Itoa(currentHTTPPort)),
				embedded.WithComponentsPath("./components/basictest"),
			)...,
		)).
		Step("Run basic test with default key", ttlTest("statestore-basic")).
		Run()

	flow.New(t, "Test Partition Key").
		// Run the Dapr sidecar with AWS DynamoDB storage.
		Step(sidecar.Run(sidecarNamePrefix,
			append(componentRuntimeOptions(),
				embedded.WithoutApp(),
				embedded.WithDaprGRPCPort(strconv.Itoa(currentGrpcPort)),
				embedded.WithDaprHTTPPort(strconv.Itoa(currentHTTPPort)),
				embedded.WithComponentsPath("./components/partition_key"),
			)...,
		)).
		Step("Run basic test with partition key", basicTest("statestore-partition-key")).
		Run()

	flow.New(t, "Test Tx operations").
		// Run the Dapr sidecar with AWS DynamoDB storage.
		Step(sidecar.Run(sidecarNamePrefix,
			append(componentRuntimeOptions(),
				embedded.WithoutApp(),
				embedded.WithDaprGRPCPort(strconv.Itoa(currentGrpcPort)),
				embedded.WithDaprHTTPPort(strconv.Itoa(currentHTTPPort)),
				embedded.WithComponentsPath("./components/basictest"),
			)...,
		)).
		Step("Run transaction test", transactionsTest("statestore-basic")).
		Run()
}

func componentRuntimeOptions() []embedded.Option {
	log := logger.NewLogger("dapr.components")

	stateRegistry := state_loader.NewRegistry()
	stateRegistry.Logger = log
	stateRegistry.RegisterComponent(dynamodb.NewDynamoDBStateStore, "aws.dynamodb")

	secretstoreRegistry := secretstores_loader.NewRegistry()
	secretstoreRegistry.Logger = log
	secretstoreRegistry.RegisterComponent(secretstore_env.NewEnvSecretStore, "local.env")

	return []embedded.Option{
		embedded.WithStates(stateRegistry),
		embedded.WithSecretStores(secretstoreRegistry),
	}
}
