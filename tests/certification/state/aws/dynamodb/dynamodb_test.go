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

package dynamoDBStorage_test

import (
	"fmt"
	"testing"

	dynamodb "github.com/dapr/components-contrib/state/aws/dynamodb"
	"github.com/dapr/components-contrib/tests/certification/embedded"
	"github.com/dapr/components-contrib/tests/certification/flow"
	"github.com/dapr/go-sdk/client"

	"github.com/dapr/components-contrib/tests/certification/flow/sidecar"
	state_loader "github.com/dapr/dapr/pkg/components/state"
	"github.com/dapr/dapr/pkg/runtime"
	dapr_testing "github.com/dapr/dapr/pkg/testing"
	"github.com/dapr/kit/logger"
	"github.com/stretchr/testify/assert"
)

const (
	sidecarNamePrefix       = "dynamodb-sidecar-"
	certificationTestPrefix = ""
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

	flow.New(t, "Test basic operations").
		// Run the Dapr sidecar with AWS DynamoDB storage.
		Step(sidecar.Run(sidecarNamePrefix,
			embedded.WithoutApp(),
			embedded.WithDaprGRPCPort(currentGrpcPort),
			embedded.WithDaprHTTPPort(currentHTTPPort),
			embedded.WithComponentsPath("./components/basictest"),
			componentRuntimeOptions())).
		Step("Run basic test with master key", basicTest("statestore-basic")).
		Run()
}

func componentRuntimeOptions() []runtime.Option {
	log := logger.NewLogger("dapr.components")

	stateRegistry := state_loader.NewRegistry()
	stateRegistry.Logger = log
	stateRegistry.RegisterComponent(dynamodb.NewDynamoDBStateStore, "aws.dynamodb")

	return []runtime.Option{
		runtime.WithStates(stateRegistry),
	}
}
