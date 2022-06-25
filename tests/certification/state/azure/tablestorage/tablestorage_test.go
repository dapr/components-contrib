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

package tablestorage_test

import (
	"fmt"
	"os/exec"
	"testing"

	state "github.com/dapr/components-contrib/state"
	table "github.com/dapr/components-contrib/state/azure/tablestorage"
	"github.com/dapr/components-contrib/tests/certification/embedded"
	"github.com/dapr/components-contrib/tests/certification/flow"
	"github.com/dapr/go-sdk/client"

	"github.com/dapr/components-contrib/secretstores"
	secretstore_env "github.com/dapr/components-contrib/secretstores/local/env"
	secretstores_loader "github.com/dapr/dapr/pkg/components/secretstores"

	"github.com/dapr/components-contrib/tests/certification/flow/sidecar"
	state_loader "github.com/dapr/dapr/pkg/components/state"
	"github.com/dapr/dapr/pkg/runtime"
	dapr_testing "github.com/dapr/dapr/pkg/testing"
	"github.com/dapr/kit/logger"
	"github.com/stretchr/testify/assert"
)

const (
	sidecarNamePrefix       = "table-sidecar-"
	certificationTestPrefix = "stable-certification-"
)

func TestAzureTableStorage(t *testing.T) {
	log := logger.NewLogger("dapr.components")
	ports, err := dapr_testing.GetFreePorts(2)
	assert.NoError(t, err)

	currentGrpcPort := ports[0]
	currentHTTPPort := ports[1]

	basicTest := func(ctx flow.Context) error {
		client, err := client.NewClientWithPort(fmt.Sprint(currentGrpcPort))
		if err != nil {
			panic(err)
		}
		defer client.Close()

		// save state, default options: strong, last-write
		err = client.SaveState(ctx, "statestore-basic", certificationTestPrefix+"key1", []byte("certificationdata"), nil)
		assert.NoError(t, err)

		// get state
		item, err := client.GetState(ctx, "statestore-basic", certificationTestPrefix+"key1", nil)
		assert.NoError(t, err)
		assert.Equal(t, "certificationdata", string(item.Value))

		// delete state
		err = client.DeleteState(ctx, "statestore-basic", certificationTestPrefix+"key1", nil)
		assert.NoError(t, err)

		return nil
	}

	NonExistingTableTest := func(ctx flow.Context) error {
		client, err := client.NewClientWithPort(fmt.Sprint(currentGrpcPort))
		if err != nil {
			panic(err)
		}
		defer client.Close()

		// save state, default options: strong, last-write
		err = client.SaveState(ctx, "statestore-newtable", certificationTestPrefix+"key1", []byte("certificationdata"), nil)
		assert.NoError(t, err)

		// get state
		item, err := client.GetState(ctx, "statestore-newtable", certificationTestPrefix+"key1", nil)
		assert.NoError(t, err)
		assert.Equal(t, "certificationdata", string(item.Value))

		// delete state
		err = client.DeleteState(ctx, "statestore-newtable", certificationTestPrefix+"key1", nil)
		assert.NoError(t, err)

		exec.Command("/bin/bash", "az storage table delete --account-name dapr3ctstorage --name NewTable")

		return nil
	}

	authTest := func(ctx flow.Context) error {
		client, err := client.NewClientWithPort(fmt.Sprint(currentGrpcPort))
		if err != nil {
			panic(err)
		}
		defer client.Close()

		// save state with go sdk with existing table
		err = client.SaveState(ctx, "statestore-key", certificationTestPrefix+"key1", []byte("certificationdata"), nil)
		assert.NoError(t, err)

		// Get data using AAD
		item, err := client.GetState(ctx, "statestore-aad", certificationTestPrefix+"key1", nil)
		assert.NoError(t, err)
		assert.Equal(t, "certificationdata", string(item.Value))

		return nil
	}

	flow.New(t, "Test basic operations, save/get/delete using existing table").
		// Run the Dapr sidecar with azure table storage.
		Step(sidecar.Run(sidecarNamePrefix,
			embedded.WithoutApp(),
			embedded.WithDaprGRPCPort(currentGrpcPort),
			embedded.WithDaprHTTPPort(currentHTTPPort),
			embedded.WithComponentsPath("./components/basictest"),
			runtime.WithSecretStores(
				secretstores_loader.New("local.env", func() secretstores.SecretStore {
					return secretstore_env.NewEnvSecretStore(log)
				}),
			),
			runtime.WithStates(
				state_loader.New("azure.tablestorage", func() state.Store {
					return table.NewAzureTablesStateStore(log)
				}),
			))).
		Step("Run basic test with existing table", basicTest).
		Run()

	flow.New(t, "Test basic operations, save/get/delete with new table").
		// Run the Dapr sidecar with azure table storage.
		Step(sidecar.Run(sidecarNamePrefix,
			embedded.WithoutApp(),
			embedded.WithDaprGRPCPort(currentGrpcPort),
			embedded.WithDaprHTTPPort(currentHTTPPort),
			embedded.WithComponentsPath("./components/nonexistingtabletest"),
			runtime.WithSecretStores(
				secretstores_loader.New("local.env", func() secretstores.SecretStore {
					return secretstore_env.NewEnvSecretStore(log)
				}),
			),
			runtime.WithStates(
				state_loader.New("azure.tablestorage", func() state.Store {
					return table.NewAzureTablesStateStore(log)
				}),
			))).
		Step("Run basic test with new table", NonExistingTableTest).
		Run()

	flow.New(t, "Test for authentication using Azure Auth layer").
		// Run the Dapr sidecar with azure table storage.
		Step(sidecar.Run(sidecarNamePrefix,
			embedded.WithoutApp(),
			embedded.WithDaprGRPCPort(currentGrpcPort),
			embedded.WithDaprHTTPPort(currentHTTPPort),
			embedded.WithComponentsPath("./components/aadtest"),
			runtime.WithSecretStores(
				secretstores_loader.New("local.env", func() secretstores.SecretStore {
					return secretstore_env.NewEnvSecretStore(log)
				}),
			),
			runtime.WithStates(
				state_loader.New("azure.tablestorage", func() state.Store {
					return table.NewAzureTablesStateStore(log)
				}),
			))).
		Step("Run AAD test", authTest).
		Run()
}
