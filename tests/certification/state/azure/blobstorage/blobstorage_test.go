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

package blobstorage_test

import (
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"testing"

	blob "github.com/dapr/components-contrib/state/azure/blobstorage"
	"github.com/dapr/components-contrib/tests/certification/embedded"
	"github.com/dapr/components-contrib/tests/certification/flow"
	"github.com/dapr/go-sdk/client"
	"github.com/dapr/kit/logger"

	secretstore_env "github.com/dapr/components-contrib/secretstores/local/env"
	secretstores_loader "github.com/dapr/dapr/pkg/components/secretstores"

	"github.com/dapr/components-contrib/tests/certification/flow/sidecar"
	state_loader "github.com/dapr/dapr/pkg/components/state"
	dapr_testing "github.com/dapr/dapr/pkg/testing"
	"github.com/stretchr/testify/assert"
)

const (
	sidecarNamePrefix       = "blob-sidecar-"
	certificationTestPrefix = "stable-certification-"
)

func TestAzureBlobStorage(t *testing.T) {
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

			// save state, default options: strong, last-write
			err = client.SaveState(ctx, statestore, certificationTestPrefix+"key1", []byte("certificationdata"), nil)
			assert.NoError(t, err)

			// get state
			item, err := client.GetState(ctx, statestore, certificationTestPrefix+"key1", nil)
			assert.NoError(t, err)
			assert.Equal(t, "certificationdata", string(item.Value))

			// delete state
			err = client.DeleteState(ctx, statestore, certificationTestPrefix+"key1", nil)
			assert.NoError(t, err)

			return nil
		}
	}

	deleteContainer := func(ctx flow.Context) error {
		output, err := exec.Command("az", "storage", "container", "delete", "--account-name", os.Getenv("AzureBlobStorageAccount"), "--account-key", os.Getenv("AzureBlobStorageAccessKey"), "--name", "nonexistingblob").CombinedOutput()
		assert.Nil(t, err, "Error while deleting the container.:\n%s", string(output))
		return nil
	}

	flow.New(t, "Test basic operations, save/get/delete using existing container").
		// Run the Dapr sidecar with azure blob storage.
		Step(sidecar.Run(sidecarNamePrefix,
			append(componentRuntimeOptions(),
				embedded.WithoutApp(),
				embedded.WithDaprGRPCPort(strconv.Itoa(currentGrpcPort)),
				embedded.WithDaprHTTPPort(strconv.Itoa(currentHTTPPort)),
				embedded.WithComponentsPath("./components/basictest"),
			)...,
		)).
		Step("Run basic test with existing container", basicTest("statestore-basic")).
		Run()

	flow.New(t, "Test basic operations, save/get/delete with new container").
		// Run the Dapr sidecar with azure blob storage.
		Step(sidecar.Run(sidecarNamePrefix,
			append(componentRuntimeOptions(),
				embedded.WithoutApp(),
				embedded.WithDaprGRPCPort(strconv.Itoa(currentGrpcPort)),
				embedded.WithDaprHTTPPort(strconv.Itoa(currentHTTPPort)),
				embedded.WithComponentsPath("./components/nonexistingcontainertest"),
			)...,
		)).
		Step("Run basic test with new table", basicTest("statestore-newcontainer")).
		Step("Delete the New Table", deleteContainer).
		Run()

	flow.New(t, "Test for authentication using Azure Auth layer").
		// Run the Dapr sidecar with azure blob storage.
		Step(sidecar.Run(sidecarNamePrefix,
			append(componentRuntimeOptions(),
				embedded.WithoutApp(),
				embedded.WithDaprGRPCPort(strconv.Itoa(currentGrpcPort)),
				embedded.WithDaprHTTPPort(strconv.Itoa(currentHTTPPort)),
				embedded.WithComponentsPath("./components/aadtest"),
			)...,
		)).
		Step("Run AAD test", basicTest("statestore-aad")).
		Run()
}

func componentRuntimeOptions() []embedded.Option {
	log := logger.NewLogger("dapr.components")

	stateRegistry := state_loader.NewRegistry()
	stateRegistry.Logger = log
	stateRegistry.RegisterComponent(blob.NewAzureBlobStorageStore, "azure.blobstorage")

	secretstoreRegistry := secretstores_loader.NewRegistry()
	secretstoreRegistry.Logger = log
	secretstoreRegistry.RegisterComponent(secretstore_env.NewEnvSecretStore, "local.env")

	return []embedded.Option{
		embedded.WithStates(stateRegistry),
		embedded.WithSecretStores(secretstoreRegistry),
	}
}
