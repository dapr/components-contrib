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

package gcp_firestore_test

import (
	"fmt"
	"strconv"
	"testing"

	firestore "github.com/dapr/components-contrib/state/gcp/firestore"
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
	sidecarNamePrefix = "firestore-sidecar-"
	key               = "key"
)

func TestGCPFirestoreStorage(t *testing.T) {
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

			// delete state
			err = client.DeleteState(ctx, statestore, stateKey, nil)
			assert.NoError(t, err)

			return nil
		}
	}

	flow.New(t, "Test basic operations").
		// Run the Dapr sidecar with GCP Firestore storage.
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

	flow.New(t, "Test entity_kind").
		Step(sidecar.Run(sidecarNamePrefix,
			append(componentRuntimeOptions(),
				embedded.WithoutApp(),
				embedded.WithDaprGRPCPort(strconv.Itoa(currentGrpcPort)),
				embedded.WithDaprHTTPPort(strconv.Itoa(currentHTTPPort)),
				embedded.WithComponentsPath("./components/entity_kind"),
			)...,
		)).
		Step("Run basic test with master key", basicTest("statestore-basic")).
		Run()

	flow.New(t, "Test NoIndex").
		Step(sidecar.Run(sidecarNamePrefix,
			append(componentRuntimeOptions(),
				embedded.WithoutApp(),
				embedded.WithDaprGRPCPort(strconv.Itoa(currentGrpcPort)),
				embedded.WithDaprHTTPPort(strconv.Itoa(currentHTTPPort)),
				embedded.WithComponentsPath("./components/noindex"),
			)...,
		)).
		Step("Run basic test with master key", basicTest("statestore-basic")).
		Run()
}

func componentRuntimeOptions() []embedded.Option {
	log := logger.NewLogger("dapr.components")

	stateRegistry := state_loader.NewRegistry()
	stateRegistry.Logger = log
	stateRegistry.RegisterComponent(firestore.NewFirestoreStateStore, "gcp.firestore")

	secretstoreRegistry := secretstores_loader.NewRegistry()
	secretstoreRegistry.Logger = log
	secretstoreRegistry.RegisterComponent(secretstore_env.NewEnvSecretStore, "local.env")

	return []embedded.Option{
		embedded.WithStates(stateRegistry),
		embedded.WithSecretStores(secretstoreRegistry),
	}
}
