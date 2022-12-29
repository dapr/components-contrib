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
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/dapr/components-contrib/state"
	state_cockroach "github.com/dapr/components-contrib/state/cockroachdb"
	"github.com/dapr/components-contrib/tests/certification/embedded"
	"github.com/dapr/components-contrib/tests/certification/flow"
	"github.com/dapr/components-contrib/tests/certification/flow/dockercompose"
	"github.com/dapr/components-contrib/tests/certification/flow/sidecar"
	state_loader "github.com/dapr/dapr/pkg/components/state"
	"github.com/dapr/dapr/pkg/runtime"
	dapr_testing "github.com/dapr/dapr/pkg/testing"
	goclient "github.com/dapr/go-sdk/client"
	"github.com/dapr/kit/logger"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
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

	stateStore := state_cockroach.New(log).(*state_cockroach.CockroachDB)
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

	basicTest := func(ctx flow.Context) error {
		client, err := goclient.NewClientWithPort(fmt.Sprint(currentGrpcPort))
		if err != nil {
			panic(err)
		}
		defer client.Close()

		err = stateStore.Ping()
		assert.Equal(t, nil, err)

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

		err = stateStore.Ping()
		assert.Equal(t, nil, err)

		resp, err := stateStore.Get(context.Background(), &state.GetRequest{
			Key: keyOneString,
		})
		assert.NoError(t, err)
		assert.Equal(t, "2", *resp.ETag) // 2 is returned since the previous etag value of "1" was incremented by 1 when the update occurred
		assert.Equal(t, "\"Overwrite Success\"", string(resp.Data))

		return nil
	}

	//ETag test
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
		assert.Equal(t, fmt.Errorf("no item was updated"), err)

		resp, err := stateStore.Get(context.Background(), &state.GetRequest{
			Key: keyOneString,
		})
		assert.NoError(t, err)
		assert.Equal(t, etag1, *resp.ETag)           //1 is returned since the default when the new data is written is a value of 1
		assert.Equal(t, "\"v1\"", string(resp.Data)) //v1 is returned since it was the only item successfully inserted with the key of keyOneString

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
		assert.Equal(t, "2", *resp.ETag) //2 is returned since the previous etag value of "1" was incremented by 1 when the update occurred
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
				{
					Operation: state.Upsert,
					Request: state.SetRequest{
						Key:      "reqKey1",
						Value:    "reqVal1",
						Metadata: map[string]string{},
					},
				},
				{
					Operation: state.Upsert,
					Request: state.SetRequest{
						Key:      "reqKey2",
						Value:    "reqVal2",
						Metadata: map[string]string{},
					},
				},
				{
					Operation: state.Upsert,
					Request: state.SetRequest{
						Key:   "reqKey3",
						Value: "reqVal3",
					},
				},
				{
					Operation: state.Upsert,
					Request: state.SetRequest{
						Key:      "reqKey1",
						Value:    "reqVal101",
						Metadata: map[string]string{},
					},
				},
				{
					Operation: state.Upsert,
					Request: state.SetRequest{
						Key:      "reqKey3",
						Value:    "reqVal103",
						Metadata: map[string]string{},
					},
				},
				{
					Operation: state.Delete,
					Request: state.DeleteRequest{
						Key:      certificationTestPrefix + "key1",
						Metadata: map[string]string{},
					},
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
		return nil
	}

	flow.New(t, "Connecting cockroachdb And Verifying majority of the tests here").
		Step(dockercompose.Run("cockroachdb", dockerComposeYAML)).
		Step("Waiting for cockroachdb readiness", flow.Sleep(30*time.Second)).
		Step(sidecar.Run(sidecarNamePrefix+"dockerDefault",
			embedded.WithoutApp(),
			embedded.WithDaprGRPCPort(currentGrpcPort),
			embedded.WithDaprHTTPPort(currentHTTPPort),
			embedded.WithComponentsPath("components/standard"),
			runtime.WithStates(stateRegistry),
		)).
		Step("Run basic test", basicTest).
		Step("Run eTag test", eTagTest).
		Step("Run transactions test", transactionsTest).
		Step("Stop cockroachdb server", dockercompose.Stop("cockroachdb", dockerComposeYAML, "cockroachdb")).
		Step("Sleep after dockercompose stop", flow.Sleep(10*time.Second)).
		Step("Start cockroachdb server", dockercompose.Start("cockroachdb", dockerComposeYAML, "cockroachdb")).
		Step("wait for component to start", flow.Sleep(10*time.Second)).
		Step("Get Values Saved Earlier And Not Expired, after cockroachdb restart", testGetAfterCockroachdbRestart).
		Run()
}
