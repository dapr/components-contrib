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

package main

import (
	"fmt"
	"testing"
	"time"

	"github.com/dapr/components-contrib/tests/certification/embedded"
	"github.com/dapr/components-contrib/tests/certification/flow"
	"github.com/dapr/components-contrib/tests/certification/flow/dockercompose"
	"github.com/dapr/components-contrib/tests/certification/flow/sidecar"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/state"
	state_postgres "github.com/dapr/components-contrib/state/postgresql"
	state_loader "github.com/dapr/dapr/pkg/components/state"
	"github.com/dapr/dapr/pkg/runtime"
	dapr_testing "github.com/dapr/dapr/pkg/testing"
	"github.com/dapr/kit/logger"

	"github.com/dapr/go-sdk/client"
)

const (
	sidecarNamePrefix       = "postgresql-sidecar-"
	dockerComposeYAML       = "docker-compose.yml"
	stateStoreName          = "statestore"
	certificationTestPrefix = "stable-certification-"
)

func TestPostgreSQL(t *testing.T) {
	log := logger.NewLogger("dapr.components")

	stateStore := state_postgres.NewPostgreSQLStateStore(log).(*state_postgres.PostgreSQL)
	ports, err := dapr_testing.GetFreePorts(2)
	assert.NoError(t, err)

	stateRegistry := state_loader.NewRegistry()
	stateRegistry.Logger = log
	stateRegistry.RegisterComponent(func(l logger.Logger) state.Store {
		return stateStore
	}, "postgresql")

	currentGrpcPort := ports[0]

	basicTest := func(ctx flow.Context) error {
		client, err := client.NewClientWithPort(fmt.Sprint(currentGrpcPort))
		if err != nil {
			panic(err)
		}
		defer client.Close()

		// save state
		err = client.SaveState(ctx, stateStoreName, certificationTestPrefix+"key1", []byte("postgresqlcert1"), nil)
		require.NoError(t, err)

		// get state
		item, err := client.GetState(ctx, stateStoreName, certificationTestPrefix+"key1", nil)
		require.NoError(t, err)
		assert.Equal(t, "postgresqlcert1", string(item.Value))

		// update state
		errUpdate := client.SaveState(ctx, stateStoreName, certificationTestPrefix+"key1", []byte("postgresqlcert2"), nil)
		require.NoError(t, errUpdate)
		item, errUpdatedGet := client.GetState(ctx, stateStoreName, certificationTestPrefix+"key1", nil)
		require.NoError(t, errUpdatedGet)
		assert.Equal(t, "postgresqlcert2", string(item.Value))

		// delete state
		err = client.DeleteState(ctx, stateStoreName, certificationTestPrefix+"key1", nil)
		require.NoError(t, err)

		return nil
	}

	eTagTest := func(ctx flow.Context) error {
		etag900invalid := "900invalid"

		err1 := stateStore.Set(&state.SetRequest{
			Key:   "k",
			Value: "v1",
		})
		require.NoError(t, err1)
		resp1, err2 := stateStore.Get(&state.GetRequest{
			Key: "k",
		})

		require.NoError(t, err2)
		err3 := stateStore.Set(&state.SetRequest{
			Key:   "k",
			Value: "v2",
			ETag:  resp1.ETag,
		})
		require.NoError(t, err3)

		resp11, err12 := stateStore.Get(&state.GetRequest{
			Key: "k",
		})
		expectedEtag := *resp11.ETag
		require.NoError(t, err12)

		err4 := stateStore.Set(&state.SetRequest{
			Key:   "k",
			Value: "v3",
			ETag:  &etag900invalid,
		})
		require.Error(t, err4)

		resp, err := stateStore.Get(&state.GetRequest{
			Key: "k",
		})

		require.NoError(t, err)
		assert.Equal(t, expectedEtag, *resp.ETag)
		assert.Equal(t, "\"v2\"", string(resp.Data))

		return nil
	}

	transactionsTest := func(ctx flow.Context) error {
		err := stateStore.Multi(&state.TransactionalStateRequest{
			Operations: []state.TransactionalStateOperation{
				{
					Operation: state.Upsert,
					Request: state.SetRequest{
						Key:   "reqKey1",
						Value: "reqVal1",
						Metadata: map[string]string{
							"ttlInSeconds": "-1",
						},
					},
				},
				{
					Operation: state.Upsert,
					Request: state.SetRequest{
						Key:   "reqKey2",
						Value: "reqVal2",
						Metadata: map[string]string{
							"ttlInSeconds": "222",
						},
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
						Key:   "reqKey1",
						Value: "reqVal101",
						Metadata: map[string]string{
							"ttlInSeconds": "50",
						},
					},
				},
				{
					Operation: state.Upsert,
					Request: state.SetRequest{
						Key:   "reqKey3",
						Value: "reqVal103",
						Metadata: map[string]string{
							"ttlInSeconds": "50",
						},
					},
				},
			},
		})
		assert.Equal(t, nil, err)
		resp1, err := stateStore.Get(&state.GetRequest{
			Key: "reqKey1",
		})
		assert.Equal(t, "\"reqVal101\"", string(resp1.Data))

		resp3, err := stateStore.Get(&state.GetRequest{
			Key: "reqKey3",
		})
		assert.Equal(t, "\"reqVal103\"", string(resp3.Data))
		return nil
	}

	testGetAfterPostgresRestart := func(ctx flow.Context) error {
		client, err := client.NewClientWithPort(fmt.Sprint(currentGrpcPort))
		if err != nil {
			panic(err)
		}
		defer client.Close()

		// save state
		_, err = client.GetState(ctx, stateStoreName, certificationTestPrefix+"key1", nil)
		assert.NoError(t, err)

		return nil
	}

	// checks the state store component is not vulnerable to SQL injection
	verifySQLInjectionTest := func(ctx flow.Context) error {
		client, err := client.NewClientWithPort(fmt.Sprint(currentGrpcPort))
		if err != nil {
			panic(err)
		}
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

		return nil
	}

	flow.New(t, "Run tests").
		Step(dockercompose.Run("db", dockerComposeYAML)).
		Step("wait for component to start", flow.Sleep(10*time.Second)).
		Step(sidecar.Run(sidecarNamePrefix+"dockerDefault",
			embedded.WithoutApp(),
			embedded.WithDaprGRPCPort(currentGrpcPort),
			embedded.WithComponentsPath("components/docker/default"),
			runtime.WithStates(stateRegistry),
		)).
		Step("Run CRUD test", basicTest).
		Step("Run eTag test", eTagTest).
		Step("Run transactions test", transactionsTest).
		Step("Run SQL injection test", verifySQLInjectionTest).
		Step("stop postgresql", dockercompose.Stop("db", dockerComposeYAML, "db")).
		Step("wait for component to stop", flow.Sleep(10*time.Second)).
		Step("start postgresql", dockercompose.Start("db", dockerComposeYAML, "db")).
		Step("wait for component to start", flow.Sleep(10*time.Second)).
		Step("Run connection test", testGetAfterPostgresRestart).
		Run()
}
