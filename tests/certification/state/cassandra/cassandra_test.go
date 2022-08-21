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

package cassandra_test

import (
	"fmt"
	"github.com/dapr/components-contrib/state"
	state_cassandra "github.com/dapr/components-contrib/state/cassandra"
	"github.com/dapr/components-contrib/tests/certification/embedded"
	"github.com/dapr/components-contrib/tests/certification/flow"
	"github.com/dapr/components-contrib/tests/certification/flow/dockercompose"
	"strconv"
	"testing"
	"time"
	//"github.com/dapr/components-contrib/tests/certification/flow/network"
	"github.com/dapr/components-contrib/tests/certification/flow/sidecar"
	state_loader "github.com/dapr/dapr/pkg/components/state"
	"github.com/dapr/dapr/pkg/runtime"
	dapr_testing "github.com/dapr/dapr/pkg/testing"
	goclient "github.com/dapr/go-sdk/client"
	"github.com/dapr/kit/logger"
	"github.com/stretchr/testify/assert"
)

const (
	sidecarNamePrefix        = "cassandra-sidecar-"
	dockerComposeYAMLCLUSTER = "docker-compose-cluster.yml"
	dockerComposeYAML        = "docker-compose-single.yml"

	stateStoreName    = "statestore"
	stateStoreCluster = "statestorecluster"

	certificationTestPrefix = "stable-certification-"
	stateStoreNoConfigError = "error saving state: rpc error: code = FailedPrecondition desc = state store is not configured"
)

func TestCassandra(t *testing.T) {
	log := logger.NewLogger("dapr.components")
	stateStore := state_cassandra.NewCassandraStateStore(log)
	ports, err := dapr_testing.GetFreePorts(2)
	assert.NoError(t, err)

	currentGrpcPort := ports[0]
	currentHTTPPort := ports[1]

	basicTest := func(ctx flow.Context) error {
		client, err := goclient.NewClientWithPort(fmt.Sprint(currentGrpcPort))
		if err != nil {
			panic(err)
		}
		defer client.Close()

		err = client.SaveState(ctx, stateStoreName, certificationTestPrefix+"key1", []byte("cassandraCert"), nil)
		assert.NoError(t, err)

		// get state
		item, err := client.GetState(ctx, stateStoreName, certificationTestPrefix+"key1", nil)
		assert.NoError(t, err)
		assert.Equal(t, "cassandraCert", string(item.Value))

		errUpdate := client.SaveState(ctx, stateStoreName, certificationTestPrefix+"key1", []byte("cassandraCertUpdate"), nil)
		assert.NoError(t, errUpdate)
		item, errUpdatedGet := client.GetState(ctx, stateStoreName, certificationTestPrefix+"key1", nil)
		assert.NoError(t, errUpdatedGet)
		assert.Equal(t, "cassandraCertUpdate", string(item.Value))

		// delete state
		err = client.DeleteState(ctx, stateStoreName, certificationTestPrefix+"key1", nil)
		assert.NoError(t, err)

		return nil
	}

	// Time-To-Live Test
	timeToLiveTest := func(ctx flow.Context) error {
		client, err := goclient.NewClientWithPort(fmt.Sprint(currentGrpcPort))
		if err != nil {
			panic(err)
		}
		defer client.Close()

		ttlInSecondsWrongValue := "mock value"
		mapOptionsWrongValue :=
			map[string]string{
				"ttlInSeconds": ttlInSecondsWrongValue,
			}

		ttlInSecondsNonExpiring := 0
		mapOptionsNonExpiring :=
			map[string]string{
				"ttlInSeconds": strconv.Itoa(ttlInSecondsNonExpiring),
			}

		ttlInSeconds := 10
		mapOptions :=
			map[string]string{
				"ttlInSeconds": strconv.Itoa(ttlInSeconds),
			}

		err1 := client.SaveState(ctx, stateStoreName, certificationTestPrefix+"ttl1", []byte("cassandraCert"), mapOptionsWrongValue)
		assert.Error(t, err1)
		err2 := client.SaveState(ctx, stateStoreName, certificationTestPrefix+"ttl2", []byte("cassandraCert2"), mapOptionsNonExpiring)
		assert.NoError(t, err2)
		err3 := client.SaveState(ctx, stateStoreName, certificationTestPrefix+"ttl3", []byte("cassandraCert3"), mapOptions)
		assert.NoError(t, err3)

		// get state
		item, err := client.GetState(ctx, stateStoreName, certificationTestPrefix+"ttl3", nil)
		assert.NoError(t, err)
		assert.Equal(t, "cassandraCert3", string(item.Value))
		time.Sleep(10 * time.Second)
		itemAgain, errAgain := client.GetState(ctx, stateStoreName, certificationTestPrefix+"ttl3", nil)
		assert.NoError(t, errAgain)
		assert.Nil(t, nil, itemAgain)

		return nil
	}

	testGetAfterCassandraRestart := func(ctx flow.Context) error {
		client, err := goclient.NewClientWithPort(fmt.Sprint(currentGrpcPort))
		if err != nil {
			panic(err)
		}
		defer client.Close()

		// get state
		item, err := client.GetState(ctx, stateStoreName, certificationTestPrefix+"ttl2", nil)
		assert.NoError(t, err)
		assert.Equal(t, "cassandraCert2", string(item.Value))

		return nil
	}

	flow.New(t, "Connecting cassandra And Ports and Verifying TTL and network tests and table creation").
		Step(dockercompose.Run("cassandra", dockerComposeYAML)).
		Step("wait", flow.Sleep(80*time.Second)).
		Step(sidecar.Run(sidecarNamePrefix+"dockerDefault",
			embedded.WithoutApp(),
			embedded.WithDaprGRPCPort(currentGrpcPort),
			embedded.WithDaprHTTPPort(currentHTTPPort),
			embedded.WithComponentsPath("components/docker/default"),
			runtime.WithStates(
				state_loader.New("cassandra", func() state.Store {
					return stateStore
				}),
			))).
		Step("wait", flow.Sleep(30*time.Second)).
		Step("Run TTL related test", timeToLiveTest).
		//Step("interrupt network",
		//	network.InterruptNetwork(10*time.Second, nil, nil, "9042:9042")).
		// Component should recover at this point.
		//Step("wait", flow.Sleep(30*time.Second)).
		Step("Run basic test again to verify reconnection occurred", basicTest).
		Step("stop cassandra server", dockercompose.Stop("cassandra", dockerComposeYAML, "cassandra")).
		Step("start cassandra server", dockercompose.Start("cassandra", dockerComposeYAML, "cassandra")).
		Step("wait", flow.Sleep(60*time.Second)).
		Step("Get Values Saved Earlier And Not Expired, after Cassandra restart", testGetAfterCassandraRestart).
		Step("Run basic test", basicTest).
		Step("reset dapr", sidecar.Stop(sidecarNamePrefix+"dockerDefault")).
		Step("wait", flow.Sleep(10*time.Second)).
		Step(sidecar.Run(sidecarNamePrefix+"dockerDefault",
			embedded.WithoutApp(),
			embedded.WithDaprGRPCPort(currentGrpcPort),
			embedded.WithDaprHTTPPort(currentHTTPPort),
			embedded.WithComponentsPath("components/docker/createTables"),
			runtime.WithStates(
				state_loader.New("cassandra", func() state.Store {
					return stateStore
				}),
			))).
		Step("wait", flow.Sleep(30*time.Second)).
		Step("Run basic test", basicTest).
		Run()

}

func TestCluster(t *testing.T) {
	log := logger.NewLogger("dapr.components")
	stateStore := state_cassandra.NewCassandraStateStore(log)
	ports, err := dapr_testing.GetFreePorts(2)
	assert.NoError(t, err)

	currentGrpcPort := ports[0]
	currentHTTPPort := ports[1]

	basicTest := func(ctx flow.Context) error {
		client, err := goclient.NewClientWithPort(fmt.Sprint(currentGrpcPort))
		if err != nil {
			panic(err)
		}
		defer client.Close()

		err = client.SaveState(ctx, stateStoreCluster, certificationTestPrefix+"key1", []byte("cassandraCert"), nil)
		assert.NoError(t, err)

		// get state
		item, err := client.GetState(ctx, stateStoreCluster, certificationTestPrefix+"key1", nil)
		assert.NoError(t, err)
		assert.Equal(t, "cassandraCert", string(item.Value))

		errUpdate := client.SaveState(ctx, stateStoreCluster, certificationTestPrefix+"key1", []byte("cassandraCertUpdate"), nil)
		assert.NoError(t, errUpdate)
		item, errUpdatedGet := client.GetState(ctx, stateStoreCluster, certificationTestPrefix+"key1", nil)
		assert.NoError(t, errUpdatedGet)
		assert.Equal(t, "cassandraCertUpdate", string(item.Value))

		// delete state
		err = client.DeleteState(ctx, stateStoreCluster, certificationTestPrefix+"key1", nil)
		assert.NoError(t, err)

		return nil
	}
	failTest := func(ctx flow.Context) error {
		client, err := goclient.NewClientWithPort(fmt.Sprint(currentGrpcPort))
		if err != nil {
			panic(err)
		}
		defer client.Close()

		err = client.SaveState(ctx, stateStoreCluster, certificationTestPrefix+"key1", []byte("cassandraCert"), nil)
		assert.NoError(t, err)

		// get state
		_, err = client.GetStateWithConsistency(ctx, stateStoreCluster, certificationTestPrefix+"key1", nil, goclient.StateConsistencyUndefined)
		assert.Error(t, err)

		return nil
	}

	flow.New(t, "Connecting cassandra And Verifying port/tables/keyspaces/consistency").
		Step(dockercompose.Run("cassandra", dockerComposeYAMLCLUSTER)).
		Step("wait", flow.Sleep(80*time.Second)).
		Step(sidecar.Run(sidecarNamePrefix+"dockerDefault",
			embedded.WithoutApp(),
			embedded.WithDaprGRPCPort(currentGrpcPort),
			embedded.WithDaprHTTPPort(currentHTTPPort),
			embedded.WithComponentsPath("components/docker/cluster"),
			runtime.WithStates(
				state_loader.New("cassandra", func() state.Store {
					return stateStore
				}),
			))).
		Step("wait", flow.Sleep(30*time.Second)).
		Step("Run basic test", basicTest).
		Step("reset dapr", sidecar.Stop(sidecarNamePrefix+"dockerDefault")).
		Step("wait", flow.Sleep(10*time.Second)).
		Step(sidecar.Run(sidecarNamePrefix+"dockerDefault",
			embedded.WithoutApp(),
			embedded.WithDaprGRPCPort(currentGrpcPort),
			embedded.WithDaprHTTPPort(currentHTTPPort),
			embedded.WithComponentsPath("components/docker/clusterFail"),
			runtime.WithStates(
				state_loader.New("cassandra", func() state.Store {
					return stateStore
				}),
			))).
		Step("Run fail test", failTest).
		Run()

}
