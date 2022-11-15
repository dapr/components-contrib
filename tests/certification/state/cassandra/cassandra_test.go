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

package cassandra_test

import (
	"fmt"
	"github.com/dapr/components-contrib/state"
	state_cassandra "github.com/dapr/components-contrib/state/cassandra"
	"github.com/dapr/components-contrib/tests/certification/embedded"
	"github.com/dapr/components-contrib/tests/certification/flow"
	"github.com/dapr/components-contrib/tests/certification/flow/dockercompose"
	"github.com/dapr/components-contrib/tests/certification/flow/network"
	"github.com/dapr/components-contrib/tests/certification/flow/sidecar"
	state_loader "github.com/dapr/dapr/pkg/components/state"
	"github.com/dapr/dapr/pkg/runtime"
	dapr_testing "github.com/dapr/dapr/pkg/testing"
	goclient "github.com/dapr/go-sdk/client"
	"github.com/dapr/kit/logger"
	"github.com/stretchr/testify/assert"
	"strconv"
	"testing"
	"time"
)

const (
	sidecarNamePrefix        = "cassandra-sidecar-"
	dockerComposeYAMLCLUSTER = "docker-compose-cluster.yml"
	dockerComposeYAML        = "docker-compose-single.yml"

	stateStoreName        = "statestore"
	stateStoreCluster     = "statestorecluster"
	stateStoreClusterFail = "statestoreclusterfail"
	stateStoreVersionFail = "statestoreversionfail"
	stateStoreFactorFail  = "statestorefactorfail"

	certificationTestPrefix = "stable-certification-"
	stateStoreNoConfigError = "error saving state: rpc error: code = FailedPrecondition desc = state store is not configured"
)

func TestCassandra(t *testing.T) {
	log := logger.NewLogger("dapr.components")
	stateStore := state_cassandra.NewCassandraStateStore(log).(*state_cassandra.Cassandra)
	ports, err := dapr_testing.GetFreePorts(2)
	assert.NoError(t, err)

	stateRegistry := state_loader.NewRegistry()
	stateRegistry.Logger = log
	stateRegistry.RegisterComponent(func(l logger.Logger) state.Store {
		return stateStore
	}, "cassandra")

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

		ttlInSeconds := 5
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
		time.Sleep(5 * time.Second)
		//entry should be expired now
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

	failTest := func(ctx flow.Context) error {
		client, err := goclient.NewClientWithPort(fmt.Sprint(currentGrpcPort + 2))
		if err != nil {
			panic(err)
		}
		defer client.Close()

		//should fail due to lack of replicas
		err = client.SaveState(ctx, stateStoreFactorFail, certificationTestPrefix+"key1", []byte("cassandraCert"), nil)
		assert.Error(t, err)

		return nil
	}

	failVerTest := func(ctx flow.Context) error {
		client, err := goclient.NewClientWithPort(fmt.Sprint(currentGrpcPort + 4))
		if err != nil {
			panic(err)
		}
		defer client.Close()
		// should fail due to unsupported version
		err = client.SaveState(ctx, stateStoreVersionFail, certificationTestPrefix+"key1", []byte("cassandraCert"), nil)
		assert.Error(t, err)

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
			runtime.WithStates(stateRegistry),
		)).
		Step("wait", flow.Sleep(30*time.Second)).
		Step("Run TTL related test", timeToLiveTest).
		Step("interrupt network",
			network.InterruptNetwork(10*time.Second, nil, nil, "9044:9042")).
		//Component should recover at this point.
		Step("wait", flow.Sleep(30*time.Second)).
		Step("Run basic test again to verify reconnection occurred", basicTest).
		Step("stop cassandra server", dockercompose.Stop("cassandra", dockerComposeYAML, "cassandra")).
		Step("start cassandra server", dockercompose.Start("cassandra", dockerComposeYAML, "cassandra")).
		Step("wait", flow.Sleep(60*time.Second)).
		Step("Get Values Saved Earlier And Not Expired, after Cassandra restart", testGetAfterCassandraRestart).
		Step("Run basic test", basicTest).
		Step(sidecar.Run(sidecarNamePrefix+"dockerDefault2",
			embedded.WithoutApp(),
			embedded.WithProfilePort(runtime.DefaultProfilePort+2),
			embedded.WithDaprGRPCPort(currentGrpcPort+2),
			embedded.WithDaprHTTPPort(currentHTTPPort+2),
			embedded.WithComponentsPath("components/docker/defaultfactorfail"),
			runtime.WithStates(stateRegistry),
		)).
		Step("wait", flow.Sleep(30*time.Second)).
		Step("Run replication factor fail test", failTest).
		Step(sidecar.Run(sidecarNamePrefix+"dockerDefault3",
			embedded.WithoutApp(),
			embedded.WithProfilePort(runtime.DefaultProfilePort+4),
			embedded.WithDaprGRPCPort(currentGrpcPort+4),
			embedded.WithDaprHTTPPort(currentHTTPPort+4),
			embedded.WithComponentsPath("components/docker/defaultverisonfail"),
			runtime.WithStates(stateRegistry),
		)).
		Step("wait", flow.Sleep(30*time.Second)).
		Step("Run replication factor fail test", failVerTest).
		Run()

}

func TestCluster(t *testing.T) {
	log := logger.NewLogger("dapr.components")
	stateStore := state_cassandra.NewCassandraStateStore(log).(*state_cassandra.Cassandra)
	ports, err := dapr_testing.GetFreePorts(2)
	assert.NoError(t, err)

	currentGrpcPort := ports[0]
	currentHTTPPort := ports[1]

	stateRegistry := state_loader.NewRegistry()
	stateRegistry.Logger = log
	stateRegistry.RegisterComponent(func(l logger.Logger) state.Store {
		return stateStore
	}, "cassandra")

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
		client, err := goclient.NewClientWithPort(fmt.Sprint(currentGrpcPort + 2))
		if err != nil {
			panic(err)
		}
		defer client.Close()

		err = client.SaveState(ctx, stateStoreClusterFail, certificationTestPrefix+"key1", []byte("cassandraCert"), nil)
		assert.NoError(t, err)

		// get state
		_, err = client.GetStateWithConsistency(ctx, stateStoreClusterFail, certificationTestPrefix+"key1", nil, goclient.StateConsistencyUndefined)
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
			runtime.WithStates(stateRegistry),
		)).
		Step("wait", flow.Sleep(30*time.Second)).
		Step("Run basic test", basicTest).
		Step(sidecar.Run(sidecarNamePrefix+"dockerDefault2",
			embedded.WithoutApp(),
			embedded.WithDaprGRPCPort(currentGrpcPort+2),
			embedded.WithDaprHTTPPort(currentHTTPPort+2),
			embedded.WithComponentsPath("components/docker/cluster-fail"),
			embedded.WithProfilePort(runtime.DefaultProfilePort+2),
			runtime.WithStates(stateRegistry),
		)).
		Step("wait", flow.Sleep(30*time.Second)).
		Step("Run consistency fail test", failTest).
		Run()

}
