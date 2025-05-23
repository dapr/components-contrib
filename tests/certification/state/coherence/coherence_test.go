/*
Copyright 2025 The Dapr Authors
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

package coherence_test

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/dapr/components-contrib/state"
	"github.com/dapr/go-sdk/client"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	state_coherence "github.com/dapr/components-contrib/state/coherence"
	"github.com/dapr/components-contrib/tests/certification/embedded"
	"github.com/dapr/components-contrib/tests/certification/flow"
	"github.com/dapr/components-contrib/tests/certification/flow/dockercompose"
	"github.com/dapr/components-contrib/tests/certification/flow/sidecar"
	state_loader "github.com/dapr/dapr/pkg/components/state"
	dapr_testing "github.com/dapr/dapr/pkg/testing"
	"github.com/dapr/kit/logger"
)

const (
	dockerComposeClusterYAML = "docker-compose.yaml"
	sidecarNamePrefix        = "coherence-sidecar-"
	stateStoreName           = "statestore"
	certificationTestPrefix  = "stable-certification-"
	testKey1                 = certificationTestPrefix + "key1"
	testKey2                 = certificationTestPrefix + "key2"
	testKey1Value            = "coherenceKey1"
	testKey2Value            = "coherenceKey2"
	testUpdateValue          = "coherenceUpdateValue"
	testNonexistentKey       = "ThisKeyDoesNotExistInTheStateStore"
	daprSideCar              = sidecarNamePrefix + "dockerClusterDefault"
)

var (
	gracefulShutdownTimeout = time.Second * 10
	currentT                *testing.T
	currentGrpcPort         int
	currentHTTPPort         int

	// Basic CRUD tests
	basicTestCRUDTests = func(ctx flow.Context) error {
		cl, err := client.NewClientWithPort(fmt.Sprint(currentGrpcPort))
		if err != nil {
			panic(err)
		}
		defer cl.Close()

		err = cl.SaveState(ctx, stateStoreName, testKey1, []byte(testKey1Value), nil)
		require.NoError(currentT, err)

		err = cl.SaveState(ctx, stateStoreName, testKey2, []byte(testKey2Value), nil)
		require.NoError(currentT, err)

		// get state
		item, err := cl.GetState(ctx, stateStoreName, testKey1, nil)
		require.NoError(currentT, err)
		assert.Equal(currentT, testKey1Value, string(item.Value))

		errUpdate := cl.SaveState(ctx, stateStoreName, testKey1, []byte(testUpdateValue), nil)
		require.NoError(currentT, errUpdate)
		item, errUpdatedGet := cl.GetState(ctx, stateStoreName, testKey1, nil)
		require.NoError(currentT, errUpdatedGet)
		assert.Equal(currentT, testUpdateValue, string(item.Value))

		// delete state
		err = cl.DeleteState(ctx, stateStoreName, testKey1, nil)
		require.NoError(currentT, err)

		assertNilValue(ctx, currentT, cl, testKey1)

		// nonexistent key
		item, err = cl.GetState(ctx, stateStoreName, testNonexistentKey, nil)
		require.NoError(currentT, err)
		assert.Nil(currentT, nil, item)

		return nil
	}

	testTTL = func(ctx flow.Context) error {
		cl, err := client.NewClientWithPort(fmt.Sprint(currentGrpcPort))
		if err != nil {
			panic(err)
		}
		defer cl.Close()

		key := certificationTestPrefix + "_expiresInOneSecondKey"
		value := "This key will expire in 5 second"

		ttlExpirationTime := 5 * time.Second
		ttlInSeconds := int(ttlExpirationTime.Seconds())
		mapOptionsExpiringKey := map[string]string{
			"ttlInSeconds": strconv.Itoa(ttlInSeconds),
		}

		errSave := cl.SaveState(ctx, stateStoreName, key, []byte(value), mapOptionsExpiringKey)
		require.NoError(currentT, errSave)

		item, errGetBeforeTTLExpiration := cl.GetState(ctx, stateStoreName, key, nil)
		require.NoError(currentT, errGetBeforeTTLExpiration)
		assert.Equal(currentT, value, string(item.Value))

		time.Sleep(6 * ttlExpirationTime)

		assertNilValue(ctx, currentT, cl, key)

		return nil
	}

	testBulkOperations = func(ctx flow.Context) error {
		cl, err := client.NewClientWithPort(fmt.Sprint(currentGrpcPort))
		if err != nil {
			panic(err)
		}
		defer cl.Close()

		// add multiple entries
		setReq := make([]*client.SetStateItem, 0)
		setReq = append(setReq, &client.SetStateItem{Key: "1", Value: []byte("value1")})
		setReq = append(setReq, &client.SetStateItem{Key: "2", Value: []byte("value2")})
		setReq = append(setReq, &client.SetStateItem{Key: "3", Value: []byte("value3")})

		errSaveBulk := cl.SaveBulkState(ctx, stateStoreName, setReq...)
		require.NoError(currentT, errSaveBulk)

		// get back multiple entries
		bulkStateItems, errGetBulk := cl.GetBulkState(ctx, stateStoreName, []string{"1", "2", "3"}, map[string]string{}, 0)
		require.NoError(currentT, errGetBulk)
		assert.Equal(currentT, 3, len(bulkStateItems))

		for _, v := range bulkStateItems {
			k := v.Key
			assert.Equal(currentT, fmt.Sprintf("value%s", k), string(v.Value))
		}

		// test bulkGet non-existent key
		bulkStateItemsNoKey, errGetBulkNoKey := cl.GetBulkState(ctx, stateStoreName, []string{"1234"}, map[string]string{}, 0)
		require.NoError(currentT, errGetBulkNoKey)
		assert.Equal(currentT, 1, len(bulkStateItemsNoKey))
		assert.Nil(currentT, bulkStateItemsNoKey[0].Value)

		// test bulkGet with partial non-existent keys
		bulkStateItems2, errGetBulk2 := cl.GetBulkState(ctx, stateStoreName, []string{"1", "2", "5000"}, map[string]string{}, 0)
		require.NoError(currentT, errGetBulk2)
		assert.Equal(currentT, 3, len(bulkStateItems2))

		for _, v := range bulkStateItems2 {
			k := v.Key
			if k == "5000" {
				assert.Nil(currentT, v.Value)
			} else {
				assert.Equal(currentT, fmt.Sprintf("value%s", k), string(v.Value))
			}
		}

		// test bulk delete
		bulkDeleteErr := cl.DeleteBulkState(ctx, stateStoreName, []string{"1", "2", "3"}, map[string]string{})
		require.NoError(currentT, bulkDeleteErr)

		assertNilValue(ctx, currentT, cl, "1")
		assertNilValue(ctx, currentT, cl, "2")
		assertNilValue(ctx, currentT, cl, "3")

		return nil
	}

	checkCoherenceReady = func(ctx flow.Context) error {
		maxTries := 30

		for i := 0; i < maxTries; i++ {
			time.Sleep(1 * time.Second)

			tr := &http.Transport{
				Proxy: http.ProxyFromEnvironment,
			}
			client := &http.Client{Transport: tr,
				Timeout: time.Duration(30) * time.Second,
				CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
					return http.ErrUseLastResponse
				}}

			req, err := http.NewRequest("GET", "http://127.0.0.1:6676/ready", bytes.NewBuffer([]byte{}))
			if err != nil {
				continue
			}

			resp, err := client.Do(req)
			if err != nil {
				continue
			}

			defer resp.Body.Close()

			if resp.StatusCode == 200 {
				return nil
			}
			continue
		}

		return fmt.Errorf("timeout after %d tries", maxTries)
	}
)

func TestCoherenceStandard(t *testing.T) {
	RunTestCoherence(t, "components/docker/default")
}

func TestCoherenceScope(t *testing.T) {
	RunTestCoherence(t, "components/docker/scope")
}

func TestCoherenceNearCacheTTL(t *testing.T) {
	RunTestCoherence(t, "components/docker/nearcachettl")
}

func TestCoherenceNearCacheMemory(t *testing.T) {
	RunTestCoherence(t, "components/docker/nearcachememory")
}

func TestCoherenceNearCacheUnits(t *testing.T) {
	RunTestCoherence(t, "components/docker/nearcacheunits")
}

func TestCoherenceNearCacheTTLAndUnits(t *testing.T) {
	RunTestCoherence(t, "components/docker/nearcachettlandunits")
}

func TestCoherenceNearCacheTTLAndMemory(t *testing.T) {
	RunTestCoherence(t, "components/docker/nearcachettlandmemory")
}

func RunTestCoherence(t *testing.T, resourcesPath string) {
	_ = os.Setenv("COHERENCE_LOG_LEVEL", "DEBUG")
	currentT = t
	log := logger.NewLogger("dapr.components")
	stateStore := state_coherence.NewCoherenceStateStore(log)

	require.NoError(t, setCurrentFreePorts())

	flow.New(t, "Connecting Coherence And Test basic operations").
		Step(dockercompose.Run("coherence", dockerComposeClusterYAML)).
		Step("Waiting for component to start...", flow.Sleep(10*time.Second)).
		Step(sidecar.Run(daprSideCar,
			append(componentRuntimeOptions(stateStore, log, "coherence"),
				embedded.WithoutApp(),
				embedded.WithGracefulShutdownDuration(gracefulShutdownTimeout),
				embedded.WithDaprGRPCPort(strconv.Itoa(currentGrpcPort)),
				embedded.WithDaprHTTPPort(strconv.Itoa(currentHTTPPort)),
				embedded.WithResourcesPath(resourcesPath),
			)...,
		)).
		Step("Waiting for Coherence to be ready...", checkCoherenceReady).
		Step("Run basic CRUD tests", basicTestCRUDTests).
		Step("Run TTL tests", testTTL).
		Step("Run Bulk Operations tests", testBulkOperations).
		Step("Stop Coherence server", dockercompose.Stop("coherence", dockerComposeClusterYAML)).
		Step("stop dapr", sidecar.Stop(sidecarNamePrefix+"dockerDefault")).
		Run()
}

func assertNilValue(ctx context.Context, t *testing.T, cl client.Client, key string) {
	item, err := cl.GetState(ctx, stateStoreName, key, nil)
	require.NoError(t, err)
	assert.NotNil(t, item.Key)
	assert.Nil(t, item.Value)
}

func componentRuntimeOptions(stateStore state.Store, log logger.Logger, stateStoreName string) []embedded.Option {
	stateRegistry := state_loader.NewRegistry()
	stateRegistry.Logger = log
	componentFactory := func(l logger.Logger) state.Store { return stateStore }

	stateRegistry.RegisterComponent(componentFactory, stateStoreName)

	return []embedded.Option{
		embedded.WithStates(stateRegistry),
	}
}

func setCurrentFreePorts() error {
	ports, err := dapr_testing.GetFreePorts(2)
	if err != nil {
		return err
	}
	currentGrpcPort = ports[0]
	currentHTTPPort = ports[1]

	return nil
}
