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

package memcached_test

import (
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/dapr/components-contrib/state"
	"github.com/dapr/go-sdk/client"

	state_memcached "github.com/dapr/components-contrib/state/memcached"
	"github.com/dapr/components-contrib/tests/certification/embedded"
	"github.com/dapr/components-contrib/tests/certification/flow"
	"github.com/dapr/components-contrib/tests/certification/flow/dockercompose"
	"github.com/dapr/components-contrib/tests/certification/flow/network"
	"github.com/dapr/components-contrib/tests/certification/flow/sidecar"
	state_loader "github.com/dapr/dapr/pkg/components/state"
	"github.com/dapr/dapr/pkg/runtime"
	dapr_testing "github.com/dapr/dapr/pkg/testing"
	"github.com/dapr/kit/logger"
	"github.com/stretchr/testify/assert"
)

const (
	sidecarNamePrefix        = "memcached-sidecar-"
	dockerComposeClusterYAML = "docker-compose.yml"
	stateStoreName           = "statestore"
	certificationTestPrefix  = "stable-certification-"
	testKey1                 = certificationTestPrefix + "key1"
	testKey2                 = certificationTestPrefix + "key2"
	testKey1Value            = "memcachedCert"
	testKey2Value            = "memcachedCert2"
	testUpdateValue          = "memcachedCertUpdate"
	testNonexistentKey       = "ThisKeyDoesNotExistInTheStateStore"
	servicePortToInterrupt   = "11211"
)

func TestMemcached(t *testing.T) {
	log := logger.NewLogger("dapr.components")
	stateStore := state_memcached.NewMemCacheStateStore(log)

	ports, err := dapr_testing.GetFreePorts(2)
	assert.NoError(t, err)

	// var rdb redis.Client
	currentGrpcPort := ports[0]
	currentHTTPPort := ports[1]

	// Basic CRUD tests
	basicTest := func(ctx flow.Context) error {
		client, err := client.NewClientWithPort(fmt.Sprint(currentGrpcPort))
		if err != nil {
			panic(err)
		}
		defer client.Close()

		err = client.SaveState(ctx, stateStoreName, testKey1, []byte(testKey1Value), nil)
		assert.NoError(t, err)

		err = client.SaveState(ctx, stateStoreName, testKey2, []byte(testKey2Value), nil)
		assert.NoError(t, err)

		// get state
		item, err := client.GetState(ctx, stateStoreName, testKey1, nil)
		assert.NoError(t, err)
		assert.Equal(t, testKey1Value, string(item.Value))

		errUpdate := client.SaveState(ctx, stateStoreName, testKey1, []byte(testUpdateValue), nil)
		assert.NoError(t, errUpdate)
		item, errUpdatedGet := client.GetState(ctx, stateStoreName, testKey1, nil)
		assert.NoError(t, errUpdatedGet)
		assert.Equal(t, testUpdateValue, string(item.Value))

		// delete state
		err = client.DeleteState(ctx, stateStoreName, testKey1, nil)
		assert.NoError(t, err)
		item, err = client.GetState(ctx, stateStoreName, testKey1, nil)
		assert.NoError(t, err)
		assert.Nil(t, nil, item)

		// nonexistent key
		item, err = client.GetState(ctx, stateStoreName, testNonexistentKey, nil)
		assert.NoError(t, err)
		assert.Nil(t, nil, item)

		return nil
	}

	// Time-To-Live Tests
	timeToLiveTestWithInvalidTTLValue := func(ctx flow.Context) error {
		client, err := client.NewClientWithPort(fmt.Sprint(currentGrpcPort))
		if err != nil {
			panic(err)
		}
		defer client.Close()

		// Invalid TTL value

		key := certificationTestPrefix + "_InvalidTTLValueKey"
		value := "with an invalid TTL this key should not be persisted."

		// TTL has to be a number
		ttlInSecondsNotNumeric := "mock value"
		mapOptionsNotNumeric := map[string]string{
			"ttlInSeconds": ttlInSecondsNotNumeric,
		}
		errNotNumeric := client.SaveState(ctx, stateStoreName, key, []byte(value), mapOptionsNotNumeric)
		assert.Error(t, errNotNumeric)

		return nil
	}

	timeToLiveTestWithNonExpiringTTL := func(ctx flow.Context) error {
		client, err := client.NewClientWithPort(fmt.Sprint(currentGrpcPort))
		if err != nil {
			panic(err)
		}
		defer client.Close()

		key := certificationTestPrefix + "_timeToLiveTestWithNonExpiringTTLKey"
		value := "This value does not expire and should be retrieved just fine"

		// Notice: we are actively setting a TTL value here: an non-expiring one.
		// This is different than the basic tests where no TTL is assigned.
		//
		// Notice that Memcached uses "0" as the non-expiring marker TTL.
		// https://github.com/memcached/memcached/wiki/Commands#set
		// OTOH Dapr uses -1 for that.
		// https://docs.dapr.io/developing-applications/building-blocks/state-management/state-store-ttl/
		// So we are using -1 here and expect the state store to translate this accordingly.
		ttlInSecondsNonExpiring := -1
		mapOptionsNonExpiring := map[string]string{
			"ttlInSeconds": strconv.Itoa(ttlInSecondsNonExpiring),
		}

		// We can successfully save...
		errSave := client.SaveState(ctx, stateStoreName, key, []byte(value), mapOptionsNonExpiring)
		assert.NoError(t, errSave)
		// and retrieve this key.
		item, errGet := client.GetState(ctx, stateStoreName, key, nil)
		assert.NoError(t, errGet)
		assert.Equal(t, value, string(item.Value))

		return nil
	}

	timeToLiveWithAOneSecondTTL := func(ctx flow.Context) error {
		client, err := client.NewClientWithPort(fmt.Sprint(currentGrpcPort))
		if err != nil {
			panic(err)
		}
		defer client.Close()

		key := certificationTestPrefix + "_expiresInOneSecondKey"
		value := "This key will self-destroy in 1 second"

		ttlExpirationTime := 1 * time.Second
		ttlInSeconds := int(ttlExpirationTime.Seconds())
		mapOptionsExpiringKey := map[string]string{
			"ttlInSeconds": strconv.Itoa(ttlInSeconds),
		}

		errSave := client.SaveState(ctx, stateStoreName, key, []byte(value), mapOptionsExpiringKey)
		assert.NoError(t, errSave)

		// get state
		item, errGetBeforeTTLExpiration := client.GetState(ctx, stateStoreName, key, nil)
		assert.NoError(t, errGetBeforeTTLExpiration)
		assert.Equal(t, value, string(item.Value))
		// Let the key expire
		time.Sleep(2 * ttlExpirationTime) // It should be safe to check in double TTL
		itemAfterTTL, errGetAfterTTL := client.GetState(ctx, stateStoreName, key, nil)
		assert.NoError(t, errGetAfterTTL)
		assert.Nil(t, nil, itemAfterTTL)

		return nil
	}

	flow.New(t, "Connecting Memcached And Test for CRUD operations").
		Step(dockercompose.Run("memcached", dockerComposeClusterYAML)).
		Step("Waiting for component to start...", flow.Sleep(5*time.Second)).
		Step(sidecar.Run(sidecarNamePrefix+"dockerClusterDefault",
			embedded.WithoutApp(),
			embedded.WithDaprGRPCPort(currentGrpcPort),
			embedded.WithDaprHTTPPort(currentHTTPPort),
			embedded.WithComponentsPath("components/docker/default"),
			componentRuntimeOptions(stateStore, log, "memcached"),
		)).
		Step("Waiting for component to load...", flow.Sleep(5*time.Second)).
		Step("Run basic test", basicTest).
		Step("Stop Memcached server", dockercompose.Stop("memcached", dockerComposeClusterYAML)).
		Run()

	flow.New(t, "Connecting Memcached And verifying TTL tests").
		Step(dockercompose.Run("memcached", dockerComposeClusterYAML)).
		Step("Waiting for component to start...", flow.Sleep(5*time.Second)).
		Step(sidecar.Run(sidecarNamePrefix+"dockerClusterDefault",
			embedded.WithoutApp(),
			embedded.WithDaprGRPCPort(currentGrpcPort),
			embedded.WithDaprHTTPPort(currentHTTPPort),
			embedded.WithComponentsPath("components/docker/default"),
			componentRuntimeOptions(stateStore, log, "memcached"),
		)).
		Step("Waiting for component to load...", flow.Sleep(5*time.Second)).
		Step("Run basic test", basicTest).
		Step("Run TTL related test: TTL not a valid number.", timeToLiveTestWithInvalidTTLValue).
		Step("Run TTL related test: TTL not expiring.", timeToLiveTestWithNonExpiringTTL).
		Step("Run TTL related test: TTL of 1 second.", timeToLiveWithAOneSecondTTL).
		Step("Stop Memcached server", dockercompose.Stop("memcached", dockerComposeClusterYAML)).
		Run()
}

func TestMemcachedNetworkInstability(t *testing.T) {
	log := logger.NewLogger("dapr.components")
	stateStore := state_memcached.NewMemCacheStateStore(log)

	ports, err := dapr_testing.GetFreePorts(2)
	assert.NoError(t, err)

	// var rdb redis.Client
	currentGrpcPort := ports[0]
	currentHTTPPort := ports[1]

	const (
		targetKey                   = certificationTestPrefix + "_TestMemcachedNetworkInstabilityKey"
		targetValue                 = "This key should still be there after the network returns"
		componentsPathFor20sTimeout = "components/docker/20secondsTimeout"
		memcachedTimeout            = 20 * time.Second
		keyTTL                      = memcachedTimeout * 4
		networkInstabilityTime      = memcachedTimeout * 2
		waitAfterInstabilityTime    = networkInstabilityTime / 2
	)

	assertKey := func(key string, value string) flow.Runnable {
		return func(ctx flow.Context) error {
			client, err := client.NewClientWithPort(fmt.Sprint(currentGrpcPort))
			if err != nil {
				panic(err)
			}
			defer client.Close()

			item, err := client.GetState(ctx, stateStoreName, key, nil)
			assert.NoError(t, err)
			assert.Equal(t, value, string(item.Value))

			return nil
		}
	}

	setKeyWithTTL := func(ttlExpirationTime time.Duration, key string, value string) flow.Runnable {
		return func(ctx flow.Context) error {
			client, err := client.NewClientWithPort(fmt.Sprint(currentGrpcPort))
			if err != nil {
				panic(err)
			}
			defer client.Close()

			ttlInSeconds := int(ttlExpirationTime.Seconds())
			mapOptionsExpiringKey := map[string]string{
				"ttlInSeconds": strconv.Itoa(ttlInSeconds),
			}

			errSave := client.SaveState(ctx, stateStoreName, key, []byte(value), mapOptionsExpiringKey)
			assert.NoError(t, errSave)
			// assert the key is there
			item, errGetBeforeTTLExpiration := client.GetState(ctx, stateStoreName, key, nil)
			assert.NoError(t, errGetBeforeTTLExpiration)
			assert.Equal(t, value, string(item.Value))

			return nil
		}
	}

	flow.New(t, "Connecting Memcached And Handling network instability").
		Step(dockercompose.Run("memcached", dockerComposeClusterYAML)).
		Step("Waiting for component to start...", flow.Sleep(5*time.Second)).
		Step(sidecar.Run(sidecarNamePrefix+"dockerClusterDefault",
			embedded.WithoutApp(),
			embedded.WithDaprGRPCPort(currentGrpcPort),
			embedded.WithDaprHTTPPort(currentHTTPPort),
			embedded.WithComponentsPath(componentsPathFor20sTimeout),
			componentRuntimeOptions(stateStore, log, "memcached"),
		)).
		Step("Waiting for component to load...", flow.Sleep(5*time.Second)).
		Step("Setup a key with a TTL of 4x memcached timeout ", setKeyWithTTL(keyTTL, targetKey, targetValue)).
		Step("Wait 1s", flow.Sleep(1*time.Second)).
		// Heads up, future developer friend: this will fail if running from WSL. :(
		Step("Interrupt network for 2x memcached timeout",
			network.InterruptNetwork(networkInstabilityTime, nil, nil, servicePortToInterrupt)).
		// Component should recover at this point.
		Step("Wait for component to recover", flow.Sleep(waitAfterInstabilityTime)).
		Step("Run basic test again to verify reconnection occurred", assertKey(targetKey, targetValue)).
		Step("Stop Memcached server", dockercompose.Stop("memcached", dockerComposeClusterYAML)).
		Run()
}

func componentRuntimeOptions(stateStore state.Store, log logger.Logger, stateStoreName string) []runtime.Option {
	stateRegistry := state_loader.NewRegistry()
	stateRegistry.Logger = log
	componentFactory := func(l logger.Logger) state.Store { return stateStore }

	stateRegistry.RegisterComponent(componentFactory, stateStoreName)

	return []runtime.Option{
		runtime.WithStates(stateRegistry),
	}
}
