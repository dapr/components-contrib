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

package redis_test

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	redis "github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"

	"github.com/dapr/components-contrib/state"
	state_redis "github.com/dapr/components-contrib/state/redis"
	"github.com/dapr/components-contrib/tests/certification/embedded"
	"github.com/dapr/components-contrib/tests/certification/flow"
	"github.com/dapr/components-contrib/tests/certification/flow/dockercompose"
	"github.com/dapr/components-contrib/tests/certification/flow/network"
	"github.com/dapr/components-contrib/tests/certification/flow/retry"
	"github.com/dapr/components-contrib/tests/certification/flow/sidecar"
	state_loader "github.com/dapr/dapr/pkg/components/state"
	dapr_testing "github.com/dapr/dapr/pkg/testing"
	"github.com/dapr/go-sdk/client"
	"github.com/dapr/kit/logger"
)

const (
	sidecarNamePrefix       = "redis-sidecar-"
	dockerComposeYAML       = "docker-compose.yml"
	stateStoreName          = "statestore"
	certificationTestPrefix = "stable-certification-"
	stateStoreNoConfigError = "error saving state: rpc error: code = FailedPrecondition desc = state store is not configured"
)

func TestRedis(t *testing.T) {
	// Dapr run takes longer than 5 seconds to becomes ready because of the
	// (ignoreErrors=true) failing redis component below, so we need to configure
	// the go-sdk to wait longer to connect.
	t.Setenv("DAPR_CLIENT_TIMEOUT_SECONDS", "10")

	log := logger.NewLogger("dapr.components")

	stateStore := state_redis.NewRedisStateStore(log).(*state_redis.StateStore)
	ports, err := dapr_testing.GetFreePorts(2)
	assert.NoError(t, err)

	stateRegistry := state_loader.NewRegistry()
	stateRegistry.Logger = log
	stateRegistry.RegisterComponent(func(l logger.Logger) state.Store {
		return stateStore
	}, "redis")

	// var rdb redis.Client
	currentGrpcPort := ports[0]
	currentHTTPPort := ports[1]

	basicTest := func(ctx flow.Context) error {
		client, err := client.NewClientWithPort(fmt.Sprint(currentGrpcPort))
		if err != nil {
			panic(err)
		}
		defer client.Close()

		err = client.SaveState(ctx, stateStoreName, certificationTestPrefix+"key1", []byte("redisCert"), nil)
		assert.NoError(t, err)

		// get state
		item, err := client.GetState(ctx, stateStoreName, certificationTestPrefix+"key1", nil)
		assert.NoError(t, err)
		assert.Equal(t, "redisCert", string(item.Value))

		errUpdate := client.SaveState(ctx, stateStoreName, certificationTestPrefix+"key1", []byte("redisCertUpdate"), nil)
		assert.NoError(t, errUpdate)
		item, errUpdatedGet := client.GetState(ctx, stateStoreName, certificationTestPrefix+"key1", nil)
		assert.NoError(t, errUpdatedGet)
		assert.Equal(t, "redisCertUpdate", string(item.Value))

		// delete state
		err = client.DeleteState(ctx, stateStoreName, certificationTestPrefix+"key1", nil)
		assert.NoError(t, err)

		return nil
	}

	// Time-To-Live Test
	timeToLiveTest := func(ctx flow.Context) error {
		client, err := client.NewClientWithPort(fmt.Sprint(currentGrpcPort))
		if err != nil {
			panic(err)
		}
		defer client.Close()

		ttlInSecondsWrongValue := "mock value"
		mapOptionsWrongValue :=
			map[string]string{
				"ttlInSeconds": ttlInSecondsWrongValue,
			}

		ttlInSecondsNonExpiring := -1
		mapOptionsNonExpiring :=
			map[string]string{
				"ttlInSeconds": strconv.Itoa(ttlInSecondsNonExpiring),
			}

		ttlInSeconds := 1
		mapOptions :=
			map[string]string{
				"ttlInSeconds": strconv.Itoa(ttlInSeconds),
			}

		rdb := redis.NewClient(&redis.Options{
			Addr:     "localhost:6379", // host:port of the redis server
			Password: "",               // no password set
			DB:       0,                // use default DB
		})

		err1 := client.SaveState(ctx, stateStoreName, certificationTestPrefix+"ttl1", []byte("redisCert"), mapOptionsWrongValue)
		assert.Error(t, err1)
		err2 := client.SaveState(ctx, stateStoreName, certificationTestPrefix+"ttl2", []byte("redisCert2"), mapOptionsNonExpiring)
		assert.NoError(t, err2)
		err3 := client.SaveState(ctx, stateStoreName, certificationTestPrefix+"ttl3", []byte("redisCert3"), mapOptions)
		assert.NoError(t, err3)

		res, err := rdb.Do(ctx.Context, "TTL", sidecarNamePrefix+"dockerDefault||"+certificationTestPrefix+"ttl3").Result()
		assert.Equal(t, nil, err)
		assert.Equal(t, int64(1), res)

		// get state
		item, err := client.GetState(ctx, stateStoreName, certificationTestPrefix+"ttl3", nil)
		assert.NoError(t, err)
		assert.Equal(t, "redisCert3", string(item.Value))
		time.Sleep(2 * time.Second)
		itemAgain, errAgain := client.GetState(ctx, stateStoreName, certificationTestPrefix+"ttl3", nil)
		assert.NoError(t, errAgain)
		assert.Nil(t, nil, itemAgain)

		return nil
	}

	testGetAfterRedisRestart := func(ctx flow.Context) error {
		client, err := client.NewClientWithPort(fmt.Sprint(currentGrpcPort))
		if err != nil {
			panic(err)
		}
		defer client.Close()

		// get state
		item, err := client.GetState(ctx, stateStoreName, certificationTestPrefix+"ttl2", nil)
		assert.NoError(t, err)
		assert.Equal(t, "redisCert2", string(item.Value))

		return nil
	}

	//ETag test
	eTagTest := func(ctx flow.Context) error {
		etag1 := "1"
		etag100 := "100"

		err1 := stateStore.Set(context.Background(), &state.SetRequest{
			Key:   "k",
			Value: "v1",
		})
		assert.Equal(t, nil, err1)
		err2 := stateStore.Set(context.Background(), &state.SetRequest{
			Key:   "k",
			Value: "v2",
			ETag:  &etag1,
		})
		assert.Equal(t, nil, err2)
		err3 := stateStore.Set(context.Background(), &state.SetRequest{
			Key:   "k",
			Value: "v3",
			ETag:  &etag100,
		})
		assert.Error(t, err3)
		resp, err := stateStore.Get(context.Background(), &state.GetRequest{
			Key: "k",
		})
		assert.Equal(t, nil, err)
		assert.Equal(t, "2", *resp.ETag)
		assert.Equal(t, "\"v2\"", string(resp.Data))

		return nil
	}

	// Transaction related test - also for Multi
	upsertTest := func(ctx flow.Context) error {
		err := stateStore.Multi(context.Background(), &state.TransactionalStateRequest{
			Operations: []state.TransactionalStateOperation{
				state.SetRequest{
					Key:   "reqKey1",
					Value: "reqVal1",
					Metadata: map[string]string{
						"ttlInSeconds": "-1",
					},
				},
				state.SetRequest{
					Key:   "reqKey2",
					Value: "reqVal2",
					Metadata: map[string]string{
						"ttlInSeconds": "222",
					},
				},
				state.SetRequest{
					Key:   "reqKey3",
					Value: "reqVal3",
				},
				state.SetRequest{
					Key:   "reqKey1",
					Value: "reqVal101",
					Metadata: map[string]string{
						"ttlInSeconds": "50",
					},
				},
				state.SetRequest{
					Key:   "reqKey3",
					Value: "reqVal103",
					Metadata: map[string]string{
						"ttlInSeconds": "50",
					},
				},
			},
		})
		assert.Equal(t, nil, err)
		resp1, err := stateStore.Get(context.Background(), &state.GetRequest{
			Key: "reqKey1",
		})
		assert.Equal(t, "2", *resp1.ETag)
		assert.Equal(t, `"reqVal101"`, string(resp1.Data))

		resp3, err := stateStore.Get(context.Background(), &state.GetRequest{
			Key: "reqKey3",
		})
		assert.Equal(t, "2", *resp3.ETag)
		assert.Equal(t, `"reqVal103"`, string(resp3.Data))
		return nil
	}

	testForStateStoreNotConfigured := func(ctx flow.Context) error {
		client, err := client.NewClientWithPort(fmt.Sprint(currentGrpcPort))
		// assert.Error(t, err)
		if err != nil {
			panic(err)
		}
		defer client.Close()

		err = client.SaveState(ctx, stateStoreName, certificationTestPrefix+"key1", []byte("redisCert"), nil)
		assert.EqualError(t, err, stateStoreNoConfigError)

		return nil
	}

	checkRedisConnection := func(ctx flow.Context) error {
		rdb := redis.NewClient(&redis.Options{
			Addr:     "localhost:6379", // host:port of the redis server
			Password: "",               // no password set
			DB:       0,                // use default DB
		})

		if err := rdb.Ping(ctx).Err(); err != nil {
			return nil
		} else {
			log.Info("Setup for Redis done")
		}
		rdb.Close()
		return nil
	}

	flow.New(t, "Connecting Redis And Verifying majority of the tests here").
		Step(dockercompose.Run("redis", dockerComposeYAML)).
		Step("Waiting for Redis readiness", retry.Do(time.Second*3, 10, checkRedisConnection)).
		Step(sidecar.Run(sidecarNamePrefix+"dockerDefault",
			embedded.WithoutApp(),
			embedded.WithDaprGRPCPort(strconv.Itoa(currentGrpcPort)),
			embedded.WithDaprHTTPPort(strconv.Itoa(currentHTTPPort)),
			embedded.WithComponentsPath("components/docker/default"),
			embedded.WithStates(stateRegistry),
		)).
		Step("Run basic test", basicTest).
		Step("Run TTL related test", timeToLiveTest).
		Step("interrupt network",
			network.InterruptNetwork(10*time.Second, nil, nil, "6379:6379")).
		// Component should recover at this point.
		Step("wait", flow.Sleep(10*time.Second)).
		Step("Run basic test again to verify reconnection occurred", basicTest).
		Step("Run eTag test", eTagTest).
		Step("Run test for Upsert", upsertTest).
		Step("stop redis server", dockercompose.Stop("redis", dockerComposeYAML, "redis")).
		Step("start redis server", dockercompose.Start("redis", dockerComposeYAML, "redis")).
		Step("Waiting for Redis readiness after Redis Restart", retry.Do(time.Second*3, 10, checkRedisConnection)).
		Step("Get Values Saved Earlier And Not Expired, after redis restart", testGetAfterRedisRestart).
		Step("stop dapr", sidecar.Stop(sidecarNamePrefix+"dockerDefault")).
		Run()

	flow.New(t, "test redis state store yaml having enableTLS true with no relevant certs/secrets").
		Step(dockercompose.Run("redis", dockerComposeYAML)).
		Step("Waiting for Redis readiness", retry.Do(time.Second*3, 10, checkRedisConnection)).
		Step(sidecar.Run(sidecarNamePrefix+"dockerDefault",
			embedded.WithoutApp(),
			embedded.WithDaprGRPCPort(strconv.Itoa(currentGrpcPort)),
			embedded.WithDaprHTTPPort(strconv.Itoa(currentHTTPPort)),
			embedded.WithComponentsPath("components/docker/enableTLSConf"),
			embedded.WithStates(stateRegistry),
		)).
		Step("Run basic test to confirm state store not yet configured", testForStateStoreNotConfigured).
		Step("stop dapr", sidecar.Stop(sidecarNamePrefix+"dockerDefault")).
		Run()
}
