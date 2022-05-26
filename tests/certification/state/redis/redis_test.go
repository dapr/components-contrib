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
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/dapr/components-contrib/state"
	state_redis "github.com/dapr/components-contrib/state/redis"
	"github.com/dapr/components-contrib/tests/certification/embedded"
	"github.com/dapr/components-contrib/tests/certification/flow"
	"github.com/dapr/components-contrib/tests/certification/flow/dockercompose"
	"github.com/dapr/components-contrib/tests/certification/flow/network"
	"github.com/dapr/components-contrib/tests/certification/flow/retry"
	"github.com/dapr/components-contrib/tests/certification/flow/sidecar"
	state_loader "github.com/dapr/dapr/pkg/components/state"
	"github.com/dapr/dapr/pkg/runtime"
	dapr_testing "github.com/dapr/dapr/pkg/testing"
	"github.com/dapr/go-sdk/client"
	"github.com/dapr/kit/logger"
	redis "github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"
)

const (
	sidecarNamePrefix       = "redis-sidecar-"
	dockerComposeYAML       = "docker-compose.yml"
	stateStoreName          = "statestore"
	certificationTestPrefix = "stable-certification-"
	stateStoreNoConfigError = "error saving state: rpc error: code = FailedPrecondition desc = state store is not configured"
)

func TestRedis(t *testing.T) {
	log := logger.NewLogger("dapr.components")
	stateStore := state_redis.NewRedisStateStore(log)
	ports, err := dapr_testing.GetFreePorts(2)
	assert.NoError(t, err)

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
		etag := "1"

		err0 := stateStore.Set(&state.SetRequest{
			Key:   "k",
			Value: "v1",
		})
		err1 := stateStore.Set(&state.SetRequest{
			Key:   "k",
			Value: "v2",
			ETag:  &etag,
		})
		etag4 := "4"
		err4 := stateStore.Set(&state.SetRequest{
			Key:   "k",
			Value: "v3",
			ETag:  &etag4,
		})
		resp, err := stateStore.Get(&state.GetRequest{
			Key: "k",
		})
		assert.Equal(t, "2", *resp.ETag, "check etag")
		assert.Equal(t, nil, err0, "failed to parse ETag")
		assert.Equal(t, nil, err1, "failed to parse ETag")
		assert.Equal(t, nil, err, "failed to parse ETag")
		assert.Error(t, err4)

		return nil
	}

	// Transaction related test - also for Multi
	upsertTest := func(ctx flow.Context) error {
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
						Value: "reqKey103",
						Metadata: map[string]string{
							"ttlInSeconds": "50",
						},
					},
				},
			},
		})
		assert.Equal(t, nil, err)
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
			embedded.WithDaprGRPCPort(currentGrpcPort),
			embedded.WithDaprHTTPPort(currentHTTPPort),
			embedded.WithComponentsPath("components/docker/default"),
			runtime.WithStates(
				state_loader.New("redis", func() state.Store {
					return stateStore
				}),
			))).
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
		Run()

	flow.New(t, "test redis state store yaml having enableTLS true with no relevant certs/secrets").
		Step(dockercompose.Run("redis", dockerComposeYAML)).
		Step("Waiting for Redis readiness", retry.Do(time.Second*3, 10, checkRedisConnection)).
		Step(sidecar.Run(sidecarNamePrefix+"dockerDefault",
			embedded.WithoutApp(),
			embedded.WithDaprGRPCPort(currentGrpcPort),
			embedded.WithDaprHTTPPort(currentHTTPPort),
			embedded.WithComponentsPath("components/docker/enableTLSConf"),
			runtime.WithStates(
				state_loader.New("redis", func() state.Store {
					return stateStore
				}),
			))).
		Step("Run basic test to confirm state store not yet configured", testForStateStoreNotConfigured).
		Run()

	flow.New(t, "test redis state store yaml having maxRetries set to non int value").
		Step(dockercompose.Run("redis", dockerComposeYAML)).
		Step("Waiting for Redis readiness", retry.Do(time.Second*3, 10, checkRedisConnection)).
		Step(sidecar.Run(sidecarNamePrefix+"dockerDefault",
			embedded.WithoutApp(),
			embedded.WithDaprGRPCPort(currentGrpcPort),
			embedded.WithDaprHTTPPort(currentHTTPPort),
			embedded.WithComponentsPath("components/docker/maxRetriesNonInt"),
			runtime.WithStates(
				state_loader.New("redis", func() state.Store {
					return stateStore
				}),
			))).
		Step("Run basic test to confirm state store not yet configured", testForStateStoreNotConfigured).
		Run()
}
