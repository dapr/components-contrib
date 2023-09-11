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
	"encoding/json"
	"strconv"
	"testing"
	"time"

	"github.com/dapr/components-contrib/configuration"
	config_redis "github.com/dapr/components-contrib/configuration/redis"
	"github.com/dapr/components-contrib/tests/certification/embedded"
	"github.com/dapr/components-contrib/tests/certification/flow"
	"github.com/dapr/components-contrib/tests/certification/flow/dockercompose"
	"github.com/dapr/components-contrib/tests/certification/flow/network"
	"github.com/dapr/components-contrib/tests/certification/flow/retry"
	"github.com/dapr/components-contrib/tests/certification/flow/sidecar"
	cu_redis "github.com/dapr/components-contrib/tests/utils/configupdater/redis"
	configuration_loader "github.com/dapr/dapr/pkg/components/configuration"
	dapr_testing "github.com/dapr/dapr/pkg/testing"
	dapr "github.com/dapr/go-sdk/client"
	"github.com/dapr/kit/logger"
	redis "github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"

	"github.com/dapr/components-contrib/tests/certification/flow/watcher"
)

type testType int

const (
	basicTest testType = iota
	allOperationsTest
	redisDBTest
)

const (
	dockerComposeYAML             = "docker-compose.yml"
	storeName                     = "configstore"
	key1                          = "key1"
	key2                          = "key2"
	subscribedMessageWaitDuration = 1000 * time.Millisecond
	sidecarName1                  = "dapr-1"
)

var subscribeID string

// Cast go-sdk ConfigurationItem to contrib ConfigurationItem
func castConfigurationItems(items map[string]*dapr.ConfigurationItem) map[string]*configuration.Item {
	configItems := make(map[string]*configuration.Item)
	for key, item := range items {
		configItems[key] = &configuration.Item{
			Value:    item.Value,
			Version:  item.Version,
			Metadata: item.Metadata,
		}
	}
	return configItems
}

// Create UpdateEvent struct from given key, val pair
func getUpdateEvent(key string, val string) configuration.UpdateEvent {
	expectedUpdateEvent := configuration.UpdateEvent{
		Items: map[string]*configuration.Item{
			key: {
				Value: val,
			},
		},
	}
	return expectedUpdateEvent
}

// Run redis commands and and add them in watcher
func runRedisCommands(ctx flow.Context, updater *cu_redis.ConfigUpdater, messages *watcher.Watcher, test testType) error {
	var scenarios []struct {
		cmd          []interface{}
		want         [][]string
		waitDuration time.Duration
		nowant       bool
	}
	switch test {
	case basicTest:
		scenarios = []struct {
			cmd          []interface{}
			want         [][]string
			waitDuration time.Duration
			nowant       bool
		}{
			{
				cmd:  []interface{}{"set", key1, "val1"},
				want: [][]string{{key1, "val1"}},
			},
			{
				cmd:  []interface{}{"mset", key1, "val1", key2, "val2"},
				want: [][]string{{key1, "val1"}, {key2, "val2"}},
			},
		}
	case allOperationsTest:
		scenarios = []struct {
			cmd          []interface{}
			want         [][]string
			waitDuration time.Duration
			nowant       bool
		}{
			{
				cmd:  []interface{}{"set", key1, "val1"},
				want: [][]string{{key1, "val1"}},
			},
			{
				cmd:  []interface{}{"mset", key1, "val1", key2, "val2"},
				want: [][]string{{key1, "val1"}, {key2, "val2"}},
			},
			{
				cmd:  []interface{}{"copy", key1, key2, "REPLACE"},
				want: [][]string{{key2, "val1"}},
			},
			{
				cmd:  []interface{}{"append", key1, "-append"},
				want: [][]string{{key1, "val1-append"}},
			},
			{
				cmd:  []interface{}{"setrange", key1, 4, "-offset"},
				want: [][]string{{key1, "val1-offset"}},
			},
			{
				cmd:  []interface{}{"expire", key1, 3},
				want: [][]string{{key1, "val1-offset"}, {key1, ""}},
				// This wait duration is required because `expire` command would generate an `expired` event for the key
				// after the expiration time (3 seconds here). Setting waitDuration to 5 seconds to wait for it.
				waitDuration: 5 * time.Second,
			},
			{
				cmd:  []interface{}{"set", key1, "val1"},
				want: [][]string{{key1, "val1"}},
			},
			{
				cmd:  []interface{}{"expire", key1, 10},
				want: [][]string{{key1, "val1"}},
			},
			{
				cmd:  []interface{}{"persist", key1},
				want: [][]string{{key1, "val1"}},
			},
			{
				cmd:  []interface{}{"expire", key1, -2},
				want: [][]string{{key1, ""}},
			},
			{
				cmd:  []interface{}{"set", key1, "val1"},
				want: [][]string{{key1, "val1"}},
			},
			{
				cmd:  []interface{}{"set", key2, "val2"},
				want: [][]string{{key2, "val2"}},
			},
			{
				cmd:  []interface{}{"rename", key2, key1},
				want: [][]string{{key1, "val2"}},
			},
			{
				cmd:  []interface{}{"move", key1, 1},
				want: [][]string{{key1, ""}},
			},
			{
				cmd:  []interface{}{"set", key1, 1},
				want: [][]string{{key1, "1"}},
			},
			{
				cmd:  []interface{}{"incr", key1},
				want: [][]string{{key1, "2"}},
			},
			{
				cmd:  []interface{}{"decr", key1},
				want: [][]string{{key1, "1"}},
			},
			{
				cmd:  []interface{}{"incrby", key1, 2},
				want: [][]string{{key1, "3"}},
			},
			{
				cmd:  []interface{}{"decrby", key1, 2},
				want: [][]string{{key1, "1"}},
			},
			{
				cmd:  []interface{}{"incrbyfloat", key1, 0.5},
				want: [][]string{{key1, "1.5"}},
			},
			{
				cmd:  []interface{}{"del", key1},
				want: [][]string{{key1, ""}},
			},
		}
	case redisDBTest:
		scenarios = []struct {
			cmd          []interface{}
			want         [][]string
			waitDuration time.Duration
			nowant       bool
		}{
			{
				cmd: []interface{}{"set", key1, "val1"},
				// For this test, client connection is set to DB 1, while the updater connects to DB 0. So no update should be received in case of set
				nowant: true,
			},
			{
				cmd: []interface{}{"move", key1, 1},
				// When key1 is moved to DB 1, update should be received to the subscriber.
				want: [][]string{{key1, "val1"}},
			},
		}
	}

	for _, scenario := range scenarios {
		if !scenario.nowant {
			for _, keyValue := range scenario.want {
				updateEvent := getUpdateEvent(keyValue[0], keyValue[1])
				updateEventInJson, err := json.Marshal(updateEvent)
				if err != nil {
					return err
				}
				messages.Expect(string(updateEventInJson))
			}
		}
		err := updater.Client.DoWrite(ctx, scenario.cmd...)
		if err != nil {
			return err
		}
		// Wait for the update to be received by the subscriber
		if scenario.waitDuration == 0 {
			time.Sleep(subscribedMessageWaitDuration)
		} else {
			time.Sleep(scenario.waitDuration)
		}
	}

	return nil
}

func TestRedis(t *testing.T) {
	log := logger.NewLogger("dapr.components")

	configStore := config_redis.NewRedisConfigurationStore(log)
	configurationRegistry := configuration_loader.NewRegistry()
	configurationRegistry.Logger = log
	configurationRegistry.RegisterComponent(func(l logger.Logger) configuration.Store {
		return configStore
	}, "redis")

	updater := cu_redis.NewRedisConfigUpdater(log).(*cu_redis.ConfigUpdater)
	updater.Init(map[string]string{
		"redisHost":     "localhost:6379",
		"redisPassword": "",
		"redisDB":       "0",
	})

	ports, err := dapr_testing.GetFreePorts(2)
	assert.NoError(t, err)
	currentGrpcPort := ports[0]
	currentHTTPPort := ports[1]

	messageWatcher := watcher.NewUnordered()

	checkRedisConnection := func(ctx flow.Context) error {
		rdb := redis.NewClient(&redis.Options{
			Addr:     "localhost:6379", // host:port of the redis server
			Password: "",               // no password set
			DB:       0,                // use default DB
		})
		defer rdb.Close()
		if err := rdb.Ping(ctx).Err(); err != nil {
			return err
		}
		log.Info("Setup for Redis done")
		return nil
	}

	subscribefn := func(keys []string, message *watcher.Watcher) flow.Runnable {
		return func(ctx flow.Context) error {
			client := sidecar.GetClient(ctx, sidecarName1)
			message.Reset()
			var errSubscribe error
			subscribeID, errSubscribe = client.SubscribeConfigurationItems(ctx, storeName, keys, func(id string, items map[string]*dapr.ConfigurationItem) {
				updateEvent := &configuration.UpdateEvent{
					Items: castConfigurationItems(items),
				}
				updateEventInJson, err := json.Marshal(updateEvent)
				assert.NoError(t, err)
				message.Observe(string(updateEventInJson))
			})
			return errSubscribe
		}
	}

	testSubscribe := func(messages *watcher.Watcher) flow.Runnable {
		return func(ctx flow.Context) error {
			messages.Reset()
			errRun := runRedisCommands(ctx, updater, messages, allOperationsTest)
			if errRun != nil {
				return errRun
			}
			messages.Assert(t, 10*time.Second)
			return nil
		}
	}

	testSubscribeBasic := func(messages *watcher.Watcher) flow.Runnable {
		return func(ctx flow.Context) error {
			messages.Reset()
			errRun := runRedisCommands(ctx, updater, messages, basicTest)
			if errRun != nil {
				return errRun
			}
			messages.Assert(t, 10*time.Second)
			return nil
		}
	}

	testredisDBMetadata := func(messages *watcher.Watcher) flow.Runnable {
		return func(ctx flow.Context) error {
			messages.Reset()
			errRun := runRedisCommands(ctx, updater, messages, redisDBTest)
			if errRun != nil {
				return errRun
			}
			messages.Assert(t, 10*time.Second)
			return nil
		}
	}

	saveBeforeRestart := func(ctx flow.Context) error {
		items := map[string]*configuration.Item{
			key1: {
				Value: "val1",
			},
		}
		err := updater.AddKey(items)
		return err
	}

	getAfterRestart := func(ctx flow.Context) error {
		client := sidecar.GetClient(ctx, sidecarName1)

		item, err := client.GetConfigurationItem(ctx, storeName, key1)
		if err != nil {
			return err
		}
		assert.Equal(t, item.Value, "val1")
		return nil
	}

	stopSubscriber := func(ctx flow.Context) error {
		client := sidecar.GetClient(ctx, sidecarName1)
		return client.UnsubscribeConfigurationItems(ctx, storeName, subscribeID)
	}

	flow.New(t, "redis certification test").
		// Run redis server
		Step(dockercompose.Run("redis", dockerComposeYAML)).
		Step("wait for redis to be ready", retry.Do(time.Second*3, 10, checkRedisConnection)).
		// Run dapr sidecar with redis configuration store component
		Step(sidecar.Run(sidecarName1,
			embedded.WithoutApp(),
			embedded.WithDaprGRPCPort(strconv.Itoa(currentGrpcPort)),
			embedded.WithDaprHTTPPort(strconv.Itoa(currentHTTPPort)),
			embedded.WithComponentsPath("components/default"),
			embedded.WithConfigurations(configurationRegistry),
		)).
		//
		// Start subscriber subscribing to keys {key1,key2}
		Step("start subscriber", subscribefn([]string{key1, key2}, messageWatcher)).
		Step("wait for subscriber to be ready", flow.Sleep(5*time.Second)).
		//Run redis commands and test updates are received by the subscriber
		Step("testSubscribe", testSubscribe(messageWatcher)).
		Step("reset", flow.Reset(messageWatcher)).
		//
		// Simulate network interruptions and verify the connection still remains intact
		Step("interrupt network",
			network.InterruptNetwork(10*time.Second, nil, nil, "6379:6379")).
		Step("testSubscribe", testSubscribeBasic(messageWatcher)).
		// Stop the subscriber
		Step("stop subscriber", stopSubscriber).
		//
		// Save a key before restarting redis server
		Step("save before restart", saveBeforeRestart).
		Step("stop redis server", dockercompose.Stop("redis", dockerComposeYAML, "redis")).
		Step("start redis server", dockercompose.Start("redis", dockerComposeYAML, "redis")).
		Step("wait for redis to be ready after restart", retry.Do(time.Second*3, 10, checkRedisConnection)).
		// Get the key after restarting redis server
		Step("get after restart", getAfterRestart).
		Run()

	flow.New(t, "Test connection to specified redisDB").
		// Run redis server
		Step(dockercompose.Run("redis", dockerComposeYAML)).
		Step("wait for redis to be ready", retry.Do(time.Second*3, 10, checkRedisConnection)).
		// Run dapr sidecar with redis configuration store component
		Step(sidecar.Run(sidecarName1,
			embedded.WithoutApp(),
			embedded.WithDaprGRPCPort(strconv.Itoa(currentGrpcPort)),
			embedded.WithDaprHTTPPort(strconv.Itoa(currentHTTPPort)),
			embedded.WithComponentsPath("components/redisDB1"),
			embedded.WithConfigurations(configurationRegistry),
		)).
		Step("start subscriber", subscribefn([]string{key1, key2}, messageWatcher)).
		Step("wait for subscriber to be ready", flow.Sleep(5*time.Second)).
		Step("verify database connected", testredisDBMetadata(messageWatcher)).
		// // Stop the subscriber
		Step("stop subscriber", stopSubscriber).
		Run()

}
