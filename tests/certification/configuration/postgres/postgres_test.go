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

package postgres_test

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/dapr/components-contrib/configuration"
	config_postgres "github.com/dapr/components-contrib/configuration/postgres"
	"github.com/dapr/components-contrib/tests/certification/embedded"
	"github.com/dapr/components-contrib/tests/certification/flow"
	"github.com/dapr/components-contrib/tests/certification/flow/dockercompose"
	"github.com/dapr/components-contrib/tests/certification/flow/network"
	"github.com/dapr/components-contrib/tests/certification/flow/retry"
	"github.com/dapr/components-contrib/tests/certification/flow/sidecar"
	cu_postgres "github.com/dapr/components-contrib/tests/utils/configupdater/postgres"
	configuration_loader "github.com/dapr/dapr/pkg/components/configuration"
	"github.com/dapr/dapr/pkg/runtime"
	dapr_testing "github.com/dapr/dapr/pkg/testing"
	dapr "github.com/dapr/go-sdk/client"
	"github.com/dapr/kit/logger"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/tests/certification/flow/watcher"
)

const (
	dockerComposeYAML   = "docker-compose.yml"
	storeName           = "configstore"
	key1                = "key1"
	key2                = "key2"
	Val1                = "val1"
	Val2                = "val2"
	sidecarName1        = "dapr-1"
	sidecarName2        = "dapr-2"
	connectionString    = "host=localhost user=postgres password=example port=5432 connect_timeout=10 database=dapr_test"
	configTable         = "configtable"
	connectionStringKey = "connectionString"
	configTableKey      = "table"
	pgNotifyChannelKey  = "pgNotifyChannel"
	pgNotifyChannel     = "config"
	portOffset          = 2
	channel1            = "channel1"
	channel2            = "channel2"
)

var (
	runID        = strings.ReplaceAll(uuid.Must(uuid.NewRandom()).String(), "-", "_")
	counter      = 0
	subscribeIDs = make(map[string][]string)
)

var updater *cu_postgres.ConfigUpdater

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

func expectMessage(message *watcher.Watcher, key string, val string, version string) {
	updateEvent := configuration.UpdateEvent{
		Items: map[string]*configuration.Item{
			key: {
				Value:   val,
				Version: version,
			},
		},
	}
	updateEventInJson, _ := json.Marshal(updateEvent)
	message.Expect(string(updateEventInJson))
}

func addKey(key, val, version string) error {
	items := make(map[string]*configuration.Item)
	items[key] = &configuration.Item{
		Value:   val,
		Version: version,
	}
	err := updater.AddKey(items)
	if err != nil {
		return fmt.Errorf("error adding key: %s", err)
	}
	return nil
}

func TestPostgres(t *testing.T) {
	log := logger.NewLogger("dapr.components")

	// Updater client to update config table
	updater = cu_postgres.NewPostgresConfigUpdater(log).(*cu_postgres.ConfigUpdater)

	ports, err := dapr_testing.GetFreePorts(2)
	require.NoError(t, err)
	currentGrpcPort := ports[0]
	currentHTTPPort := ports[1]

	watcher1 := watcher.NewUnordered()
	watcher2 := watcher.NewUnordered()
	watcher3 := watcher.NewUnordered()
	watcher4 := watcher.NewUnordered()

	// Initialize the updater
	initUpdater := func(ctx flow.Context) error {
		err := updater.Init(map[string]string{
			connectionStringKey: connectionString,
			configTableKey:      configTable,
		})
		if err != nil {
			return err
		}
		return nil
	}

	// Create triggers for given channels
	createTriggers := func(channels []string) flow.Runnable {
		return func(ctx flow.Context) error {
			for _, channel := range channels {
				err := updater.CreateTrigger(channel)
				if err != nil {
					return err
				}
			}
			return nil
		}
	}

	// Tests store init in different scenarios
	initTest := func(ctx flow.Context) error {
		md := configuration.Metadata{
			Base: metadata.Base{
				Name: "inittest",
				Properties: map[string]string{
					connectionStringKey: connectionString,
				},
			},
		}
		t.Run("Init with non-existing table", func(t *testing.T) {
			md.Base.Properties[configTableKey] = "configtable2"
			storeobj := config_postgres.NewPostgresConfigurationStore(log).(*config_postgres.ConfigurationStore)
			err := storeobj.Init(ctx, md)
			require.Error(t, err)
			require.Equal(t, err.Error(), "postgreSQL configuration table 'configtable2' does not exist")
		})
		t.Run("Init with upper cased tablename", func(t *testing.T) {
			upperCasedConfigTable := strings.ToUpper(configTable)
			md.Base.Properties[configTableKey] = upperCasedConfigTable
			storeobj := config_postgres.NewPostgresConfigurationStore(log).(*config_postgres.ConfigurationStore)
			err := storeobj.Init(ctx, md)
			require.Error(t, err)
			require.Equal(t, err.Error(), "invalid table name 'CONFIGTABLE'. non-alphanumerics or upper cased table names are not supported")
		})
		t.Run("Init with existing table", func(t *testing.T) {
			md.Base.Properties[configTableKey] = configTable
			storeobj := config_postgres.NewPostgresConfigurationStore(log).(*config_postgres.ConfigurationStore)
			err := storeobj.Init(ctx, md)
			require.NoError(t, err)
		})
		return nil
	}

	// Subscribes to given keys and channel
	subscribefn := func(keys []string, channel string, sidecarName string, message *watcher.Watcher) flow.Runnable {
		return func(ctx flow.Context) error {
			client := sidecar.GetClient(ctx, sidecarName)
			message.Reset()
			subscribeID, errSubscribe := client.SubscribeConfigurationItems(ctx, storeName, keys, func(id string, items map[string]*dapr.ConfigurationItem) {
				updateEvent := &configuration.UpdateEvent{
					Items: castConfigurationItems(items),
				}
				updateEventInJson, err := json.Marshal(updateEvent)
				require.NoError(t, err)
				message.Observe(string(updateEventInJson))
			}, func(md map[string]string) {
				md[pgNotifyChannelKey] = channel
			})
			if subscribeIDs[sidecarName] == nil {
				subscribeIDs[sidecarName] = make([]string, 0)
			}
			subscribeIDs[sidecarName] = append(subscribeIDs[sidecarName], subscribeID)
			return errSubscribe
		}
	}

	testGet := func(ctx flow.Context) error {
		client := sidecar.GetClient(ctx, sidecarName1)
		// Delete key1 if it exists
		err := updater.DeleteKey([]string{key1})
		require.NoError(t, err, "error deleting key")

		// Add key1 without any version
		err = addKey(key1, "val-no-version", "")
		require.NoError(t, err, "error adding key")
		items, err := client.GetConfigurationItems(ctx, storeName, []string{key1})
		require.NoError(t, err)
		require.Equal(t, 1, len(items))
		// Should return the value added
		require.Equal(t, "val-no-version", items[key1].Value)

		// Add key1 with version 1
		err = addKey(key1, "val-version-1", "1")
		require.NoError(t, err, "error adding key")
		items, err = client.GetConfigurationItems(ctx, storeName, []string{key1})
		require.NoError(t, err)
		require.Equal(t, 1, len(items))
		// Should return the value with version 1
		require.Equal(t, "val-version-1", items[key1].Value)

		// Add key1 with version 2
		err = addKey(key1, "val-version-2", "2")
		require.NoError(t, err, "error adding key")
		items, err = client.GetConfigurationItems(ctx, storeName, []string{key1})
		require.NoError(t, err)
		require.Equal(t, 1, len(items))
		// Should return the value with version 2
		require.Equal(t, "val-version-2", items[key1].Value)

		// Add key1 with non-numeric version
		err = addKey(key1, "val-non-numeric-version", "non-numeric")
		require.NoError(t, err, "error adding key")
		items, err = client.GetConfigurationItems(ctx, storeName, []string{key1})
		require.NoError(t, err)
		require.Equal(t, 1, len(items))
		// Should return the value with version 2
		require.Equal(t, "val-version-2", items[key1].Value)

		// Delete key1
		err = updater.DeleteKey([]string{key1})
		require.NoError(t, err, "error deleting key")
		return nil
	}

	// Tests subscribe by adding key and checking if the update event is received
	testSubscribe := func(messages ...*watcher.Watcher) flow.Runnable {
		return func(ctx flow.Context) error {
			for _, message := range messages {
				message.Reset()
				expectMessage(message, key1, Val1, "")
				expectMessage(message, key2, Val2, "")
			}

			err := addKey(key1, Val1, "")
			require.NoError(t, err, "error adding key")
			err = addKey(key2, Val2, "")
			require.NoError(t, err, "error adding key")

			for _, message := range messages {
				message.Assert(t, 10*time.Second)
			}
			return nil
		}
	}

	// Saves a key into config table before restart
	saveBeforeRestart := func(ctx flow.Context) error {
		items := map[string]*configuration.Item{
			key1: {
				Value: Val1,
			},
		}
		err := updater.AddKey(items)
		return err
	}

	// Checks if the key is present after restart
	getAfterRestart := func(ctx flow.Context) error {
		client := sidecar.GetClient(ctx, sidecarName1)

		item, err := client.GetConfigurationItem(ctx, storeName, key1)
		if err != nil {
			return err
		}
		require.Equal(t, item.Value, Val1)
		return nil

	}

	// Checks if the connection to postgres db is successful
	checkPostgresConnection := func(ctx flow.Context) error {
		config, err := pgxpool.ParseConfig(connectionString)
		if err != nil {
			return fmt.Errorf("postgres configuration store connection error : %w", err)
		}
		pool, err := pgxpool.NewWithConfig(ctx, config)
		if err != nil {
			return fmt.Errorf("postgres configuration store connection error : %w", err)
		}
		err = pool.Ping(ctx)
		if err != nil {
			return fmt.Errorf("postgres configuration store ping error : %w", err)
		}
		log.Info("Setup for Postgres done")
		return nil
	}

	stopSubscribers := func(sidecars []string) flow.Runnable {
		return func(ctx flow.Context) error {
			for _, sidecarName := range sidecars {
				client := sidecar.GetClient(ctx, sidecarName)
				for _, subscribeID := range subscribeIDs[sidecarName] {
					err := client.UnsubscribeConfigurationItems(ctx, storeName, subscribeID)
					if err != nil {
						return err
					}
				}
				subscribeIDs[sidecarName] = nil
			}
			return nil
		}
	}

	flow.New(t, "postgres certification test").
		// Run Postgres server
		Step(dockercompose.Run("db", dockerComposeYAML)).
		Step("wait for postgres to be ready", retry.Do(time.Second*3, 10, checkPostgresConnection)).
		Step("initialize updater", initUpdater).
		Step("Init test", initTest).
		//Running Dapr Sidecar `dapr-1`
		Step(sidecar.Run(sidecarName1,
			append(componentRuntimeOptions(),
				embedded.WithoutApp(),
				embedded.WithDaprGRPCPort(strconv.Itoa(currentGrpcPort)),
				embedded.WithDaprHTTPPort(strconv.Itoa(currentHTTPPort)),
				embedded.WithComponentsPath("components/default"),
			)...,
		)).
		Step("Test get", testGet).
		// Creating triggers for subscribers
		Step("Creating trigger", createTriggers([]string{channel1, channel2})).
		//Running Dapr Sidecar `dapr-2`
		Step(sidecar.Run(sidecarName2,
			append(componentRuntimeOptions(),
				embedded.WithoutApp(),
				embedded.WithDaprGRPCPort(strconv.Itoa(currentGrpcPort+portOffset)),
				embedded.WithDaprHTTPPort(strconv.Itoa(currentHTTPPort+portOffset)),
				embedded.WithComponentsPath("components/default"),
				embedded.WithProfilePort(strconv.Itoa(runtime.DefaultProfilePort+portOffset)),
			)...,
		)).
		//
		// Start subscribers subscribing to {key1} using different channels
		Step("start subscriber 1", subscribefn([]string{key1, key2}, channel1, sidecarName1, watcher1)).
		Step("start subscriber 2", subscribefn([]string{key1, key2}, channel2, sidecarName1, watcher2)).
		Step("start subscriber 3", subscribefn([]string{key1, key2}, channel1, sidecarName2, watcher3)).
		Step("start subscriber 4", subscribefn([]string{key1, key2}, channel2, sidecarName2, watcher4)).
		Step("wait for subscriber to be ready", flow.Sleep(5*time.Second)).
		// Add key in postgres and verify the update event is received by the subscriber
		Step("testSubscribe", testSubscribe(watcher1, watcher2, watcher3, watcher4)).
		Step("reset", flow.Reset(watcher1, watcher2, watcher3, watcher4)).
		// Simulate network interruptions and verify the connection still remains intact
		Step("interrupt network",
			network.InterruptNetwork(10*time.Second, nil, nil, "5432:5432")).
		Step("testSubscribe", testSubscribe(watcher1, watcher2, watcher3, watcher4)).
		Step("stop subscribers", stopSubscribers([]string{sidecarName1, sidecarName2})).
		Step("reset", flow.Reset(watcher1, watcher2, watcher3, watcher4)).
		// Save a key before restarting postgres server
		Step("Save before restart", saveBeforeRestart).
		Step("stop postgresql", dockercompose.Stop("db", dockerComposeYAML, "db")).
		Step("wait for component to stop", flow.Sleep(5*time.Second)).
		Step("start postgresql", dockercompose.Start("db", dockerComposeYAML, "db")).
		Step("wait for component to start", flow.Sleep(5*time.Second)).
		// Verify the key is still present after restart
		Step("run connection test", getAfterRestart).
		Run()

}
func componentRuntimeOptions() []embedded.Option {
	log := logger.NewLogger("dapr.components")

	configurationRegistry := configuration_loader.NewRegistry()
	configurationRegistry.Logger = log
	configurationRegistry.RegisterComponent(config_postgres.NewPostgresConfigurationStore, "postgres")

	return []embedded.Option{
		embedded.WithConfigurations(configurationRegistry),
	}
}
