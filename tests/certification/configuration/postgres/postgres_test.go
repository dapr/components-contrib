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
	"github.com/stretchr/testify/assert"
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
	configTable         = "configTable"
	connectionStringKey = "connectionString"
	configTableKey      = "table"
	configTable2        = "configTable2"
	pgNotifyChannelKey  = "pgNotifyChannel"
	pgNotifyChannel     = "config"
	portOffset          = 2
	channel1            = "channel1"
	channel2            = "channel2"
)

var (
	runID   = strings.ReplaceAll(uuid.Must(uuid.NewRandom()).String(), "-", "_")
	counter = 0
)

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

func TestPostgres(t *testing.T) {
	log := logger.NewLogger("dapr.components")

	// Updater client to update config table
	updater := cu_postgres.NewPostgresConfigUpdater(log).(*cu_postgres.ConfigUpdater)

	ports, err := dapr_testing.GetFreePorts(2)
	assert.NoError(t, err)
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
			md.Base.Properties[configTableKey] = configTable2
			storeobj := config_postgres.NewPostgresConfigurationStore(log).(*config_postgres.ConfigurationStore)
			err := storeobj.Init(ctx, md)
			require.Error(t, err)
			require.Equal(t, err.Error(), "postgreSQL configuration table - 'configTable2' does not exist")
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
	subscribefn := func(task *flow.AsyncTask, keys []string, channel string, sidecarName string, message *watcher.Watcher) flow.Runnable {
		return func(ctx flow.Context) error {
			client := sidecar.GetClient(ctx, sidecarName)
			message.Reset()
			errSubscribe := client.SubscribeConfigurationItems(*task, storeName, keys, func(id string, items map[string]*dapr.ConfigurationItem) {
				updateEvent := &configuration.UpdateEvent{
					Items: castConfigurationItems(items),
				}
				updateEventInJson, err := json.Marshal(updateEvent)
				assert.NoError(t, err)
				message.Observe(string(updateEventInJson))
			}, func(md map[string]string) {
				md[pgNotifyChannelKey] = channel
			})
			return errSubscribe
		}
	}

	// Tests subscribe by adding key and checking if the update event is received
	testSubscribe := func(messages ...*watcher.Watcher) flow.Runnable {
		return func(ctx flow.Context) error {
			for _, message := range messages {
				message.Reset()
				updateEvent := getUpdateEvent(key1, Val1)
				updateEventInJson, err := json.Marshal(updateEvent)
				if err != nil {
					return fmt.Errorf("error marshalling update event: %s", err)
				}
				message.Expect(string(updateEventInJson))
			}

			items := make(map[string]*configuration.Item)
			items[key1] = &configuration.Item{
				Value: Val1,
			}
			err := updater.AddKey(items)
			if err != nil {
				return fmt.Errorf("error adding key: %s", err)
			}

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
		assert.Equal(t, item.Value, Val1)
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

	var task1, task2, task3, task4 flow.AsyncTask
	flow.New(t, "postgres certification test").
		// Run Postgres server
		Step(dockercompose.Run("db", dockerComposeYAML)).
		Step("wait for postgres to be ready", retry.Do(time.Second*3, 10, checkPostgresConnection)).
		Step("initialize updater", initUpdater).
		Step("Init test", initTest).
		Step("Creating trigger", createTriggers([]string{channel1, channel2})).
		//Running Dapr Sidecars
		Step(sidecar.Run(sidecarName1,
			embedded.WithoutApp(),
			embedded.WithDaprGRPCPort(currentGrpcPort),
			embedded.WithDaprHTTPPort(currentHTTPPort),
			embedded.WithComponentsPath("components/default"),
			componentRuntimeOptions(),
		)).
		Step(sidecar.Run(sidecarName2,
			embedded.WithoutApp(),
			embedded.WithDaprGRPCPort(currentGrpcPort+portOffset),
			embedded.WithDaprHTTPPort(currentHTTPPort+portOffset),
			embedded.WithComponentsPath("components/default"),
			embedded.WithProfilePort(runtime.DefaultProfilePort+portOffset),
			componentRuntimeOptions(),
		)).
		//
		// Start subscribers subscribing to {key1} using different channels
		StepAsync("start subscriber 1", &task1, subscribefn(&task1, []string{key1}, channel1, sidecarName1, watcher1)).
		StepAsync("start subscriber 2", &task2, subscribefn(&task2, []string{key1}, channel2, sidecarName1, watcher2)).
		StepAsync("start subscriber 3", &task3, subscribefn(&task3, []string{key1}, channel1, sidecarName2, watcher3)).
		StepAsync("start subscriber 4", &task4, subscribefn(&task4, []string{key1}, channel2, sidecarName2, watcher4)).
		Step("wait for subscriber to be ready", flow.Sleep(5*time.Second)).
		// Add key in postgres and verify the update event is received by the subscriber
		Step("testSubscribe", testSubscribe(watcher1, watcher2, watcher3, watcher4)).
		Step("reset", flow.Reset(watcher1, watcher2, watcher3, watcher4)).
		// Simulate network interruptions and verify the connection still remains intact
		Step("interrupt network",
			network.InterruptNetwork(10*time.Second, nil, nil, "5432:5432")).
		Step("testSubscribe", testSubscribe(watcher1, watcher2, watcher3, watcher4)).
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
func componentRuntimeOptions() []runtime.Option {
	log := logger.NewLogger("dapr.components")

	configurationRegistry := configuration_loader.NewRegistry()
	configurationRegistry.Logger = log
	configurationRegistry.RegisterComponent(config_postgres.NewPostgresConfigurationStore, "postgres")

	return []runtime.Option{
		runtime.WithConfigurations(configurationRegistry),
	}
}
