/*
Copyright 2026 The Dapr Authors
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

package kubernetes_test

import (
	"encoding/json"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/configuration"
	config_kubernetes "github.com/dapr/components-contrib/configuration/kubernetes"
	"github.com/dapr/components-contrib/tests/certification/embedded"
	"github.com/dapr/components-contrib/tests/certification/flow"
	"github.com/dapr/components-contrib/tests/certification/flow/retry"
	"github.com/dapr/components-contrib/tests/certification/flow/sidecar"
	"github.com/dapr/components-contrib/tests/certification/flow/watcher"
	cu_kubernetes "github.com/dapr/components-contrib/tests/utils/configupdater/kubernetes"
	configuration_loader "github.com/dapr/dapr/pkg/components/configuration"
	dapr_testing "github.com/dapr/dapr/pkg/testing"
	dapr "github.com/dapr/go-sdk/client"
	"github.com/dapr/kit/logger"
)

const (
	storeName    = "configstore"
	key1         = "key1"
	key2         = "key2"
	val1         = "val1"
	val2         = "val2"
	sidecarName1 = "dapr-1"
)

var subscribeIDs []string

// castConfigurationItems converts go-sdk ConfigurationItem to contrib ConfigurationItem.
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

func TestKubernetes(t *testing.T) {
	log := logger.NewLogger("dapr.components")

	configurationRegistry := configuration_loader.NewRegistry()
	configurationRegistry.Logger = log
	configurationRegistry.RegisterComponent(config_kubernetes.NewKubernetesConfigMapStore, "kubernetes")

	updater := cu_kubernetes.NewKubernetesConfigUpdater(log).(*cu_kubernetes.ConfigUpdater)

	ports, err := dapr_testing.GetFreePorts(2)
	require.NoError(t, err)
	currentGrpcPort := ports[0]
	currentHTTPPort := ports[1]

	messageWatcher := watcher.NewUnordered()

	// Initialize the updater — creates the ConfigMap if it doesn't exist
	initUpdater := func(ctx flow.Context) error {
		return updater.Init(map[string]string{
			"configMapName": "dapr-cert-test",
			"namespace":     "default",
		})
	}

	// Verify the K8s API is reachable and ConfigMap exists
	checkConnection := func(ctx flow.Context) error {
		items := map[string]*configuration.Item{
			"healthcheck": {Value: "ok"},
		}
		if err := updater.AddKey(items); err != nil {
			return err
		}
		return updater.DeleteKey([]string{"healthcheck"})
	}

	// Get configuration items through the Dapr sidecar
	testGet := func(ctx flow.Context) error {
		client := sidecar.GetClient(ctx, sidecarName1)

		// Add keys
		err := updater.AddKey(map[string]*configuration.Item{
			key1: {Value: val1},
			key2: {Value: val2},
		})
		require.NoError(t, err)

		// Give the informer time to sync
		time.Sleep(2 * time.Second)

		// Get specific keys
		items, err := client.GetConfigurationItems(ctx, storeName, []string{key1})
		require.NoError(t, err)
		require.Len(t, items, 1)
		require.Equal(t, val1, items[key1].Value)

		// Get all keys
		items, err = client.GetConfigurationItems(ctx, storeName, []string{key1, key2})
		require.NoError(t, err)
		require.Len(t, items, 2)
		require.Equal(t, val1, items[key1].Value)
		require.Equal(t, val2, items[key2].Value)

		// Get non-existent key
		items, err = client.GetConfigurationItems(ctx, storeName, []string{"nonexistent"})
		require.NoError(t, err)
		require.Empty(t, items)

		return nil
	}

	// Subscribe to configuration changes
	subscribeFn := func(keys []string, message *watcher.Watcher) flow.Runnable {
		return func(ctx flow.Context) error {
			client := sidecar.GetClient(ctx, sidecarName1)
			message.Reset()
			subID, errSubscribe := client.SubscribeConfigurationItems(ctx, storeName, keys,
				func(id string, items map[string]*dapr.ConfigurationItem) {
					updateEvent := &configuration.UpdateEvent{
						Items: castConfigurationItems(items),
					}
					// Strip version since it's system-assigned (resourceVersion)
					for _, item := range updateEvent.Items {
						item.Version = ""
					}
					updateEventJSON, err := json.Marshal(updateEvent)
					require.NoError(t, err)
					message.Observe(string(updateEventJSON))
				})
			subscribeIDs = append(subscribeIDs, subID)
			return errSubscribe
		}
	}

	// Test that subscription notifications arrive after updates
	testSubscribe := func(messages *watcher.Watcher) flow.Runnable {
		return func(ctx flow.Context) error {
			messages.Reset()

			// Expect update for key1
			updateEvent := configuration.UpdateEvent{
				Items: map[string]*configuration.Item{
					key1: {Value: "updated-val1", Metadata: map[string]string{}},
				},
			}
			updateEventJSON, _ := json.Marshal(updateEvent)
			messages.Expect(string(updateEventJSON))

			// Update the key
			err := updater.UpdateKey(map[string]*configuration.Item{
				key1: {Value: "updated-val1"},
			})
			require.NoError(t, err)

			messages.Assert(t, 30*time.Second)
			return nil
		}
	}

	// Test that delete notifications arrive with deleted metadata
	testDelete := func(messages *watcher.Watcher) flow.Runnable {
		return func(ctx flow.Context) error {
			messages.Reset()

			// Expect delete notification for key2
			updateEvent := configuration.UpdateEvent{
				Items: map[string]*configuration.Item{
					key2: {Value: "", Metadata: map[string]string{"deleted": "true"}},
				},
			}
			updateEventJSON, _ := json.Marshal(updateEvent)
			messages.Expect(string(updateEventJSON))

			// Delete the key
			err := updater.DeleteKey([]string{key2})
			require.NoError(t, err)

			messages.Assert(t, 30*time.Second)
			return nil
		}
	}

	// Unsubscribe and verify no further messages
	stopSubscribers := func(ctx flow.Context) error {
		client := sidecar.GetClient(ctx, sidecarName1)
		for _, subID := range subscribeIDs {
			err := client.UnsubscribeConfigurationItems(ctx, storeName, subID)
			if err != nil {
				return err
			}
		}
		subscribeIDs = nil
		return nil
	}

	// Verify data persists (ConfigMap is not ephemeral)
	testGetAfterUpdate := func(ctx flow.Context) error {
		client := sidecar.GetClient(ctx, sidecarName1)
		items, err := client.GetConfigurationItems(ctx, storeName, []string{key1})
		require.NoError(t, err)
		require.Len(t, items, 1)
		require.Equal(t, "updated-val1", items[key1].Value)
		return nil
	}

	flow.New(t, "kubernetes configmap certification test").
		// Initialize the ConfigMap via the updater
		Step("initialize updater", retry.Do(time.Second*3, 10, initUpdater)).
		Step("verify K8s connection", checkConnection).
		// Run embedded Dapr sidecar with the kubernetes configuration component
		Step(sidecar.Run(sidecarName1,
			embedded.WithoutApp(),
			embedded.WithDaprGRPCPort(strconv.Itoa(currentGrpcPort)),
			embedded.WithDaprHTTPPort(strconv.Itoa(currentHTTPPort)),
			embedded.WithComponentsPath("components/default"),
			embedded.WithConfigurations(configurationRegistry),
		)).
		// Test Get operations
		Step("test get", testGet).
		// Subscribe and test update notifications
		Step("start subscriber", subscribeFn([]string{key1, key2}, messageWatcher)).
		Step("wait for subscriber to be ready", flow.Sleep(5*time.Second)).
		Step("test subscribe updates", testSubscribe(messageWatcher)).
		Step("reset", flow.Reset(messageWatcher)).
		// Test delete notifications
		Step("test delete notifications", testDelete(messageWatcher)).
		Step("reset", flow.Reset(messageWatcher)).
		// Unsubscribe
		Step("stop subscribers", stopSubscribers).
		// Verify data persistence
		Step("verify data after unsubscribe", testGetAfterUpdate).
		Run()
}
