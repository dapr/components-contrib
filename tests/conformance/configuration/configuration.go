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

package configuration

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/configuration"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/tests/conformance/utils"
	"github.com/dapr/components-contrib/tests/utils/configupdater"
	postgres_updater "github.com/dapr/components-contrib/tests/utils/configupdater/postgres"
)

const (
	keyCount               = 10
	v1                     = "1.0.0"
	defaultMaxReadDuration = 30 * time.Second
	defaultWaitDuration    = 5 * time.Second
	postgresComponent      = "postgresql"
	kubernetesComponent    = "kubernetes"
	pgNotifyChannelKey     = "pgNotifyChannel"
	pgNotifyChannel        = "config"
)

type TestConfig struct {
	utils.CommonConfig
	// IgnoreVersion indicates the component uses system-assigned versions
	// (e.g. Kubernetes ConfigMap resourceVersion) rather than user-defined versions.
	// When true, conformance tests compare items by value and metadata only.
	IgnoreVersion bool
}

func NewTestConfig(componentName string, operations []string, configMap map[string]interface{}) TestConfig {
	tc := TestConfig{
		CommonConfig: utils.CommonConfig{
			ComponentType: "configuration",
			ComponentName: componentName,
			Operations:    utils.NewStringSet(operations...),
		},
		IgnoreVersion: strings.HasPrefix(componentName, kubernetesComponent),
	}

	return tc
}

func getKeys(mymap map[string]*configuration.Item) []string {
	keys := []string{}
	for key := range mymap {
		keys = append(keys, key)
	}
	return keys
}

func mergeMaps(map1, map2 map[string]*configuration.Item) map[string]*configuration.Item {
	ret := make(map[string]*configuration.Item)
	for key, val := range map1 {
		ret[key] = val
	}
	for key, val := range map2 {
		ret[key] = val
	}
	return ret
}

// Generates key-value pairs
func generateKeyValues(runID string, counter int, keyCount int, version string) (map[string]*configuration.Item, int) {
	m := make(map[string]*configuration.Item, keyCount)
	k := counter
	for ; k < counter+keyCount; k++ {
		key := runID + "_key_" + strconv.Itoa(k)
		val := runID + "_val_" + strconv.Itoa(k)
		m[key] = &configuration.Item{
			Value:    val,
			Version:  version,
			Metadata: map[string]string{},
		}
	}
	return m, k
}

// Updates `mymap` with new values for every key
func updateKeyValues(mymap map[string]*configuration.Item, runID string, counter int, version string) (map[string]*configuration.Item, int) {
	m := make(map[string]*configuration.Item, len(mymap))
	k := counter
	for key := range mymap {
		updatedVal := runID + "_val_" + strconv.Itoa(k)
		m[key] = &configuration.Item{
			Value:    updatedVal,
			Version:  version,
			Metadata: map[string]string{},
		}
		k++
	}
	return m, k
}

func updateAwaitingMessages(awaitingMessages map[string]map[string]struct{}, updatedValues map[string]*configuration.Item, ignoreVersion bool) {
	for key, val := range updatedValues {
		if _, ok := awaitingMessages[key]; !ok {
			awaitingMessages[key] = make(map[string]struct{})
		}
		var valString string
		if ignoreVersion {
			valString = getStringItemIgnoreVersion(val)
		} else {
			valString = getStringItem(val)
		}
		awaitingMessages[key][valString] = struct{}{}
	}
}

func getStringItem(item *configuration.Item) string {
	if item == nil {
		return ""
	}
	jsonItem, err := json.Marshal(*item)
	if err != nil {
		panic(err)
	}
	return string(jsonItem)
}

// getStringItemIgnoreVersion serializes an Item without the Version field,
// for components where versions are system-assigned (e.g. Kubernetes ConfigMap resourceVersion).
func getStringItemIgnoreVersion(item *configuration.Item) string {
	if item == nil {
		return ""
	}
	normalized := configuration.Item{
		Value:    item.Value,
		Metadata: item.Metadata,
	}
	jsonItem, err := json.Marshal(normalized)
	if err != nil {
		panic(err)
	}
	return string(jsonItem)
}

// assertItemsEqualValues compares items by Value and Metadata only, ignoring Version.
func assertItemsEqualValues(t *testing.T, expected, actual map[string]*configuration.Item) {
	t.Helper()
	assert.Equal(t, len(expected), len(actual), "item count mismatch")
	for k, expectedItem := range expected {
		actualItem, ok := actual[k]
		assert.True(t, ok, "missing key %s", k)
		if ok {
			assert.Equal(t, expectedItem.Value, actualItem.Value, "value mismatch for key %s", k)
			assert.Equal(t, expectedItem.Metadata, actualItem.Metadata, "metadata mismatch for key %s", k)
		}
	}
}

func ConformanceTests(t *testing.T, props map[string]string, store configuration.Store, updater configupdater.Updater, config TestConfig, component string) {
	var subscribeIDs []string
	initValues1 := make(map[string]*configuration.Item)
	initValues2 := make(map[string]*configuration.Item)
	initValues := make(map[string]*configuration.Item)
	newValues := make(map[string]*configuration.Item)
	runID := strings.ReplaceAll(uuid.Must(uuid.NewRandom()).String(), "-", "_")
	counter := 0

	ignoreVersion := config.IgnoreVersion

	awaitingMessages1 := make(map[string]map[string]struct{}, keyCount*4)
	awaitingMessages2 := make(map[string]map[string]struct{}, keyCount*4)
	awaitingMessages3 := make(map[string]map[string]struct{}, keyCount*4)
	processedC1 := make(chan *configuration.UpdateEvent, keyCount*4)
	processedC2 := make(chan *configuration.UpdateEvent, keyCount*4)
	processedC3 := make(chan *configuration.UpdateEvent, keyCount*4)

	t.Run("init", func(t *testing.T) {
		// Initializing config updater. It has to be initialized before the store to create the table
		err := updater.Init(props)
		require.NoError(t, err)

		// Creating trigger for postgres config updater
		if strings.HasPrefix(component, postgresComponent) {
			err = updater.(*postgres_updater.ConfigUpdater).CreateTrigger(pgNotifyChannel)
			require.NoError(t, err)
		}

		// Initializing store
		err = store.Init(t.Context(), configuration.Metadata{
			Base: metadata.Base{
				Properties: props,
			},
		})
		require.NoError(t, err)
	})

	if t.Failed() {
		t.Fatal("initialization failed")
	}

	t.Run("insert initial keys", func(t *testing.T) {
		initValues1, counter = generateKeyValues(runID, counter, keyCount, v1)
		initValues2, counter = generateKeyValues(runID, counter, keyCount, v1)
		initValues = mergeMaps(initValues1, initValues2)
		err := updater.AddKey(initValues)
		require.NoError(t, err, "expected no error on adding keys")
	})

	t.Run("get", func(t *testing.T) {
		t.Run("get with non-empty key list", func(t *testing.T) {
			keys := getKeys(initValues1)

			req := &configuration.GetRequest{
				Keys:     keys,
				Metadata: make(map[string]string),
			}

			resp, err := store.Get(t.Context(), req)
			require.NoError(t, err)
			if ignoreVersion {
				assertItemsEqualValues(t, initValues1, resp.Items)
			} else {
				assert.Equal(t, initValues1, resp.Items)
			}
		})

		t.Run("get with empty key list", func(t *testing.T) {
			keys := []string{}

			req := &configuration.GetRequest{
				Keys:     keys,
				Metadata: make(map[string]string),
			}

			resp, err := store.Get(t.Context(), req)
			require.NoError(t, err)
			if ignoreVersion {
				assertItemsEqualValues(t, initValues, resp.Items)
			} else {
				assert.Equal(t, initValues, resp.Items)
			}
		})

		t.Run("get with non-existent key list", func(t *testing.T) {
			newValues, counter = generateKeyValues(runID, counter, keyCount, v1)
			keys := getKeys(newValues)
			expectedResponse := make(map[string]*configuration.Item)

			req := &configuration.GetRequest{
				Keys:     keys,
				Metadata: make(map[string]string),
			}

			resp, err := store.Get(t.Context(), req)
			require.NoError(t, err)
			assert.Equal(t, expectedResponse, resp.Items)
		})
	})

	t.Run("subscribe", func(t *testing.T) {
		subscribeMetadata := make(map[string]string)
		if strings.HasPrefix(component, postgresComponent) {
			subscribeMetadata[pgNotifyChannelKey] = pgNotifyChannel
		}
		t.Run("subscriber 1 with non-empty key list", func(t *testing.T) {
			keys := getKeys(initValues1)
			ID, err := store.Subscribe(t.Context(),
				&configuration.SubscribeRequest{
					Keys:     keys,
					Metadata: subscribeMetadata,
				},
				func(ctx context.Context, e *configuration.UpdateEvent) error {
					processedC1 <- e
					return nil
				})
			require.NoError(t, err, "expected no error on subscribe")
			subscribeIDs = append(subscribeIDs, ID)
		})

		t.Run("subscriber 2 with non-empty key list", func(t *testing.T) {
			keys := getKeys(initValues)
			ID, err := store.Subscribe(t.Context(),
				&configuration.SubscribeRequest{
					Keys:     keys,
					Metadata: subscribeMetadata,
				},
				func(ctx context.Context, e *configuration.UpdateEvent) error {
					processedC2 <- e
					return nil
				})
			require.NoError(t, err, "expected no error on subscribe")
			subscribeIDs = append(subscribeIDs, ID)
		})

		t.Run("subscriber 3 with empty key list", func(t *testing.T) {
			keys := []string{}
			ID, err := store.Subscribe(t.Context(),
				&configuration.SubscribeRequest{
					Keys:     keys,
					Metadata: subscribeMetadata,
				},
				func(ctx context.Context, e *configuration.UpdateEvent) error {
					processedC3 <- e
					return nil
				})
			require.NoError(t, err, "expected no error on subscribe")
			subscribeIDs = append(subscribeIDs, ID)
		})

		t.Run("wait", func(t *testing.T) {
			time.Sleep(defaultWaitDuration)
		})

		t.Run("update key values and verify messages received", func(t *testing.T) {
			initValues1, counter = updateKeyValues(initValues1, runID, counter, v1)
			errUpdate1 := updater.UpdateKey(initValues1)
			require.NoError(t, errUpdate1, "expected no error on updating keys")

			updateAwaitingMessages(awaitingMessages1, initValues1, ignoreVersion)
			updateAwaitingMessages(awaitingMessages2, initValues1, ignoreVersion)
			updateAwaitingMessages(awaitingMessages3, initValues1, ignoreVersion)

			// Update initValues2
			initValues2, counter = updateKeyValues(initValues2, runID, counter, v1)
			errUpdate2 := updater.UpdateKey(initValues2)
			require.NoError(t, errUpdate2, "expected no error on updating keys")

			updateAwaitingMessages(awaitingMessages2, initValues2, ignoreVersion)
			updateAwaitingMessages(awaitingMessages3, initValues2, ignoreVersion)

			newValues, counter = generateKeyValues(runID, counter, keyCount, v1)
			errAdd := updater.AddKey(newValues)
			require.NoError(t, errAdd, "expected no error on adding new keys")

			updateAwaitingMessages(awaitingMessages3, newValues, ignoreVersion)

			verifyMessagesReceived(t, processedC1, awaitingMessages1, ignoreVersion)
			verifyMessagesReceived(t, processedC2, awaitingMessages2, ignoreVersion)
			verifyMessagesReceived(t, processedC3, awaitingMessages3, ignoreVersion)
		})

		t.Run("delete keys and verify messages received", func(t *testing.T) {
			// Delete initValues2
			errDelete := updater.DeleteKey(getKeys(initValues2))
			require.NoError(t, errDelete, "expected no error on updating keys")
			if strings.HasPrefix(component, kubernetesComponent) {
				// Kubernetes ConfigMap delete notifications include {"deleted": "true"} metadata
				for k := range initValues2 {
					initValues2[k] = &configuration.Item{
						Metadata: map[string]string{"deleted": "true"},
					}
				}
			} else if !strings.HasPrefix(component, postgresComponent) {
				for k := range initValues2 {
					initValues2[k] = &configuration.Item{}
				}
			}

			updateAwaitingMessages(awaitingMessages2, initValues2, ignoreVersion)
			updateAwaitingMessages(awaitingMessages3, initValues2, ignoreVersion)

			verifyMessagesReceived(t, processedC2, awaitingMessages2, ignoreVersion)
			verifyMessagesReceived(t, processedC3, awaitingMessages3, ignoreVersion)
		})
	})

	t.Run("unsubscribe", func(t *testing.T) {
		t.Run("unsubscribe subscriber 1", func(t *testing.T) {
			err := store.Unsubscribe(t.Context(),
				&configuration.UnsubscribeRequest{
					ID: subscribeIDs[0],
				},
			)
			require.NoError(t, err, "expected no error in unsubscribe")
		})

		t.Run("update key values and verify subscriber 1 receives no messages", func(t *testing.T) {
			initValues1, counter = updateKeyValues(initValues1, runID, counter, v1)
			errUpdate := updater.UpdateKey(initValues1)
			require.NoError(t, errUpdate, "expected no error on updating keys")

			updateAwaitingMessages(awaitingMessages2, initValues1, ignoreVersion)
			updateAwaitingMessages(awaitingMessages3, initValues1, ignoreVersion)

			verifyMessagesReceived(t, processedC2, awaitingMessages2, ignoreVersion)
			verifyMessagesReceived(t, processedC3, awaitingMessages3, ignoreVersion)
			verifyNoMessagesReceived(t, processedC1)
		})

		t.Run("unsubscribe subscriber 2", func(t *testing.T) {
			err := store.Unsubscribe(t.Context(),
				&configuration.UnsubscribeRequest{
					ID: subscribeIDs[1],
				},
			)
			require.NoError(t, err, "expected no error in unsubscribe")
		})

		t.Run("update key values and verify subscriber 2 receives no messages", func(t *testing.T) {
			initValues1, counter = updateKeyValues(initValues1, runID, counter, v1)
			errUpdate := updater.UpdateKey(initValues1)
			require.NoError(t, errUpdate, "expected no error on updating keys")

			updateAwaitingMessages(awaitingMessages3, initValues1, ignoreVersion)

			verifyMessagesReceived(t, processedC3, awaitingMessages3, ignoreVersion)
			verifyNoMessagesReceived(t, processedC2)
		})

		t.Run("unsubscribe subscriber 3", func(t *testing.T) {
			err := store.Unsubscribe(t.Context(),
				&configuration.UnsubscribeRequest{
					ID: subscribeIDs[2],
				},
			)
			require.NoError(t, err, "expected no error in unsubscribe")
		})

		t.Run("update key values and verify subscriber 3 receives no messages", func(t *testing.T) {
			initValues1, counter = updateKeyValues(initValues1, runID, counter, v1)
			errUpdate := updater.UpdateKey(initValues1)
			require.NoError(t, errUpdate, "expected no error on updating keys")

			verifyNoMessagesReceived(t, processedC3)
		})
	})
}

func verifyNoMessagesReceived(t *testing.T, processedChan chan *configuration.UpdateEvent) {
	waiting := true
	timeout := time.After(defaultMaxReadDuration)
	for waiting {
		select {
		case <-processedChan:
			assert.FailNow(t, "expected no messages after unsubscribe")
		case <-timeout:
			waiting = false
		}
	}
}

func verifyMessagesReceived(t *testing.T, processedChan chan *configuration.UpdateEvent, awaitingMessages map[string]map[string]struct{}, ignoreVersion bool) {
	waiting := true
	timeout := time.After(defaultMaxReadDuration)
	for waiting {
		select {
		case processed := <-processedChan:
			for key, receivedItem := range processed.Items {
				items, keyExists := awaitingMessages[key]
				assert.True(t, keyExists)

				var stringReceivedItem string
				if ignoreVersion {
					stringReceivedItem = getStringItemIgnoreVersion(receivedItem)
				} else {
					stringReceivedItem = getStringItem(receivedItem)
				}
				_, itemExists := items[stringReceivedItem]
				assert.True(t, itemExists)

				delete(awaitingMessages[key], stringReceivedItem)
				if len(awaitingMessages[key]) == 0 {
					delete(awaitingMessages, key)
				}
				waiting = len(awaitingMessages) > 0
			}
		case <-timeout:
			t.Errorf("Timeout waiting for the subscribed keys")
			waiting = false
		}
	}
	assert.Empty(t, awaitingMessages, "expected to read all subscribed configuration updates")
}
