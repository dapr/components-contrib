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
	"testing"
	"time"

	"github.com/dapr/components-contrib/configuration"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/tests/conformance/utils"
	"github.com/dapr/components-contrib/tests/utils/configupdater"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

const (
	keyCount               = 10
	v1                     = "1.0.0"
	defaultMaxReadDuration = 60 * time.Second
)

type TestConfig struct {
	utils.CommonConfig
}

func NewTestConfig(componentName string, allOperations bool, operations []string, configMap map[string]interface{}) TestConfig {
	tc := TestConfig{
		utils.CommonConfig{
			ComponentType: "configuration",
			ComponentName: componentName,
			AllOperations: allOperations,
			Operations:    utils.NewStringSet(operations...),
		},
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

func generateKeyValues(runID string, counter int, keyCount int, version string) map[string]*configuration.Item {
	m := make(map[string]*configuration.Item, keyCount)
	for k := counter; k < counter+keyCount; k++ {
		key := runID + "-key-" + strconv.Itoa(k)
		val := runID + "-val-" + strconv.Itoa(k)
		m[key] = &configuration.Item{
			Value:    val,
			Version:  version,
			Metadata: map[string]string{},
		}
	}
	return m
}

func updateKeyValues(mymap map[string]*configuration.Item, runID string, counter int, version string) map[string]*configuration.Item {
	m := make(map[string]*configuration.Item, len(mymap))
	k := counter
	for key := range mymap {
		updatedVal := runID + "-val-" + strconv.Itoa(k)
		m[key] = &configuration.Item{
			Value:    updatedVal,
			Version:  version,
			Metadata: map[string]string{},
		}
		k++
	}
	return m
}

func updateAwaitingMessages(awaitingMessages map[string]map[string]struct{}, updatedValues map[string]*configuration.Item) map[string]map[string]struct{} {
	for key, val := range updatedValues {
		if _, ok := awaitingMessages[key]; !ok {
			awaitingMessages[key] = make(map[string]struct{})
		}
		valString := getStringItem(val)
		awaitingMessages[key][valString] = struct{}{}
	}
	return awaitingMessages
}

func getStringItem(item *configuration.Item) string {
	jsonItem, err := json.Marshal(*item)
	if err != nil {
		panic(err)
	}
	return string(jsonItem)
}

func ConformanceTests(t *testing.T, props map[string]string, store configuration.Store, updater configupdater.Updater, config TestConfig) {

	initValues := make(map[string]*configuration.Item)

	runID := uuid.Must(uuid.NewRandom()).String()
	counter := 0

	awaitingMessages1 := make(map[string]map[string]struct{}, keyCount*4)
	awaitingMessages2 := make(map[string]map[string]struct{}, keyCount*4)

	processedC1 := make(chan *configuration.UpdateEvent, keyCount*4)
	processedC2 := make(chan *configuration.UpdateEvent, keyCount*4)

	var subscribeIDs []string

	t.Run("init", func(t *testing.T) {
		err := store.Init(configuration.Metadata{
			Base: metadata.Base{Properties: props},
		})
		assert.Nil(t, err)

		err = updater.Init(props)
		assert.Nil(t, err)
	})

	t.Run("insert initial keys", func(t *testing.T) {
		//Insert initial keys
		initValues = generateKeyValues(runID, counter, keyCount, v1)
		err := updater.AddKey(initValues)
		assert.NoError(t, err, "expected no error on adding keys")
		counter += keyCount
	})

	if config.HasOperation("get") {
		t.Run("get with non-empty keys", func(t *testing.T) {
			keys := getKeys(initValues)

			req := &configuration.GetRequest{
				Keys:     keys,
				Metadata: make(map[string]string),
			}

			resp, err := store.Get(context.Background(), req)
			assert.Nil(t, err)
			assert.Equal(t, initValues, resp.Items)
		})

		t.Run("get with empty keys", func(t *testing.T) {
			keys := []string{}

			req := &configuration.GetRequest{
				Keys:     keys,
				Metadata: make(map[string]string),
			}

			resp, err := store.Get(context.Background(), req)
			assert.Nil(t, err)
			assert.Equal(t, initValues, resp.Items)
		})
	}

	if config.HasOperation("subscribe") {
		t.Run("subscribe with non-empty keys", func(t *testing.T) {
			keys := getKeys(initValues)
			Id1, err := store.Subscribe(context.Background(),
				&configuration.SubscribeRequest{
					Keys:     keys,
					Metadata: make(map[string]string),
				},
				func(ctx context.Context, e *configuration.UpdateEvent) error {
					processedC1 <- e
					return nil
				})
			assert.NoError(t, err, "expected no error on subscribe")
			subscribeIDs = append(subscribeIDs, Id1)
		})

		t.Run("subscribe with empty keys", func(t *testing.T) {
			keys := []string{}
			Id2, err := store.Subscribe(context.Background(),
				&configuration.SubscribeRequest{
					Keys:     keys,
					Metadata: make(map[string]string),
				},
				func(ctx context.Context, e *configuration.UpdateEvent) error {
					processedC2 <- e
					return nil
				})
			assert.NoError(t, err, "expected no error on subscribe")
			subscribeIDs = append(subscribeIDs, Id2)
		})

		t.Run("Update Keys/Add new keys", func(t *testing.T) {
			//Update existing keys
			updatedValues := updateKeyValues(initValues, runID, counter, v1)
			counter += len(initValues)
			errUpdate := updater.UpdateKey(updatedValues)
			assert.NoError(t, errUpdate, "expected no error on updating keys")
			//Both Subscriber should receive these updates
			awaitingMessages1 = updateAwaitingMessages(awaitingMessages1, updatedValues)
			awaitingMessages2 = updateAwaitingMessages(awaitingMessages2, updatedValues)

			//Add new keys
			newValues := generateKeyValues(runID, counter, keyCount, v1)
			counter += keyCount
			errAdd := updater.AddKey(newValues)
			assert.NoError(t, errAdd, "expected no error on adding new keys")
			//Only Subscriber 2 should receive these messages
			awaitingMessages2 = updateAwaitingMessages(awaitingMessages2, newValues)
		})

		t.Run("verify messages received for non-empty keys", func(t *testing.T) {
			verifyMessages(t, processedC1, awaitingMessages1)
		})

		t.Run("verify messages received for empty keys", func(t *testing.T) {
			verifyMessages(t, processedC2, awaitingMessages2)
		})
	}

	if config.HasOperation("unsubscribe") {
		t.Run("Unsubscribe from all subscriptions", func(t *testing.T) {
			for _, id := range subscribeIDs {
				err := store.Unsubscribe(context.Background(),
					&configuration.UnsubscribeRequest{
						ID: id,
					},
				)
				assert.NoError(t, err, "expected no error in unsubscribe")
			}
		})

		//Update existing keys
		t.Run("Update keys again", func(t *testing.T) {
			updatedValues := updateKeyValues(initValues, runID, counter, v1)
			counter += len(initValues)
			errUpdate := updater.UpdateKey(updatedValues)
			assert.NoError(t, errUpdate, "expected no error on updating keys")
		})

		t.Run("verify no messages received after unsubscribe", func(t *testing.T) {
			waiting := true
			timeout := time.After(defaultMaxReadDuration)
			for waiting {
				select {
				case <-processedC1:
					assert.FailNow(t, "expected no messages after unsubscribe")
				case <-processedC2:
					assert.FailNow(t, "expected no messages after unsubscribe")
				case <-timeout:
					waiting = false
				}
			}
		})
	}
}

func verifyMessages(t *testing.T, processedChan chan *configuration.UpdateEvent, awaitingMessages map[string]map[string]struct{}) {
	waiting := true
	timeout := time.After(defaultMaxReadDuration)
	for waiting {
		select {
		case processed := <-processedChan:
			for key, receivedItem := range processed.Items {
				items, keyExists := awaitingMessages[key]
				assert.True(t, keyExists)

				stringReceivedItem := getStringItem(receivedItem)

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
