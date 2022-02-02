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

package pubsub

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"

	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/components-contrib/tests/conformance/utils"
	"github.com/dapr/kit/config"
)

const (
	defaultPubsubName             = "pubusub"
	defaultTopicName              = "testTopic"
	defaultMessageCount           = 10
	defaultMaxReadDuration        = 60 * time.Second
	defaultWaitDurationToPublish  = 5 * time.Second
	defaultCheckInOrderProcessing = true
)

type TestConfig struct {
	utils.CommonConfig
	PubsubName             string            `mapstructure:"pubsubName"`
	TestTopicName          string            `mapstructure:"testTopicName"`
	PublishMetadata        map[string]string `mapstructure:"publishMetadata"`
	SubscribeMetadata      map[string]string `mapstructure:"subscribeMetadata"`
	MessageCount           int               `mapstructure:"messageCount"`
	MaxReadDuration        time.Duration     `mapstructure:"maxReadDuration"`
	WaitDurationToPublish  time.Duration     `mapstructure:"waitDurationToPublish"`
	CheckInOrderProcessing bool              `mapstructure:"checkInOrderProcessing"`
}

func NewTestConfig(componentName string, allOperations bool, operations []string, configMap map[string]interface{}) (TestConfig, error) {
	// Populate defaults
	tc := TestConfig{
		CommonConfig: utils.CommonConfig{
			ComponentType: "pubsub",
			ComponentName: componentName,
			AllOperations: allOperations,
			Operations:    utils.NewStringSet(operations...),
		},
		PubsubName:             defaultPubsubName,
		TestTopicName:          defaultTopicName,
		MessageCount:           defaultMessageCount,
		MaxReadDuration:        defaultMaxReadDuration,
		WaitDurationToPublish:  defaultWaitDurationToPublish,
		PublishMetadata:        map[string]string{},
		SubscribeMetadata:      map[string]string{},
		CheckInOrderProcessing: defaultCheckInOrderProcessing,
	}

	err := config.Decode(configMap, &tc)

	return tc, err
}

func ConformanceTests(t *testing.T, props map[string]string, ps pubsub.PubSub, config TestConfig) {
	// Properly close pubsub
	defer ps.Close()

	actualReadCount := 0

	// Init
	t.Run("init", func(t *testing.T) {
		err := ps.Init(pubsub.Metadata{
			Properties: props,
		})
		assert.NoError(t, err, "expected no error on setting up pubsub")
	})

	// Generate a unique ID for this run to isolate messages to this test
	// and prevent messages still stored in a locally running broker
	// from being considered as part of this test.
	runID := uuid.Must(uuid.NewRandom()).String()
	awaitingMessages := make(map[string]struct{}, 20)
	var mu sync.Mutex
	processedMessages := make(map[int]struct{}, 20)
	processedC := make(chan string, config.MessageCount*2)
	errorCount := 0
	dataPrefix := "message-" + runID + "-"
	var outOfOrder bool

	// Subscribe
	if config.HasOperation("subscribe") { // nolint: nestif
		t.Run("subscribe", func(t *testing.T) {
			var counter int
			var lastSequence int
			err := ps.Subscribe(pubsub.SubscribeRequest{
				Topic:    config.TestTopicName,
				Metadata: config.SubscribeMetadata,
			}, func(ctx context.Context, msg *pubsub.NewMessage) error {
				dataString := string(msg.Data)
				if !strings.HasPrefix(dataString, dataPrefix) {
					t.Logf("Ignoring message without expected prefix")

					return nil
				}

				sequence, err := strconv.Atoi(dataString[len(dataPrefix):])
				if err != nil {
					t.Logf("Message did not contain a sequence number")
					assert.Fail(t, "message did not contain a sequence number")

					return err
				}

				// Ignore already processed messages
				// in case we receive a redelivery from the broker
				// during retries.
				mu.Lock()
				_, alreadyProcessed := processedMessages[sequence]
				mu.Unlock()
				if alreadyProcessed {
					t.Logf("Message was already processed: %d", sequence)

					return nil
				}

				counter++

				if sequence < lastSequence {
					outOfOrder = true
					t.Logf("Message received out of order: expected sequence >= %d, got %d", lastSequence, sequence)
				}

				lastSequence = sequence

				// This behavior is standard to repro a failure of one message in a batch.
				if errorCount < 2 || counter%5 == 0 {
					// First message errors just to give time for more messages to pile up.
					// Second error is to force an error in a batch.
					errorCount++
					// Sleep to allow messages to pile up and be delivered as a batch.
					time.Sleep(1 * time.Second)
					t.Logf("Simulating subscriber error")

					return errors.Errorf("conf test simulated error")
				}

				t.Logf("Simulating subscriber success")
				actualReadCount++

				mu.Lock()
				processedMessages[sequence] = struct{}{}
				mu.Unlock()

				processedC <- dataString

				return nil
			})
			assert.NoError(t, err, "expected no error on subscribe")
		})
	}

	// Publish
	if config.HasOperation("publish") {
		// Some pubsub, like Kafka need to wait for Subscriber to be up before messages can be consumed.
		// So, wait for some time here.
		time.Sleep(config.WaitDurationToPublish)
		t.Run("publish", func(t *testing.T) {
			for k := 1; k <= config.MessageCount; k++ {
				data := []byte(fmt.Sprintf("%s%d", dataPrefix, k))
				err := ps.Publish(&pubsub.PublishRequest{
					Data:       data,
					PubsubName: config.PubsubName,
					Topic:      config.TestTopicName,
					Metadata:   config.PublishMetadata,
				})
				if err == nil {
					awaitingMessages[string(data)] = struct{}{}
				}
				assert.NoError(t, err, "expected no error on publishing data %s", data)
			}
		})
	}

	// Verify read
	if config.HasOperation("subscribe") {
		t.Run("verify read", func(t *testing.T) {
			t.Logf("waiting for %v to complete read", config.MaxReadDuration)
			timer := time.NewTimer(config.MaxReadDuration)
			defer timer.Stop()
			waiting := true
			for waiting {
				select {
				case processed := <-processedC:
					delete(awaitingMessages, processed)
					waiting = len(awaitingMessages) > 0
				case <-timer.C:
					// Break out after the mamimum read duration has elapsed
					waiting = false
				}
			}
			assert.False(t, config.CheckInOrderProcessing && outOfOrder, "received messages out of order")
			assert.Empty(t, awaitingMessages, "expected to read %v messages", config.MessageCount)
		})
	}
}
