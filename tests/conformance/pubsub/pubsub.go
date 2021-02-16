// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package pubsub

import (
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"
	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/util/sets"

	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/components-contrib/tests/conformance/utils"
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
	pubsubName             string
	testTopicName          string
	publishMetadata        map[string]string
	subscribeMetadata      map[string]string
	messageCount           int
	maxReadDuration        time.Duration
	waitDurationToPublish  time.Duration
	checkInOrderProcessing bool
}

func NewTestConfig(componentName string, allOperations bool, operations []string, config map[string]string) TestConfig {
	tc := TestConfig{
		CommonConfig: utils.CommonConfig{
			ComponentType: "pubsub",
			ComponentName: componentName,
			AllOperations: allOperations,
			Operations:    sets.NewString(operations...)},
		pubsubName:             defaultPubsubName,
		testTopicName:          defaultTopicName,
		messageCount:           defaultMessageCount,
		maxReadDuration:        defaultMaxReadDuration,
		waitDurationToPublish:  defaultWaitDurationToPublish,
		publishMetadata:        map[string]string{},
		subscribeMetadata:      map[string]string{},
		checkInOrderProcessing: defaultCheckInOrderProcessing,
	}

	for k, v := range config {
		switch k {
		case "pubsubName":
			tc.pubsubName = v
		case "testTopicName":
			tc.testTopicName = v
		case "messageCount":
			if val, err := strconv.Atoi(v); err == nil {
				tc.messageCount = val
			}
		case "maxReadDuration":
			if val, err := strconv.Atoi(v); err == nil {
				tc.maxReadDuration = time.Duration(val) * time.Millisecond
			}
		case "waitDurationToPublish":
			if val, err := strconv.Atoi(v); err == nil {
				tc.waitDurationToPublish = time.Duration(val) * time.Millisecond
			}
		case "checkInOrderProcessing":
			if val, err := strconv.ParseBool(v); err == nil {
				tc.checkInOrderProcessing = val
			}
		default:
			if strings.HasPrefix(k, "publish_") {
				tc.publishMetadata[strings.TrimPrefix(k, "publish_")] = v
			} else if strings.HasPrefix(k, "subscribe_") {
				tc.subscribeMetadata[strings.TrimPrefix(k, "subscribe_")] = v
			}
		}
	}

	return tc
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
	runID := uuid.NewV4()
	awaitingMessages := make(map[string]struct{}, 20)
	processedC := make(chan string, config.messageCount*2)
	errorCount := 0
	dataPrefix := "message-" + runID.String() + "-"
	var outOfOrder bool

	// Subscribe
	if config.HasOperation("subscribe") {
		t.Run("subscribe", func(t *testing.T) {
			var counter int
			var lastSequence int
			err := ps.Subscribe(pubsub.SubscribeRequest{
				Topic:    config.testTopicName,
				Metadata: config.subscribeMetadata,
			}, func(msg *pubsub.NewMessage) error {
				dataString := string(msg.Data)
				if !strings.HasPrefix(dataString, dataPrefix) {
					t.Logf("Ignoring message without expected prefix")
					return nil
				}

				counter++

				sequence, err := strconv.Atoi(dataString[len(dataPrefix):])
				if err != nil {
					t.Logf("Message did not contain a sequence number")
					assert.Fail(t, "message did not contain a sequence number")
					return err
				}

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
		time.Sleep(config.waitDurationToPublish)
		t.Run("publish", func(t *testing.T) {
			for k := 1; k <= config.messageCount; k++ {
				data := []byte(fmt.Sprintf("%s%d", dataPrefix, k))
				err := ps.Publish(&pubsub.PublishRequest{
					Data:       data,
					PubsubName: config.pubsubName,
					Topic:      config.testTopicName,
					Metadata:   config.publishMetadata,
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
			t.Logf("waiting for %v to complete read", config.maxReadDuration)
			waiting := true
			for waiting {
				select {
				case processed := <-processedC:
					delete(awaitingMessages, processed)
					waiting = len(awaitingMessages) > 0
				case <-time.After(config.maxReadDuration):
					// Break out after the mamimum read duration has elapsed
					waiting = false
				}
			}
			assert.False(t, config.checkInOrderProcessing && outOfOrder, "received messages out of order")
			assert.Empty(t, awaitingMessages, "expected to read %v messages", config.messageCount)
		})
	}
}
