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
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/components-contrib/tests/conformance/utils"
	"github.com/dapr/kit/config"
)

const (
	defaultPubsubName             = "pubusub"
	defaultTopicName              = "testTopic"
	defaultTopicNameBulk          = "testTopicBulk"
	defaultMultiTopic1Name        = "multiTopic1"
	defaultMultiTopic2Name        = "multiTopic2"
	defaultMessageCount           = 10
	defaultMaxReadDuration        = 60 * time.Second
	defaultWaitDurationToPublish  = 5 * time.Second
	defaultCheckInOrderProcessing = true
	defaultMaxBulkCount           = 5
	defaultMaxBulkAwaitDurationMs = 500
	bulkSubStartingKey            = 1000
)

type TestConfig struct {
	utils.CommonConfig
	PubsubName             string            `mapstructure:"pubsubName"`
	TestTopicName          string            `mapstructure:"testTopicName"`
	TestTopicForBulkSub    string            `mapstructure:"testTopicForBulkSub"`
	TestMultiTopic1Name    string            `mapstructure:"testMultiTopic1Name"`
	TestMultiTopic2Name    string            `mapstructure:"testMultiTopic2Name"`
	PublishMetadata        map[string]string `mapstructure:"publishMetadata"`
	SubscribeMetadata      map[string]string `mapstructure:"subscribeMetadata"`
	BulkSubscribeMetadata  map[string]string `mapstructure:"bulkSubscribeMetadata"`
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
		TestMultiTopic1Name:    defaultMultiTopic1Name,
		TestMultiTopic2Name:    defaultMultiTopic2Name,
		MessageCount:           defaultMessageCount,
		MaxReadDuration:        defaultMaxReadDuration,
		WaitDurationToPublish:  defaultWaitDurationToPublish,
		PublishMetadata:        map[string]string{},
		SubscribeMetadata:      map[string]string{},
		BulkSubscribeMetadata:  map[string]string{},
		CheckInOrderProcessing: defaultCheckInOrderProcessing,
		TestTopicForBulkSub:    defaultTopicNameBulk,
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
			Base: metadata.Base{Properties: props},
		})
		assert.NoError(t, err, "expected no error on setting up pubsub")
	})

	t.Run("ping", func(t *testing.T) {
		err := pubsub.Ping(ps)
		// TODO: Ideally, all stable components should implenment ping function,
		// so will only assert assert.Nil(t, err) finally, i.e. when current implementation
		// implements ping in existing stable components
		if err != nil {
			assert.EqualError(t, err, "ping is not implemented by this pubsub")
		} else {
			assert.Nil(t, err)
		}
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
	ctx := context.Background()
	awaitingMessagesBulk := make(map[string]struct{}, 20)
	processedMessagesBulk := make(map[int]struct{}, 20)
	processedCBulk := make(chan string, config.MessageCount*2)
	errorCountBulk := 0
	var muBulk sync.Mutex

	// Subscribe
	if config.HasOperation("subscribe") { //nolint:nestif
		t.Run("subscribe", func(t *testing.T) {
			var counter int
			var lastSequence int
			err := ps.Subscribe(ctx, pubsub.SubscribeRequest{
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

				// Only consider order when we receive a message for the first time
				// Messages that fail and are re-queued will naturally come out of order
				if errorCount == 0 {
					if sequence < lastSequence {
						outOfOrder = true
						t.Logf("Message received out of order: expected sequence >= %d, got %d", lastSequence, sequence)
					}

					lastSequence = sequence
				}

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

	// Bulk Subscribe
	if config.HasOperation("bulksubscribe") { //nolint:nestif
		t.Run("bulkSubscribe", func(t *testing.T) {
			bS, ok := ps.(pubsub.BulkSubscriber)
			if !ok {
				t.Fatalf("cannot run bulkSubscribe conformance, BulkSubscriber interface not implemented by the component %s", config.ComponentName)
			}
			var counter int
			var lastSequence int
			config.BulkSubscribeMetadata[metadata.MaxBulkSubCountKey] = strconv.Itoa(defaultMaxBulkCount)
			config.BulkSubscribeMetadata[metadata.MaxBulkSubAwaitDurationMsKey] = strconv.Itoa(defaultMaxBulkAwaitDurationMs)
			err := bS.BulkSubscribe(ctx, pubsub.SubscribeRequest{
				Topic:    config.TestTopicForBulkSub,
				Metadata: config.BulkSubscribeMetadata,
			}, func(ctx context.Context, bulkMsg *pubsub.BulkMessage) ([]pubsub.BulkSubscribeResponseEntry, error) {
				bulkResponses := make([]pubsub.BulkSubscribeResponseEntry, len(bulkMsg.Entries))
				hasAnyError := false
				for i, msg := range bulkMsg.Entries {
					dataString := string(msg.Event)
					if !strings.HasPrefix(dataString, dataPrefix) {
						t.Logf("Ignoring message without expected prefix")
						bulkResponses[i].EntryId = msg.EntryId
						bulkResponses[i].Error = nil
						continue
					}
					sequence, err := strconv.Atoi(dataString[len(dataPrefix):])
					if err != nil {
						t.Logf("Message did not contain a sequence number")
						assert.Fail(t, "message did not contain a sequence number")
						bulkResponses[i].EntryId = msg.EntryId
						bulkResponses[i].Error = err
						hasAnyError = true
						continue
					}
					// Ignore already processed messages
					// in case we receive a redelivery from the broker
					// during retries.
					muBulk.Lock()
					_, alreadyProcessed := processedMessagesBulk[sequence]
					muBulk.Unlock()
					if alreadyProcessed {
						t.Logf("Message was already processed: %d", sequence)

						bulkResponses[i].EntryId = msg.EntryId
						bulkResponses[i].Error = nil
						continue
					}
					counter++

					// Only consider order when we receive a message for the first time
					// Messages that fail and are re-queued will naturally come out of order
					if errorCountBulk == 0 {
						if sequence < lastSequence {
							outOfOrder = true
							t.Logf("Message received out of order: expected sequence >= %d, got %d", lastSequence, sequence)
						}

						lastSequence = sequence
					}

					// This behavior is standard to repro a failure of one message in a batch.
					if errorCountBulk < 2 || counter%5 == 0 {
						// First message errors just to give time for more messages to pile up.
						// Second error is to force an error in a batch.
						errorCountBulk++
						// Sleep to allow messages to pile up and be delivered as a batch.
						time.Sleep(1 * time.Second)
						t.Logf("Simulating subscriber error")

						bulkResponses[i].EntryId = msg.EntryId
						bulkResponses[i].Error = errors.Errorf("conf test simulated error")
						hasAnyError = true
						continue
					}

					t.Logf("Simulating subscriber success")
					actualReadCount++

					muBulk.Lock()
					processedMessagesBulk[sequence] = struct{}{}
					muBulk.Unlock()

					processedCBulk <- dataString

					bulkResponses[i].EntryId = msg.EntryId
					bulkResponses[i].Error = nil
				}
				if hasAnyError {
					return bulkResponses, errors.Errorf("Few messages errorred out")
				}
				return bulkResponses, nil
			})
			assert.NoError(t, err, "expected no error on bulk subscribe")
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
				assert.NoError(t, err, "expected no error on publishing data %s on topic %s", data, config.TestTopicName)
			}
			if config.HasOperation("bulksubscribe") {
				_, ok := ps.(pubsub.BulkSubscriber)
				if !ok {
					t.Fatalf("cannot run bulkSubscribe conformance, BulkSubscriber interface not implemented by the component %s", config.ComponentName)
				}
				for k := bulkSubStartingKey; k <= (bulkSubStartingKey + config.MessageCount); k++ {
					data := []byte(fmt.Sprintf("%s%d", dataPrefix, k))
					err := ps.Publish(&pubsub.PublishRequest{
						Data:       data,
						PubsubName: config.PubsubName,
						Topic:      config.TestTopicForBulkSub,
						Metadata:   config.PublishMetadata,
					})
					if err == nil {
						awaitingMessagesBulk[string(data)] = struct{}{}
					}
					assert.NoError(t, err, "expected no error on publishing data %s on topic %s", data, config.TestTopicForBulkSub)
				}
			}
		})
	}

	// assumes that publish operation is run only once for publishing config.MessageCount number of events
	// bulkpublish needs to be run after publish operation
	if config.HasOperation("bulkpublish") {
		t.Run("bulkPublish", func(t *testing.T) {
			bP, ok := ps.(pubsub.BulkPublisher)
			if !ok {
				t.Fatalf("cannot run bulkPublish conformance, BulkPublisher interface not implemented by the component %s", config.ComponentName)
			}
			// only run the test if BulkPublish is implemented
			// Some pubsub, like Kafka need to wait for Subscriber to be up before messages can be consumed.
			// So, wait for some time here.
			time.Sleep(config.WaitDurationToPublish)
			req := pubsub.BulkPublishRequest{
				PubsubName: config.PubsubName,
				Topic:      config.TestTopicName,
				Metadata:   config.PublishMetadata,
				Entries:    make([]pubsub.BulkMessageEntry, config.MessageCount),
			}
			entryMap := map[string][]byte{}
			// setting k to one value more than the previously published list of events.
			// assuming that publish test is run only once and bulkPublish is run right after that
			for i, k := 0, config.MessageCount+1; i < config.MessageCount; {
				data := []byte(fmt.Sprintf("%s%d", dataPrefix, k))
				strK := strconv.Itoa(k)
				req.Entries[i].EntryId = strK
				req.Entries[i].ContentType = "text/plain"
				req.Entries[i].Metadata = config.PublishMetadata
				req.Entries[i].Event = data
				entryMap[strK] = data
				t.Logf("Adding message with ID %d for bulk publish", k)
				k++
				i++
			}

			t.Logf("Calling Bulk Publish on component %s", config.ComponentName)
			res, err := bP.BulkPublish(context.Background(), &req)
			if err == nil {
				for _, status := range res.Statuses {
					if status.Status == pubsub.PublishSucceeded {
						data := entryMap[status.EntryId]
						t.Logf("adding to awaited messages %s", data)
						awaitingMessages[string(data)] = struct{}{}
					}
				}
			}
			// here only the success case is tested for bulkPublish similar to publish.
			// For scenarios on partial failures, those will be tested as part of certification tests if possible.
			assert.NoError(t, err, "expected no error on bulk publishing on topic %s", config.TestTopicName)
		})
	}

	// Verify read
	if (config.HasOperation("publish") || config.HasOperation("bulkpublish")) && config.HasOperation("subscribe") {
		t.Run("verify read", func(t *testing.T) {
			t.Logf("waiting for %v to complete read", config.MaxReadDuration)
			timeout := time.After(config.MaxReadDuration)
			waiting := true
			for waiting {
				select {
				case processed := <-processedC:
					t.Logf("deleting %s processed message", processed)
					delete(awaitingMessages, processed)
					waiting = len(awaitingMessages) > 0
				case <-timeout:
					// Break out after the mamimum read duration has elapsed
					waiting = false
				}
			}
			assert.False(t, config.CheckInOrderProcessing && outOfOrder, "received messages out of order")
			assert.Empty(t, awaitingMessages, "expected to read %v messages", config.MessageCount)
		})
	}

	// Verify read on bulk subscription
	if config.HasOperation("publish") && config.HasOperation("bulksubscribe") {
		t.Run("verify read on bulk subscription", func(t *testing.T) {
			_, ok := ps.(pubsub.BulkSubscriber)
			if !ok {
				t.Fatalf("cannot run bulkSubscribe conformance, BulkSubscriber interface not implemented by the component %s", config.ComponentName)
			}
			t.Logf("waiting for %v to complete read for bulk subscription", config.MaxReadDuration)
			timeout := time.After(config.MaxReadDuration)
			waiting := true
			for waiting {
				select {
				case processed := <-processedCBulk:
					delete(awaitingMessagesBulk, processed)
					waiting = len(awaitingMessagesBulk) > 0
				case <-timeout:
					// Break out after the mamimum read duration has elapsed
					waiting = false
				}
			}
			assert.False(t, config.CheckInOrderProcessing && outOfOrder, "received messages out of order")
			assert.Empty(t, awaitingMessagesBulk, "expected to read %v messages", config.MessageCount)
		})
	}

	// Multiple handlers
	if config.HasOperation("multiplehandlers") {
		received1Ch := make(chan string)
		received2Ch := make(chan string)
		subscribe1Ctx, subscribe1Cancel := context.WithCancel(context.Background())
		subscribe2Ctx, subscribe2Cancel := context.WithCancel(context.Background())
		defer func() {
			subscribe1Cancel()
			subscribe2Cancel()
			close(received1Ch)
			close(received2Ch)
		}()

		t.Run("mutiple handlers", func(t *testing.T) {
			createMultiSubscriber(t, subscribe1Ctx, received1Ch, ps, config.TestMultiTopic1Name, config.SubscribeMetadata, dataPrefix)
			createMultiSubscriber(t, subscribe2Ctx, received2Ch, ps, config.TestMultiTopic2Name, config.SubscribeMetadata, dataPrefix)

			sent1Ch := make(chan string)
			sent2Ch := make(chan string)
			allSentCh := make(chan bool)
			defer func() {
				close(sent1Ch)
				close(sent2Ch)
				close(allSentCh)
			}()
			wait := receiveInBackground(t, config.MaxReadDuration, received1Ch, received2Ch, sent1Ch, sent2Ch, allSentCh)

			for k := (config.MessageCount + 1); k <= (config.MessageCount * 2); k++ {
				data := []byte(fmt.Sprintf("%s%d", dataPrefix, k))
				var topic string
				if k%2 == 0 {
					topic = config.TestMultiTopic1Name
					sent1Ch <- string(data)
				} else {
					topic = config.TestMultiTopic2Name
					sent2Ch <- string(data)
				}
				err := ps.Publish(&pubsub.PublishRequest{
					Data:       data,
					PubsubName: config.PubsubName,
					Topic:      topic,
					Metadata:   config.PublishMetadata,
				})
				assert.NoError(t, err, "expected no error on publishing data %s on topic %s", data, topic)
			}
			allSentCh <- true
			t.Logf("waiting for %v to complete read", config.MaxReadDuration)
			<-wait
		})

		t.Run("stop subscribers", func(t *testing.T) {
			sent1Ch := make(chan string)
			sent2Ch := make(chan string)
			allSentCh := make(chan bool)
			defer func() {
				close(allSentCh)
			}()

			for i := 0; i < 3; i++ {
				switch i {
				case 1: // On iteration 1, close the first subscriber
					subscribe1Cancel()
					close(sent1Ch)
					sent1Ch = nil
					time.Sleep(config.WaitDurationToPublish)
				case 2: // On iteration 2, close the second subscriber
					subscribe2Cancel()
					close(sent2Ch)
					sent2Ch = nil
					time.Sleep(config.WaitDurationToPublish)
				}

				wait := receiveInBackground(t, config.MaxReadDuration, received1Ch, received2Ch, sent1Ch, sent2Ch, allSentCh)

				offset := config.MessageCount * (i + 2)
				for k := offset + 1; k <= (offset + config.MessageCount); k++ {
					data := []byte(fmt.Sprintf("%s%d", dataPrefix, k))
					var topic string
					if k%2 == 0 {
						topic = config.TestMultiTopic1Name
						if sent1Ch != nil {
							sent1Ch <- string(data)
						}
					} else {
						topic = config.TestMultiTopic2Name
						if sent2Ch != nil {
							sent2Ch <- string(data)
						}
					}
					err := ps.Publish(&pubsub.PublishRequest{
						Data:       data,
						PubsubName: config.PubsubName,
						Topic:      topic,
						Metadata:   config.PublishMetadata,
					})
					assert.NoError(t, err, "expected no error on publishing data %s on topic %s", data, topic)
				}

				allSentCh <- true
				t.Logf("waiting for %v to complete read", config.MaxReadDuration)
				<-wait
			}
		})
	}
}

func receiveInBackground(t *testing.T, timeout time.Duration, received1Ch <-chan string, received2Ch <-chan string, sent1Ch <-chan string, sent2Ch <-chan string, allSentCh <-chan bool) <-chan struct{} {
	done := make(chan struct{})

	go func() {
		receivedTopic1 := make([]string, 0)
		expectedTopic1 := make([]string, 0)
		receivedTopic2 := make([]string, 0)
		expectedTopic2 := make([]string, 0)
		to := time.NewTimer(timeout)
		allSent := false

		defer func() {
			to.Stop()
			close(done)
		}()

		for {
			select {
			case msg := <-received1Ch:
				receivedTopic1 = append(receivedTopic1, msg)
			case msg := <-received2Ch:
				receivedTopic2 = append(receivedTopic2, msg)
			case msg := <-sent1Ch:
				expectedTopic1 = append(expectedTopic1, msg)
			case msg := <-sent2Ch:
				expectedTopic2 = append(expectedTopic2, msg)
			case v := <-allSentCh:
				allSent = v
			case <-to.C:
				assert.Failf(t, "timeout while waiting for messages in multihandlers", "receivedTopic1=%v expectedTopic1=%v receivedTopic2=%v expectedTopic2=%v", receivedTopic1, expectedTopic1, receivedTopic2, expectedTopic2)
				return
			}

			if allSent && compareReceivedAndExpected(receivedTopic1, expectedTopic1) && compareReceivedAndExpected(receivedTopic2, expectedTopic2) {
				return
			}
		}
	}()

	return done
}

func compareReceivedAndExpected(received []string, expected []string) bool {
	sort.Strings(received)
	sort.Strings(expected)
	return reflect.DeepEqual(received, expected)
}

func createMultiSubscriber(t *testing.T, subscribeCtx context.Context, ch chan<- string, ps pubsub.PubSub, topic string, subscribeMetadata map[string]string, dataPrefix string) {
	err := ps.Subscribe(subscribeCtx, pubsub.SubscribeRequest{
		Topic:    topic,
		Metadata: subscribeMetadata,
	}, func(ctx context.Context, msg *pubsub.NewMessage) error {
		dataString := string(msg.Data)
		if !strings.HasPrefix(dataString, dataPrefix) {
			t.Log("Ignoring message without expected prefix", dataString)
			return nil
		}
		ch <- string(msg.Data)
		return nil
	})
	require.NoError(t, err, "expected no error on subscribe")
}
