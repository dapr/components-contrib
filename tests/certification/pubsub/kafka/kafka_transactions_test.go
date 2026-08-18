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

package kafka_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	contribMetadata "github.com/dapr/components-contrib/metadata"
	contribPubsub "github.com/dapr/components-contrib/pubsub"
	pubsub_kafka "github.com/dapr/components-contrib/pubsub/kafka"
	"github.com/dapr/kit/logger"

	"github.com/dapr/components-contrib/tests/certification/flow"
	"github.com/dapr/components-contrib/tests/certification/flow/dockercompose"
	"github.com/dapr/components-contrib/tests/certification/flow/network"
	"github.com/dapr/components-contrib/tests/certification/flow/retry"
)

const txnTokenKey = "__txnToken"

// txnProps builds component metadata for the transactions scenarios.
func txnProps(extra map[string]string) map[string]string {
	props := map[string]string{
		"brokers":       strings.Join(brokers, ","),
		"authType":      "none",
		"initialOffset": "oldest",
	}
	for k, v := range extra {
		props[k] = v
	}
	return props
}

func newTxnPubSub(ctx flow.Context, props map[string]string) contribPubsub.PubSub {
	k := pubsub_kafka.NewKafka(logger.NewLogger("kafka-txn-cert"))
	err := k.Init(ctx, contribPubsub.Metadata{
		Base: contribMetadata.Base{Properties: props},
	})
	require.NoError(ctx, err)
	return k
}

// readTopicRecords drains every partition of topic with the given isolation
// level, collecting record values that arrive within the window.
func readTopicRecords(topic string, isolation sarama.IsolationLevel, window time.Duration) ([]string, error) {
	config := sarama.NewConfig()
	config.Version = sarama.V2_0_0_0 //nolint:nosnakecase
	config.ClientID = "kafka-txn-cert-reader"
	config.Consumer.IsolationLevel = isolation

	client, err := sarama.NewConsumer(brokers, config)
	if err != nil {
		return nil, err
	}
	defer client.Close()

	partitions, err := client.Partitions(topic)
	if err != nil {
		return nil, err
	}

	consumers := make([]sarama.PartitionConsumer, 0, len(partitions))
	for _, partition := range partitions {
		pc, err := client.ConsumePartition(topic, partition, sarama.OffsetOldest)
		if err != nil {
			for _, opened := range consumers {
				opened.Close()
			}
			return nil, err
		}
		consumers = append(consumers, pc)
	}

	var (
		mu      sync.Mutex
		records []string
		wg      sync.WaitGroup
	)
	for _, pc := range consumers {
		wg.Add(1)
		go func(pc sarama.PartitionConsumer) {
			defer wg.Done()
			defer pc.Close()
			timer := time.NewTimer(window)
			defer timer.Stop()
			for {
				select {
				case msg := <-pc.Messages():
					mu.Lock()
					records = append(records, string(msg.Value))
					mu.Unlock()
				case <-timer.C:
					return
				}
			}
		}(pc)
	}
	wg.Wait()
	return records, nil
}

// newRawTxnProducer creates a plain sarama transactional producer used to
// seed topics with committed and aborted records.
func newRawTxnProducer(transactionalID string) (sarama.SyncProducer, error) {
	config := sarama.NewConfig()
	config.Version = sarama.V2_0_0_0 //nolint:nosnakecase
	config.ClientID = "kafka-txn-cert-seeder"
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Return.Successes = true
	config.Producer.Idempotent = true
	config.Producer.Transaction.ID = transactionalID
	config.Net.MaxOpenRequests = 1
	return sarama.NewSyncProducer(brokers, config)
}

// fetchCommittedOffset returns the consumer group's committed offset for one
// partition (-1 when nothing is committed yet).
func fetchCommittedOffset(group, topic string, partition int32) (int64, error) {
	config := sarama.NewConfig()
	config.Version = sarama.V2_0_0_0 //nolint:nosnakecase
	admin, err := sarama.NewClusterAdmin(brokers, config)
	if err != nil {
		return -1, err
	}
	defer admin.Close()

	resp, err := admin.ListConsumerGroupOffsets(group, map[string][]int32{topic: {partition}})
	if err != nil {
		return -1, err
	}
	block := resp.GetBlock(topic, partition)
	if block == nil {
		return -1, errors.New("no offset block returned")
	}
	if block.Err != sarama.ErrNoError {
		return -1, block.Err
	}
	return block.Offset, nil
}

func TestKafkaTransactions(t *testing.T) {
	flow.New(t, "kafka transactions certification").
		Step(dockercompose.Run(clusterName, dockerComposeYAML)).
		Step("wait for broker sockets", network.WaitForAddresses(5*time.Minute, brokers...)).
		Step("wait", flow.Sleep(5*time.Second)).
		Step("wait for kafka readiness", retry.Do(10*time.Second, 30, func(ctx flow.Context) error {
			config := sarama.NewConfig()
			config.ClientID = "test-consumer"
			client, err := sarama.NewConsumer(brokers, config)
			if err != nil {
				return err
			}
			defer client.Close()
			_, err = client.ConsumePartition("myTopic", 0, sarama.OffsetOldest)
			return err
		})).
		//
		// Scenario 1: transactional bulk publish is atomic. A batch with an
		// oversized entry fails as a whole: entries sent before the failure
		// are aborted with the transaction and never become visible to
		// read_committed consumers. Without transactions those entries would
		// remain visible (partial batch).
		Step("transactional bulk publish is atomic", func(ctx flow.Context) error {
			topic := "txn-atomic-" + uuid.NewString()
			pub := newTxnPubSub(ctx, txnProps(map[string]string{
				"producerTransactionsEnabled": "true",
				"maxMessageBytes":             "1024",
				"consumerGroup":               "unused-" + uuid.NewString(),
			}))
			defer pub.Close()

			bulkPub, ok := pub.(contribPubsub.BulkPublisher)
			require.True(ctx, ok, "kafka pubsub must implement BulkPublisher")

			oversized := strings.Repeat("x", 8192)
			_, err := bulkPub.BulkPublish(ctx, &contribPubsub.BulkPublishRequest{
				Topic: topic,
				Entries: []contribPubsub.BulkMessageEntry{
					{EntryId: "0", Event: []byte("small-1")},
					{EntryId: "1", Event: []byte(oversized)},
					{EntryId: "2", Event: []byte("small-2")},
				},
			})
			require.Error(ctx, err, "bulk publish with an oversized entry must fail")

			records, err := readTopicRecords(topic, sarama.ReadCommitted, 5*time.Second)
			require.NoError(ctx, err)
			require.Empty(ctx, records, "aborted batch must not be visible to read_committed consumers")

			// Control: a valid batch commits and is fully visible (also
			// proves the reader window works against this topic).
			_, err = bulkPub.BulkPublish(ctx, &contribPubsub.BulkPublishRequest{
				Topic: topic,
				Entries: []contribPubsub.BulkMessageEntry{
					{EntryId: "0", Event: []byte("ok-1")},
					{EntryId: "1", Event: []byte("ok-2")},
					{EntryId: "2", Event: []byte("ok-3")},
				},
			})
			require.NoError(ctx, err)

			records, err = readTopicRecords(topic, sarama.ReadCommitted, 5*time.Second)
			require.NoError(ctx, err)
			require.ElementsMatch(ctx, []string{"ok-1", "ok-2", "ok-3"}, records)
			return nil
		}).
		//
		// Scenario 2: consumerIsolationLevel. A record from an aborted
		// transaction is delivered by a default (read_uncommitted) component
		// but hidden from a read_committed one.
		Step("read_committed subscriber does not see aborted records", func(ctx flow.Context) error {
			topic := "txn-isolation-" + uuid.NewString()

			seeder, err := newRawTxnProducer("txn-cert-seeder-" + uuid.NewString())
			require.NoError(ctx, err)
			defer seeder.Close()

			// The same key pins ghost and real to one partition, so a
			// subscriber that delivered "real" has necessarily fetched past
			// "ghost"'s offset.
			require.NoError(ctx, seeder.BeginTxn())
			_, _, err = seeder.SendMessage(&sarama.ProducerMessage{Topic: topic, Key: sarama.StringEncoder("iso-key"), Value: sarama.StringEncoder("ghost")})
			require.NoError(ctx, err)
			require.NoError(ctx, seeder.AbortTxn())

			require.NoError(ctx, seeder.BeginTxn())
			_, _, err = seeder.SendMessage(&sarama.ProducerMessage{Topic: topic, Key: sarama.StringEncoder("iso-key"), Value: sarama.StringEncoder("real")})
			require.NoError(ctx, err)
			require.NoError(ctx, seeder.CommitTxn())

			subscribeCollect := func(isolation string) (func() []string, contribPubsub.PubSub) {
				sub := newTxnPubSub(ctx, txnProps(map[string]string{
					"consumerGroup":          "iso-" + isolation + "-" + uuid.NewString(),
					"consumerIsolationLevel": isolation,
					"consumeRetryEnabled":    "false",
				}))
				var mu sync.Mutex
				var seen []string
				err := sub.Subscribe(ctx, contribPubsub.SubscribeRequest{Topic: topic}, func(_ context.Context, msg *contribPubsub.NewMessage) error {
					mu.Lock()
					seen = append(seen, string(msg.Data))
					mu.Unlock()
					return nil
				})
				if err != nil {
					// Don't leak the component's clients on the failure path.
					sub.Close()
				}
				require.NoError(ctx, err)
				return func() []string {
					mu.Lock()
					defer mu.Unlock()
					return append([]string(nil), seen...)
				}, sub
			}

			uncommittedSeen, uncommittedSub := subscribeCollect("read_uncommitted")
			defer uncommittedSub.Close()
			committedSeen, committedSub := subscribeCollect("read_committed")
			defer committedSub.Close()

			require.Eventually(ctx, func() bool {
				u, c := uncommittedSeen(), committedSeen()
				return slices.Contains(u, "ghost") && slices.Contains(u, "real") && slices.Contains(c, "real")
			}, 90*time.Second, 500*time.Millisecond, "expected read_uncommitted to deliver ghost+real and read_committed to deliver real")

			// "real" shares ghost's partition and was delivered, so the
			// read_committed subscriber has fetched past ghost's offset —
			// the ghost can no longer arrive late.
			require.NotContains(ctx, committedSeen(), "ghost", "read_committed subscriber must never see aborted records")
			return nil
		}).
		//
		// Scenario 3: consume-transform-produce. The handler publishes an
		// output with the delivery's transaction token and fails twice before
		// succeeding. The failed attempts' outputs are aborted (never visible
		// to read_committed), exactly one output commits, and the input
		// offset commits atomically with it.
		Step("consume-transform-produce commits outputs and offset atomically", func(ctx flow.Context) error {
			inTopic := "txn-ctp-in-" + uuid.NewString()
			outTopic := "txn-ctp-out-" + uuid.NewString()
			group := "ctp-" + uuid.NewString()

			seeder, err := newRawTxnProducer("txn-cert-ctp-seeder-" + uuid.NewString())
			require.NoError(ctx, err)
			defer seeder.Close()
			require.NoError(ctx, seeder.BeginTxn())
			partition, offset, err := seeder.SendMessage(&sarama.ProducerMessage{Topic: inTopic, Value: sarama.StringEncoder("input-1")})
			require.NoError(ctx, err)
			require.NoError(ctx, seeder.CommitTxn())

			comp := newTxnPubSub(ctx, txnProps(map[string]string{
				"consumerGroup":               "",
				"consumerID":                  group,
				"consumerTransactionsEnabled": "true",
				"consumeRetryEnabled":         "true",
				"backOffDuration":             "200ms",
				"backOffMaxRetries":           "10",
			}))
			defer comp.Close()

			var attempts atomic.Int32
			err = comp.Subscribe(ctx, contribPubsub.SubscribeRequest{Topic: inTopic}, func(hctx context.Context, msg *contribPubsub.NewMessage) error {
				token := msg.Metadata[txnTokenKey]
				if token == "" {
					return errors.New("delivery is missing the transaction token")
				}
				n := attempts.Add(1)
				if perr := comp.Publish(hctx, &contribPubsub.PublishRequest{
					Topic:    outTopic,
					Data:     []byte(fmt.Sprintf("out-attempt-%d", n)),
					Metadata: map[string]string{txnTokenKey: token},
				}); perr != nil {
					return perr
				}
				if n <= 2 {
					return errors.New("simulated processing failure")
				}
				return nil
			})
			require.NoError(ctx, err)

			require.Eventually(ctx, func() bool {
				return attempts.Load() >= 3
			}, 90*time.Second, 500*time.Millisecond, "expected the handler to be retried until success")

			// The input offset committed atomically with the output; once it
			// is visible, all transactions for this delivery are closed.
			require.Eventually(ctx, func() bool {
				committedOffset, oerr := fetchCommittedOffset(group, inTopic, partition)
				return oerr == nil && committedOffset == offset+1
			}, 60*time.Second, time.Second, "expected the input offset to be committed transactionally")

			// Exactly one output is visible to read_committed consumers: the
			// successful attempt's. The two aborted attempts' outputs exist in
			// the log but are hidden.
			committed, err := readTopicRecords(outTopic, sarama.ReadCommitted, 5*time.Second)
			require.NoError(ctx, err)
			require.Equal(ctx, []string{"out-attempt-3"}, committed, "only the successful attempt's output may be visible")

			uncommitted, err := readTopicRecords(outTopic, sarama.ReadUncommitted, 5*time.Second)
			require.NoError(ctx, err)
			require.ElementsMatch(ctx, []string{"out-attempt-1", "out-attempt-2", "out-attempt-3"}, uncommitted,
				"the aborted attempts' outputs exist in the log but only as aborted records")
			return nil
		}).
		//
		// Scenario 4: bulk subscribe is all-or-nothing. Every attempt
		// publishes an output into the batch transaction before the first
		// attempt fails a single entry: the whole batch (outputs included)
		// aborts, is redelivered whole, and offsets plus outputs commit only
		// when every entry succeeds.
		Step("bulk subscribe batch is all-or-nothing", func(ctx flow.Context) error {
			topic := "txn-bulk-" + uuid.NewString()
			outTopic := topic + "-out"
			group := "bulk-" + uuid.NewString()

			seeder, err := newRawTxnProducer("txn-cert-bulk-seeder-" + uuid.NewString())
			require.NoError(ctx, err)
			defer seeder.Close()
			require.NoError(ctx, seeder.BeginTxn())
			var partition int32
			var lastOffset int64
			for i := range 3 {
				// The same key keeps the whole batch on one partition.
				partition, lastOffset, err = seeder.SendMessage(&sarama.ProducerMessage{
					Topic: topic,
					Key:   sarama.StringEncoder("batch-key"),
					Value: sarama.StringEncoder(fmt.Sprintf("bulk-%d", i)),
				})
				require.NoError(ctx, err)
			}
			require.NoError(ctx, seeder.CommitTxn())

			comp := newTxnPubSub(ctx, txnProps(map[string]string{
				"consumerGroup":               group,
				"consumerTransactionsEnabled": "true",
				"consumeRetryEnabled":         "true",
				"backOffDuration":             "200ms",
				"backOffMaxRetries":           "10",
			}))
			defer comp.Close()

			// The count/ticker race in the bulk buffer can flush a partial
			// first batch, so the handler must not assume 3 entries per
			// invocation: it fails the first batch it sees (whole batch must
			// abort) and succeeds afterwards, publishing one output per
			// invocation INTO the batch transaction before deciding.
			var (
				mu          sync.Mutex
				invocations int
				failedBatch []string
				succeeded   [][]string
			)
			bulkSub, ok := comp.(contribPubsub.BulkSubscriber)
			require.True(ctx, ok, "kafka pubsub must implement BulkSubscriber")
			err = bulkSub.BulkSubscribe(ctx, contribPubsub.SubscribeRequest{
				Topic: topic,
				BulkSubscribeConfig: contribPubsub.BulkSubscribeConfig{
					MaxMessagesCount:   3,
					MaxAwaitDurationMs: 500,
				},
			}, func(_ context.Context, msg *contribPubsub.BulkMessage) ([]contribPubsub.BulkSubscribeResponseEntry, error) {
				values := make([]string, 0, len(msg.Entries))
				for _, e := range msg.Entries {
					values = append(values, string(e.Event))
				}
				mu.Lock()
				invocations++
				n := invocations
				fail := failedBatch == nil
				if fail {
					failedBatch = values
				} else {
					succeeded = append(succeeded, values)
				}
				mu.Unlock()

				// The output joins the batch transaction: on the failing
				// attempt it must be aborted with the batch.
				if perr := comp.Publish(context.Background(), &contribPubsub.PublishRequest{
					Topic:    outTopic,
					Data:     []byte(fmt.Sprintf("bulk-out-%d", n)),
					Metadata: map[string]string{txnTokenKey: msg.Metadata[txnTokenKey]},
				}); perr != nil {
					return nil, perr
				}

				if fail {
					return []contribPubsub.BulkSubscribeResponseEntry{
						{EntryId: msg.Entries[0].EntryId, Error: errors.New("simulated entry failure")},
					}, nil
				}
				return nil, nil
			})
			require.NoError(ctx, err)

			// All three inputs are eventually consumed and their offsets
			// committed — only via full-batch success.
			require.Eventually(ctx, func() bool {
				committedOffset, oerr := fetchCommittedOffset(group, topic, partition)
				return oerr == nil && committedOffset == lastOffset+1
			}, 90*time.Second, time.Second, "expected the batch offsets to commit after full success")

			mu.Lock()
			failed := append([]string(nil), failedBatch...)
			successes := make([][]string, len(succeeded))
			copy(successes, succeeded)
			totalInvocations := invocations
			mu.Unlock()

			require.GreaterOrEqual(ctx, totalInvocations, 2, "the failed batch must have been redelivered")
			// All-or-nothing redelivery: every value of the failed batch is
			// redelivered and eventually processed in a successful batch.
			redelivered := []string{}
			for _, batch := range successes {
				redelivered = append(redelivered, batch...)
			}
			for _, v := range failed {
				require.Contains(ctx, redelivered, v, "a value from the aborted batch was never redelivered")
			}
			require.ElementsMatch(ctx, []string{"bulk-0", "bulk-1", "bulk-2"}, redelivered, "every input processed exactly once across successful batches")

			// The failing attempt's output was aborted with its batch; each
			// successful batch committed exactly one output.
			committedOut, err := readTopicRecords(outTopic, sarama.ReadCommitted, 5*time.Second)
			require.NoError(ctx, err)
			require.Len(ctx, committedOut, len(successes), "one committed output per successful batch, none from the aborted one")
			require.NotContains(ctx, committedOut, "bulk-out-1", "the failed attempt's output must be aborted")

			uncommittedOut, err := readTopicRecords(outTopic, sarama.ReadUncommitted, 5*time.Second)
			require.NoError(ctx, err)
			require.Len(ctx, uncommittedOut, totalInvocations, "every attempt's output exists in the log, aborted ones included")
			return nil
		}).
		Run()
}
