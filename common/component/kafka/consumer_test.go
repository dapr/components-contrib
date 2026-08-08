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

package kafka

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/kit/logger"
	"github.com/dapr/kit/retry"
)

// Mock implementations
type mockConsumerGroupSession struct {
	mock.Mock
	ctx    context.Context
	cancel context.CancelFunc
}

func (m *mockConsumerGroupSession) Claims() map[string][]int32 {
	args := m.Called()
	return args.Get(0).(map[string][]int32)
}

func (m *mockConsumerGroupSession) MemberID() string {
	args := m.Called()
	return args.String(0)
}

func (m *mockConsumerGroupSession) GenerationID() int32 {
	args := m.Called()
	//nolint:gosec // Ignoring integer overflow in test code
	return int32(args.Int(0))
}

func (m *mockConsumerGroupSession) MarkOffset(topic string, partition int32, offset int64, metadata string) {
	m.Called(topic, partition, offset, metadata)
}

func (m *mockConsumerGroupSession) ResetOffset(topic string, partition int32, offset int64, metadata string) {
	m.Called(topic, partition, offset, metadata)
}

func (m *mockConsumerGroupSession) MarkMessage(msg *sarama.ConsumerMessage, metadata string) {
	m.Called(msg, metadata)
}

func (m *mockConsumerGroupSession) Context() context.Context {
	return m.ctx
}

func (m *mockConsumerGroupSession) Commit() {
	m.Called()
}

type mockConsumerGroupClaim struct {
	mock.Mock
	messages chan *sarama.ConsumerMessage
	topic    string
}

func (m *mockConsumerGroupClaim) Topic() string {
	return m.topic
}

func (m *mockConsumerGroupClaim) Partition() int32 {
	args := m.Called()
	//nolint:gosec // Ignoring integer overflow in test code
	return int32(args.Int(0))
}

func (m *mockConsumerGroupClaim) InitialOffset() int64 {
	args := m.Called()
	return int64(args.Int(0))
}

func (m *mockConsumerGroupClaim) HighWaterMarkOffset() int64 {
	args := m.Called()
	return int64(args.Int(0))
}

func (m *mockConsumerGroupClaim) Messages() <-chan *sarama.ConsumerMessage {
	return m.messages
}

func TestConsumerTransactions(t *testing.T) {
	newMessage := func(offset int64) *sarama.ConsumerMessage {
		return &sarama.ConsumerMessage{Topic: "mytopic", Partition: 3, Offset: offset, Value: []byte("v")}
	}

	arrange := func(t *testing.T, handlerConfig SubscriptionHandlerConfig, factory func(pc ProducerConfig) (sarama.SyncProducer, error)) (*Kafka, *consumer, *claimTxn, *mockConsumerGroupSession) {
		t.Helper()
		k := &Kafka{
			logger:               logger.NewLogger("kafka_test"),
			consumerGroup:        "group1",
			consumerTxnEnabled:   true,
			txnIDPrefix:          "pfx",
			txnSessions:          make(map[string]*txnSession),
			subscribeTopics:      TopicHandlerConfig{"mytopic": handlerConfig},
			claimProducerFactory: factory,
		}
		c := &consumer{k: k}
		ct := k.newClaimTxn("mytopic", 3)
		ctx, cancel := context.WithCancel(t.Context())
		t.Cleanup(cancel)
		session := &mockConsumerGroupSession{ctx: ctx, cancel: cancel}
		return k, c, ct, session
	}

	t.Run("claim transactional id is stable per partition", func(t *testing.T) {
		_, _, ct, _ := arrange(t, SubscriptionHandlerConfig{}, nil)
		require.Equal(t, "pfx-group1-mytopic-3", ct.transactionalID())
	})

	t.Run("claim producer is created transactional with the stable id", func(t *testing.T) {
		fake := &fakeTxnProducer{}
		var got ProducerConfig
		factory := func(pc ProducerConfig) (sarama.SyncProducer, error) {
			got = pc
			return fake, nil
		}
		handler := func(context.Context, *NewEvent) error { return nil }
		k, c, ct, session := arrange(t, SubscriptionHandlerConfig{Handler: handler}, factory)
		k.producerConfig.TransactionTimeout = 90 * time.Second
		session.On("MarkMessage", mock.Anything, "").Return()
		session.On("Commit").Return()

		require.NoError(t, c.doCallbackTxn(session, newMessage(42), ct))

		require.True(t, got.TransactionsEnabled, "the claim producer must be transactional")
		require.Equal(t, "pfx-group1-mytopic-3", got.TransactionalID)
		require.Equal(t, 90*time.Second, got.TransactionTimeout)
	})

	t.Run("claim producer is reused across deliveries", func(t *testing.T) {
		fake := &fakeTxnProducer{}
		factoryCalls := 0
		factory := func(ProducerConfig) (sarama.SyncProducer, error) {
			factoryCalls++
			return fake, nil
		}
		handler := func(context.Context, *NewEvent) error { return nil }
		_, c, ct, session := arrange(t, SubscriptionHandlerConfig{Handler: handler}, factory)
		session.On("MarkMessage", mock.Anything, "").Return()
		session.On("Commit").Return()

		require.NoError(t, c.doCallbackTxn(session, newMessage(42), ct))
		require.NoError(t, c.doCallbackTxn(session, newMessage(43), ct))

		require.Equal(t, 1, factoryCalls, "a healthy claim producer must be reused, not recreated per delivery")
		require.Equal(t, 2, fake.begins)
	})

	t.Run("claim producer creation failure fails the delivery", func(t *testing.T) {
		factory := func(ProducerConfig) (sarama.SyncProducer, error) {
			return nil, errors.New("dial failed")
		}
		handler := func(context.Context, *NewEvent) error {
			t.Error("handler must not run when the claim producer cannot be created")
			return nil
		}
		_, c, ct, session := arrange(t, SubscriptionHandlerConfig{Handler: handler}, factory)

		err := c.doCallbackTxn(session, newMessage(42), ct)

		require.ErrorContains(t, err, "failed to create transactional producer for mytopic/3")
		require.ErrorContains(t, err, "dial failed")
		session.AssertNotCalled(t, "MarkMessage", mock.Anything, mock.Anything)
	})

	t.Run("swallowed publish error still finishes through the transactional path", func(t *testing.T) {
		// sarama adds the partition to the transaction on the send attempt,
		// so a failed-and-swallowed publish must not take the record-less
		// fallback (which would commit partial outputs plus the offset
		// out-of-band).
		fake := &fakeTxnProducer{
			sendErr: errors.New("send failed"),
			status:  sarama.ProducerTxnFlagAbortableError,
		}
		var k *Kafka
		handler := func(ctx context.Context, msg *NewEvent) error {
			// Publish fails, app swallows the error and acks anyway.
			_ = k.Publish(ctx, "out-topic", []byte("out"), map[string]string{txnTokenMetadataKey: msg.Metadata[txnTokenMetadataKey]})
			return nil
		}
		var c *consumer
		var ct *claimTxn
		var session *mockConsumerGroupSession
		k, c, ct, session = arrange(t, SubscriptionHandlerConfig{Handler: handler}, func(ProducerConfig) (sarama.SyncProducer, error) { return fake, nil })
		session.On("GenerationID").Return(7)
		session.On("MemberID").Return("member-a")

		err := c.doCallbackTxn(session, newMessage(42), ct)

		// The errored transaction refuses to commit, so the delivery fails
		// and is retried — the offset never commits alongside the partially
		// failed outputs (neither transactionally nor via the fallback).
		require.Error(t, err)
		require.Equal(t, 1, fake.addOffsetCalls, "the offset must go through the transaction, not the record-less fallback")
		require.Equal(t, 1, fake.aborts)
		session.AssertNotCalled(t, "MarkMessage", mock.Anything, mock.Anything)
	})

	t.Run("begin error on the bulk claim producer drops it for recreation", func(t *testing.T) {
		fake := &fakeTxnProducer{
			beginErr: errors.New("transition not allowed"),
			status:   sarama.ProducerTxnFlagEndTransaction | sarama.ProducerTxnFlagCommittingTransaction,
		}
		bulkHandler := func(context.Context, *KafkaBulkMessage) ([]pubsub.BulkSubscribeResponseEntry, error) {
			t.Error("handler must not run when the transaction cannot begin")
			return nil, nil
		}
		handlerConfig := SubscriptionHandlerConfig{IsBulkSubscribe: true, BulkHandler: bulkHandler}
		_, c, ct, session := arrange(t, handlerConfig, func(ProducerConfig) (sarama.SyncProducer, error) { return fake, nil })
		messages := []*sarama.ConsumerMessage{newMessage(10), newMessage(11)}

		err := c.doBulkCallbackTxn(session, messages, bulkHandler, "mytopic", ct)

		require.ErrorContains(t, err, "transition not allowed")
		require.True(t, fake.closed, "a producer wedged in a no-exit transaction state must be dropped")
		require.Nil(t, ct.producer)
	})

	t.Run("ConsumeClaim routes deliveries through the claim transaction", func(t *testing.T) {
		fake := &fakeTxnProducer{}
		factoryCalls := 0
		factory := func(ProducerConfig) (sarama.SyncProducer, error) {
			factoryCalls++
			return fake, nil
		}
		var cancel context.CancelFunc
		handler := func(context.Context, *NewEvent) error {
			cancel() // stop the claim loop after the first delivery
			return nil
		}
		var c *consumer
		var session *mockConsumerGroupSession
		_, c, _, session = arrange(t, SubscriptionHandlerConfig{Handler: handler}, factory)
		cancel = session.cancel
		session.On("MarkMessage", mock.Anything, "").Return()
		session.On("Commit").Return()
		claim := &mockConsumerGroupClaim{messages: make(chan *sarama.ConsumerMessage, 1), topic: "mytopic"}
		claim.On("Partition").Return(3)
		claim.messages <- newMessage(42)

		err := c.ConsumeClaim(session, claim)

		require.NoError(t, err)
		require.Equal(t, 1, factoryCalls, "the delivery must be dispatched through the claim transaction")
		require.True(t, fake.closed, "the claim producer must be released when the claim ends")
	})

	t.Run("offset-join failure aborts the transaction", func(t *testing.T) {
		fake := &fakeTxnProducer{
			addOffsetErr: errors.New("offsets rejected"),
			status:       sarama.ProducerTxnFlagAbortableError,
		}
		var k *Kafka
		handler := func(ctx context.Context, msg *NewEvent) error {
			return k.Publish(ctx, "out-topic", []byte("out"), map[string]string{txnTokenMetadataKey: msg.Metadata[txnTokenMetadataKey]})
		}
		var c *consumer
		var ct *claimTxn
		var session *mockConsumerGroupSession
		k, c, ct, session = arrange(t, SubscriptionHandlerConfig{Handler: handler}, func(ProducerConfig) (sarama.SyncProducer, error) { return fake, nil })
		session.On("GenerationID").Return(7)
		session.On("MemberID").Return("member-a")

		err := c.doCallbackTxn(session, newMessage(42), ct)

		require.ErrorContains(t, err, "offsets rejected")
		require.Equal(t, 1, fake.aborts)
		require.Equal(t, 0, fake.commits)
		session.AssertNotCalled(t, "MarkMessage", mock.Anything, mock.Anything)
	})

	t.Run("begin error on the claim producer drops it for recreation", func(t *testing.T) {
		fake := &fakeTxnProducer{
			beginErr: errors.New("transition not allowed"),
			status:   sarama.ProducerTxnFlagEndTransaction | sarama.ProducerTxnFlagCommittingTransaction,
		}
		handler := func(context.Context, *NewEvent) error { return nil }
		_, c, ct, session := arrange(t, SubscriptionHandlerConfig{Handler: handler}, func(ProducerConfig) (sarama.SyncProducer, error) { return fake, nil })

		err := c.doCallbackTxn(session, newMessage(42), ct)

		require.ErrorContains(t, err, "transition not allowed")
		require.True(t, fake.closed, "a producer wedged in a no-exit transaction state must be dropped")
		require.Nil(t, ct.producer)
	})

	t.Run("bulk batch of nil messages is a no-op", func(t *testing.T) {
		factoryCalls := 0
		factory := func(ProducerConfig) (sarama.SyncProducer, error) {
			factoryCalls++
			return &fakeTxnProducer{}, nil
		}
		bulkHandler := func(context.Context, *KafkaBulkMessage) ([]pubsub.BulkSubscribeResponseEntry, error) {
			t.Fatal("handler must not run for an all-nil batch")
			return nil, nil
		}
		_, c, ct, session := arrange(t, SubscriptionHandlerConfig{IsBulkSubscribe: true, BulkHandler: bulkHandler}, factory)

		err := c.doBulkCallbackTxn(session, []*sarama.ConsumerMessage{nil, nil}, bulkHandler, "mytopic", ct)

		require.NoError(t, err)
		require.Equal(t, 0, factoryCalls, "no producer needed for an empty batch")
	})

	t.Run("outputs and offset commit in one transaction", func(t *testing.T) {
		fake := &fakeTxnProducer{}
		var k *Kafka
		var token string
		handler := func(ctx context.Context, msg *NewEvent) error {
			token = msg.Metadata[txnTokenMetadataKey]
			require.NotEmpty(t, token)
			return k.Publish(ctx, "out-topic", []byte("out"), map[string]string{txnTokenMetadataKey: token, "h1": "v1"})
		}
		var c *consumer
		var ct *claimTxn
		var session *mockConsumerGroupSession
		k, c, ct, session = arrange(t, SubscriptionHandlerConfig{Handler: handler}, func(ProducerConfig) (sarama.SyncProducer, error) { return fake, nil })
		session.On("GenerationID").Return(7)
		session.On("MemberID").Return("member-a")

		err := c.doCallbackTxn(session, newMessage(42), ct)

		require.NoError(t, err)
		require.Equal(t, 1, fake.begins)
		require.Equal(t, 1, fake.sends)
		require.Equal(t, 1, fake.addOffsetCalls)
		require.Equal(t, 1, fake.commits)
		require.Equal(t, 0, fake.aborts)
		require.Equal(t, int64(42), fake.offsetMsg.Offset)
		require.Equal(t, "group1", fake.groupMetadata.GroupID)
		require.Equal(t, int32(7), fake.groupMetadata.GenerationID)
		require.Equal(t, "member-a", fake.groupMetadata.MemberID)
		// The token is transport plumbing, never a record header.
		require.NotNil(t, fake.lastMsg)
		for _, h := range fake.lastMsg.Headers {
			require.NotEqual(t, txnTokenMetadataKey, string(h.Key))
		}
		// The offset commit rides in the transaction; marking it as well could
		// let a stale autocommit regress it.
		session.AssertNotCalled(t, "MarkMessage", mock.Anything, mock.Anything)
		require.Empty(t, k.txnSessions)
	})

	t.Run("handler error aborts the transaction", func(t *testing.T) {
		fake := &fakeTxnProducer{status: sarama.ProducerTxnFlagInTransaction}
		handler := func(context.Context, *NewEvent) error { return errors.New("app failed") }
		_, c, ct, session := arrange(t, SubscriptionHandlerConfig{Handler: handler}, func(ProducerConfig) (sarama.SyncProducer, error) { return fake, nil })

		err := c.doCallbackTxn(session, newMessage(42), ct)

		require.ErrorContains(t, err, "app failed")
		require.Equal(t, 1, fake.aborts)
		require.Equal(t, 0, fake.commits)
		require.Equal(t, 0, fake.addOffsetCalls)
		session.AssertNotCalled(t, "MarkMessage", mock.Anything, mock.Anything)
	})

	t.Run("publish after the handler returned fails loudly", func(t *testing.T) {
		fake := &fakeTxnProducer{}
		var k *Kafka
		var token string
		handler := func(ctx context.Context, msg *NewEvent) error {
			token = msg.Metadata[txnTokenMetadataKey]
			return nil
		}
		var c *consumer
		var ct *claimTxn
		var session *mockConsumerGroupSession
		k, c, ct, session = arrange(t, SubscriptionHandlerConfig{Handler: handler}, func(ProducerConfig) (sarama.SyncProducer, error) { return fake, nil })
		// No publish happened during the handler, so the offset commits via
		// the synchronous mark path.
		session.On("MarkMessage", mock.Anything, "").Return()
		session.On("Commit").Return()

		require.NoError(t, c.doCallbackTxn(session, newMessage(42), ct))

		err := k.Publish(t.Context(), "out-topic", []byte("late"), map[string]string{txnTokenMetadataKey: token})
		require.ErrorIs(t, err, errTxnTokenClosed)
		require.Equal(t, 0, fake.sends)
	})

	t.Run("publish with unknown token fails loudly", func(t *testing.T) {
		k, _, _, _ := arrange(t, SubscriptionHandlerConfig{}, nil)

		err := k.Publish(t.Context(), "out-topic", []byte("x"), map[string]string{txnTokenMetadataKey: "bogus"})

		require.ErrorIs(t, err, errTxnTokenClosed)
	})

	t.Run("fatal commit drops the claim producer and the next delivery recreates it", func(t *testing.T) {
		fakes := []*fakeTxnProducer{
			{commitErr: errors.New("commit failed"), status: sarama.ProducerTxnFlagFatalError},
			{},
		}
		factoryCalls := 0
		factory := func(ProducerConfig) (sarama.SyncProducer, error) {
			p := fakes[factoryCalls]
			factoryCalls++
			return p, nil
		}
		handler := func(context.Context, *NewEvent) error { return nil }
		_, c, ct, session := arrange(t, SubscriptionHandlerConfig{Handler: handler}, factory)
		session.On("MarkMessage", mock.Anything, "").Return()
		session.On("Commit").Return()

		err := c.doCallbackTxn(session, newMessage(42), ct)
		require.ErrorContains(t, err, "commit failed")
		require.True(t, fakes[0].closed)
		require.Nil(t, ct.producer)

		require.NoError(t, c.doCallbackTxn(session, newMessage(43), ct))
		require.Equal(t, 2, factoryCalls)
		require.Equal(t, 1, fakes[1].commits)
	})

	t.Run("bulk batch commits outputs and the last offset in one transaction", func(t *testing.T) {
		fake := &fakeTxnProducer{}
		var k *Kafka
		bulkHandler := func(ctx context.Context, msg *KafkaBulkMessage) ([]pubsub.BulkSubscribeResponseEntry, error) {
			token := msg.Metadata[txnTokenMetadataKey]
			require.NotEmpty(t, token)
			require.Len(t, msg.Entries, 3)
			return nil, k.Publish(ctx, "out-topic", []byte("out"), map[string]string{txnTokenMetadataKey: token})
		}
		handlerConfig := SubscriptionHandlerConfig{IsBulkSubscribe: true, BulkHandler: bulkHandler}
		var c *consumer
		var ct *claimTxn
		var session *mockConsumerGroupSession
		k, c, ct, session = arrange(t, handlerConfig, func(ProducerConfig) (sarama.SyncProducer, error) { return fake, nil })
		session.On("GenerationID").Return(7)
		session.On("MemberID").Return("member-a")
		messages := []*sarama.ConsumerMessage{newMessage(10), newMessage(11), newMessage(12)}

		err := c.doBulkCallbackTxn(session, messages, bulkHandler, "mytopic", ct)

		require.NoError(t, err)
		require.Equal(t, 1, fake.begins)
		require.Equal(t, 1, fake.sends)
		require.Equal(t, 1, fake.commits)
		require.Equal(t, int64(12), fake.offsetMsg.Offset)
		session.AssertNotCalled(t, "MarkMessage", mock.Anything, mock.Anything)
	})

	t.Run("delivery without outputs commits the offset synchronously", func(t *testing.T) {
		// sarama silently skips offset commits of record-less transactions,
		// so a handler that publishes nothing must fall back to a
		// synchronous mark+commit.
		fake := &fakeTxnProducer{}
		handler := func(context.Context, *NewEvent) error { return nil }
		_, c, ct, session := arrange(t, SubscriptionHandlerConfig{Handler: handler}, func(ProducerConfig) (sarama.SyncProducer, error) { return fake, nil })
		msg := newMessage(42)
		session.On("MarkMessage", msg, "").Return()
		session.On("Commit").Return()

		err := c.doCallbackTxn(session, msg, ct)

		require.NoError(t, err)
		require.Equal(t, 1, fake.begins)
		require.Equal(t, 1, fake.commits, "the empty transaction is still ended")
		require.Equal(t, 0, fake.addOffsetCalls, "no transactional offset commit for a record-less transaction")
		session.AssertCalled(t, "MarkMessage", msg, "")
		session.AssertCalled(t, "Commit")
	})

	t.Run("bulk per-entry error aborts the whole batch", func(t *testing.T) {
		fake := &fakeTxnProducer{status: sarama.ProducerTxnFlagInTransaction}
		bulkHandler := func(ctx context.Context, msg *KafkaBulkMessage) ([]pubsub.BulkSubscribeResponseEntry, error) {
			return []pubsub.BulkSubscribeResponseEntry{
				{EntryId: "0"},
				{EntryId: "1", Error: errors.New("entry 1 failed")},
				{EntryId: "2"},
			}, nil
		}
		handlerConfig := SubscriptionHandlerConfig{IsBulkSubscribe: true, BulkHandler: bulkHandler}
		_, c, ct, session := arrange(t, handlerConfig, func(ProducerConfig) (sarama.SyncProducer, error) { return fake, nil })
		messages := []*sarama.ConsumerMessage{newMessage(10), newMessage(11), newMessage(12)}

		err := c.doBulkCallbackTxn(session, messages, bulkHandler, "mytopic", ct)

		require.ErrorContains(t, err, "entry 1 failed")
		require.Equal(t, 1, fake.aborts)
		require.Equal(t, 0, fake.commits)
		require.Equal(t, 0, fake.addOffsetCalls)
		session.AssertNotCalled(t, "MarkMessage", mock.Anything, mock.Anything)
	})
}

func Test_ConsumeClaim(t *testing.T) {
	t.Run("single message", func(t *testing.T) {
		t.Run("no retry", func(t *testing.T) {
			// Setup
			k := &Kafka{
				logger:              logger.NewLogger("test"),
				consumeRetryEnabled: false,
				subscribeTopics:     make(map[string]SubscriptionHandlerConfig),
			}
			consumer := &consumer{
				k:     k,
				mutex: sync.Mutex{},
			}

			t.Run("successfully consume message", func(t *testing.T) {
				topic := "test-topic-success"
				msg := &sarama.ConsumerMessage{
					Topic:     topic,
					Partition: 0,
					Offset:    1,
					Key:       []byte("test-key"),
					Value:     []byte("test-value"),
					Headers:   nil,
				}

				ctx, cancel := context.WithCancel(t.Context())
				mockSession := &mockConsumerGroupSession{ctx: ctx, cancel: cancel}
				mockSession.On("MarkMessage", msg, "").Return()

				mockClaim := &mockConsumerGroupClaim{
					messages: make(chan *sarama.ConsumerMessage, 1),
					topic:    topic,
				}

				wg := sync.WaitGroup{}
				wg.Add(1)

				k.subscribeTopics[topic] = SubscriptionHandlerConfig{
					Handler: func(ctx context.Context, event *NewEvent) error {
						assert.Equal(t, topic, event.Topic)
						assert.Equal(t, "test-value", string(event.Data))
						wg.Done()
						return nil
					},
				}

				// Send message and cancel context
				mockClaim.messages <- msg
				go func() {
					wg.Wait()
					cancel()
				}()

				// Test
				err := consumer.ConsumeClaim(mockSession, mockClaim)
				require.NoError(t, err)
				mockSession.AssertExpectations(t)
			})

			t.Run("failed to consume message", func(t *testing.T) {
				topic := "test-topic-failure"
				msg := &sarama.ConsumerMessage{
					Topic:     topic,
					Partition: 0,
					Offset:    1,
					Key:       []byte("test-key"),
					Value:     []byte("test-value"),
					Headers:   nil,
				}

				ctx, cancel := context.WithCancel(t.Context())
				mockSession := &mockConsumerGroupSession{ctx: ctx, cancel: cancel}

				mockClaim := &mockConsumerGroupClaim{
					messages: make(chan *sarama.ConsumerMessage, 1),
					topic:    topic,
				}

				wg := sync.WaitGroup{}
				wg.Add(1)

				k.subscribeTopics[topic] = SubscriptionHandlerConfig{
					Handler: func(ctx context.Context, event *NewEvent) error {
						wg.Done()
						return errors.New("test error")
					},
				}

				// Send message and cancel context
				mockClaim.messages <- msg
				go func() {
					wg.Wait()
					cancel()
				}()

				// Test
				err := consumer.ConsumeClaim(mockSession, mockClaim)
				require.NoError(t, err)
				mockSession.AssertNotCalled(t, "MarkMessage", msg, "")
			})
		})

		t.Run("retry", func(t *testing.T) {
			// Setup
			k := &Kafka{
				logger:              logger.NewLogger("test"),
				consumeRetryEnabled: true,
				backOffConfig: retry.Config{
					Policy:     retry.PolicyConstant,
					MaxRetries: 0,
				},
				subscribeTopics: make(map[string]SubscriptionHandlerConfig),
			}
			consumer := &consumer{
				k:     k,
				mutex: sync.Mutex{},
			}

			t.Run("successfully consume message", func(t *testing.T) {
				topic := "test-topic-success"
				msg := &sarama.ConsumerMessage{
					Topic:     topic,
					Partition: 0,
					Offset:    1,
					Key:       []byte("test-key"),
					Value:     []byte("test-value"),
					Headers:   nil,
				}

				ctx, cancel := context.WithCancel(t.Context())
				mockSession := &mockConsumerGroupSession{ctx: ctx, cancel: cancel}
				mockSession.On("MarkMessage", msg, "").Return()

				mockClaim := &mockConsumerGroupClaim{
					messages: make(chan *sarama.ConsumerMessage, 1),
					topic:    topic,
				}

				wg := sync.WaitGroup{}
				wg.Add(1)

				k.subscribeTopics[topic] = SubscriptionHandlerConfig{
					Handler: func(ctx context.Context, event *NewEvent) error {
						assert.Equal(t, topic, event.Topic)
						assert.Equal(t, "test-value", string(event.Data))
						wg.Done()
						return nil
					},
				}

				// Send message and cancel context
				mockClaim.messages <- msg
				go func() {
					wg.Wait()
					cancel()
				}()

				// Test
				err := consumer.ConsumeClaim(mockSession, mockClaim)
				require.NoError(t, err)
				mockSession.AssertExpectations(t)
			})

			t.Run("failed to consume message", func(t *testing.T) {
				topic := "test-topic-failure"
				msg := &sarama.ConsumerMessage{
					Topic:     topic,
					Partition: 0,
					Offset:    1,
					Key:       []byte("test-key"),
					Value:     []byte("test-value"),
					Headers:   nil,
				}

				ctx, cancel := context.WithCancel(t.Context())
				mockSession := &mockConsumerGroupSession{ctx: ctx, cancel: cancel}

				mockClaim := &mockConsumerGroupClaim{
					messages: make(chan *sarama.ConsumerMessage, 1),
					topic:    topic,
				}

				wg := sync.WaitGroup{}
				wg.Add(1)

				k.subscribeTopics[topic] = SubscriptionHandlerConfig{
					Handler: func(ctx context.Context, event *NewEvent) error {
						wg.Done()
						return errors.New("test error")
					},
				}

				// Send message and cancel context
				mockClaim.messages <- msg
				go func() {
					wg.Wait()
					cancel()
				}()

				// Test
				err := consumer.ConsumeClaim(mockSession, mockClaim)
				require.NoError(t, err)
				mockSession.AssertNotCalled(t, "MarkMessage", msg, "")
			})

			t.Run("exits on context cancel", func(t *testing.T) {
				topic := "test-topic-cancel"
				msg := &sarama.ConsumerMessage{
					Topic:     topic,
					Partition: 0,
					Offset:    1,
					Key:       []byte("test-key"),
					Value:     []byte("test-value"),
					Headers:   nil,
				}

				msg2 := &sarama.ConsumerMessage{
					Topic:     topic,
					Partition: 0,
					Offset:    2,
					Key:       []byte("test-key"),
					Value:     []byte("test-value-2"),
					Headers:   nil,
				}

				ctx, cancel := context.WithCancel(t.Context())
				mockSession := &mockConsumerGroupSession{ctx: ctx, cancel: cancel}

				mockClaim := &mockConsumerGroupClaim{
					messages: make(chan *sarama.ConsumerMessage, 2),
					topic:    topic,
				}

				k.subscribeTopics[topic] = SubscriptionHandlerConfig{
					Handler: func(ctx context.Context, event *NewEvent) error {
						// This must never be test-value-2
						assert.Equal(t, "test-value", string(event.Data))
						cancel()
						return ctx.Err()
					},
				}

				// Send multiple messages to make sure there are more than one message in the channel.
				mockClaim.messages <- msg
				mockClaim.messages <- msg2

				go func() {
					// Let it run for a bit before canceling.
					time.Sleep(50 * time.Millisecond)
					cancel()
				}()

				// Test
				err := consumer.ConsumeClaim(mockSession, mockClaim)
				require.NoError(t, err)
				mockSession.AssertNotCalled(t, "MarkMessage", msg, "")
			})
		})
	})

	t.Run("bulk subscribe resets ticker after count-based flush", func(t *testing.T) {
		// Regression test: the await ticker must be reset after a count-based
		// flush so the next partial batch waits a fresh MaxAwaitDuration window.
		// Without the reset, the ticker keeps firing on its original schedule
		// and a partial batch can flush long before its own MaxAwaitDuration
		// window has elapsed.
		topic := "test-topic-bulk-ticker-reset"
		const (
			maxCount = 3
			awaitMs  = 200
		)

		k := &Kafka{
			logger:              logger.NewLogger("test"),
			consumeRetryEnabled: false,
			subscribeTopics:     make(map[string]SubscriptionHandlerConfig),
		}
		c := &consumer{
			k:     k,
			mutex: sync.Mutex{},
		}

		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		mockSession := &mockConsumerGroupSession{ctx: ctx, cancel: cancel}
		mockSession.On("MarkMessage", mock.Anything, "").Return()

		mockClaim := &mockConsumerGroupClaim{
			messages: make(chan *sarama.ConsumerMessage, maxCount+1),
			topic:    topic,
		}

		var (
			flushMu    sync.Mutex
			flushTimes []time.Time
			flushSizes []int
		)

		k.subscribeTopics[topic] = SubscriptionHandlerConfig{
			IsBulkSubscribe: true,
			SubscribeConfig: pubsub.BulkSubscribeConfig{
				MaxMessagesCount:   maxCount,
				MaxAwaitDurationMs: awaitMs,
			},
			BulkHandler: func(_ context.Context, msg *KafkaBulkMessage) ([]pubsub.BulkSubscribeResponseEntry, error) {
				flushMu.Lock()
				flushTimes = append(flushTimes, time.Now())
				flushSizes = append(flushSizes, len(msg.Entries))
				flushMu.Unlock()
				return nil, nil
			},
		}

		done := make(chan struct{})
		go func() {
			defer close(done)
			_ = c.ConsumeClaim(mockSession, mockClaim)
		}()

		// Wait half of MaxAwaitDuration so the count-flush happens around the
		// midpoint of the original ticker's period. The un-reset ticker would
		// then fire ~awaitMs/2 after the count-flush; the reset ticker fires
		// ~awaitMs after.
		time.Sleep(time.Duration(awaitMs/2) * time.Millisecond)

		for range maxCount {
			mockClaim.messages <- &sarama.ConsumerMessage{
				Topic: topic,
				Value: []byte("count-msg"),
			}
		}

		require.Eventually(t, func() bool {
			flushMu.Lock()
			defer flushMu.Unlock()
			return len(flushTimes) >= 1
		}, time.Second, 5*time.Millisecond, "count-based flush did not happen")

		flushMu.Lock()
		countFlushAt := flushTimes[0]
		countFlushSize := flushSizes[0]
		flushMu.Unlock()
		require.Equal(t, maxCount, countFlushSize, "first flush should be the full count batch")

		// Partial message arrives just after the count-flush. Its flush must
		// wait a full MaxAwaitDuration window from the count-flush.
		mockClaim.messages <- &sarama.ConsumerMessage{
			Topic: topic,
			Value: []byte("partial-msg"),
		}

		require.Eventually(t, func() bool {
			flushMu.Lock()
			defer flushMu.Unlock()
			return len(flushTimes) >= 2
		}, 2*time.Second, 5*time.Millisecond, "partial batch flush did not happen")

		flushMu.Lock()
		partialFlushAt := flushTimes[1]
		partialFlushSize := flushSizes[1]
		flushMu.Unlock()
		require.Equal(t, 1, partialFlushSize, "second flush should be the partial batch")

		gap := partialFlushAt.Sub(countFlushAt)
		// With the fix, gap ≈ awaitMs. Without the fix, gap ≈ awaitMs/2 because
		// the ticker continues on its original schedule. Use 0.8*awaitMs as the
		// threshold to absorb scheduler jitter.
		minGap := time.Duration(awaitMs*8/10) * time.Millisecond
		require.GreaterOrEqual(t, gap, minGap,
			"partial batch flushed %s after count-flush (want >= %s); ticker was not reset",
			gap, minGap)

		cancel()
		<-done
	})
}
