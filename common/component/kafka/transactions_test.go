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

package kafka

import (
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/IBM/sarama"
	saramamocks "github.com/IBM/sarama/mocks"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/common/component/kafka/mocks"
	"github.com/dapr/kit/logger"
)

func TestTxnSessionGuards(t *testing.T) {
	t.Run("send after end fails loudly", func(t *testing.T) {
		fake := &fakeTxnProducer{}
		sess := &txnSession{producer: fake, open: true}

		sess.end()

		err := sess.send(func(p sarama.SyncProducer) error {
			_, _, sendErr := p.SendMessage(&sarama.ProducerMessage{})
			return sendErr
		})
		require.ErrorIs(t, err, errTxnTokenClosed)
		require.Equal(t, 0, fake.sends, "a closed session must never reach the producer")
	})

	t.Run("end waits out an in-flight send", func(t *testing.T) {
		// This is the guard that keeps a late publish from landing after
		// CommitTxn started — or into the next transaction on the same
		// producer. end() must block until the in-flight send returns.
		fake := &fakeTxnProducer{}
		sess := &txnSession{producer: fake, open: true}

		started := make(chan struct{})
		release := make(chan struct{})
		sendDone := make(chan error, 1)
		go func() {
			sendDone <- sess.send(func(p sarama.SyncProducer) error {
				close(started)
				<-release
				_, _, sendErr := p.SendMessage(&sarama.ProducerMessage{})
				return sendErr
			})
		}()
		<-started

		endDone := make(chan struct{})
		go func() {
			sess.end()
			close(endDone)
		}()

		select {
		case <-endDone:
			t.Fatal("end() returned while a send was in flight — the send could land after CommitTxn started")
		case <-time.After(100 * time.Millisecond):
		}

		close(release)
		require.NoError(t, <-sendDone)
		<-endDone

		require.Equal(t, 1, fake.sends, "the in-flight send completed before end() returned")
		require.ErrorIs(t, sess.send(func(sarama.SyncProducer) error { return nil }), errTxnTokenClosed)
	})
}

func TestInitTransactionsWiring(t *testing.T) {
	k := NewKafka(logger.NewLogger("kafka_test"))
	k.mockProducer = saramamocks.NewSyncProducer(t, saramamocks.NewTestConfig())
	k.mockConsumerGroup = mocks.NewConsumerGroup()
	t.Cleanup(func() { _ = k.Close() })

	err := k.Init(t.Context(), map[string]string{
		"brokers":                     "localhost:9092",
		"authType":                    "none",
		"consumerGroup":               "g1",
		"consumerTransactionsEnabled": "true",
		"producerTransactionsEnabled": "true",
		"version":                     "2.5.0",
		"transactionalIdPrefix":       "pfx",
		"transactionTimeout":          "90s",
	})
	require.NoError(t, err)

	// Offsets commit either inside the delivery's transaction or via an
	// explicit synchronous commit; a background autocommit could regress a
	// transactional offset commit.
	require.False(t, k.config.Consumer.Offsets.AutoCommit.Enable)
	// Consumer transactions imply read_committed.
	require.Equal(t, sarama.ReadCommitted, k.config.Consumer.IsolationLevel)
	require.True(t, k.consumerTxnEnabled)
	require.NotNil(t, k.txnSessions)
	require.Equal(t, "pfx", k.txnIDPrefix)
	require.True(t, k.producerConfig.TransactionsEnabled)
	require.True(t, strings.HasPrefix(k.producerConfig.TransactionalID, "pfx-"))
	require.Equal(t, 90*time.Second, k.producerConfig.TransactionTimeout)
}

func TestCloseTeardown(t *testing.T) {
	t.Run("uncontended close tears down consumer group and producer", func(t *testing.T) {
		fake := &fakeTxnProducer{}
		groupClosed := false
		group := mocks.NewConsumerGroup().WithCloseFn(func() error {
			groupClosed = true
			return nil
		})
		k := &Kafka{
			logger:  logger.NewLogger("kafka_test"),
			clients: &clients{producer: fake, consumerGroup: group},
		}

		require.NoError(t, k.Close())

		require.True(t, groupClosed)
		require.True(t, fake.closed)
		require.Nil(t, k.clients.producer)
		require.Nil(t, k.clients.consumerGroup)
	})

	t.Run("close does not hold clientsLock across the consumer group close", func(t *testing.T) {
		// sarama's ConsumerGroup.Close() blocks until every in-flight
		// handler returns, and a handler mid-publish needs clientsLock
		// (latestClients). Holding clientsLock across the group close is
		// therefore a shutdown deadlock. The fake's CloseFn models the
		// draining handler: it must be able to complete a latestClients call
		// while the group close is in progress.
		fake := &fakeTxnProducer{}
		var k *Kafka
		group := mocks.NewConsumerGroup().WithCloseFn(func() error {
			done := make(chan struct{})
			go func() {
				_, _ = k.latestClients()
				close(done)
			}()
			select {
			case <-done:
				return nil
			case <-time.After(2 * time.Second):
				return errors.New("a publish deadlocked while the consumer group was closing")
			}
		})
		k = &Kafka{
			logger:  logger.NewLogger("kafka_test"),
			clients: &clients{producer: fake, consumerGroup: group},
		}

		require.NoError(t, k.Close())
	})

	t.Run("contended close defers the producer close until the publish finishes", func(t *testing.T) {
		// A transactional publish (or a producer-recreation dial) can hold
		// txnMu for minutes. Close must still tear down the consumer group
		// immediately — a prompt group close triggers an immediate rebalance
		// so peers take over the partitions (dapr/components-contrib#3907) —
		// and must not close the producer under the live send. The producer
		// is not abandoned either: Close() also runs on component reload, so
		// it has to be closed once the publish releases txnMu.
		fake := &closeSignalProducer{closed: make(chan struct{})}
		groupClosed := false
		group := mocks.NewConsumerGroup().WithCloseFn(func() error {
			groupClosed = true
			return nil
		})
		k := &Kafka{
			logger:  logger.NewLogger("kafka_test"),
			clients: &clients{producer: fake, consumerGroup: group},
		}

		k.txnMu.Lock()

		start := time.Now()
		err := k.Close()
		elapsed := time.Since(start)

		require.NoError(t, err)
		require.Less(t, elapsed, 2*time.Second, "Close must not wait out the in-flight transactional publish")
		require.True(t, groupClosed, "the consumer group must close regardless of txnMu")
		require.Nil(t, k.clients.consumerGroup)

		select {
		case <-fake.closed:
			t.Fatal("the producer must not be closed under a live transactional send")
		case <-time.After(200 * time.Millisecond):
		}

		// The publish finishes and releases txnMu.
		k.txnMu.Unlock()

		select {
		case <-fake.closed:
		case <-time.After(10 * time.Second):
			t.Fatal("the producer must be closed once the in-flight publish releases txnMu")
		}
		k.clientsLock.Lock()
		require.Nil(t, k.clients.producer)
		k.clientsLock.Unlock()
	})
}

// closeSignalProducer reports its Close over a channel so a deferred close
// can be awaited without racing on a plain bool.
type closeSignalProducer struct {
	sarama.SyncProducer

	closed chan struct{}
}

func (p *closeSignalProducer) Close() error {
	close(p.closed)
	return nil
}

func TestTransactionalProducerClosed(t *testing.T) {
	// After Close, transactional publishes must fail with "component is
	// closed" instead of resurrecting a producer. (k.config is nil here: any
	// recreation attempt would panic, so this test passing also proves no
	// dial is attempted.)
	k := &Kafka{
		clients: &clients{},
		logger:  logger.NewLogger("kafka_test"),
	}
	k.closed.Store(true)

	_, err := k.transactionalProducer()

	require.ErrorContains(t, err, "component is closed")
}

func TestTransactionalProducerClosedBeatsMock(t *testing.T) {
	// The closed check runs before the mock short-circuit, so the closed
	// contract is uniform between mock-backed tests and real clients.
	k := &Kafka{
		mockProducer: saramamocks.NewSyncProducer(t, saramamocks.NewTestConfig()),
		logger:       logger.NewLogger("kafka_test"),
	}
	k.closed.Store(true)

	_, err := k.transactionalProducer()

	require.ErrorContains(t, err, "component is closed")
}

// blockingCloseProducer holds its Close open until released, standing in for
// sarama's producer shutdown draining in-flight work.
type blockingCloseProducer struct {
	sarama.SyncProducer

	entered chan struct{}
	release chan struct{}
}

func (p *blockingCloseProducer) Close() error {
	close(p.entered)
	<-p.release
	return nil
}

func TestInvalidateProducerReleasesClientsLock(t *testing.T) {
	// The producer close drains in-flight work and can take as long as the
	// broker makes it take. Holding clientsLock across it would stall
	// Close()'s consumer-group teardown and every publish for that whole
	// drain, which is the prompt-rebalance contract of
	// dapr/components-contrib#3907.
	fake := &blockingCloseProducer{entered: make(chan struct{}), release: make(chan struct{})}
	k := &Kafka{
		logger:  logger.NewLogger("kafka_test"),
		clients: &clients{producer: fake},
	}

	done := make(chan struct{})
	go func() {
		k.invalidateProducer(fake)
		close(done)
	}()

	select {
	case <-fake.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("the producer close was never reached")
	}

	acquired := make(chan struct{})
	go func() {
		k.clientsLock.Lock()
		k.clientsLock.Unlock() //nolint:staticcheck
		close(acquired)
	}()
	select {
	case <-acquired:
	case <-time.After(10 * time.Second):
		t.Fatal("clientsLock is held across the producer close")
	}

	close(fake.release)
	<-done

	k.clientsLock.Lock()
	require.Nil(t, k.clients.producer)
	k.clientsLock.Unlock()
}

// staleCoordinatorClient records RefreshCoordinator calls; every other
// sarama.Client method is left nil because dropStaleCoordinator uses none.
type staleCoordinatorClient struct {
	sarama.Client

	refreshed []string
}

func (c *staleCoordinatorClient) RefreshCoordinator(group string) error {
	c.refreshed = append(c.refreshed, group)
	return nil
}

func TestDropStaleCoordinator(t *testing.T) {
	// sarama caches the group coordinator until something asks for a refresh,
	// so a redelivery that keeps reusing the claim producer would keep
	// talking to the broker that just said it is no longer the coordinator.
	t.Run("refreshes on the coordinator-moved errors", func(t *testing.T) {
		for _, kerr := range []sarama.KError{
			sarama.ErrNotCoordinatorForConsumer,
			sarama.ErrConsumerCoordinatorNotAvailable,
		} {
			client := &staleCoordinatorClient{}
			dropStaleCoordinator(client, "group1", kerr)
			require.Equal(t, []string{"group1"}, client.refreshed, "expected a refresh for %v", kerr)
		}
	})

	t.Run("leaves the cache alone for every other error", func(t *testing.T) {
		for _, kerr := range []sarama.KError{
			sarama.ErrNoError,
			sarama.ErrIllegalGeneration,
			sarama.ErrUnknownMemberId,
			sarama.ErrOffsetMetadataTooLarge,
		} {
			client := &staleCoordinatorClient{}
			dropStaleCoordinator(client, "group1", kerr)
			require.Empty(t, client.refreshed, "unexpected refresh for %v", kerr)
		}
	})
}

func TestConsumerGroupReadsRaceWithClose(t *testing.T) {
	// Pause and Resume hold subscribeLock; Close detaches
	// clients.consumerGroup under clientsLock, which subscribeLock does not
	// exclude — Close has already released it by the time it swaps the field.
	// A reader going straight to the field is therefore racing the detach on
	// a two-word interface value, and a torn read would leave PauseAll
	// running on a nil group. Under -race this fails against readers that do
	// not go through currentConsumerGroup; both of them have to be reverted
	// to see it every time, because one reader still taking clientsLock in
	// its loop orders many of the other's reads against the detach.
	group := mocks.NewConsumerGroup()
	k := &Kafka{
		logger:  logger.NewLogger("kafka_test"),
		clients: &clients{consumerGroup: group},
	}

	stop := make(chan struct{})
	errs := make(chan error, 2)
	var wg sync.WaitGroup
	wg.Add(2)
	// Assertions stay on the test goroutine: testify's require calls
	// runtime.Goexit, which reports unreliably from a spawned one.
	reader := func(call func() error) {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				if err := call(); err != nil {
					errs <- err
					return
				}
			}
		}
	}
	go reader(func() error { return k.Pause(t.Context()) })
	go reader(func() error { return k.Resume(t.Context()) })

	// Let the readers get going, detach underneath them, then let them run on.
	time.Sleep(20 * time.Millisecond)
	require.NoError(t, k.Close())
	time.Sleep(20 * time.Millisecond)
	close(stop)
	wg.Wait()
	close(errs)

	for err := range errs {
		require.NoError(t, err)
	}
	require.Nil(t, k.currentConsumerGroup(), "Close detaches the group")
}

func TestTransactionalIDIsInjective(t *testing.T) {
	id := func(prefix, group, topic string, partition int32) string {
		k := &Kafka{txnIDPrefix: prefix, consumerGroup: group}
		return (&claimTxn{k: k, topic: topic, partition: partition}).transactionalID()
	}

	t.Run("hyphenated group and topic cannot collide", func(t *testing.T) {
		// Joining the parts with "-" was ambiguous, and "-" is legal in both
		// consumer group ids and topic names. Two components landing on the
		// same id fence each other's producers on every transaction, with no
		// error naming the cause.
		require.NotEqual(t, id("dapr", "a-b", "c", 0), id("dapr", "a", "b-c", 0))
	})

	t.Run("a prefix cannot collide with a group either", func(t *testing.T) {
		// The digest is fixed width, so the id parses from the right —
		// partition, digest, prefix — and no prefix can eat into the rest.
		require.NotEqual(t, id("dapr-a", "b", "c", 0), id("dapr", "a-b", "c", 0))
	})

	t.Run("stable across replicas and distinct per partition", func(t *testing.T) {
		// Two replicas of the same app resolve the same id for a partition,
		// which is what lets the new owner fence the old one. Nothing in the
		// derivation may vary per process.
		replicaA := &Kafka{txnIDPrefix: "pfx", consumerGroup: "group1"}
		replicaB := &Kafka{txnIDPrefix: "pfx", consumerGroup: "group1"}
		claimA := &claimTxn{k: replicaA, topic: "mytopic", partition: 3}
		claimB := &claimTxn{k: replicaB, topic: "mytopic", partition: 3}
		require.Equal(t, claimA.transactionalID(), claimB.transactionalID())

		require.NotEqual(t, id("pfx", "group1", "mytopic", 3), id("pfx", "group1", "mytopic", 4))
	})

	t.Run("the prefix stays verbatim so prefix ACLs keep working", func(t *testing.T) {
		require.True(t, strings.HasPrefix(id("my-app", "group1", "mytopic", 3), "my-app-"))
	})

	t.Run("format is the documented one", func(t *testing.T) {
		// metadata.yaml tells operators to recompute this to map an id back
		// to a group and topic, so the derivation is a published contract.
		require.Equal(t, "pfx-a122fd13f2b4fa56-3", id("pfx", "group1", "mytopic", 3))
	})
}
