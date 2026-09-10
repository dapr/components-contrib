//go:build integration_test
// +build integration_test

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

package rabbitmq

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	mdata "github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/kit/logger"
)

const (
	testRabbitMQURL = "amqp://test:test@localhost:5672"
)

// TestSubscriptionRestart verifies that restarting one subscription does not
// disrupt other active subscriptions sharing the same connection/channel.
//
// Regression test for https://github.com/dapr/java-sdk/issues/1701 where
// reusing consumer tags on the shared channel caused a connection-level
// "attempt to reuse consumer tag" exception that killed all subscriptions.
func TestSubscriptionRestart(t *testing.T) {
	// Verify RabbitMQ is reachable
	conn, err := amqp.Dial(testRabbitMQURL)
	require.NoError(t, err, "RabbitMQ must be running at %s", testRabbitMQURL)
	conn.Close()

	log := logger.NewLogger("test")

	r := NewRabbitMQ(log).(*rabbitMQ)
	err = r.Init(t.Context(), pubsub.Metadata{Base: mdata.Base{
		Properties: map[string]string{
			metadataConnectionStringKey: testRabbitMQURL,
			metadataConsumerIDKey:       "integration-test",
			metadataDurableKey:          "true",
			metadataDeleteWhenUnusedKey: "false",
			metadataRequeueInFailureKey: "true",
		},
	}})
	require.NoError(t, err)
	defer r.Close()

	topicStable := "stable-topic"
	topicRestart := "restart-topic"

	var stableCount atomic.Int32
	var restartCount atomic.Int32

	stableHandler := func(_ context.Context, msg *pubsub.NewMessage) error {
		stableCount.Add(1)
		return nil
	}
	restartHandler := func(_ context.Context, msg *pubsub.NewMessage) error {
		restartCount.Add(1)
		return nil
	}

	// Subscribe to both topics
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	err = r.Subscribe(ctx, pubsub.SubscribeRequest{Topic: topicStable}, stableHandler)
	require.NoError(t, err)

	// Use a separate context for the restart topic so we can cancel it independently
	restartCtx, restartCancel := context.WithCancel(t.Context())

	err = r.Subscribe(restartCtx, pubsub.SubscribeRequest{Topic: topicRestart}, restartHandler)
	require.NoError(t, err)

	// Phase 1: Verify both subscriptions receive messages
	publishN(t, r, topicStable, 5)
	publishN(t, r, topicRestart, 5)

	assert.Eventually(t, func() bool {
		return stableCount.Load() >= 5 && restartCount.Load() >= 5
	}, 10*time.Second, 100*time.Millisecond, "both subscriptions should receive messages")

	t.Logf("Phase 1 passed: stable=%d, restart=%d", stableCount.Load(), restartCount.Load())

	// Phase 2: Cancel the restart subscription (simulates stopping a streaming subscription)
	restartCancel()
	time.Sleep(2 * time.Second)

	// Phase 3: Re-subscribe to the restart topic.
	// Before the fix, this would reuse the same consumer tag and cause
	// RabbitMQ to throw a connection-level "attempt to reuse consumer tag" error,
	// killing the stable subscription too.
	restartCount.Store(0)
	stableCount.Store(0)

	restartCtx2, restartCancel2 := context.WithCancel(t.Context())
	defer restartCancel2()

	err = r.Subscribe(restartCtx2, pubsub.SubscribeRequest{Topic: topicRestart}, restartHandler)
	require.NoError(t, err, "re-subscribe should succeed without connection errors")

	// Phase 4: Verify the stable subscription was NOT disrupted
	publishN(t, r, topicStable, 5)
	publishN(t, r, topicRestart, 5)

	assert.Eventually(t, func() bool {
		return stableCount.Load() >= 5
	}, 10*time.Second, 100*time.Millisecond,
		"stable subscription must still work after restart (got %d messages)", stableCount.Load())

	assert.Eventually(t, func() bool {
		return restartCount.Load() >= 5
	}, 10*time.Second, 100*time.Millisecond,
		"restarted subscription must receive messages (got %d messages)", restartCount.Load())

	t.Logf("Phase 4 passed: stable=%d, restart=%d", stableCount.Load(), restartCount.Load())
}

// TestMultipleSubscriptionsIsolation verifies that multiple concurrent
// subscriptions operate independently on the shared channel.
func TestMultipleSubscriptionsIsolation(t *testing.T) {
	conn, err := amqp.Dial(testRabbitMQURL)
	require.NoError(t, err, "RabbitMQ must be running at %s", testRabbitMQURL)
	conn.Close()

	log := logger.NewLogger("test")

	r := NewRabbitMQ(log).(*rabbitMQ)
	err = r.Init(t.Context(), pubsub.Metadata{Base: mdata.Base{
		Properties: map[string]string{
			metadataConnectionStringKey: testRabbitMQURL,
			metadataConsumerIDKey:       "isolation-test",
			metadataDurableKey:          "true",
			metadataDeleteWhenUnusedKey: "false",
		},
	}})
	require.NoError(t, err)
	defer r.Close()

	const numTopics = 5
	const msgsPerTopic = 10

	var counts [numTopics]atomic.Int32

	// Subscribe to all topics
	for i := range numTopics {
		topic := fmt.Sprintf("isolation-topic-%d", i)
		idx := i
		err := r.Subscribe(t.Context(), pubsub.SubscribeRequest{Topic: topic}, func(_ context.Context, msg *pubsub.NewMessage) error {
			counts[idx].Add(1)
			return nil
		})
		require.NoError(t, err)
	}

	// Publish concurrently
	var wg sync.WaitGroup
	for i := range numTopics {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			topic := fmt.Sprintf("isolation-topic-%d", i)
			publishN(t, r, topic, msgsPerTopic)
		}(i)
	}
	wg.Wait()

	// All topics should receive their messages
	assert.Eventually(t, func() bool {
		for i := range numTopics {
			if counts[i].Load() < msgsPerTopic {
				return false
			}
		}
		return true
	}, 15*time.Second, 100*time.Millisecond, "all topics should receive messages")

	for i := range numTopics {
		t.Logf("topic %d: %d messages", i, counts[i].Load())
	}
}

func publishN(t *testing.T, r pubsub.PubSub, topic string, n int) {
	t.Helper()
	for i := range n {
		err := r.Publish(t.Context(), &pubsub.PublishRequest{
			Topic: topic,
			Data:  []byte(fmt.Sprintf("msg-%d", i)),
		})
		require.NoError(t, err)
	}
}

// declareExchangeOutOfBand creates an exchange the way an external owner (the
// RabbitMQ Cluster Kubernetes Topology Operator, Terraform, rabbitmqadmin)
// would, i.e. without any involvement from the component.
func declareExchangeOutOfBand(t *testing.T, name, kind string, args amqp.Table) {
	t.Helper()

	conn, err := amqp.Dial(testRabbitMQURL)
	require.NoError(t, err, "RabbitMQ must be running at %s", testRabbitMQURL)
	defer conn.Close()

	ch, err := conn.Channel()
	require.NoError(t, err)
	defer ch.Close()

	// durable, not auto-deleted: the properties an operator-managed exchange
	// normally has, and deliberately different from what the component would
	// declare by default (fanout, autoDelete=true).
	err = ch.ExchangeDeclare(name, kind, true, false, false, false, args)
	require.NoError(t, err)

	t.Cleanup(func() {
		conn, err := amqp.Dial(testRabbitMQURL)
		if err != nil {
			return
		}
		defer conn.Close()
		ch, err := conn.Channel()
		if err != nil {
			return
		}
		defer ch.Close()
		_ = ch.ExchangeDelete(name, false, false)
	})
}

// TestPassiveExchangeDeclareUsesExistingExchange verifies that a component
// configured with exchangeDeclareMode=passive binds to an exchange it did not
// create, instead of trying to declare its own.
func TestPassiveExchangeDeclareUsesExistingExchange(t *testing.T) {
	const topic = "passive-topic"
	declareExchangeOutOfBand(t, topic, amqp.ExchangeTopic, nil)

	log := logger.NewLogger("test")
	r := NewRabbitMQ(log).(*rabbitMQ)
	err := r.Init(t.Context(), pubsub.Metadata{Base: mdata.Base{
		Properties: map[string]string{
			metadataConnectionStringKey:    testRabbitMQURL,
			metadataConsumerIDKey:          "passive-test",
			metadataExchangeDeclareModeKey: exchangeDeclareModePassive,
			metadataExchangeKindKey:        amqp.ExchangeTopic,
			metadataDurableKey:             "true",
			metadataDeleteWhenUnusedKey:    "false",
		},
	}})
	require.NoError(t, err)
	defer r.Close()

	var received atomic.Int32
	err = r.Subscribe(t.Context(), pubsub.SubscribeRequest{
		Topic:    topic,
		Metadata: map[string]string{reqMetadataRoutingKey: "orders.#"},
	}, func(_ context.Context, msg *pubsub.NewMessage) error {
		received.Add(1)
		return nil
	})
	require.NoError(t, err)

	for i := range 5 {
		err = r.Publish(t.Context(), &pubsub.PublishRequest{
			Topic:    topic,
			Data:     []byte(fmt.Sprintf("msg-%d", i)),
			Metadata: map[string]string{reqMetadataRoutingKey: "orders.created"},
		})
		require.NoError(t, err)
	}

	assert.Eventually(t, func() bool {
		return received.Load() >= 5
	}, 10*time.Second, 100*time.Millisecond, "messages published through an externally managed exchange should be delivered")
}

// TestActiveExchangeDeclareCollidesWithExistingExchange documents the failure
// that exchangeDeclareMode=passive exists to avoid: an exchange created by an
// external owner with different properties makes the component's own
// exchange.declare fail with PRECONDITION_FAILED.
func TestActiveExchangeDeclareCollidesWithExistingExchange(t *testing.T) {
	const topic = "collision-topic"
	declareExchangeOutOfBand(t, topic, amqp.ExchangeTopic, nil)

	log := logger.NewLogger("test")
	r := NewRabbitMQ(log).(*rabbitMQ)
	err := r.Init(t.Context(), pubsub.Metadata{Base: mdata.Base{
		Properties: map[string]string{
			metadataConnectionStringKey: testRabbitMQURL,
			metadataConsumerIDKey:       "collision-test",
			// Defaults: fanout, autoDelete=true. Every property differs from
			// the exchange declared above.
		},
	}})
	require.NoError(t, err)
	defer r.Close()

	err = r.Publish(t.Context(), &pubsub.PublishRequest{Topic: topic, Data: []byte("msg")})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "already exists with properties that differ")
	assert.Contains(t, err.Error(), metadataExchangeDeclareModeKey)
}

// TestPassiveExchangeDeclareMissingExchange verifies that a passive component
// reports a clear error rather than silently creating the exchange.
func TestPassiveExchangeDeclareMissingExchange(t *testing.T) {
	log := logger.NewLogger("test")
	r := NewRabbitMQ(log).(*rabbitMQ)
	err := r.Init(t.Context(), pubsub.Metadata{Base: mdata.Base{
		Properties: map[string]string{
			metadataConnectionStringKey:    testRabbitMQURL,
			metadataConsumerIDKey:          "passive-missing-test",
			metadataExchangeDeclareModeKey: exchangeDeclareModePassive,
			metadataExchangeKindKey:        amqp.ExchangeTopic,
		},
	}})
	require.NoError(t, err)
	defer r.Close()

	err = r.Publish(t.Context(), &pubsub.PublishRequest{Topic: "no-such-exchange", Data: []byte("msg")})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "does not exist")
}

// consistentHashPluginEnabled reports whether the broker has the
// rabbitmq_consistent_hash_exchange plugin enabled.
func consistentHashPluginEnabled(t *testing.T) bool {
	t.Helper()

	conn, err := amqp.Dial(testRabbitMQURL)
	require.NoError(t, err, "RabbitMQ must be running at %s", testRabbitMQURL)
	defer conn.Close()

	ch, err := conn.Channel()
	require.NoError(t, err)

	const probe = "consistent-hash-probe"
	if err = ch.ExchangeDeclare(probe, exchangeKindConsistentHash, false, true, false, false, nil); err != nil {
		return false
	}

	_ = ch.ExchangeDelete(probe, false, false)
	_ = ch.Close()

	return true
}

// TestConsistentHashExchangePartitionsByRoutingKey verifies partitioned
// ordering: messages sharing a routing key always land on the same queue, while
// the key space as a whole is spread across the bound queues.
func TestConsistentHashExchangePartitionsByRoutingKey(t *testing.T) {
	if !consistentHashPluginEnabled(t) {
		t.Skip("rabbitmq_consistent_hash_exchange plugin is not enabled on the broker")
	}

	const topic = "consistent-hash-topic"
	declareExchangeOutOfBand(t, topic, exchangeKindConsistentHash, nil)

	log := logger.NewLogger("test")
	r := NewRabbitMQ(log).(*rabbitMQ)
	err := r.Init(t.Context(), pubsub.Metadata{Base: mdata.Base{
		Properties: map[string]string{
			metadataConnectionStringKey:    testRabbitMQURL,
			metadataConsumerIDKey:          "consistent-hash-test",
			metadataExchangeDeclareModeKey: exchangeDeclareModePassive,
			metadataExchangeKindKey:        exchangeKindConsistentHash,
			metadataDurableKey:             "true",
			metadataDeleteWhenUnusedKey:    "false",
			// Strict in-order processing within each partition.
			pubsub.ConcurrencyKey: string(pubsub.Single),
		},
	}})
	require.NoError(t, err)
	defer r.Close()

	const numPartitions = 2
	var (
		mu       sync.Mutex
		received = make(map[string][]string, numPartitions) // partition queue -> keys seen
		total    atomic.Int32
	)

	for i := range numPartitions {
		queueName := fmt.Sprintf("consistent-hash-partition-%d", i)
		err = r.Subscribe(t.Context(), pubsub.SubscribeRequest{
			Topic: topic,
			Metadata: map[string]string{
				metadataQueueNameKey: queueName,
				// For a consistent hash exchange the binding key is the
				// bucket weight of the bound queue.
				reqMetadataRoutingKey: "1",
			},
		}, func(_ context.Context, msg *pubsub.NewMessage) error {
			mu.Lock()
			received[queueName] = append(received[queueName], string(msg.Data))
			mu.Unlock()
			total.Add(1)
			return nil
		})
		require.NoError(t, err)
	}

	const numKeys = 20
	const msgsPerKey = 5
	for k := range numKeys {
		key := fmt.Sprintf("key-%d", k)
		for i := range msgsPerKey {
			err = r.Publish(t.Context(), &pubsub.PublishRequest{
				Topic:    topic,
				Data:     []byte(fmt.Sprintf("%s/%d", key, i)),
				Metadata: map[string]string{reqMetadataRoutingKey: key},
			})
			require.NoError(t, err)
		}
	}

	require.Eventually(t, func() bool {
		return total.Load() >= numKeys*msgsPerKey
	}, 30*time.Second, 100*time.Millisecond, "every published message should be delivered exactly once")

	mu.Lock()
	defer mu.Unlock()

	// Each key must be handled by exactly one partition, otherwise per-key
	// ordering is not preserved.
	keyToQueue := make(map[string]string, numKeys)
	for queueName, msgs := range received {
		for _, msg := range msgs {
			key := strings.Split(msg, "/")[0]
			if prev, ok := keyToQueue[key]; ok {
				assert.Equalf(t, prev, queueName, "key %s was delivered to more than one partition", key)
				continue
			}
			keyToQueue[key] = queueName
		}
	}
	assert.Len(t, keyToQueue, numKeys)

	// And the key space must actually be spread, otherwise the exchange is not
	// hashing on the routing key at all.
	assert.Len(t, received, numPartitions, "keys should be spread across every bound partition")
}
