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
