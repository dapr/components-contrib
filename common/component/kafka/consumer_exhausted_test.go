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
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/kit/logger"
	"github.com/dapr/kit/retry"
)

// Test_ConsumeClaim_RetriesExhausted covers dapr/components-contrib#4362.
//
// A handler that returns pubsub.ErrRetriesExhausted has permanently given up
// on the message: its retry policy ran out and there was no dead letter topic
// to divert it to. The offset must be committed so that decision survives a
// rebalance or a restart, rather than leaving the partition parked behind a
// message that will only exhaust again on redelivery.
//
// Every other error still means "redeliver this", and must leave the offset
// alone. That distinction is the whole point of the sentinel: a cancelled
// context comes back routinely during a rebalance, and committing on it would
// drop a message that nobody has processed.
func Test_ConsumeClaim_RetriesExhausted(t *testing.T) {
	newMessage := func(topic string) *sarama.ConsumerMessage {
		return &sarama.ConsumerMessage{
			Topic:     topic,
			Partition: 0,
			Offset:    1,
			Key:       []byte("test-key"),
			Value:     []byte("test-value"),
		}
	}

	// run drives ConsumeClaim over a single message and returns how many times
	// the handler was invoked.
	run := func(t *testing.T, k *Kafka, topic string, msg *sarama.ConsumerMessage, session *mockConsumerGroupSession, handlerErr error) int64 {
		t.Helper()

		var calls atomic.Int64
		k.subscribeTopics[topic] = SubscriptionHandlerConfig{
			Handler: func(context.Context, *NewEvent) error {
				calls.Add(1)
				return handlerErr
			},
		}

		claim := &mockConsumerGroupClaim{
			messages: make(chan *sarama.ConsumerMessage, 1),
			topic:    topic,
		}
		claim.messages <- msg

		// End the claim once the handler has been left alone for a moment, so
		// ConsumeClaim returns instead of blocking on an empty channel.
		go func() {
			for calls.Load() == 0 {
				time.Sleep(time.Millisecond)
			}
			time.Sleep(time.Millisecond * 50)
			session.cancel()
		}()

		consumer := &consumer{k: k, mutex: sync.Mutex{}}
		require.NoError(t, consumer.ConsumeClaim(session, claim))

		return calls.Load()
	}

	t.Run("no retry", func(t *testing.T) {
		k := &Kafka{
			logger:              logger.NewLogger("test"),
			consumeRetryEnabled: false,
			subscribeTopics:     make(map[string]SubscriptionHandlerConfig),
		}

		t.Run("exhausted retries commit the offset", func(t *testing.T) {
			topic := "exhausted-no-retry"
			msg := newMessage(topic)

			ctx, cancel := context.WithCancel(t.Context())
			session := &mockConsumerGroupSession{ctx: ctx, cancel: cancel}
			session.On("MarkMessage", msg, "").Return()

			run(t, k, topic, msg, session, fmt.Errorf("app kept failing: %w", pubsub.ErrRetriesExhausted))

			session.AssertNumberOfCalls(t, "MarkMessage", 1)
		})

		t.Run("a plain delivery error leaves the offset alone", func(t *testing.T) {
			topic := "failed-no-retry"
			msg := newMessage(topic)

			ctx, cancel := context.WithCancel(t.Context())
			session := &mockConsumerGroupSession{ctx: ctx, cancel: cancel}

			run(t, k, topic, msg, session, errors.New("app returned 500"))

			session.AssertNotCalled(t, "MarkMessage", msg, "")
		})

		t.Run("a cancelled context leaves the offset alone", func(t *testing.T) {
			topic := "cancelled-no-retry"
			msg := newMessage(topic)

			ctx, cancel := context.WithCancel(t.Context())
			session := &mockConsumerGroupSession{ctx: ctx, cancel: cancel}

			run(t, k, topic, msg, session, context.Canceled)

			session.AssertNotCalled(t, "MarkMessage", msg, "")
		})
	})

	t.Run("retry", func(t *testing.T) {
		// MaxRetries -1 is the component default: without treating exhaustion
		// as permanent, this loop would redeliver the message forever.
		k := &Kafka{
			logger:              logger.NewLogger("test"),
			consumeRetryEnabled: true,
			backOffConfig: retry.Config{
				Policy:     retry.PolicyConstant,
				Duration:   time.Millisecond,
				MaxRetries: -1,
			},
			subscribeTopics: make(map[string]SubscriptionHandlerConfig),
		}

		t.Run("exhausted retries are permanent and commit the offset", func(t *testing.T) {
			topic := "exhausted-retry"
			msg := newMessage(topic)

			ctx, cancel := context.WithCancel(t.Context())
			session := &mockConsumerGroupSession{ctx: ctx, cancel: cancel}
			session.On("MarkMessage", msg, "").Return()

			calls := run(t, k, topic, msg, session, fmt.Errorf("app kept failing: %w", pubsub.ErrRetriesExhausted))

			assert.Equal(t, int64(1), calls,
				"the component retry loop must not redeliver a message the caller has permanently given up on")
			session.AssertNumberOfCalls(t, "MarkMessage", 1)
		})
	})
}
