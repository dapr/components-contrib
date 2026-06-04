/*
Copyright 2024 The Dapr Authors
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
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/common/component/kafka/mocks"
	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/kit/logger"
)

func Test_reloadConsumerGroup(t *testing.T) {
	t.Run("if reload called with no topics and not closed, expect return and cancel called", func(t *testing.T) {
		var consumeCalled atomic.Bool
		ctx, cancel := context.WithCancel(t.Context())
		t.Cleanup(cancel)

		cg := mocks.NewConsumerGroup().WithConsumeFn(func(context.Context, []string, sarama.ConsumerGroupHandler) error {
			consumeCalled.Store(true)
			return nil
		})

		k := &Kafka{
			logger:            logger.NewLogger("test"),
			mockConsumerGroup: cg,
			subscribeTopics:   nil,
			closeCh:           make(chan struct{}),
			consumerCancel:    cancel,
		}

		k.reloadConsumerGroup()

		require.Error(t, ctx.Err())
		assert.False(t, consumeCalled.Load())
	})

	t.Run("if reload called with topics but is closed, expect return and cancel called", func(t *testing.T) {
		var consumeCalled atomic.Bool
		ctx, cancel := context.WithCancel(t.Context())
		t.Cleanup(cancel)

		cg := mocks.NewConsumerGroup().WithConsumeFn(func(context.Context, []string, sarama.ConsumerGroupHandler) error {
			consumeCalled.Store(true)
			return nil
		})
		k := &Kafka{
			logger:            logger.NewLogger("test"),
			mockConsumerGroup: cg,
			consumerCancel:    cancel,
			closeCh:           make(chan struct{}),
			subscribeTopics:   TopicHandlerConfig{"foo": SubscriptionHandlerConfig{}},
		}

		k.closed.Store(true)

		k.reloadConsumerGroup()

		require.Error(t, ctx.Err())
		assert.False(t, consumeCalled.Load())
	})

	t.Run("if reload called with topics, expect Consume to be called. If cancelled return", func(t *testing.T) {
		var consumeCalled atomic.Bool
		var consumeCancel atomic.Bool
		cg := mocks.NewConsumerGroup().WithConsumeFn(func(ctx context.Context, _ []string, _ sarama.ConsumerGroupHandler) error {
			consumeCalled.Store(true)
			<-ctx.Done()
			consumeCancel.Store(true)
			return nil
		})
		k := &Kafka{
			logger:            logger.NewLogger("test"),
			mockConsumerGroup: cg,
			consumerCancel:    nil,
			closeCh:           make(chan struct{}),
			subscribeTopics:   TopicHandlerConfig{"foo": SubscriptionHandlerConfig{}},
		}

		k.reloadConsumerGroup()

		assert.Eventually(t, consumeCalled.Load, time.Second, time.Millisecond)
		assert.False(t, consumeCancel.Load())
		assert.NotNil(t, k.consumerCancel)

		k.consumerCancel()
		k.consumerWG.Wait()
	})

	t.Run("Consume retries if returns non-context cancel error", func(t *testing.T) {
		var consumeCalled atomic.Int64
		cg := mocks.NewConsumerGroup().WithConsumeFn(func(ctx context.Context, _ []string, _ sarama.ConsumerGroupHandler) error {
			consumeCalled.Add(1)
			return errors.New("some error")
		})
		k := &Kafka{
			logger:               logger.NewLogger("test"),
			mockConsumerGroup:    cg,
			consumerCancel:       nil,
			closeCh:              make(chan struct{}),
			subscribeTopics:      TopicHandlerConfig{"foo": SubscriptionHandlerConfig{}},
			consumeRetryInterval: time.Millisecond,
		}

		k.reloadConsumerGroup()

		assert.Eventually(t, func() bool {
			return consumeCalled.Load() > 10
		}, time.Second, time.Millisecond)

		assert.NotNil(t, k.consumerCancel)

		called := consumeCalled.Load()
		k.consumerCancel()
		k.consumerWG.Wait()
		assert.InDelta(t, called, consumeCalled.Load(), 1)
	})

	t.Run("Consume return immediately if returns a context cancelled error", func(t *testing.T) {
		var consumeCalled atomic.Int64
		cg := mocks.NewConsumerGroup().WithConsumeFn(func(ctx context.Context, _ []string, _ sarama.ConsumerGroupHandler) error {
			consumeCalled.Add(1)
			if consumeCalled.Load() == 5 {
				return context.Canceled
			}
			return errors.New("some error")
		})
		k := &Kafka{
			logger:               logger.NewLogger("test"),
			mockConsumerGroup:    cg,
			consumerCancel:       nil,
			closeCh:              make(chan struct{}),
			subscribeTopics:      map[string]SubscriptionHandlerConfig{"foo": {}},
			consumeRetryInterval: time.Millisecond,
		}

		k.reloadConsumerGroup()

		assert.Eventually(t, func() bool {
			return consumeCalled.Load() == 5
		}, time.Second, time.Millisecond)

		k.consumerWG.Wait()
		assert.Equal(t, int64(5), consumeCalled.Load())
	})

	t.Run("Calling reloadConsumerGroup causes context to be cancelled and Consume called again (close by closed)", func(t *testing.T) {
		var consumeCalled atomic.Int64
		var cancelCalled atomic.Int64
		cg := mocks.NewConsumerGroup().WithConsumeFn(func(ctx context.Context, _ []string, _ sarama.ConsumerGroupHandler) error {
			consumeCalled.Add(1)
			<-ctx.Done()
			cancelCalled.Add(1)
			return nil
		})
		k := &Kafka{
			logger:               logger.NewLogger("test"),
			mockConsumerGroup:    cg,
			consumerCancel:       nil,
			closeCh:              make(chan struct{}),
			subscribeTopics:      map[string]SubscriptionHandlerConfig{"foo": {}},
			consumeRetryInterval: time.Millisecond,
		}

		k.reloadConsumerGroup()
		assert.Eventually(t, func() bool {
			return consumeCalled.Load() == 1
		}, time.Second, time.Millisecond)
		assert.Equal(t, int64(0), cancelCalled.Load())

		k.reloadConsumerGroup()
		assert.Eventually(t, func() bool {
			return consumeCalled.Load() == 2
		}, time.Second, time.Millisecond)
		assert.Equal(t, int64(1), cancelCalled.Load())

		k.closed.Store(true)
		k.reloadConsumerGroup()
		assert.Equal(t, int64(2), cancelCalled.Load())
		assert.Equal(t, int64(2), consumeCalled.Load())
	})

	t.Run("Calling reloadConsumerGroup causes context to be cancelled and Consume called again (close by no subscriptions)", func(t *testing.T) {
		var consumeCalled atomic.Int64
		var cancelCalled atomic.Int64
		cg := mocks.NewConsumerGroup().WithConsumeFn(func(ctx context.Context, _ []string, _ sarama.ConsumerGroupHandler) error {
			consumeCalled.Add(1)
			<-ctx.Done()
			cancelCalled.Add(1)
			return nil
		})
		k := &Kafka{
			logger:               logger.NewLogger("test"),
			mockConsumerGroup:    cg,
			consumerCancel:       nil,
			closeCh:              make(chan struct{}),
			subscribeTopics:      map[string]SubscriptionHandlerConfig{"foo": {}},
			consumeRetryInterval: time.Millisecond,
		}

		k.reloadConsumerGroup()
		assert.Eventually(t, func() bool {
			return consumeCalled.Load() == 1
		}, time.Second, time.Millisecond)
		assert.Equal(t, int64(0), cancelCalled.Load())

		k.reloadConsumerGroup()
		assert.Eventually(t, func() bool {
			return consumeCalled.Load() == 2
		}, time.Second, time.Millisecond)
		assert.Equal(t, int64(1), cancelCalled.Load())

		k.subscribeTopics = nil
		k.reloadConsumerGroup()
		assert.Equal(t, int64(2), cancelCalled.Load())
		assert.Equal(t, int64(2), consumeCalled.Load())
	})

	t.Run("Cancel context whit shutdown error closes consumer group with one subscriber", func(t *testing.T) {
		var consumeCalled atomic.Int64
		var cancelCalled atomic.Int64
		var closeCalled atomic.Int64
		waitCh := make(chan struct{})
		cg := mocks.NewConsumerGroup().
			WithConsumeFn(func(ctx context.Context, _ []string, _ sarama.ConsumerGroupHandler) error {
				consumeCalled.Add(1)
				<-ctx.Done()
				cancelCalled.Add(1)
				return nil
			}).WithCloseFn(func() error {
			closeCalled.Add(1)
			waitCh <- struct{}{}
			return nil
		})

		k := &Kafka{
			logger:               logger.NewLogger("test"),
			mockConsumerGroup:    cg,
			consumerCancel:       nil,
			closeCh:              make(chan struct{}),
			subscribeTopics:      map[string]SubscriptionHandlerConfig{"foo": {}},
			consumeRetryInterval: time.Millisecond,
		}
		c, err := k.latestClients()
		require.NoError(t, err)

		k.clients = c
		ctx, cancel := context.WithCancelCause(t.Context())
		k.Subscribe(ctx, SubscriptionHandlerConfig{}, "foo")
		assert.Eventually(t, func() bool {
			return consumeCalled.Load() == 1
		}, time.Second, time.Millisecond)
		assert.Equal(t, int64(0), cancelCalled.Load())
		cancel(pubsub.ErrGracefulShutdown)
		<-waitCh
		assert.Equal(t, int64(1), closeCalled.Load())
	})

	t.Run("On graceful shutdown PauseAll is called before Close", func(t *testing.T) {
		var pauseAllCalled atomic.Int64
		var closeCalled atomic.Int64
		var pauseAllAt atomic.Int64
		var closeAt atomic.Int64
		var seq atomic.Int64
		waitCh := make(chan struct{})
		cg := mocks.NewConsumerGroup().
			WithConsumeFn(func(ctx context.Context, _ []string, _ sarama.ConsumerGroupHandler) error {
				<-ctx.Done()
				return nil
			}).
			WithPauseAllFn(func() {
				pauseAllCalled.Add(1)
				pauseAllAt.Store(seq.Add(1))
			}).
			WithCloseFn(func() error {
				closeCalled.Add(1)
				closeAt.Store(seq.Add(1))
				waitCh <- struct{}{}
				return nil
			})

		k := &Kafka{
			logger:               logger.NewLogger("test"),
			mockConsumerGroup:    cg,
			consumerCancel:       nil,
			closeCh:              make(chan struct{}),
			subscribeTopics:      map[string]SubscriptionHandlerConfig{"foo": {}},
			consumeRetryInterval: time.Millisecond,
		}
		c, err := k.latestClients()
		require.NoError(t, err)
		k.clients = c

		ctx, cancel := context.WithCancelCause(t.Context())
		k.Subscribe(ctx, SubscriptionHandlerConfig{}, "foo")

		cancel(pubsub.ErrGracefulShutdown)
		<-waitCh

		assert.Equal(t, int64(1), pauseAllCalled.Load(), "PauseAll should be called exactly once on graceful shutdown")
		assert.Equal(t, int64(1), closeCalled.Load(), "Close should be called exactly once on graceful shutdown")
		assert.Less(t, pauseAllAt.Load(), closeAt.Load(), "PauseAll should be called before Close")
	})

	t.Run("Non-graceful shutdown does not call PauseAll", func(t *testing.T) {
		var pauseAllCalled atomic.Int64
		var consumeCalled atomic.Int64
		cg := mocks.NewConsumerGroup().
			WithConsumeFn(func(ctx context.Context, _ []string, _ sarama.ConsumerGroupHandler) error {
				consumeCalled.Add(1)
				<-ctx.Done()
				return nil
			}).
			WithPauseAllFn(func() {
				pauseAllCalled.Add(1)
			})

		k := &Kafka{
			logger:               logger.NewLogger("test"),
			mockConsumerGroup:    cg,
			consumerCancel:       nil,
			closeCh:              make(chan struct{}),
			subscribeTopics:      map[string]SubscriptionHandlerConfig{"foo": {}},
			consumeRetryInterval: time.Millisecond,
		}
		c, err := k.latestClients()
		require.NoError(t, err)
		k.clients = c

		ctx, cancel := context.WithCancelCause(t.Context())
		k.Subscribe(ctx, SubscriptionHandlerConfig{}, "foo")
		assert.Eventually(t, func() bool { return consumeCalled.Load() == 1 }, time.Second, time.Millisecond)

		cancel(errors.New("non-graceful cancel"))
		// Wait for the unsubscribe goroutine to finish.
		k.wg.Wait()
		assert.Equal(t, int64(0), pauseAllCalled.Load(), "PauseAll should not be called for non-graceful shutdown")
	})

	t.Run("Cancelling one of many subscribers does NOT close the consumer group", func(t *testing.T) {
		var closeCalled atomic.Int64
		cg := mocks.NewConsumerGroup().WithCloseFn(func() error {
			closeCalled.Add(1)
			return nil
		})

		k := &Kafka{
			logger:               logger.NewLogger("test"),
			mockConsumerGroup:    cg,
			consumerCancel:       nil,
			closeCh:              make(chan struct{}),
			subscribeTopics:      map[string]SubscriptionHandlerConfig{},
			consumeRetryInterval: time.Millisecond,
		}
		c, err := k.latestClients()
		require.NoError(t, err)
		k.clients = c

		cancelFns := make([]context.CancelCauseFunc, 0, 100)
		for i := range 100 {
			ctx, cancel := context.WithCancelCause(t.Context())
			cancelFns = append(cancelFns, cancel)
			k.Subscribe(ctx, SubscriptionHandlerConfig{}, fmt.Sprintf("foo%d", i))
		}

		// Cancel one of the 100 subscribers with the graceful shutdown cause.
		// The other 99 still hold subscriptions on the same consumer group, so
		// Close MUST NOT fire — doing so would race with the new consume
		// goroutine reloadConsumerGroup just started for the remaining topics.
		cancelFns[0](pubsub.ErrGracefulShutdown)

		// Give the unsubscribe goroutine ample time to run.
		assert.Never(t, func() bool { return closeCalled.Load() > 0 }, time.Second, time.Millisecond*10,
			"consumer group must not be closed while other subscriptions are still active")
	})

	t.Run("Cancelling all subscribers with shutdown error closes the consumer group exactly once", func(t *testing.T) {
		var closeCalled atomic.Int64
		var consumeErrors atomic.Int64
		closedFlag := atomic.Bool{}

		cg := mocks.NewConsumerGroup().
			WithConsumeFn(func(ctx context.Context, _ []string, _ sarama.ConsumerGroupHandler) error {
				if closedFlag.Load() {
					// Sarama would return this error if Consume is called on a
					// closed group. Track it so we can assert no spurious
					// "consumer group closed" errors leak through during
					// the unsubscribe cascade.
					consumeErrors.Add(1)
					return errors.New("kafka: tried to use a consumer group that was closed")
				}
				<-ctx.Done()
				return nil
			}).
			WithCloseFn(func() error {
				closeCalled.Add(1)
				closedFlag.Store(true)
				return nil
			})

		k := &Kafka{
			logger:               logger.NewLogger("test"),
			mockConsumerGroup:    cg,
			consumerCancel:       nil,
			closeCh:              make(chan struct{}),
			subscribeTopics:      map[string]SubscriptionHandlerConfig{},
			consumeRetryInterval: time.Hour, // make sure retry sleep doesn't kick in during the test
		}
		c, err := k.latestClients()
		require.NoError(t, err)
		k.clients = c

		const N = 100
		cancelFns := make([]context.CancelCauseFunc, 0, N)
		for i := range N {
			ctx, cancel := context.WithCancelCause(t.Context())
			cancelFns = append(cancelFns, cancel)
			k.Subscribe(ctx, SubscriptionHandlerConfig{}, fmt.Sprintf("foo%d", i))
		}

		// Cancel all subscribers with the graceful-shutdown cause in parallel
		// to exercise the race where multiple Subscribe goroutines race for
		// the subscribe lock while siblings still hold subscriptions on the
		// same shared consumer group.
		for _, cancel := range cancelFns {
			cancel(pubsub.ErrGracefulShutdown)
		}

		// Wait for all Subscribe goroutines to finish.
		k.wg.Wait()

		assert.Equal(t, int64(1), closeCalled.Load(),
			"consumer group should be closed exactly once after the last subscription exits")
		assert.Equal(t, int64(0), consumeErrors.Load(),
			"no consume goroutine should see the consumer group closed during the unsubscribe cascade")
	})

	t.Run("Closing subscriptions with no error or no ErrGracefulShutdown does not close consumer group", func(t *testing.T) {
		var closeCalled atomic.Int64
		waitCh := make(chan struct{})
		cg := mocks.NewConsumerGroup().WithCloseFn(func() error {
			closeCalled.Add(1)
			waitCh <- struct{}{}
			return nil
		})

		k := &Kafka{
			logger:               logger.NewLogger("test"),
			mockConsumerGroup:    cg,
			consumerCancel:       nil,
			closeCh:              make(chan struct{}),
			subscribeTopics:      map[string]SubscriptionHandlerConfig{"foo": {}},
			consumeRetryInterval: time.Millisecond,
		}
		c, err := k.latestClients()
		require.NoError(t, err)

		k.clients = c
		cancelFns := make([]context.CancelCauseFunc, 0, 100)
		for i := range 100 {
			ctx, cancel := context.WithCancelCause(t.Context())
			cancelFns = append(cancelFns, cancel)
			k.Subscribe(ctx, SubscriptionHandlerConfig{}, fmt.Sprintf("foo%d", i))
		}
		cancelFns[0](errors.New("some error"))
		time.Sleep(1 * time.Second)
		assert.Equal(t, int64(0), closeCalled.Load())

		cancelFns[4](nil)
		time.Sleep(1 * time.Second)
		assert.Equal(t, int64(0), closeCalled.Load())
	})
}

func Test_PauseResume(t *testing.T) {
	t.Run("Pause forwards to consumerGroup.PauseAll", func(t *testing.T) {
		var pauseAllCalled atomic.Int64
		cg := mocks.NewConsumerGroup().WithPauseAllFn(func() {
			pauseAllCalled.Add(1)
		})
		k := &Kafka{
			logger:            logger.NewLogger("test"),
			mockConsumerGroup: cg,
			closeCh:           make(chan struct{}),
		}
		c, err := k.latestClients()
		require.NoError(t, err)
		k.clients = c

		require.NoError(t, k.Pause(t.Context()))
		assert.Equal(t, int64(1), pauseAllCalled.Load())

		// Idempotent: calling again should still succeed.
		require.NoError(t, k.Pause(t.Context()))
		assert.Equal(t, int64(2), pauseAllCalled.Load())
	})

	t.Run("Resume forwards to consumerGroup.ResumeAll", func(t *testing.T) {
		var resumeAllCalled atomic.Int64
		cg := mocks.NewConsumerGroup().WithResumeAllFn(func() {
			resumeAllCalled.Add(1)
		})
		k := &Kafka{
			logger:            logger.NewLogger("test"),
			mockConsumerGroup: cg,
			closeCh:           make(chan struct{}),
		}
		c, err := k.latestClients()
		require.NoError(t, err)
		k.clients = c

		require.NoError(t, k.Resume(t.Context()))
		assert.Equal(t, int64(1), resumeAllCalled.Load())

		// Idempotent: calling again should still succeed.
		require.NoError(t, k.Resume(t.Context()))
		assert.Equal(t, int64(2), resumeAllCalled.Load())
	})

	t.Run("Pause is a no-op when consumerGroup is nil", func(t *testing.T) {
		k := &Kafka{
			logger:  logger.NewLogger("test"),
			closeCh: make(chan struct{}),
			clients: &clients{}, // consumerGroup is nil
		}
		require.NoError(t, k.Pause(t.Context()))
		require.NoError(t, k.Resume(t.Context()))
	})

	t.Run("Pause is a no-op when clients is nil", func(t *testing.T) {
		k := &Kafka{
			logger:  logger.NewLogger("test"),
			closeCh: make(chan struct{}),
		}
		require.NoError(t, k.Pause(t.Context()))
		require.NoError(t, k.Resume(t.Context()))
	})
}

func Test_Subscribe(t *testing.T) {
	t.Run("Calling subscribe with no topics should not consume", func(t *testing.T) {
		var consumeCalled atomic.Int64
		var cancelCalled atomic.Int64
		cg := mocks.NewConsumerGroup().WithConsumeFn(func(ctx context.Context, _ []string, _ sarama.ConsumerGroupHandler) error {
			consumeCalled.Add(1)
			<-ctx.Done()
			cancelCalled.Add(1)
			return nil
		})
		k := &Kafka{
			logger:               logger.NewLogger("test"),
			mockConsumerGroup:    cg,
			consumerCancel:       nil,
			closeCh:              make(chan struct{}),
			consumeRetryInterval: time.Millisecond,
			subscribeTopics:      make(TopicHandlerConfig),
		}

		k.Subscribe(t.Context(), SubscriptionHandlerConfig{})

		assert.Nil(t, k.consumerCancel)
		assert.Equal(t, int64(0), consumeCalled.Load())
		assert.Equal(t, int64(0), cancelCalled.Load())
	})

	t.Run("Calling subscribe when closed should not consume", func(t *testing.T) {
		var consumeCalled atomic.Int64
		var cancelCalled atomic.Int64
		cg := mocks.NewConsumerGroup().WithConsumeFn(func(ctx context.Context, _ []string, _ sarama.ConsumerGroupHandler) error {
			consumeCalled.Add(1)
			<-ctx.Done()
			cancelCalled.Add(1)
			return nil
		})
		k := &Kafka{
			logger:               logger.NewLogger("test"),
			mockConsumerGroup:    cg,
			consumerCancel:       nil,
			closeCh:              make(chan struct{}),
			consumeRetryInterval: time.Millisecond,
			subscribeTopics:      make(TopicHandlerConfig),
		}

		k.closed.Store(true)

		k.Subscribe(t.Context(), SubscriptionHandlerConfig{}, "abc")

		assert.Nil(t, k.consumerCancel)
		assert.Equal(t, int64(0), consumeCalled.Load())
		assert.Equal(t, int64(0), cancelCalled.Load())
	})

	t.Run("Subscribe should subscribe to a topic until context is cancelled", func(t *testing.T) {
		var consumeCalled atomic.Int64
		var cancelCalled atomic.Int64
		var consumeTopics atomic.Value
		cg := mocks.NewConsumerGroup().WithConsumeFn(func(ctx context.Context, topics []string, _ sarama.ConsumerGroupHandler) error {
			consumeTopics.Store(topics)
			consumeCalled.Add(1)
			<-ctx.Done()
			cancelCalled.Add(1)
			return nil
		})
		k := &Kafka{
			logger:               logger.NewLogger("test"),
			mockConsumerGroup:    cg,
			consumerCancel:       nil,
			closeCh:              make(chan struct{}),
			consumeRetryInterval: time.Millisecond,
			subscribeTopics:      make(TopicHandlerConfig),
		}

		ctx, cancel := context.WithCancel(t.Context())
		k.Subscribe(ctx, SubscriptionHandlerConfig{}, "abc")

		assert.Eventually(t, func() bool {
			return consumeCalled.Load() == 1
		}, time.Second, time.Millisecond)
		assert.Equal(t, int64(0), cancelCalled.Load())

		cancel()

		assert.Eventually(t, func() bool {
			return cancelCalled.Load() == 1
		}, time.Second, time.Millisecond)
		assert.Equal(t, int64(1), consumeCalled.Load())

		assert.Equal(t, []string{"abc"}, consumeTopics.Load())
	})

	t.Run("Calling subscribe multiple times with new topics should re-consume will full topics list", func(t *testing.T) {
		var consumeCalled atomic.Int64
		var cancelCalled atomic.Int64
		var consumeTopics atomic.Value
		cg := mocks.NewConsumerGroup().WithConsumeFn(func(ctx context.Context, topics []string, _ sarama.ConsumerGroupHandler) error {
			consumeTopics.Store(topics)
			consumeCalled.Add(1)
			<-ctx.Done()
			cancelCalled.Add(1)
			return nil
		})
		k := &Kafka{
			logger:               logger.NewLogger("test"),
			mockConsumerGroup:    cg,
			consumerCancel:       nil,
			closeCh:              make(chan struct{}),
			consumeRetryInterval: time.Millisecond,
			subscribeTopics:      make(TopicHandlerConfig),
		}

		ctx, cancel := context.WithCancel(t.Context())
		k.Subscribe(ctx, SubscriptionHandlerConfig{}, "abc")

		assert.Eventually(t, func() bool {
			return consumeCalled.Load() == 1
		}, time.Second, time.Millisecond)
		assert.Equal(t, int64(0), cancelCalled.Load())
		assert.Equal(t, []string{"abc"}, consumeTopics.Load())
		assert.Equal(t, TopicHandlerConfig{"abc": SubscriptionHandlerConfig{}}, k.subscribeTopics)

		k.Subscribe(ctx, SubscriptionHandlerConfig{}, "def")
		assert.Equal(t, TopicHandlerConfig{
			"abc": SubscriptionHandlerConfig{},
			"def": SubscriptionHandlerConfig{},
		}, k.subscribeTopics)

		assert.Eventually(t, func() bool {
			return consumeCalled.Load() == 2
		}, time.Second, time.Millisecond)
		assert.Equal(t, int64(1), cancelCalled.Load())
		assert.ElementsMatch(t, []string{"abc", "def"}, consumeTopics.Load())

		cancel()
		assert.Eventually(t, func() bool {
			return cancelCalled.Load() == 3
		}, time.Second, time.Millisecond)
		assert.Equal(t, int64(3), consumeCalled.Load())

		k.Subscribe(ctx, SubscriptionHandlerConfig{})
		assert.Nil(t, k.consumerCancel)
		assert.Empty(t, k.subscribeTopics)
	})

	t.Run("Consume return immediately if returns a context cancelled error", func(t *testing.T) {
		var consumeCalled atomic.Int64
		cg := mocks.NewConsumerGroup().WithConsumeFn(func(ctx context.Context, _ []string, _ sarama.ConsumerGroupHandler) error {
			consumeCalled.Add(1)
			if consumeCalled.Load() == 5 {
				return context.Canceled
			}
			return errors.New("some error")
		})
		k := &Kafka{
			logger:               logger.NewLogger("test"),
			mockConsumerGroup:    cg,
			consumerCancel:       nil,
			closeCh:              make(chan struct{}),
			subscribeTopics:      make(TopicHandlerConfig),
			consumeRetryInterval: time.Millisecond,
		}

		k.Subscribe(t.Context(), SubscriptionHandlerConfig{}, "foo")
		assert.Equal(t, TopicHandlerConfig{"foo": SubscriptionHandlerConfig{}}, k.subscribeTopics)
		assert.Eventually(t, func() bool {
			return consumeCalled.Load() == 5
		}, time.Second, time.Millisecond)
		k.consumerWG.Wait()
		assert.Equal(t, int64(5), consumeCalled.Load())
		assert.Equal(t, TopicHandlerConfig{"foo": SubscriptionHandlerConfig{}}, k.subscribeTopics)
	})

	t.Run("Consume dynamically changes topics which are being consumed", func(t *testing.T) {
		var consumeTopics atomic.Value
		var consumeCalled atomic.Int64
		var cancelCalled atomic.Int64
		cg := mocks.NewConsumerGroup().WithConsumeFn(func(ctx context.Context, topics []string, _ sarama.ConsumerGroupHandler) error {
			consumeTopics.Store(topics)
			consumeCalled.Add(1)
			<-ctx.Done()
			cancelCalled.Add(1)
			return nil
		})
		k := &Kafka{
			logger:               logger.NewLogger("test"),
			mockConsumerGroup:    cg,
			consumerCancel:       nil,
			closeCh:              make(chan struct{}),
			subscribeTopics:      make(TopicHandlerConfig),
			consumeRetryInterval: time.Millisecond,
		}

		ctx1, cancel1 := context.WithCancel(t.Context())
		k.Subscribe(ctx1, SubscriptionHandlerConfig{}, "abc")
		assert.Eventually(t, func() bool {
			return consumeCalled.Load() == 1
		}, time.Second, time.Millisecond)
		assert.ElementsMatch(t, []string{"abc"}, consumeTopics.Load())
		assert.Equal(t, int64(0), cancelCalled.Load())

		ctx2, cancel2 := context.WithCancel(t.Context())
		k.Subscribe(ctx2, SubscriptionHandlerConfig{}, "def")
		assert.Eventually(t, func() bool {
			return consumeCalled.Load() == 2
		}, time.Second, time.Millisecond)
		assert.ElementsMatch(t, []string{"abc", "def"}, consumeTopics.Load())
		assert.Equal(t, int64(1), cancelCalled.Load())

		ctx3, cancel3 := context.WithCancel(t.Context())
		k.Subscribe(ctx3, SubscriptionHandlerConfig{}, "123")
		assert.Eventually(t, func() bool {
			return consumeCalled.Load() == 3
		}, time.Second, time.Millisecond)
		assert.ElementsMatch(t, []string{"abc", "def", "123"}, consumeTopics.Load())
		assert.Equal(t, int64(2), cancelCalled.Load())

		cancel2()
		assert.Eventually(t, func() bool {
			return consumeCalled.Load() == 4
		}, time.Second, time.Millisecond)
		assert.ElementsMatch(t, []string{"abc", "123"}, consumeTopics.Load())
		assert.Equal(t, int64(3), cancelCalled.Load())

		ctx2, cancel2 = context.WithCancel(t.Context())
		k.Subscribe(ctx2, SubscriptionHandlerConfig{}, "456")
		assert.Eventually(t, func() bool {
			return consumeCalled.Load() == 5
		}, time.Second, time.Millisecond)
		assert.ElementsMatch(t, []string{"abc", "123", "456"}, consumeTopics.Load())
		assert.Equal(t, int64(4), cancelCalled.Load())

		cancel1()
		cancel3()

		assert.Eventually(t, func() bool {
			return consumeCalled.Load() == 7
		}, time.Second, time.Millisecond)
		assert.ElementsMatch(t, []string{"456"}, consumeTopics.Load())
		assert.Equal(t, int64(6), cancelCalled.Load())

		cancel2()
		assert.Eventually(t, func() bool {
			return cancelCalled.Load() == 7
		}, time.Second, time.Millisecond)
		assert.Empty(t, k.subscribeTopics)
		assert.Equal(t, int64(7), consumeCalled.Load())
	})

	t.Run("Can call Subscribe concurrently", func(t *testing.T) {
		var cancelCalled atomic.Int64
		var consumeCalled atomic.Int64
		cg := mocks.NewConsumerGroup().WithConsumeFn(func(ctx context.Context, topics []string, _ sarama.ConsumerGroupHandler) error {
			consumeCalled.Add(1)
			<-ctx.Done()
			cancelCalled.Add(1)
			return nil
		})
		k := &Kafka{
			logger:               logger.NewLogger("test"),
			mockConsumerGroup:    cg,
			consumerCancel:       nil,
			closeCh:              make(chan struct{}),
			subscribeTopics:      make(TopicHandlerConfig),
			consumeRetryInterval: time.Millisecond,
		}

		ctx, cancel := context.WithCancel(t.Context())
		for i := range 100 {
			go func(i int) {
				k.Subscribe(ctx, SubscriptionHandlerConfig{}, strconv.Itoa(i))
			}(i)
		}

		assert.Eventually(t, func() bool {
			return consumeCalled.Load() == 100
		}, time.Second, time.Millisecond)
		assert.Equal(t, int64(99), cancelCalled.Load())
		cancel()
		assert.Eventually(t, func() bool {
			return cancelCalled.Load() == 199
		}, time.Second, time.Millisecond)
		assert.Equal(t, int64(199), consumeCalled.Load())
	})
}
