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
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/common/component/kafka/mocks"
	"github.com/dapr/kit/logger"
)

func Test_reloadConsumerGroup(t *testing.T) {
	t.Run("if reload called with no topics and not closed, expect return and cancel called", func(t *testing.T) {
		var consumeCalled atomic.Bool
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)

		cg := &ConsumerGroup{
			groupID: "",
			cg: mocks.NewConsumerGroup().WithConsumeFn(func(context.Context, []string, sarama.ConsumerGroupHandler) error {
				consumeCalled.Store(true)
				return nil
			}),
			subscribeTopics: nil,
			consumerCancel:  cancel,
		}

		k := &Kafka{
			logger:         logger.NewLogger("test"),
			consumerGroups: map[string]*ConsumerGroup{cg.groupID: cg},
			closeCh:        make(chan struct{}),
		}

		k.reloadConsumerGroup(cg)

		require.Error(t, ctx.Err())
		assert.False(t, consumeCalled.Load())
	})

	t.Run("if reload called with topics but is closed, expect return and cancel called", func(t *testing.T) {
		var consumeCalled atomic.Bool
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)

		cg := &ConsumerGroup{
			groupID: "",
			cg: mocks.NewConsumerGroup().WithConsumeFn(func(context.Context, []string, sarama.ConsumerGroupHandler) error {
				consumeCalled.Store(true)
				return nil
			}),
			subscribeTopics: TopicHandlerConfig{"foo": SubscriptionHandlerConfig{}},
			consumerCancel:  cancel,
		}
		k := &Kafka{
			logger:         logger.NewLogger("test"),
			consumerGroups: map[string]*ConsumerGroup{cg.groupID: cg},
			closeCh:        make(chan struct{}),
		}

		k.closed.Store(true)

		k.reloadConsumerGroup(cg)

		require.Error(t, ctx.Err())
		assert.False(t, consumeCalled.Load())
	})

	t.Run("if reload called with topics, expect Consume to be called. If cancelled return", func(t *testing.T) {
		var consumeCalled atomic.Bool
		var consumeCancel atomic.Bool

		cg := &ConsumerGroup{
			groupID: "",
			cg: mocks.NewConsumerGroup().WithConsumeFn(func(ctx context.Context, _ []string, _ sarama.ConsumerGroupHandler) error {
				consumeCalled.Store(true)
				<-ctx.Done()
				consumeCancel.Store(true)
				return nil
			}),
			subscribeTopics: TopicHandlerConfig{"foo": SubscriptionHandlerConfig{}},
			consumerCancel:  nil,
		}

		k := &Kafka{
			logger:         logger.NewLogger("test"),
			consumerGroups: map[string]*ConsumerGroup{cg.groupID: cg},
			closeCh:        make(chan struct{}),
		}

		k.reloadConsumerGroup(cg)

		assert.Eventually(t, consumeCalled.Load, time.Second, time.Millisecond)
		assert.False(t, consumeCancel.Load())
		assert.NotNil(t, cg.consumerCancel)

		cg.consumerCancel()
		cg.consumerWG.Wait()
	})

	t.Run("Consume retries if returns non-context cancel error", func(t *testing.T) {
		var consumeCalled atomic.Int64

		cg := &ConsumerGroup{
			groupID: "",
			cg: mocks.NewConsumerGroup().WithConsumeFn(func(ctx context.Context, _ []string, _ sarama.ConsumerGroupHandler) error {
				consumeCalled.Add(1)
				return errors.New("some error")
			}),
			subscribeTopics: TopicHandlerConfig{"foo": SubscriptionHandlerConfig{}},
			consumerCancel:  nil,
		}
		k := &Kafka{
			logger:               logger.NewLogger("test"),
			consumerGroups:       map[string]*ConsumerGroup{cg.groupID: cg},
			closeCh:              make(chan struct{}),
			consumeRetryInterval: time.Millisecond,
		}

		k.reloadConsumerGroup(cg)

		assert.Eventually(t, func() bool {
			return consumeCalled.Load() > 10
		}, time.Second, time.Millisecond)

		assert.NotNil(t, cg.consumerCancel)

		called := consumeCalled.Load()
		cg.consumerCancel()
		cg.consumerWG.Wait()
		assert.InDelta(t, called, consumeCalled.Load(), 1)
	})

	t.Run("Consume return immediately if returns a context cancelled error", func(t *testing.T) {
		var consumeCalled atomic.Int64

		cg := &ConsumerGroup{
			groupID: "",
			cg: mocks.NewConsumerGroup().WithConsumeFn(func(ctx context.Context, _ []string, _ sarama.ConsumerGroupHandler) error {
				consumeCalled.Add(1)
				if consumeCalled.Load() == 5 {
					return context.Canceled
				}
				return errors.New("some error")
			}),
			subscribeTopics: map[string]SubscriptionHandlerConfig{"foo": {}},
			consumerCancel:  nil,
		}
		k := &Kafka{
			logger:               logger.NewLogger("test"),
			consumerGroups:       map[string]*ConsumerGroup{cg.groupID: cg},
			closeCh:              make(chan struct{}),
			consumeRetryInterval: time.Millisecond,
		}

		k.reloadConsumerGroup(cg)

		assert.Eventually(t, func() bool {
			return consumeCalled.Load() == 5
		}, time.Second, time.Millisecond)

		cg.consumerWG.Wait()
		assert.Equal(t, int64(5), consumeCalled.Load())
	})

	t.Run("Calling reloadConsumerGroup causes context to be cancelled and Consume called again (close by closed)", func(t *testing.T) {
		var consumeCalled atomic.Int64
		var cancelCalled atomic.Int64

		cg := &ConsumerGroup{
			groupID: "",
			cg: mocks.NewConsumerGroup().WithConsumeFn(func(ctx context.Context, _ []string, _ sarama.ConsumerGroupHandler) error {
				consumeCalled.Add(1)
				<-ctx.Done()
				cancelCalled.Add(1)
				return nil
			}),
			subscribeTopics: map[string]SubscriptionHandlerConfig{"foo": {}},
			consumerCancel:  nil,
		}
		k := &Kafka{
			logger:               logger.NewLogger("test"),
			consumerGroups:       map[string]*ConsumerGroup{cg.groupID: cg},
			closeCh:              make(chan struct{}),
			consumeRetryInterval: time.Millisecond,
		}

		k.reloadConsumerGroup(cg)
		assert.Eventually(t, func() bool {
			return consumeCalled.Load() == 1
		}, time.Second, time.Millisecond)
		assert.Equal(t, int64(0), cancelCalled.Load())

		k.reloadConsumerGroup(cg)
		assert.Eventually(t, func() bool {
			return consumeCalled.Load() == 2
		}, time.Second, time.Millisecond)
		assert.Equal(t, int64(1), cancelCalled.Load())

		k.closed.Store(true)
		k.reloadConsumerGroup(cg)
		assert.Equal(t, int64(2), cancelCalled.Load())
		assert.Equal(t, int64(2), consumeCalled.Load())
	})

	t.Run("Calling reloadConsumerGroup causes context to be cancelled and Consume called again (close by no subscriptions)", func(t *testing.T) {
		var consumeCalled atomic.Int64
		var cancelCalled atomic.Int64

		cg := &ConsumerGroup{
			groupID: "",
			cg: mocks.NewConsumerGroup().WithConsumeFn(func(ctx context.Context, _ []string, _ sarama.ConsumerGroupHandler) error {
				consumeCalled.Add(1)
				<-ctx.Done()
				cancelCalled.Add(1)
				return nil
			}),
			subscribeTopics: map[string]SubscriptionHandlerConfig{"foo": {}},
			consumerCancel:  nil,
		}

		k := &Kafka{
			logger:               logger.NewLogger("test"),
			consumerGroups:       map[string]*ConsumerGroup{cg.groupID: cg},
			closeCh:              make(chan struct{}),
			consumeRetryInterval: time.Millisecond,
		}

		k.reloadConsumerGroup(cg)
		assert.Eventually(t, func() bool {
			return consumeCalled.Load() == 1
		}, time.Second, time.Millisecond)
		assert.Equal(t, int64(0), cancelCalled.Load())

		k.reloadConsumerGroup(cg)
		assert.Eventually(t, func() bool {
			return consumeCalled.Load() == 2
		}, time.Second, time.Millisecond)
		assert.Equal(t, int64(1), cancelCalled.Load())

		cg.subscribeTopics = nil
		k.reloadConsumerGroup(cg)
		assert.Equal(t, int64(2), cancelCalled.Load())
		assert.Equal(t, int64(2), consumeCalled.Load())
	})
}

func Test_Subscribe(t *testing.T) {
	t.Run("Calling subscribe with no topics should not consume", func(t *testing.T) {
		var consumeCalled atomic.Int64
		var cancelCalled atomic.Int64

		cg := &ConsumerGroup{
			groupID: "",
			cg: mocks.NewConsumerGroup().WithConsumeFn(func(ctx context.Context, _ []string, _ sarama.ConsumerGroupHandler) error {
				consumeCalled.Add(1)
				<-ctx.Done()
				cancelCalled.Add(1)
				return nil
			}),
			subscribeTopics: make(TopicHandlerConfig),
			consumerCancel:  nil,
		}

		k := &Kafka{
			logger:               logger.NewLogger("test"),
			consumerGroups:       map[string]*ConsumerGroup{cg.groupID: cg},
			closeCh:              make(chan struct{}),
			consumeRetryInterval: time.Millisecond,
		}

		k.Subscribe(context.Background(), SubscriptionHandlerConfig{})

		assert.Nil(t, cg.consumerCancel)
		assert.Equal(t, int64(0), consumeCalled.Load())
		assert.Equal(t, int64(0), cancelCalled.Load())
	})

	t.Run("Calling subscribe when closed should not consume", func(t *testing.T) {
		var consumeCalled atomic.Int64
		var cancelCalled atomic.Int64

		cg := &ConsumerGroup{
			groupID: "",
			cg: mocks.NewConsumerGroup().WithConsumeFn(func(ctx context.Context, _ []string, _ sarama.ConsumerGroupHandler) error {
				consumeCalled.Add(1)
				<-ctx.Done()
				cancelCalled.Add(1)
				return nil
			}),
			subscribeTopics: make(TopicHandlerConfig),
			consumerCancel:  nil,
		}

		k := &Kafka{
			logger:               logger.NewLogger("test"),
			consumerGroups:       map[string]*ConsumerGroup{cg.groupID: cg},
			closeCh:              make(chan struct{}),
			consumeRetryInterval: time.Millisecond,
		}

		k.closed.Store(true)

		k.Subscribe(context.Background(), SubscriptionHandlerConfig{}, "abc")

		assert.Nil(t, cg.consumerCancel)
		assert.Equal(t, int64(0), consumeCalled.Load())
		assert.Equal(t, int64(0), cancelCalled.Load())
	})

	t.Run("Subscribe should subscribe to a topic until context is cancelled", func(t *testing.T) {
		var consumeCalled atomic.Int64
		var cancelCalled atomic.Int64
		var consumeTopics atomic.Value

		cg := &ConsumerGroup{
			groupID: "",
			cg: mocks.NewConsumerGroup().WithConsumeFn(func(ctx context.Context, topics []string, _ sarama.ConsumerGroupHandler) error {
				consumeTopics.Store(topics)
				consumeCalled.Add(1)
				<-ctx.Done()
				cancelCalled.Add(1)
				return nil
			}),
			subscribeTopics: make(TopicHandlerConfig),
			consumerCancel:  nil,
		}
		k := &Kafka{
			logger:               logger.NewLogger("test"),
			consumerGroups:       map[string]*ConsumerGroup{cg.groupID: cg},
			closeCh:              make(chan struct{}),
			consumeRetryInterval: time.Millisecond,
		}

		ctx, cancel := context.WithCancel(context.Background())
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

		cg := &ConsumerGroup{
			groupID: "",
			cg: mocks.NewConsumerGroup().WithConsumeFn(func(ctx context.Context, topics []string, _ sarama.ConsumerGroupHandler) error {
				consumeTopics.Store(topics)
				consumeCalled.Add(1)
				<-ctx.Done()
				cancelCalled.Add(1)
				return nil
			}),
			subscribeTopics: make(TopicHandlerConfig),
			consumerCancel:  nil,
		}
		k := &Kafka{
			logger:               logger.NewLogger("test"),
			consumerGroups:       map[string]*ConsumerGroup{cg.groupID: cg},
			closeCh:              make(chan struct{}),
			consumeRetryInterval: time.Millisecond,
		}
		ctx, cancel := context.WithCancel(context.Background())
		k.Subscribe(ctx, SubscriptionHandlerConfig{}, "abc")

		assert.Eventually(t, func() bool {
			return consumeCalled.Load() == 1
		}, time.Second, time.Millisecond)
		assert.Equal(t, int64(0), cancelCalled.Load())
		assert.Equal(t, []string{"abc"}, consumeTopics.Load())
		assert.Equal(t, TopicHandlerConfig{"abc": SubscriptionHandlerConfig{}}, cg.subscribeTopics)

		k.Subscribe(ctx, SubscriptionHandlerConfig{}, "def")
		assert.Equal(t, TopicHandlerConfig{
			"abc": SubscriptionHandlerConfig{},
			"def": SubscriptionHandlerConfig{},
		}, cg.subscribeTopics)

		assert.Eventually(t, func() bool {
			return consumeCalled.Load() == 2
		}, time.Second, time.Millisecond)
		assert.Equal(t, int64(1), cancelCalled.Load())
		assert.ElementsMatch(t, []string{"abc", "def"}, consumeTopics.Load())

		cancel()
		assert.Eventually(t, func() bool {
			return consumeCalled.Load() == 3
		}, time.Second, time.Millisecond)
		assert.Equal(t, int64(3), cancelCalled.Load())

		k.Subscribe(ctx, SubscriptionHandlerConfig{})
		assert.Nil(t, cg.consumerCancel)
		assert.Empty(t, cg.subscribeTopics)
	})

	t.Run("Consume return immediately if returns a context cancelled error", func(t *testing.T) {
		var consumeCalled atomic.Int64
		cg := &ConsumerGroup{
			groupID: "",
			cg: mocks.NewConsumerGroup().WithConsumeFn(func(ctx context.Context, _ []string, _ sarama.ConsumerGroupHandler) error {
				consumeCalled.Add(1)
				if consumeCalled.Load() == 5 {
					return context.Canceled
				}
				return errors.New("some error")
			}),
			subscribeTopics: make(TopicHandlerConfig),
			consumerCancel:  nil,
		}
		k := &Kafka{
			logger:               logger.NewLogger("test"),
			consumerGroups:       map[string]*ConsumerGroup{cg.groupID: cg},
			closeCh:              make(chan struct{}),
			consumeRetryInterval: time.Millisecond,
		}

		k.Subscribe(context.Background(), SubscriptionHandlerConfig{}, "foo")
		assert.Equal(t, TopicHandlerConfig{"foo": SubscriptionHandlerConfig{}}, cg.subscribeTopics)
		assert.Eventually(t, func() bool {
			return consumeCalled.Load() == 5
		}, time.Second, time.Millisecond)
		cg.consumerWG.Wait()
		assert.Equal(t, int64(5), consumeCalled.Load())
		assert.Equal(t, TopicHandlerConfig{"foo": SubscriptionHandlerConfig{}}, cg.subscribeTopics)
	})

	t.Run("Consume dynamically changes topics which are being consumed", func(t *testing.T) {
		var consumeTopics atomic.Value
		var consumeCalled atomic.Int64
		var cancelCalled atomic.Int64
		cg := &ConsumerGroup{
			groupID: "",
			cg: mocks.NewConsumerGroup().WithConsumeFn(func(ctx context.Context, topics []string, _ sarama.ConsumerGroupHandler) error {
				consumeTopics.Store(topics)
				consumeCalled.Add(1)
				<-ctx.Done()
				cancelCalled.Add(1)
				return nil
			}),
			subscribeTopics: make(TopicHandlerConfig),
			consumerCancel:  nil,
		}
		k := &Kafka{
			logger:               logger.NewLogger("test"),
			consumerGroups:       map[string]*ConsumerGroup{cg.groupID: cg},
			closeCh:              make(chan struct{}),
			consumeRetryInterval: time.Millisecond,
		}

		ctx1, cancel1 := context.WithCancel(context.Background())
		k.Subscribe(ctx1, SubscriptionHandlerConfig{}, "abc")
		assert.Eventually(t, func() bool {
			return consumeCalled.Load() == 1
		}, time.Second, time.Millisecond)
		assert.ElementsMatch(t, []string{"abc"}, consumeTopics.Load())
		assert.Equal(t, int64(0), cancelCalled.Load())

		ctx2, cancel2 := context.WithCancel(context.Background())
		k.Subscribe(ctx2, SubscriptionHandlerConfig{}, "def")
		assert.Eventually(t, func() bool {
			return consumeCalled.Load() == 2
		}, time.Second, time.Millisecond)
		assert.ElementsMatch(t, []string{"abc", "def"}, consumeTopics.Load())
		assert.Equal(t, int64(1), cancelCalled.Load())

		ctx3, cancel3 := context.WithCancel(context.Background())
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

		ctx2, cancel2 = context.WithCancel(context.Background())
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
		assert.Empty(t, cg.subscribeTopics)
		assert.Equal(t, int64(7), consumeCalled.Load())
	})

	t.Run("Can call Subscribe concurrently", func(t *testing.T) {
		var cancelCalled atomic.Int64
		var consumeCalled atomic.Int64
		cg := &ConsumerGroup{
			groupID: "",
			cg: mocks.NewConsumerGroup().WithConsumeFn(func(ctx context.Context, topics []string, _ sarama.ConsumerGroupHandler) error {
				consumeCalled.Add(1)
				<-ctx.Done()
				cancelCalled.Add(1)
				return nil
			}),
			subscribeTopics: make(TopicHandlerConfig),
			consumerCancel:  nil,
		}
		k := &Kafka{
			logger:               logger.NewLogger("test"),
			consumerGroups:       map[string]*ConsumerGroup{cg.groupID: cg},
			closeCh:              make(chan struct{}),
			consumeRetryInterval: time.Millisecond,
		}

		ctx, cancel := context.WithCancel(context.Background())
		for i := 0; i < 100; i++ {
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

	t.Run("Calling subscribe with consumerGroup override adds new consumer group", func(t *testing.T) {
		var consumeCalledCg1 atomic.Int64
		var cancelCalledCg1 atomic.Int64
		var consumeCalledCg2 atomic.Int64
		var cancelCalledCg2 atomic.Int64

		cg := &ConsumerGroup{
			groupID: "",
			cg: mocks.NewConsumerGroup().WithConsumeFn(func(ctx context.Context, _ []string, _ sarama.ConsumerGroupHandler) error {
				consumeCalledCg1.Add(1)
				<-ctx.Done()
				cancelCalledCg1.Add(1)
				return nil
			}),
			subscribeTopics: TopicHandlerConfig{"foo": SubscriptionHandlerConfig{}},
			consumerCancel:  nil,
		}

		var cgFactory ConsumerGroupFactory = func(brokers []string, groupID string, config *sarama.Config) (*ConsumerGroup, error) {
			return &ConsumerGroup{
				groupID: groupID,
				cg: mocks.NewConsumerGroup().WithConsumeFn(func(ctx context.Context, _ []string, _ sarama.ConsumerGroupHandler) error {
					consumeCalledCg2.Add(1)
					<-ctx.Done()
					cancelCalledCg2.Add(1)
					return nil
				}),
				subscribeTopics: make(TopicHandlerConfig),
				consumerCancel:  nil,
			}, nil
		}

		k := &Kafka{
			logger:               logger.NewLogger("test"),
			consumerGroups:       map[string]*ConsumerGroup{cg.groupID: cg},
			closeCh:              make(chan struct{}),
			consumeRetryInterval: time.Millisecond,
			consumerGroupFactory: cgFactory,
		}

		k.Subscribe(context.Background(), SubscriptionHandlerConfig{ConsumerGroupID: "cgOverride"}, "foo")

		assert.Equal(t, 2, len(k.consumerGroups))
		cgAct2, ok := k.consumerGroups["cgOverride"]
		assert.True(t, ok)
		assert.Equal(t, cgAct2.subscribeTopics, TopicHandlerConfig{"foo": {ConsumerGroupID: "cgOverride"}})
		assert.Eventually(t, func() bool {
			return consumeCalledCg2.Load() == 1
		}, time.Second, time.Millisecond)
		assert.Equal(t, int64(0), cancelCalledCg2.Load())

		cgActDefault, ok := k.consumerGroups[""]
		assert.True(t, ok)
		assert.Equal(t, cgActDefault.subscribeTopics, TopicHandlerConfig{"foo": {}})
		assert.Equal(t, int64(0), consumeCalledCg1.Load())
		assert.Equal(t, int64(0), cancelCalledCg1.Load())

	})

	t.Run("Calling subscribe with consumerGroup override same as default keeps same consumer group", func(t *testing.T) {
		var consumeCalledCg1 atomic.Int64
		var cancelCalledCg1 atomic.Int64
		var consumeCalledCg2 atomic.Int64
		var cancelCalledCg2 atomic.Int64
		groupID := "default"

		cg := &ConsumerGroup{
			groupID: groupID,
			cg: mocks.NewConsumerGroup().WithConsumeFn(func(ctx context.Context, _ []string, _ sarama.ConsumerGroupHandler) error {
				consumeCalledCg1.Add(1)
				<-ctx.Done()
				cancelCalledCg1.Add(1)
				return nil
			}),
			subscribeTopics: TopicHandlerConfig{"foo": SubscriptionHandlerConfig{}},
			consumerCancel:  nil,
		}

		var cgFactory ConsumerGroupFactory = func(brokers []string, groupID string, config *sarama.Config) (*ConsumerGroup, error) {
			return &ConsumerGroup{
				groupID: groupID,
				cg: mocks.NewConsumerGroup().WithConsumeFn(func(ctx context.Context, _ []string, _ sarama.ConsumerGroupHandler) error {
					consumeCalledCg2.Add(1)
					<-ctx.Done()
					cancelCalledCg2.Add(1)
					return nil
				}),
				subscribeTopics: make(TopicHandlerConfig),
				consumerCancel:  nil,
			}, nil
		}

		k := &Kafka{
			logger:                 logger.NewLogger("test"),
			consumerGroups:         map[string]*ConsumerGroup{cg.groupID: cg},
			closeCh:                make(chan struct{}),
			consumeRetryInterval:   time.Millisecond,
			consumerGroupFactory:   cgFactory,
			defaultConsumerGroupID: groupID,
		}

		k.Subscribe(context.Background(), SubscriptionHandlerConfig{ConsumerGroupID: groupID}, "foo")

		assert.Equal(t, 1, len(k.consumerGroups))
		cgAct, ok := k.consumerGroups[groupID]
		assert.True(t, ok)
		assert.Equal(t, cgAct.subscribeTopics, TopicHandlerConfig{"foo": {ConsumerGroupID: groupID}})
		assert.Eventually(t, func() bool {
			return consumeCalledCg1.Load() == 1
		}, time.Second, time.Millisecond)
		assert.Equal(t, int64(1), consumeCalledCg1.Load())
		assert.Equal(t, int64(0), cancelCalledCg1.Load())

		assert.Equal(t, int64(0), consumeCalledCg2.Load())
		assert.Equal(t, int64(0), cancelCalledCg2.Load())

		k.Subscribe(context.Background(), SubscriptionHandlerConfig{ConsumerGroupID: "override"}, "foo", "foo2")

		assert.Equal(t, 2, len(k.consumerGroups))
		cgAct2, ok := k.consumerGroups["override"]
		assert.True(t, ok)
		assert.Equal(t, cgAct2.subscribeTopics, TopicHandlerConfig{"foo": {ConsumerGroupID: "override"}, "foo2": {ConsumerGroupID: "override"}})
		assert.Eventually(t, func() bool {
			return consumeCalledCg2.Load() == 1
		}, time.Second, time.Millisecond)
		assert.Equal(t, int64(1), consumeCalledCg1.Load())
		assert.Equal(t, int64(0), cancelCalledCg1.Load())

		assert.Equal(t, int64(0), cancelCalledCg2.Load())
	})

}
