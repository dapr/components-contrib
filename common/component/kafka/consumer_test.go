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
}
