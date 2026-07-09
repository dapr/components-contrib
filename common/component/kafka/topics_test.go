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
	"testing"

	"github.com/IBM/sarama"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/common/component/kafka/mocks"
	"github.com/dapr/kit/logger"
)

func TestEnsureTopic(t *testing.T) {
	t.Run("no-op when numPartitions is 0", func(t *testing.T) {
		k := &Kafka{
			logger:        logger.NewLogger("kafka_test"),
			numPartitions: 0,
		}
		err := k.ensureTopic("my-topic")
		require.NoError(t, err)
	})

	t.Run("no-op when numPartitions is negative", func(t *testing.T) {
		k := &Kafka{
			logger:        logger.NewLogger("kafka_test"),
			numPartitions: -1,
		}
		err := k.ensureTopic("my-topic")
		require.NoError(t, err)
	})

	t.Run("calls CreateTopic with correct args", func(t *testing.T) {
		var gotTopic string
		var gotDetail *sarama.TopicDetail
		var gotValidate bool

		admin := mocks.NewClusterAdmin().WithCreateTopicFn(
			func(topic string, detail *sarama.TopicDetail, validateOnly bool) error {
				gotTopic = topic
				gotDetail = detail
				gotValidate = validateOnly
				return nil
			},
		)

		k := &Kafka{
			logger:            logger.NewLogger("kafka_test"),
			numPartitions:     6,
			replicationFactor: 3,
			clients: &clients{
				admin: admin,
			},
		}

		err := k.ensureTopic("orders")
		require.NoError(t, err)
		require.Equal(t, "orders", gotTopic)
		require.NotNil(t, gotDetail)
		require.Equal(t, int32(6), gotDetail.NumPartitions)
		require.Equal(t, int16(3), gotDetail.ReplicationFactor)
		require.False(t, gotValidate)
	})

	t.Run("treats ErrTopicAlreadyExists as success", func(t *testing.T) {
		admin := mocks.NewClusterAdmin().WithCreateTopicFn(
			func(string, *sarama.TopicDetail, bool) error {
				return sarama.ErrTopicAlreadyExists
			},
		)

		k := &Kafka{
			logger:            logger.NewLogger("kafka_test"),
			numPartitions:     3,
			replicationFactor: 1,
			clients: &clients{
				admin: admin,
			},
		}

		err := k.ensureTopic("existing-topic")
		require.NoError(t, err)

		// Verify it's cached so subsequent calls don't hit CreateTopic again.
		_, loaded := k.ensuredTopics.Load("existing-topic")
		require.True(t, loaded)
	})

	t.Run("returns error for other CreateTopic failures", func(t *testing.T) {
		admin := mocks.NewClusterAdmin().WithCreateTopicFn(
			func(string, *sarama.TopicDetail, bool) error {
				return errors.New("broker unavailable")
			},
		)

		k := &Kafka{
			logger:            logger.NewLogger("kafka_test"),
			numPartitions:     3,
			replicationFactor: 1,
			clients: &clients{
				admin: admin,
			},
		}

		err := k.ensureTopic("fail-topic")
		require.Error(t, err)
		require.Contains(t, err.Error(), "failed to create topic fail-topic")
		require.Contains(t, err.Error(), "broker unavailable")

		// Should NOT be cached on failure.
		_, loaded := k.ensuredTopics.Load("fail-topic")
		require.False(t, loaded)
	})

	t.Run("idempotent after successful create", func(t *testing.T) {
		callCount := 0
		admin := mocks.NewClusterAdmin().WithCreateTopicFn(
			func(string, *sarama.TopicDetail, bool) error {
				callCount++
				return nil
			},
		)

		k := &Kafka{
			logger:            logger.NewLogger("kafka_test"),
			numPartitions:     3,
			replicationFactor: 1,
			clients: &clients{
				admin: admin,
			},
		}

		err := k.ensureTopic("cached-topic")
		require.NoError(t, err)
		require.Equal(t, 1, callCount)

		// Second call should be a no-op (cached).
		err = k.ensureTopic("cached-topic")
		require.NoError(t, err)
		require.Equal(t, 1, callCount) // Still 1 - not called again.
	})
}
