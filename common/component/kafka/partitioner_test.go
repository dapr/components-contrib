package kafka

import (
	"testing"

	"github.com/IBM/sarama"
	"github.com/stretchr/testify/require"
)

func TestDaprPartitioner(t *testing.T) {
	partitioner := newDaprPartitioner("test-topic")
	numPartitions := int32(4)

	t.Run("returns explicit partition when set", func(t *testing.T) {
		msg := &sarama.ProducerMessage{Partition: 2}
		partition, err := partitioner.Partition(msg, numPartitions)
		require.NoError(t, err)
		require.Equal(t, int32(2), partition)
	})

	t.Run("returns partition 0 when explicitly set", func(t *testing.T) {
		msg := &sarama.ProducerMessage{Partition: 0}
		partition, err := partitioner.Partition(msg, numPartitions)
		require.NoError(t, err)
		require.Equal(t, int32(0), partition)
	})

	t.Run("returns error when partition exceeds range", func(t *testing.T) {
		msg := &sarama.ProducerMessage{Partition: 5}
		_, err := partitioner.Partition(msg, numPartitions)
		require.Error(t, err)
		require.Contains(t, err.Error(), "out of range")
	})

	t.Run("returns error when partition equals numPartitions", func(t *testing.T) {
		msg := &sarama.ProducerMessage{Partition: 4}
		_, err := partitioner.Partition(msg, numPartitions)
		require.Error(t, err)
	})

	t.Run("delegates to hash partitioner when partition is negative", func(t *testing.T) {
		msg := &sarama.ProducerMessage{
			Partition: -1,
			Key:       sarama.StringEncoder("test-key"),
		}
		partition, err := partitioner.Partition(msg, numPartitions)
		require.NoError(t, err)
		require.GreaterOrEqual(t, partition, int32(0))
		require.Less(t, partition, numPartitions)

		// Same key should produce same partition (hash consistency)
		msg2 := &sarama.ProducerMessage{
			Partition: -1,
			Key:       sarama.StringEncoder("test-key"),
		}
		partition2, err := partitioner.Partition(msg2, numPartitions)
		require.NoError(t, err)
		require.Equal(t, partition, partition2)
	})

	t.Run("delegates to hash partitioner when partition is unset sentinel", func(t *testing.T) {
		msg := &sarama.ProducerMessage{
			Partition: -1,
		}
		partition, err := partitioner.Partition(msg, numPartitions)
		require.NoError(t, err)
		require.GreaterOrEqual(t, partition, int32(0))
		require.Less(t, partition, numPartitions)
	})

	t.Run("RequiresConsistency returns true", func(t *testing.T) {
		require.True(t, partitioner.RequiresConsistency())
	})
}
