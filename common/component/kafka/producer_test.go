package kafka

import (
	"regexp"
	"testing"

	"github.com/IBM/sarama"
	saramamocks "github.com/IBM/sarama/mocks"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/kit/logger"
)

func arrangeKafkaWithAssertions(t *testing.T, msgCheckers ...saramamocks.MessageChecker) *Kafka {
	config := saramamocks.NewTestConfig()
	config.Producer.Partitioner = newDaprPartitioner
	mockP := saramamocks.NewSyncProducer(t, config)

	for _, msgChecker := range msgCheckers {
		mockP.ExpectSendMessageWithMessageCheckerFunctionAndSucceed(msgChecker)
	}

	return &Kafka{
		mockProducer: mockP,
		logger:       logger.NewLogger("kafka_test"),
	}
}

func getSaramaHeadersFromMetadata(metadata map[string]string) []sarama.RecordHeader {
	headers := make([]sarama.RecordHeader, 0, len(metadata))

	for key, value := range metadata {
		headers = append(headers, sarama.RecordHeader{
			Key:   []byte(key),
			Value: []byte(value),
		})
	}

	return headers
}

func createMessageAsserter(t *testing.T, expectedKey sarama.Encoder, expectedHeaders map[string]string) saramamocks.MessageChecker {
	return func(msg *sarama.ProducerMessage) error {
		require.Equal(t, expectedKey, msg.Key)
		require.ElementsMatch(t, getSaramaHeadersFromMetadata(expectedHeaders), msg.Headers)
		return nil
	}
}

func createMessageAsserterWithPartition(t *testing.T, expectedKey sarama.Encoder, expectedHeaders map[string]string, expectedPartition int32) saramamocks.MessageChecker {
	return func(msg *sarama.ProducerMessage) error {
		require.Equal(t, expectedKey, msg.Key)
		require.Equal(t, expectedPartition, msg.Partition)
		require.ElementsMatch(t, getSaramaHeadersFromMetadata(expectedHeaders), msg.Headers)
		return nil
	}
}

func TestPublish(t *testing.T) {
	ctx := t.Context()

	t.Run("produce message without partition key", func(t *testing.T) {
		// arrange
		metadata := map[string]string{
			"a": "a",
		}
		messageAsserter := createMessageAsserter(t, nil, metadata)
		k := arrangeKafkaWithAssertions(t, messageAsserter)

		// act
		err := k.Publish(ctx, "a", []byte("a"), metadata)

		// assert
		require.NoError(t, err)
	})

	t.Run("produce message with partition key when partitionKey in metadata", func(t *testing.T) {
		// arrange
		metadata := map[string]string{
			"a":            "a",
			"partitionKey": "key",
		}
		messageAsserter := createMessageAsserter(t, sarama.StringEncoder("key"), metadata)
		k := arrangeKafkaWithAssertions(t, messageAsserter)

		// act
		err := k.Publish(ctx, "a", []byte("a"), metadata)

		// assert
		require.NoError(t, err)
	})

	t.Run("produce message with partition key when __key in metadata", func(t *testing.T) {
		// arrange
		metadata := map[string]string{
			"a":     "a",
			"__key": "key",
		}
		messageAsserter := createMessageAsserter(t, sarama.StringEncoder("key"), metadata)
		k := arrangeKafkaWithAssertions(t, messageAsserter)

		// act
		err := k.Publish(ctx, "a", []byte("a"), metadata)

		// assert
		require.NoError(t, err)
	})

	t.Run("produce message with excluded headers", func(t *testing.T) {
		// arrange
		metadataIn := map[string]string{
			"a":     "a",
			"b":     "bVal",
			"c":     "cVal",
			"__key": "key",
		}

		metadataOut := map[string]string{
			"a":     "a",
			"__key": "key",
		}
		messageAsserter := createMessageAsserter(t, sarama.StringEncoder("key"), metadataOut)
		k := arrangeKafkaWithAssertions(t, messageAsserter)
		k.excludeHeaderMetaRegex = regexp.MustCompile("^b|c$")

		// act
		err := k.Publish(ctx, "a", []byte("a"), metadataIn)

		// assert
		require.NoError(t, err)
	})

	t.Run("produce message with partitionNumber", func(t *testing.T) {
		// arrange
		metadata := map[string]string{
			"a":               "a",
			"partitionNumber": "3",
		}
		messageAsserter := createMessageAsserterWithPartition(t, nil, metadata, 3)
		k := arrangeKafkaWithAssertions(t, messageAsserter)

		// act
		err := k.Publish(ctx, "a", []byte("a"), metadata)

		// assert
		require.NoError(t, err)
	})

	t.Run("produce message with partitionNumber zero", func(t *testing.T) {
		// arrange
		metadata := map[string]string{
			"partitionNumber": "0",
		}
		messageAsserter := createMessageAsserterWithPartition(t, nil, metadata, 0)
		k := arrangeKafkaWithAssertions(t, messageAsserter)

		// act
		err := k.Publish(ctx, "a", []byte("a"), metadata)

		// assert
		require.NoError(t, err)
	})

	t.Run("produce message with both partitionNumber and partitionKey", func(t *testing.T) {
		// arrange
		metadata := map[string]string{
			"partitionKey":    "key",
			"partitionNumber": "2",
		}
		messageAsserter := createMessageAsserterWithPartition(t, sarama.StringEncoder("key"), metadata, 2)
		k := arrangeKafkaWithAssertions(t, messageAsserter)

		// act
		err := k.Publish(ctx, "a", []byte("a"), metadata)

		// assert
		require.NoError(t, err)
	})

	t.Run("produce message with invalid partitionNumber returns error", func(t *testing.T) {
		// arrange
		metadata := map[string]string{
			"partitionNumber": "abc",
		}
		k := arrangeKafkaWithAssertions(t)

		// act
		err := k.Publish(ctx, "a", []byte("a"), metadata)

		// assert
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid partitionNumber")
	})

	t.Run("produce message with negative partitionNumber returns error", func(t *testing.T) {
		// arrange
		metadata := map[string]string{
			"partitionNumber": "-1",
		}
		k := arrangeKafkaWithAssertions(t)

		// act
		err := k.Publish(ctx, "a", []byte("a"), metadata)

		// assert
		require.Error(t, err)
		require.Contains(t, err.Error(), "non-negative")
	})
}

func TestBulkPublish(t *testing.T) {
	ctx := t.Context()
	metadata := map[string]string{
		"common": "common",
	}

	t.Run("bulk produce messages without partition key", func(t *testing.T) {
		// arrange
		entries := []pubsub.BulkMessageEntry{
			{
				EntryId:     "0",
				Event:       []byte("a"),
				ContentType: "a",
				Metadata:    map[string]string{"b": "b"},
			},
			{
				EntryId:     "0",
				Event:       []byte("a"),
				ContentType: "a",
				Metadata:    map[string]string{"c": "c"},
			},
		}
		messageAsserters := []saramamocks.MessageChecker{
			createMessageAsserter(t, nil, map[string]string{"b": "b", "common": "common"}),
			createMessageAsserter(t, nil, map[string]string{"c": "c", "common": "common"}),
		}
		k := arrangeKafkaWithAssertions(t, messageAsserters...)

		// act
		_, err := k.BulkPublish(ctx, "a", entries, metadata)

		// assert
		require.NoError(t, err)
	})

	t.Run("bulk produce messages with partition key when partitionKey in entry metadata", func(t *testing.T) {
		// arrange
		entries := []pubsub.BulkMessageEntry{
			{
				EntryId:     "0",
				Event:       []byte("a"),
				ContentType: "a",
				Metadata:    map[string]string{"partitionKey": "key"},
			},
			{
				EntryId:     "0",
				Event:       []byte("a"),
				ContentType: "a",
				Metadata:    map[string]string{"c": "c"},
			},
		}
		messageAsserters := []saramamocks.MessageChecker{
			createMessageAsserter(t, sarama.StringEncoder("key"), map[string]string{"partitionKey": "key", "common": "common"}),
			createMessageAsserter(t, nil, map[string]string{"c": "c", "common": "common"}),
		}
		k := arrangeKafkaWithAssertions(t, messageAsserters...)

		// act
		_, err := k.BulkPublish(ctx, "a", entries, metadata)

		// assert
		require.NoError(t, err)
	})

	t.Run("bulk produce messages with partition key when __key in entry metadata", func(t *testing.T) {
		// arrange
		entries := []pubsub.BulkMessageEntry{
			{
				EntryId:     "0",
				Event:       []byte("a"),
				ContentType: "a",
				Metadata:    map[string]string{"__key": "key"},
			},
			{
				EntryId:     "0",
				Event:       []byte("a"),
				ContentType: "a",
				Metadata:    map[string]string{"c": "c"},
			},
		}
		messageAsserters := []saramamocks.MessageChecker{
			createMessageAsserter(t, sarama.StringEncoder("key"), map[string]string{"__key": "key", "common": "common"}),
			createMessageAsserter(t, nil, map[string]string{"c": "c", "common": "common"}),
		}
		k := arrangeKafkaWithAssertions(t, messageAsserters...)

		// act
		_, err := k.BulkPublish(ctx, "a", entries, metadata)

		// assert
		require.NoError(t, err)
	})

	t.Run("bulk produce messages with excluded headers", func(t *testing.T) {
		// arrange
		entries := []pubsub.BulkMessageEntry{
			{
				EntryId:     "0",
				Event:       []byte("a"),
				ContentType: "a",
				Metadata:    map[string]string{"__key": "key", "a": "a", "b": "b", "c": "c"},
			},
			{
				EntryId:     "0",
				Event:       []byte("a"),
				ContentType: "a",
				Metadata:    map[string]string{"c": "c"},
			},
		}
		messageAsserters := []saramamocks.MessageChecker{
			createMessageAsserter(t, sarama.StringEncoder("key"), map[string]string{"__key": "key", "common": "common", "a": "a"}),
			createMessageAsserter(t, nil, map[string]string{"common": "common"}),
		}
		k := arrangeKafkaWithAssertions(t, messageAsserters...)
		k.excludeHeaderMetaRegex = regexp.MustCompile("^b|c$")

		// act
		_, err := k.BulkPublish(ctx, "a", entries, metadata)

		// assert
		require.NoError(t, err)
	})

	t.Run("bulk produce messages with partitionNumber in entry metadata", func(t *testing.T) {
		// arrange
		entries := []pubsub.BulkMessageEntry{
			{
				EntryId:     "0",
				Event:       []byte("a"),
				ContentType: "a",
				Metadata:    map[string]string{"partitionNumber": "2"},
			},
			{
				EntryId:     "1",
				Event:       []byte("b"),
				ContentType: "a",
				Metadata:    map[string]string{"c": "c"},
			},
		}
		messageAsserters := []saramamocks.MessageChecker{
			createMessageAsserterWithPartition(t, nil, map[string]string{"partitionNumber": "2", "common": "common"}, 2),
			createMessageAsserter(t, nil, map[string]string{"c": "c", "common": "common"}),
		}
		k := arrangeKafkaWithAssertions(t, messageAsserters...)

		// act
		_, err := k.BulkPublish(ctx, "a", entries, metadata)

		// assert
		require.NoError(t, err)
	})

	t.Run("bulk produce messages with invalid partitionNumber returns error", func(t *testing.T) {
		// arrange
		entries := []pubsub.BulkMessageEntry{
			{
				EntryId:     "0",
				Event:       []byte("a"),
				ContentType: "a",
				Metadata:    map[string]string{"partitionNumber": "notanumber"},
			},
		}
		k := arrangeKafkaWithAssertions(t)

		// act
		_, err := k.BulkPublish(ctx, "a", entries, metadata)

		// assert
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid partitionNumber")
	})

	t.Run("bulk produce messages with negative partitionNumber returns error", func(t *testing.T) {
		// arrange
		entries := []pubsub.BulkMessageEntry{
			{
				EntryId:     "0",
				Event:       []byte("a"),
				ContentType: "a",
				Metadata:    map[string]string{"partitionNumber": "-5"},
			},
		}
		k := arrangeKafkaWithAssertions(t)

		// act
		_, err := k.BulkPublish(ctx, "a", entries, metadata)

		// assert
		require.Error(t, err)
		require.Contains(t, err.Error(), "non-negative")
	})
}
