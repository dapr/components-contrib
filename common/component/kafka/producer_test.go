package kafka

import (
	"context"
	"testing"

	"github.com/IBM/sarama"
	saramamocks "github.com/IBM/sarama/mocks"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/kit/logger"
)

func arrangeKafkaWithAssertions(t *testing.T, msgCheckers ...saramamocks.MessageChecker) *Kafka {
	cfg := saramamocks.NewTestConfig()
	mockProducer := saramamocks.NewSyncProducer(t, cfg)

	for _, msgChecker := range msgCheckers {
		mockProducer.ExpectSendMessageWithMessageCheckerFunctionAndSucceed(msgChecker)
	}

	return &Kafka{
		producer: mockProducer,
		config:   cfg,
		logger:   logger.NewLogger("kafka_test"),
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

func TestPublish(t *testing.T) {
	ctx := context.Background()

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
}

func TestBulkPublish(t *testing.T) {
	ctx := context.Background()
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
}
