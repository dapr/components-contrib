package kafka

import (
	"context"
	"testing"

	"github.com/IBM/sarama"
	saramamocks "github.com/IBM/sarama/mocks"
	"github.com/stretchr/testify/require"

	"github.com/dapr/kit/logger"
)

func arrangeKafkaExpectMessage(t *testing.T, cf saramamocks.MessageChecker) *Kafka {
	cfg := saramamocks.NewTestConfig()
	mockProducer := saramamocks.NewSyncProducer(t, cfg)
	mockProducer.ExpectSendMessageWithMessageCheckerFunctionAndSucceed(cf)

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

func TestPublish(t *testing.T) {
	ctx := context.Background()

	t.Run("produce message without partition key", func(t *testing.T) {
		// arrange
		metadata := map[string]string{
			"a": "a",
		}
		k := arrangeKafkaExpectMessage(t, func(msg *sarama.ProducerMessage) error {
			require.Equal(t, nil, msg.Key)
			require.Equal(t, getSaramaHeadersFromMetadata(metadata), msg.Headers)
			return nil
		})

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
		k := arrangeKafkaExpectMessage(t, func(msg *sarama.ProducerMessage) error {
			require.Equal(t, sarama.StringEncoder("key"), msg.Key)
			require.Equal(t, getSaramaHeadersFromMetadata(metadata), msg.Headers)

			return nil
		})

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
		k := arrangeKafkaExpectMessage(t, func(msg *sarama.ProducerMessage) error {
			require.Equal(t, sarama.StringEncoder("key"), msg.Key)
			require.Equal(t, getSaramaHeadersFromMetadata(metadata), msg.Headers)

			return nil
		})

		// act
		err := k.Publish(ctx, "a", []byte("a"), metadata)

		// assert
		require.NoError(t, err)
	})
}
