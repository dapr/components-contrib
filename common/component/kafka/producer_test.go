package kafka

import (
	"errors"
	"regexp"
	"strings"
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

// fakeTxnProducer is a minimal SyncProducer for exercising the transaction
// error paths that sarama's mocks cannot simulate (commit failures, fatal
// producer states). Unimplemented interface methods panic via the embedded
// nil interface, which is fine for these tests.
type fakeTxnProducer struct {
	sarama.SyncProducer

	sendErr   error
	commitErr error
	abortErr  error
	status    sarama.ProducerTxnStatusFlag

	begins  int
	sends   int
	commits int
	aborts  int
	closed  bool
}

func (f *fakeTxnProducer) SendMessage(*sarama.ProducerMessage) (int32, int64, error) {
	f.sends++
	return 0, 0, f.sendErr
}

func (f *fakeTxnProducer) SendMessages([]*sarama.ProducerMessage) error {
	f.sends++
	return f.sendErr
}

func (f *fakeTxnProducer) BeginTxn() error { f.begins++; return nil }

func (f *fakeTxnProducer) CommitTxn() error { f.commits++; return f.commitErr }

func (f *fakeTxnProducer) AbortTxn() error { f.aborts++; return f.abortErr }

func (f *fakeTxnProducer) TxnStatus() sarama.ProducerTxnStatusFlag { return f.status }

func (f *fakeTxnProducer) IsTransactional() bool { return true }

func (f *fakeTxnProducer) Close() error { f.closed = true; return nil }

// arrangeTxnKafka injects the fake through k.clients (not mockProducer) so
// the tests can observe producer invalidation after fatal errors.
func arrangeTxnKafka(fake *fakeTxnProducer) *Kafka {
	return &Kafka{
		clients:        &clients{producer: fake},
		producerConfig: ProducerConfig{TransactionsEnabled: true},
		logger:         logger.NewLogger("kafka_test"),
	}
}

func TestPublishTransactions(t *testing.T) {
	ctx := t.Context()

	t.Run("send happens inside begin/commit", func(t *testing.T) {
		config := saramamocks.NewTestConfig()
		config.Producer.Partitioner = newDaprPartitioner
		config.Version = sarama.V2_0_0_0 //nolint:nosnakecase
		config.Producer.Transaction.ID = "test-txn"
		config.Producer.Idempotent = true
		config.Producer.RequiredAcks = sarama.WaitForAll
		config.Net.MaxOpenRequests = 1
		// The mock errors if SendMessage is called while no transaction is
		// open, so this asserts the begin -> send -> commit ordering.
		mockP := saramamocks.NewSyncProducer(t, config)
		mockP.ExpectSendMessageWithMessageCheckerFunctionAndSucceed(func(*sarama.ProducerMessage) error { return nil })
		k := &Kafka{
			mockProducer:   mockP,
			producerConfig: ProducerConfig{TransactionsEnabled: true},
			logger:         logger.NewLogger("kafka_test"),
		}

		err := k.Publish(ctx, "a", []byte("a"), nil)

		require.NoError(t, err)
		require.Equal(t, sarama.ProducerTxnFlagReady, mockP.TxnStatus())
	})

	t.Run("transactions disabled leaves the plain send path untouched", func(t *testing.T) {
		fake := &fakeTxnProducer{}
		k := arrangeTxnKafka(fake)
		k.producerConfig.TransactionsEnabled = false

		err := k.Publish(ctx, "a", []byte("a"), nil)

		require.NoError(t, err)
		require.Equal(t, 0, fake.begins)
		require.Equal(t, 0, fake.commits)
	})

	t.Run("send error aborts the transaction and keeps the producer", func(t *testing.T) {
		fake := &fakeTxnProducer{
			sendErr: errors.New("send failed"),
			status:  sarama.ProducerTxnFlagAbortableError,
		}
		k := arrangeTxnKafka(fake)

		err := k.Publish(ctx, "a", []byte("a"), nil)

		require.ErrorContains(t, err, "send failed")
		require.Equal(t, 1, fake.begins)
		require.Equal(t, 1, fake.aborts)
		require.Equal(t, 0, fake.commits)
		require.False(t, fake.closed)
		require.Same(t, sarama.SyncProducer(fake), k.clients.producer)
	})

	t.Run("send error with plain in-transaction status still aborts", func(t *testing.T) {
		fake := &fakeTxnProducer{
			sendErr: errors.New("send failed"),
			status:  sarama.ProducerTxnFlagInTransaction,
		}
		k := arrangeTxnKafka(fake)

		err := k.Publish(ctx, "a", []byte("a"), nil)

		require.ErrorContains(t, err, "send failed")
		require.Equal(t, 1, fake.aborts)
	})

	t.Run("commit error with abortable status aborts and keeps the producer", func(t *testing.T) {
		fake := &fakeTxnProducer{
			commitErr: errors.New("commit failed"),
			status:    sarama.ProducerTxnFlagAbortableError,
		}
		k := arrangeTxnKafka(fake)

		err := k.Publish(ctx, "a", []byte("a"), nil)

		require.ErrorContains(t, err, "commit failed")
		require.Equal(t, 1, fake.commits)
		require.Equal(t, 1, fake.aborts)
		require.False(t, fake.closed)
	})

	t.Run("fatal transaction state drops the producer for recreation", func(t *testing.T) {
		fake := &fakeTxnProducer{
			commitErr: errors.New("commit failed"),
			status:    sarama.ProducerTxnFlagFatalError,
		}
		k := arrangeTxnKafka(fake)

		err := k.Publish(ctx, "a", []byte("a"), nil)

		require.ErrorContains(t, err, "commit failed")
		require.Equal(t, 0, fake.aborts)
		require.True(t, fake.closed)
		require.Nil(t, k.clients.producer)
	})

	t.Run("abort failure drops the producer and joins both errors", func(t *testing.T) {
		fake := &fakeTxnProducer{
			sendErr:  errors.New("send failed"),
			abortErr: errors.New("abort failed"),
			status:   sarama.ProducerTxnFlagAbortableError,
		}
		k := arrangeTxnKafka(fake)

		err := k.Publish(ctx, "a", []byte("a"), nil)

		require.ErrorContains(t, err, "send failed")
		require.ErrorContains(t, err, "abort failed")
		require.True(t, fake.closed)
		require.Nil(t, k.clients.producer)
	})

	t.Run("bulk publish abort marks every entry failed", func(t *testing.T) {
		fake := &fakeTxnProducer{
			sendErr: errors.New("send failed"),
			status:  sarama.ProducerTxnFlagAbortableError,
		}
		k := arrangeTxnKafka(fake)
		entries := []pubsub.BulkMessageEntry{
			{EntryId: "0", Event: []byte("a")},
			{EntryId: "1", Event: []byte("b")},
		}

		res, err := k.BulkPublish(ctx, "a", entries, nil)

		require.ErrorContains(t, err, "send failed")
		require.Equal(t, 1, fake.aborts)
		require.Len(t, res.FailedEntries, 2)
	})

	t.Run("bulk publish commit success returns empty response", func(t *testing.T) {
		fake := &fakeTxnProducer{}
		k := arrangeTxnKafka(fake)
		entries := []pubsub.BulkMessageEntry{
			{EntryId: "0", Event: []byte("a")},
		}

		res, err := k.BulkPublish(ctx, "a", entries, nil)

		require.NoError(t, err)
		require.Equal(t, 1, fake.begins)
		require.Equal(t, 1, fake.commits)
		require.Empty(t, res.FailedEntries)
	})
}

func TestBuildTransactionalID(t *testing.T) {
	t.Run("explicit prefix wins", func(t *testing.T) {
		id := buildTransactionalID("myprefix", "client", "group")
		require.True(t, strings.HasPrefix(id, "myprefix-"))
		require.Greater(t, len(id), len("myprefix-"))
	})

	t.Run("falls back to client id, then consumer group, then dapr", func(t *testing.T) {
		require.True(t, strings.HasPrefix(buildTransactionalID("", "client", "group"), "client-"))
		require.True(t, strings.HasPrefix(buildTransactionalID("", "", "group"), "group-"))
		require.True(t, strings.HasPrefix(buildTransactionalID("", "", ""), "dapr-"))
	})

	t.Run("ids are unique per call", func(t *testing.T) {
		first := buildTransactionalID("p", "", "")
		second := buildTransactionalID("p", "", "")
		require.NotEqual(t, first, second)
	})
}

func TestApplySyncProducerConfig(t *testing.T) {
	t.Run("transactional settings applied", func(t *testing.T) {
		config := sarama.NewConfig()
		applySyncProducerConfig(config, ProducerConfig{
			RequiredAcks:        sarama.WaitForAll,
			RetryMax:            5,
			TransactionsEnabled: true,
			TransactionalID:     "txid",
		})
		require.True(t, config.Producer.Idempotent)
		require.Equal(t, "txid", config.Producer.Transaction.ID)
		require.Equal(t, 1, config.Net.MaxOpenRequests)
		require.True(t, config.Producer.Return.Successes)
	})

	t.Run("transactions disabled leaves idempotence off", func(t *testing.T) {
		config := sarama.NewConfig()
		applySyncProducerConfig(config, ProducerConfig{RequiredAcks: sarama.WaitForLocal, RetryMax: 3})
		require.False(t, config.Producer.Idempotent)
		require.Empty(t, config.Producer.Transaction.ID)
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
