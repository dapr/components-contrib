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
	"fmt"
	"maps"
	"strconv"

	"github.com/IBM/sarama"
	"github.com/google/uuid"

	"github.com/dapr/components-contrib/pubsub"
)

// parsePartitionNumber parses and validates a partition number string.
func parsePartitionNumber(value string) (int32, error) {
	pNum, err := strconv.ParseInt(value, 10, 32)
	if err != nil {
		return -1, fmt.Errorf("invalid partitionNumber metadata value %q: %w", value, err)
	}
	if pNum < 0 {
		return -1, fmt.Errorf("partitionNumber must be non-negative, got %d", pNum)
	}
	return int32(pNum), nil
}

// ProducerConfig holds the producer-specific tunables derived from component metadata.
// Separating them avoids threading the entire KafkaMetadata struct through callers
// that only have access to the sarama.Config copy.
type ProducerConfig struct {
	RequiredAcks    sarama.RequiredAcks
	RetryMax        int
	MaxMessageBytes int
	// TransactionsEnabled wraps every publish in a Kafka transaction on an
	// idempotent producer.
	TransactionsEnabled bool
	// TransactionalID is used as the producer's transactional.id when
	// TransactionsEnabled is true. It must be unique per live producer:
	// two producers sharing an ID fence each other.
	TransactionalID string
}

// applySyncProducerConfig applies the producer tunables to a sarama config,
// including the settings sarama requires for an idempotent/transactional
// producer (acks=all, a single in-flight request).
func applySyncProducerConfig(config *sarama.Config, pc ProducerConfig) {
	config.Producer.RequiredAcks = pc.RequiredAcks
	config.Producer.Retry.Max = pc.RetryMax
	config.Producer.Return.Successes = true

	if pc.MaxMessageBytes > 0 {
		config.Producer.MaxMessageBytes = pc.MaxMessageBytes
	}

	if pc.TransactionsEnabled {
		config.Producer.Idempotent = true
		config.Producer.Transaction.ID = pc.TransactionalID
		config.Net.MaxOpenRequests = 1
	}
}

// buildTransactionalID derives the producer's transactional.id. The random
// suffix keeps scaled replicas from fencing each other; a transaction
// abandoned by a crashed instance is aborted by the broker once the
// transaction timeout elapses.
func buildTransactionalID(prefix, clientID, consumerGroup string) string {
	if prefix == "" {
		switch {
		case clientID != "":
			prefix = clientID
		case consumerGroup != "":
			prefix = consumerGroup
		default:
			prefix = "dapr"
		}
	}
	return prefix + "-" + uuid.NewString()
}

// GetSyncProducer creates a new Sarama SyncProducer using the provided base
// config and producer tunables. RequiredAcks and RetryMax are applied from
// ProducerConfig so callers can override the previously hard-coded defaults
// (WaitForAll, 5 retries) via component metadata.
func GetSyncProducer(config sarama.Config, brokers []string, pc ProducerConfig) (sarama.SyncProducer, error) {
	// Apply SyncProducer-specific properties to a copy of the base config.
	applySyncProducerConfig(&config, pc)

	saramaClient, err := sarama.NewClient(brokers, &config)
	if err != nil {
		return nil, err
	}

	producer, err := sarama.NewSyncProducerFromClient(saramaClient)
	if err != nil {
		return nil, err
	}

	return producer, nil
}

// withPublishTxn runs send inside a Kafka transaction on the shared
// producer. A sarama producer supports a single open transaction at a time,
// so transactional publishes are serialized on txnMu; that throughput cost
// is part of opting into transactions.
func (k *Kafka) withPublishTxn(producer sarama.SyncProducer, send func() error) error {
	k.txnMu.Lock()
	defer k.txnMu.Unlock()

	if err := producer.BeginTxn(); err != nil {
		return fmt.Errorf("kafka: begin transaction: %w", err)
	}

	if err := send(); err != nil {
		return k.endTxnWithError(producer, err)
	}

	if err := producer.CommitTxn(); err != nil {
		return k.endTxnWithError(producer, fmt.Errorf("kafka: commit transaction: %w", err))
	}

	return nil
}

// endTxnWithError cleans up an open transaction after cause so the producer
// can be reused. A fatal transaction state poisons the producer entirely: it
// is dropped and lazily recreated by the next publish (same transactional.id,
// so the broker bumps the epoch and aborts the stale transaction).
func (k *Kafka) endTxnWithError(producer sarama.SyncProducer, cause error) error {
	status := producer.TxnStatus()

	if status&sarama.ProducerTxnFlagFatalError != 0 {
		k.logger.Errorf("Kafka producer in fatal transaction state, recreating producer. Cause: %v", cause)
		k.invalidateProducer(producer)
		return cause
	}

	if status&(sarama.ProducerTxnFlagAbortableError|sarama.ProducerTxnFlagInTransaction) != 0 {
		if abortErr := producer.AbortTxn(); abortErr != nil {
			k.logger.Errorf("Kafka producer failed to abort transaction, recreating producer. Cause: %v", abortErr)
			k.invalidateProducer(producer)
			return errors.Join(cause, fmt.Errorf("kafka: abort transaction: %w", abortErr))
		}
	}

	return cause
}

// Publish message to Kafka cluster.
func (k *Kafka) Publish(_ context.Context, topic string, data []byte, metadata map[string]string) error {
	clients, err := k.latestClients()
	if err != nil || clients == nil {
		return fmt.Errorf("failed to get latest Kafka clients: %w", err)
	}
	if clients.producer == nil {
		return errors.New("component is closed")
	}

	// k.logger.Debugf("Publishing topic %v with data: %v", topic, string(data))
	k.logger.Debugf("Publishing on topic %v", topic)

	serializedData, err := k.SerializeValue(topic, data, metadata)
	if err != nil {
		return err
	}
	msg := &sarama.ProducerMessage{
		Topic:     topic,
		Value:     sarama.ByteEncoder(serializedData),
		Partition: -1,
	}

	for name, value := range metadata {
		switch name {
		case key, keyMetadataKey:
			msg.Key = sarama.StringEncoder(value)
		case partitionNumberKey:
			pNum, perr := parsePartitionNumber(value)
			if perr != nil {
				return perr
			}
			msg.Partition = pNum
		}

		if msg.Headers == nil {
			msg.Headers = make([]sarama.RecordHeader, 0, len(metadata))
		}
		// skip metadata that is excluded from headers
		if k.excludeHeaderMetaRegex != nil && k.excludeHeaderMetaRegex.MatchString(name) {
			k.logger.Debugf("Skipping metadata %v that is excluded from headers", name)
			continue
		}
		msg.Headers = append(msg.Headers, sarama.RecordHeader{
			Key:   []byte(name),
			Value: []byte(value),
		})
	}

	var (
		partition int32
		offset    int64
	)
	if k.producerConfig.TransactionsEnabled {
		err = k.withPublishTxn(clients.producer, func() error {
			var sendErr error
			partition, offset, sendErr = clients.producer.SendMessage(msg)
			return sendErr
		})
	} else {
		partition, offset, err = clients.producer.SendMessage(msg)
	}

	k.logger.Debugf("Partition: %v, offset: %v", partition, offset)

	if err != nil {
		return err
	}

	return nil
}

func (k *Kafka) BulkPublish(_ context.Context, topic string, entries []pubsub.BulkMessageEntry, metadata map[string]string) (pubsub.BulkPublishResponse, error) {
	clients, err := k.latestClients()
	if err != nil || clients == nil {
		err = fmt.Errorf("failed to get latest Kafka clients: %w", err)
		return pubsub.NewBulkPublishResponse(entries, err), err
	}
	if clients.producer == nil {
		err := errors.New("component is closed")
		return pubsub.NewBulkPublishResponse(entries, err), err
	}
	k.logger.Debugf("Bulk Publishing on topic %v", topic)

	msgs := []*sarama.ProducerMessage{}
	for _, entry := range entries {
		serializedData, err := k.SerializeValue(topic, entry.Event, metadata)
		if err != nil {
			return k.mapKafkaProducerErrors(err, entries), err
		}
		msg := &sarama.ProducerMessage{
			Topic:     topic,
			Value:     sarama.ByteEncoder(serializedData),
			Partition: -1,
		}
		// From Sarama documentation
		// This field is used to hold arbitrary data you wish to include so it
		// will be available when receiving on the Successes and Errors channels.
		// Sarama completely ignores this field and is only to be used for
		// pass-through data.
		// This pass thorugh field is used for mapping errors, as seen in the mapKafkaProducerErrors method
		// The EntryId will be unique for this request and the ProducerMessage is returned on the Errros channel,
		// the metadata in that field is compared to the entry metadata to generate the right response on partial failures
		msg.Metadata = entry.EntryId

		if entry.Metadata == nil {
			entry.Metadata = make(map[string]string)
		}
		maps.Copy(entry.Metadata, metadata)

		for name, value := range entry.Metadata {
			switch name {
			case key, keyMetadataKey:
				msg.Key = sarama.StringEncoder(value)
			case partitionNumberKey:
				pNum, err := parsePartitionNumber(value)
				if err != nil {
					return pubsub.NewBulkPublishResponse(entries, err), err
				}
				msg.Partition = pNum
			}

			if msg.Headers == nil {
				msg.Headers = make([]sarama.RecordHeader, 0, len(metadata))
			}
			// skip metadata that is excluded from headers
			if k.excludeHeaderMetaRegex != nil && k.excludeHeaderMetaRegex.MatchString(name) {
				k.logger.Debugf("Skipping metadata %v that is excluded from headers", name)
				continue
			}
			msg.Headers = append(msg.Headers, sarama.RecordHeader{
				Key:   []byte(name),
				Value: []byte(value),
			})
		}

		msgs = append(msgs, msg)
	}

	if k.producerConfig.TransactionsEnabled {
		if err := k.withPublishTxn(clients.producer, func() error {
			return clients.producer.SendMessages(msgs)
		}); err != nil {
			// The transaction was aborted: entries that were sent before the
			// failure are not visible to consumers either, so the whole batch
			// failed and per-entry error mapping would be misleading.
			return pubsub.NewBulkPublishResponse(entries, err), err
		}
		return pubsub.BulkPublishResponse{}, nil
	}

	if err := clients.producer.SendMessages(msgs); err != nil {
		// map the returned error to different entries
		return k.mapKafkaProducerErrors(err, entries), err
	}

	return pubsub.BulkPublishResponse{}, nil
}

// mapKafkaProducerErrors to correct response statuses
func (k *Kafka) mapKafkaProducerErrors(err error, entries []pubsub.BulkMessageEntry) pubsub.BulkPublishResponse {
	var pErrs sarama.ProducerErrors
	if !errors.As(err, &pErrs) {
		// Ideally this condition should not be executed, but in the scenario that the err is not of sarama.ProducerErrors type
		// return a default error that all messages have failed
		return pubsub.NewBulkPublishResponse(entries, err)
	}
	resp := pubsub.BulkPublishResponse{
		FailedEntries: make([]pubsub.BulkPublishResponseFailedEntry, 0, len(entries)),
	}
	// used in the case of the partial success scenario
	alreadySeen := map[string]struct{}{}

	for _, pErr := range pErrs {
		if entryId, ok := pErr.Msg.Metadata.(string); ok { //nolint:stylecheck
			alreadySeen[entryId] = struct{}{}
			resp.FailedEntries = append(resp.FailedEntries, pubsub.BulkPublishResponseFailedEntry{
				EntryId: entryId,
				Error:   pErr.Err,
			})
		} else {
			// Ideally this condition should not be executed, but in the scenario that the Metadata field
			// is not of string type return a default error that all messages have failed
			k.logger.Warnf("error parsing bulk errors from Kafka, returning default error response of all failed")
			return pubsub.NewBulkPublishResponse(entries, err)
		}
	}
	return resp
}
