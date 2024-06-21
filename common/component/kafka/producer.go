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
	"maps"

	"github.com/IBM/sarama"

	"github.com/dapr/components-contrib/pubsub"
)

func getSyncProducer(config sarama.Config, brokers []string, maxMessageBytes int) (sarama.SyncProducer, error) {
	// Add SyncProducer specific properties to copy of base config
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5
	config.Producer.Return.Successes = true

	if maxMessageBytes > 0 {
		config.Producer.MaxMessageBytes = maxMessageBytes
	}

	producer, err := sarama.NewSyncProducer(brokers, &config)
	if err != nil {
		return nil, err
	}

	return producer, nil
}

// Publish message to Kafka cluster.
func (k *Kafka) Publish(_ context.Context, topic string, data []byte, metadata map[string]string) error {
	if k.producer == nil {
		return errors.New("component is closed")
	}
	// k.logger.Debugf("Publishing topic %v with data: %v", topic, string(data))
	k.logger.Debugf("Publishing on topic %v", topic)

	serializedData, err := k.SerializeValue(topic, data, metadata)
	if err != nil {
		return err
	}
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(serializedData),
	}

	for name, value := range metadata {
		switch name {
		case key, keyMetadataKey:
			msg.Key = sarama.StringEncoder(value)
		}

		if msg.Headers == nil {
			msg.Headers = make([]sarama.RecordHeader, 0, len(metadata))
		}
		msg.Headers = append(msg.Headers, sarama.RecordHeader{
			Key:   []byte(name),
			Value: []byte(value),
		})
	}

	partition, offset, err := k.producer.SendMessage(msg)

	k.logger.Debugf("Partition: %v, offset: %v", partition, offset)

	if err != nil {
		return err
	}

	return nil
}

func (k *Kafka) BulkPublish(_ context.Context, topic string, entries []pubsub.BulkMessageEntry, metadata map[string]string) (pubsub.BulkPublishResponse, error) {
	if k.producer == nil {
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
			Topic: topic,
			Value: sarama.ByteEncoder(serializedData),
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
			}

			if msg.Headers == nil {
				msg.Headers = make([]sarama.RecordHeader, 0, len(metadata))
			}
			msg.Headers = append(msg.Headers, sarama.RecordHeader{
				Key:   []byte(name),
				Value: []byte(value),
			})
		}

		msgs = append(msgs, msg)
	}

	if err := k.producer.SendMessages(msgs); err != nil {
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
