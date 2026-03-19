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
	"net/url"
	"strconv"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"github.com/cenkalti/backoff/v4"

	"github.com/dapr/kit/retry"
)

type consumer struct {
	k     *Kafka
	mutex sync.Mutex
}

func (consumer *consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	b := consumer.k.backOffConfig.NewBackOffWithContext(session.Context())
	isBulkSubscribe := consumer.k.checkBulkSubscribe(claim.Topic())

	handlerConfig, err := consumer.k.GetTopicHandlerConfig(claim.Topic())
	if err != nil {
		return fmt.Errorf("error getting bulk handler config for topic %s: %w", claim.Topic(), err)
	}
	if isBulkSubscribe {
		ticker := time.NewTicker(time.Duration(handlerConfig.SubscribeConfig.MaxAwaitDurationMs) * time.Millisecond)
		defer ticker.Stop()
		messages := make([]*sarama.ConsumerMessage, 0, handlerConfig.SubscribeConfig.MaxMessagesCount)
		for {
			select {
			case <-session.Context().Done():
				return consumer.flushBulkMessages(claim, messages, session, handlerConfig.BulkHandler, b)
			case message := <-claim.Messages():
				consumer.mutex.Lock()
				if message != nil {
					messages = append(messages, message)
					if len(messages) >= handlerConfig.SubscribeConfig.MaxMessagesCount {
						consumer.flushBulkMessages(claim, messages, session, handlerConfig.BulkHandler, b)
						messages = messages[:0]
					}
				}
				consumer.mutex.Unlock()
			case <-ticker.C:
				consumer.mutex.Lock()
				consumer.flushBulkMessages(claim, messages, session, handlerConfig.BulkHandler, b)
				messages = messages[:0]
				consumer.mutex.Unlock()
			}
		}
	} else {
		for {
			select {
			// Should return when `session.Context()` is done.
			// If not, will raise `ErrRebalanceInProgress` or `read tcp <ip>:<port>: i/o timeout` when kafka rebalance. see:
			// https://github.com/IBM/sarama/issues/1192
			// Make sure the check for session context done happens before the next message is processed.
			// There is a possibility that the pod takes some time to shutdown and in case of a poison pill message, the `retry` would get interrupted (as expected),
			// but the next message would be processed as a result,
			// therefore dropping the poison pill message regardless of resiliency policy.
			case <-session.Context().Done():
				return nil
			case message, ok := <-claim.Messages():
				if !ok {
					return nil
				}

				if consumer.k.consumeRetryEnabled {
					if err := retry.NotifyRecover(func() error {
						return consumer.doCallback(session, message)
					}, b, func(err error, d time.Duration) {
						consumer.k.logger.Warnf("Error processing Kafka message: %s/%d/%d [key=%s]. Error: %v. Retrying...", message.Topic, message.Partition, message.Offset, asBase64String(message.Key), err)
					}, func() {
						consumer.k.logger.Infof("Successfully processed Kafka message after it previously failed: %s/%d/%d [key=%s]", message.Topic, message.Partition, message.Offset, asBase64String(message.Key))
					}); err != nil {
						consumer.k.logger.Errorf("Too many failed attempts at processing Kafka message: %s/%d/%d [key=%s]. Error: %v.", message.Topic, message.Partition, message.Offset, asBase64String(message.Key), err)
						if errors.Is(session.Context().Err(), context.Canceled) {
							// If the context is canceled, we should not attempt to consume any more messages. Exiting the loop.
							// Otherwise, there is a race condition when this loop keeps processing messages from the claim.Messages() channel
							// before the session.Context().Done() is closed. If there are other messages that can successfully be processed,
							// they will be marked as processed and this failing message will be lost.
							return nil
						}
					}
				} else {
					err := consumer.doCallback(session, message)
					if err != nil {
						consumer.k.logger.Errorf("Error processing Kafka message: %s/%d/%d [key=%s]. Error: %v.", message.Topic, message.Partition, message.Offset, asBase64String(message.Key), err)
					}
				}
			}
		}
	}
}

func (consumer *consumer) flushBulkMessages(claim sarama.ConsumerGroupClaim,
	messages []*sarama.ConsumerMessage, session sarama.ConsumerGroupSession,
	handler BulkEventHandler, b backoff.BackOff,
) error {
	if len(messages) > 0 {
		if consumer.k.consumeRetryEnabled {
			if err := retry.NotifyRecover(func() error {
				return consumer.doBulkCallback(session, messages, handler, claim.Topic())
			}, b, func(err error, d time.Duration) {
				consumer.k.logger.Warnf("Error processing Kafka bulk messages: %s. Error: %v. Retrying...", claim.Topic(), err)
			}, func() {
				consumer.k.logger.Infof("Successfully processed Kafka message after it previously failed: %s", claim.Topic())
			}); err != nil {
				consumer.k.logger.Errorf("Too many failed attempts at processing Kafka message: %s. Error: %v.", claim.Topic(), err)
			}
		} else {
			err := consumer.doBulkCallback(session, messages, handler, claim.Topic())
			if err != nil {
				consumer.k.logger.Errorf("Error processing Kafka message: %s. Error: %v.", claim.Topic(), err)
			}
			return err
		}
	}
	return nil
}

// buildOffsetMapForTxn builds a map of topic -> []*PartitionOffsetMetadata for
// AddOffsetsToTxn. Each partition appears once with the next offset to read (max offset+1).
func buildOffsetMapForTxn(messages []*sarama.ConsumerMessage) map[string][]*sarama.PartitionOffsetMetadata {
	// topic -> partition -> next offset (offset+1)
	nextByTopicPartition := make(map[string]map[int32]int64)
	for _, msg := range messages {
		if msg == nil {
			continue
		}
		if nextByTopicPartition[msg.Topic] == nil {
			nextByTopicPartition[msg.Topic] = make(map[int32]int64)
		}
		next := msg.Offset + 1
		if cur, ok := nextByTopicPartition[msg.Topic][msg.Partition]; !ok || next > cur {
			nextByTopicPartition[msg.Topic][msg.Partition] = next
		}
	}
	result := make(map[string][]*sarama.PartitionOffsetMetadata)
	for topic, partMap := range nextByTopicPartition {
		for partition, nextOffset := range partMap {
			result[topic] = append(result[topic], &sarama.PartitionOffsetMetadata{
				Partition: partition,
				Offset:    nextOffset,
			})
		}
	}
	return result
}

func (consumer *consumer) doBulkCallback(session sarama.ConsumerGroupSession,
	messages []*sarama.ConsumerMessage, handler BulkEventHandler, topic string,
) error {
	consumer.k.logger.Debugf("Processing Kafka bulk message: %s", topic)
	messageValues := make([]KafkaBulkMessageEntry, len(messages))

	for i, message := range messages {
		if message != nil {
			metadata := GetEventMetadata(message, consumer.k)
			handlerConfig, err := consumer.k.GetTopicHandlerConfig(message.Topic)
			if err != nil {
				return err
			}
			messageVal, err := consumer.k.DeserializeValue(message, handlerConfig)
			if err != nil {
				return err
			}
			childMessage := KafkaBulkMessageEntry{
				EntryId:  strconv.Itoa(i),
				Event:    messageVal,
				Metadata: metadata,
			}
			messageValues[i] = childMessage
		}
	}
	event := KafkaBulkMessage{
		Topic:   topic,
		Entries: messageValues,
	}

	var err error
	if consumer.k.enableExactlyOnceSemantics {
		err = consumer.doBulkCallbackEOS(session, messages, handler, topic, &event)
	} else {
		responses, handlerErr := handler(session.Context(), &event)
		err = handlerErr
		if err != nil {
			for i, resp := range responses {
				if i >= len(messageValues) || resp.EntryId != messageValues[i].EntryId {
					break
				}
				if resp.Error != nil {
					break
				}
				session.MarkMessage(messages[i], "")
			}
		} else {
			for _, message := range messages {
				session.MarkMessage(message, "")
			}
		}
	}
	return err
}

// doBulkCallbackEOS runs the handler inside a transaction and commits offsets via AddOffsetsToTxn.
func (consumer *consumer) doBulkCallbackEOS(session sarama.ConsumerGroupSession,
	messages []*sarama.ConsumerMessage, handler BulkEventHandler, topic string, event *KafkaBulkMessage,
) error {
	clients, err := consumer.k.latestClients()
	if err != nil || clients == nil {
		return fmt.Errorf("failed to get Kafka clients for EOS: %w", err)
	}
	producer := clients.producer
	txnProducer, ok := producer.(interface {
		BeginTxn() error
		AddOffsetsToTxn(offsets map[string][]*sarama.PartitionOffsetMetadata, groupID string) error
		CommitTxn() error
		AbortTxn() error
	})
	if !ok {
		return errors.New("exactly-once semantics enabled but producer is not transactional")
	}

	consumer.k.eosMu.Lock()
	defer consumer.k.eosMu.Unlock()

	if err := txnProducer.BeginTxn(); err != nil {
		return fmt.Errorf("BeginTxn: %w", err)
	}

	responses, handlerErr := handler(session.Context(), event)
	if handlerErr != nil {
		if abortErr := txnProducer.AbortTxn(); abortErr != nil {
			consumer.k.logger.Warnf("AbortTxn after handler error: %v", abortErr)
		}
		return handlerErr
	}

	// Partial success: mark successful entries only by committing their offsets in the txn
	// We still commit the whole txn; for partial failure we only add offsets for successful entries
	if responses != nil && len(responses) != len(messages) {
		// Build offset map only for successfully processed messages
		var successMsgs []*sarama.ConsumerMessage
		for i, resp := range responses {
			if i >= len(messages) {
				break
			}
			if resp.EntryId == event.Entries[i].EntryId && resp.Error == nil {
				successMsgs = append(successMsgs, messages[i])
			}
		}
		if len(successMsgs) > 0 {
			offsetMap := buildOffsetMapForTxn(successMsgs)
			if err := txnProducer.AddOffsetsToTxn(offsetMap, consumer.k.consumerGroup); err != nil {
				if abortErr := txnProducer.AbortTxn(); abortErr != nil {
					consumer.k.logger.Warnf("AbortTxn after AddOffsetsToTxn error: %v", abortErr)
				}
				return fmt.Errorf("AddOffsetsToTxn: %w", err)
			}
		}
	} else {
		offsetMap := buildOffsetMapForTxn(messages)
		if len(offsetMap) > 0 {
			if err := txnProducer.AddOffsetsToTxn(offsetMap, consumer.k.consumerGroup); err != nil {
				if abortErr := txnProducer.AbortTxn(); abortErr != nil {
					consumer.k.logger.Warnf("AbortTxn after AddOffsetsToTxn error: %v", abortErr)
				}
				return fmt.Errorf("AddOffsetsToTxn: %w", err)
			}
		}
	}

	if err := txnProducer.CommitTxn(); err != nil {
		return fmt.Errorf("CommitTxn: %w", err)
	}
	return nil
}

func (consumer *consumer) doCallback(session sarama.ConsumerGroupSession, message *sarama.ConsumerMessage) error {
	consumer.k.logger.Debugf("Processing Kafka message: %s/%d/%d [key=%s]", message.Topic, message.Partition, message.Offset, asBase64String(message.Key))
	handlerConfig, err := consumer.k.GetTopicHandlerConfig(message.Topic)
	if err != nil {
		return err
	}
	if !handlerConfig.IsBulkSubscribe && handlerConfig.Handler == nil {
		return errors.New("invalid handler config for subscribe call")
	}

	messageVal, err := consumer.k.DeserializeValue(message, handlerConfig)
	if err != nil {
		return err
	}
	event := NewEvent{
		Topic: message.Topic,
		Data:  messageVal,
	}
	event.Metadata = GetEventMetadata(message, consumer.k)

	if consumer.k.enableExactlyOnceSemantics {
		return consumer.doCallbackEOS(session, message, handlerConfig.Handler, &event)
	}

	err = handlerConfig.Handler(session.Context(), &event)
	if err == nil {
		session.MarkMessage(message, "")
	}
	return err
}

// doCallbackEOS runs the single-message handler inside a transaction and commits offset via AddOffsetsToTxn.
func (consumer *consumer) doCallbackEOS(session sarama.ConsumerGroupSession, message *sarama.ConsumerMessage, handler func(context.Context, *NewEvent) error, event *NewEvent) error {
	clients, err := consumer.k.latestClients()
	if err != nil || clients == nil {
		return fmt.Errorf("failed to get Kafka clients for EOS: %w", err)
	}
	producer := clients.producer
	txnProducer, ok := producer.(interface {
		BeginTxn() error
		AddOffsetsToTxn(offsets map[string][]*sarama.PartitionOffsetMetadata, groupID string) error
		CommitTxn() error
		AbortTxn() error
	})
	if !ok {
		return errors.New("exactly-once semantics enabled but producer is not transactional")
	}

	consumer.k.eosMu.Lock()
	defer consumer.k.eosMu.Unlock()

	if err := txnProducer.BeginTxn(); err != nil {
		return fmt.Errorf("BeginTxn: %w", err)
	}

	if err := handler(session.Context(), event); err != nil {
		if abortErr := txnProducer.AbortTxn(); abortErr != nil {
			consumer.k.logger.Warnf("AbortTxn after handler error: %v", abortErr)
		}
		return err
	}

	offsetMap := buildOffsetMapForTxn([]*sarama.ConsumerMessage{message})
	if err := txnProducer.AddOffsetsToTxn(offsetMap, consumer.k.consumerGroup); err != nil {
		if abortErr := txnProducer.AbortTxn(); abortErr != nil {
			consumer.k.logger.Warnf("AbortTxn after AddOffsetsToTxn error: %v", abortErr)
		}
		return fmt.Errorf("AddOffsetsToTxn: %w", err)
	}

	if err := txnProducer.CommitTxn(); err != nil {
		return fmt.Errorf("CommitTxn: %w", err)
	}
	return nil
}

func GetEventMetadata(message *sarama.ConsumerMessage, kafka *Kafka) map[string]string {
	if message != nil {
		metadata := make(map[string]string, len(message.Headers)+5)
		if message.Key != nil {
			if kafka.escapeHeaders {
				metadata[keyMetadataKey] = url.QueryEscape(string(message.Key))
			} else {
				metadata[keyMetadataKey] = string(message.Key)
			}
		}
		metadata[offsetMetadataKey] = strconv.FormatInt(message.Offset, 10)
		metadata[topicMetadataKey] = message.Topic
		metadata[timestampMetadataKey] = strconv.FormatInt(message.Timestamp.UnixMilli(), 10)
		metadata[partitionMetadataKey] = strconv.FormatInt(int64(message.Partition), 10)
		for _, header := range message.Headers {
			// skip headers that are excluded from metadata
			if kafka.excludeHeaderMetaRegex != nil && kafka.excludeHeaderMetaRegex.MatchString(string(header.Key)) {
				kafka.logger.Debugf("Skipping header %v that is excluded from metadata", string(header.Key))
				continue
			}
			if kafka.escapeHeaders {
				metadata[string(header.Key)] = url.QueryEscape(string(header.Value))
			} else {
				metadata[string(header.Key)] = string(header.Value)
			}
		}
		return metadata
	}
	return nil
}

func (consumer *consumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (consumer *consumer) Setup(sarama.ConsumerGroupSession) error {
	return nil
}

// checkBulkSubscribe checks if a bulk handler and config are correctly registered for provided topic
func (k *Kafka) checkBulkSubscribe(topic string) bool {
	if bulkHandlerConfig, ok := k.subscribeTopics[topic]; ok &&
		bulkHandlerConfig.IsBulkSubscribe &&
		bulkHandlerConfig.BulkHandler != nil && (bulkHandlerConfig.SubscribeConfig.MaxMessagesCount > 0) &&
		bulkHandlerConfig.SubscribeConfig.MaxAwaitDurationMs > 0 {
		return true
	}
	return false
}

// GetTopicBulkHandler returns the handlerConfig for a topic
func (k *Kafka) GetTopicHandlerConfig(topic string) (SubscriptionHandlerConfig, error) {
	handlerConfig, ok := k.subscribeTopics[topic]
	if ok && ((handlerConfig.IsBulkSubscribe && handlerConfig.BulkHandler != nil) ||
		(!handlerConfig.IsBulkSubscribe && handlerConfig.Handler != nil)) {
		return handlerConfig, nil
	}
	return SubscriptionHandlerConfig{},
		fmt.Errorf("any handler for messages of topic %s not found", topic)
}
