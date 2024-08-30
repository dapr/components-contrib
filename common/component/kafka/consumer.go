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

func (consumer *consumer) doBulkCallback(session sarama.ConsumerGroupSession,
	messages []*sarama.ConsumerMessage, handler BulkEventHandler, topic string,
) error {
	consumer.k.logger.Debugf("Processing Kafka bulk message: %s", topic)
	messageValues := make([]KafkaBulkMessageEntry, len(messages))

	for i, message := range messages {
		if message != nil {
			metadata := GetEventMetadata(message, consumer.k.escapeHeaders)
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
	responses, err := handler(session.Context(), &event)

	if err != nil {
		for i, resp := range responses {
			// An extra check to confirm that runtime returned responses are in order
			if resp.EntryId != messageValues[i].EntryId {
				return errors.New("entry id mismatch while processing bulk messages")
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
	return err
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
	event.Metadata = GetEventMetadata(message, consumer.k.escapeHeaders)

	err = handlerConfig.Handler(session.Context(), &event)
	if err == nil {
		session.MarkMessage(message, "")
	}
	return err
}

func GetEventMetadata(message *sarama.ConsumerMessage, escapeHeaders bool) map[string]string {
	if message != nil {
		metadata := make(map[string]string, len(message.Headers)+5)
		if message.Key != nil {
			if escapeHeaders {
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
			if escapeHeaders {
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
