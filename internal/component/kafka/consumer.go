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
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/cenkalti/backoff/v4"

	"github.com/dapr/kit/retry"
)

type consumer struct {
	k       *Kafka
	ready   chan bool
	running chan struct{}
	once    sync.Once
}

func (consumer *consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	b := consumer.k.backOffConfig.NewBackOffWithContext(session.Context())
	for message := range claim.Messages() {
		if consumer.k.consumeRetryEnabled {
			if err := retry.NotifyRecover(func() error {
				return consumer.doCallback(session, message)
			}, b, func(err error, d time.Duration) {
				consumer.k.logger.Errorf("Error processing Kafka message: %s/%d/%d [key=%s]. Error: %v. Retrying...", message.Topic, message.Partition, message.Offset, asBase64String(message.Key), err)
			}, func() {
				consumer.k.logger.Infof("Successfully processed Kafka message after it previously failed: %s/%d/%d [key=%s]", message.Topic, message.Partition, message.Offset, asBase64String(message.Key))
			}); err != nil {
				return err
			}
		} else {
			err := consumer.doCallback(session, message)
			consumer.k.logger.Errorf("Error processing Kafka message: %s/%d/%d [key=%s]. Error: %v.", message.Topic, message.Partition, message.Offset, asBase64String(message.Key), err)
		}
	}

	return nil
}

func (consumer *consumer) doCallback(session sarama.ConsumerGroupSession, message *sarama.ConsumerMessage) error {
	consumer.k.logger.Debugf("Processing Kafka message: %s/%d/%d [key=%s]", message.Topic, message.Partition, message.Offset, asBase64String(message.Key))
	handler, err := consumer.k.GetTopicHandler(message.Topic)
	if err != nil {
		return err
	}

	// get metadata from headers
	metadata := make(map[string]string)
	for _, header := range message.Headers {
		// header starts with "ce_", it's cloudevent metadata
		key := string(header.Key)
		if len(key) > 3 && key[:3] == "ce_" {
			metadata[key[3:]] = string(header.Value)
		}
	}

	event := NewEvent{
		Topic:    message.Topic,
		Data:     message.Value,
		Metadata: metadata,
	}
	err = handler(session.Context(), &event)
	if err == nil {
		session.MarkMessage(message, "")
	}

	return err
}

func (consumer *consumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (consumer *consumer) Setup(sarama.ConsumerGroupSession) error {
	consumer.once.Do(func() {
		close(consumer.ready)
	})

	return nil
}

// AddTopicHandler adds a topic handler
func (k *Kafka) AddTopicHandler(topic string, handler EventHandler) {
	k.subscribeLock.Lock()
	k.subscribeTopics[topic] = handler
	k.subscribeLock.Unlock()
}

// RemoveTopicHandler removes a topic handler
func (k *Kafka) RemoveTopicHandler(topic string) {
	k.subscribeLock.Lock()
	delete(k.subscribeTopics, topic)
	k.subscribeLock.Unlock()
}

// GetTopicHandler returns the handler for a topic
func (k *Kafka) GetTopicHandler(topic string) (EventHandler, error) {
	handler, ok := k.subscribeTopics[topic]
	if !ok || handler == nil {
		return nil, fmt.Errorf("handler for messages of topic %s not found", topic)
	}

	return handler, nil
}

// Subscribe to topic in the Kafka cluster, in a background goroutine
func (k *Kafka) Subscribe(ctx context.Context) error {
	if k.consumerGroup == "" {
		return errors.New("kafka: consumerGroup must be set to subscribe")
	}

	k.subscribeLock.Lock()
	defer k.subscribeLock.Unlock()

	// Close resources and reset synchronization primitives
	k.closeSubscriptionResources()

	topics := k.subscribeTopics.TopicList()
	if len(topics) == 0 {
		// Nothing to subscribe to
		return nil
	}

	cg, err := sarama.NewConsumerGroup(k.brokers, k.consumerGroup, k.config)
	if err != nil {
		return err
	}

	k.cg = cg

	ctx, cancel := context.WithCancel(ctx)
	k.cancel = cancel

	ready := make(chan bool)
	k.consumer = consumer{
		k:       k,
		ready:   ready,
		running: make(chan struct{}),
	}

	go func() {
		k.logger.Debugf("Subscribed and listening to topics: %s", topics)

		for {
			// If the context was cancelled, as is the case when handling SIGINT and SIGTERM below, then this pops
			// us out of the consume loop
			if ctx.Err() != nil {
				break
			}

			k.logger.Debugf("Starting loop to consume.")

			// Consume the requested topics
			bo := backoff.WithContext(backoff.NewConstantBackOff(k.consumeRetryInterval), ctx)
			innerErr := retry.NotifyRecover(func() error {
				if ctxErr := ctx.Err(); ctxErr != nil {
					return backoff.Permanent(ctxErr)
				}
				return k.cg.Consume(ctx, topics, &(k.consumer))
			}, bo, func(err error, t time.Duration) {
				k.logger.Errorf("Error consuming %v. Retrying...: %v", topics, err)
			}, func() {
				k.logger.Infof("Recovered consuming %v", topics)
			})
			if innerErr != nil && !errors.Is(innerErr, context.Canceled) {
				k.logger.Errorf("Permanent error consuming %v: %v", topics, innerErr)
			}
		}

		k.logger.Debugf("Closing ConsumerGroup for topics: %v", topics)
		err := k.cg.Close()
		if err != nil {
			k.logger.Errorf("Error closing consumer group: %v", err)
		}

		close(k.consumer.running)
	}()

	<-ready

	return nil
}

// Close down consumer group resources, refresh once.
func (k *Kafka) closeSubscriptionResources() {
	if k.cg != nil {
		k.cancel()
		err := k.cg.Close()
		if err != nil {
			k.logger.Errorf("Error closing consumer group: %v", err)
		}

		k.consumer.once.Do(func() {
			// Wait for shutdown to be complete
			<-k.consumer.running
			close(k.consumer.ready)
			k.consumer.once = sync.Once{}
		})
	}
}
