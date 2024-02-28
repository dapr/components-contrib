/*
Copyright 2024 The Dapr Authors
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
	"time"
)

// Subscribe adds a handler and configuration for a topic, and subscribes.
// Unsubscribes to the topic on context cancel.
func (k *Kafka) Subscribe(ctx context.Context, handlerConfig SubscriptionHandlerConfig, topics ...string) {
	k.subscribeLock.Lock()
	defer k.subscribeLock.Unlock()
	for _, topic := range topics {
		k.subscribeTopics[topic] = handlerConfig
	}

	k.logger.Debugf("Subscribing to topic: %v", topics)

	k.reloadConsumerGroup()

	k.wg.Add(1)
	go func() {
		defer k.wg.Done()
		select {
		case <-ctx.Done():
		case <-k.closeCh:
		}

		k.subscribeLock.Lock()
		defer k.subscribeLock.Unlock()

		k.logger.Debugf("Unsubscribing to topic: %v", topics)

		for _, topic := range topics {
			delete(k.subscribeTopics, topic)
		}

		k.reloadConsumerGroup()
	}()
}

// reloadConsumerGroup reloads the consumer group with the new topics.
func (k *Kafka) reloadConsumerGroup() {
	if k.consumerCancel != nil {
		k.consumerCancel()
		k.consumerCancel = nil
		k.consumerWG.Wait()
	}

	if len(k.subscribeTopics) == 0 || k.closed.Load() {
		return
	}

	topics := k.subscribeTopics.TopicList()

	k.logger.Debugf("Subscribed and listening to topics: %s", topics)

	consumer := &consumer{k: k}

	ctx, cancel := context.WithCancel(context.Background())
	k.consumerCancel = cancel

	k.consumerWG.Add(1)
	go func() {
		defer k.consumerWG.Done()
		k.consume(ctx, topics, consumer)
		k.logger.Debugf("Closing ConsumerGroup for topics: %v", topics)
	}()
}

func (k *Kafka) consume(ctx context.Context, topics []string, consumer *consumer) {
	for {
		err := k.cg.Consume(ctx, topics, consumer)
		if errors.Is(err, context.Canceled) {
			return
		}
		if err != nil {
			k.logger.Errorf("Error consuming %v. Retrying...: %v", topics, err)
		}

		select {
		case <-k.closeCh:
			return
		case <-ctx.Done():
			return
		case <-time.After(k.consumeRetryInterval):
		}
	}
}
