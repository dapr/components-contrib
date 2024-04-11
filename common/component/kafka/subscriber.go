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
	"fmt"
	"time"
)

func (k *Kafka) GetOrCreateConsumerGroup(handlerConfig SubscriptionHandlerConfig) (*ConsumerGroup, error) {
	cg, ok := k.consumerGroups[handlerConfig.ConsumerGroupID]
	if ok {
		return cg, nil
	}

	cg, err := NewConsumerGroup(k.brokers, handlerConfig.ConsumerGroupID, k.config)
	if err != nil {
		return nil, err
	}
	k.consumerGroups[handlerConfig.ConsumerGroupID] = cg
	return cg, nil
}

// Subscribe adds a handler and configuration for a topic, and subscribes.
// Unsubscribes to the topic on context cancel.
func (k *Kafka) Subscribe(ctx context.Context, handlerConfig SubscriptionHandlerConfig, topics ...string) error {
	k.subscribeLock.Lock()
	defer k.subscribeLock.Unlock()
	var cg *ConsumerGroup
	// If a consumer group override is specified, retrieve group if exists or create it
	if len(handlerConfig.ConsumerGroupID) > 0 {
		cgEx, err := k.GetOrCreateConsumerGroup(handlerConfig)
		if err != nil {
			return err
		}
		cg = cgEx
		// Otherwise use default consumer group registered on init
	} else {
		cgDefault, ok := k.consumerGroups[k.defaultConsumerGroupID]
		if !ok {
			return fmt.Errorf("undefined default consumer group: %s", k.defaultConsumerGroupID)
		}
		cg = cgDefault
	}
	for _, topic := range topics {
		cg.subscribeTopics[topic] = handlerConfig
	}

	k.logger.Debugf("Subscribing to topic: %v", topics)

	k.reloadConsumerGroup(cg)

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
			delete(cg.subscribeTopics, topic)
		}

		k.reloadConsumerGroup(cg)
	}()

	return nil
}

// reloadConsumerGroup reloads the consumer group with the new topics.
func (k *Kafka) reloadConsumerGroup(consumerGroup *ConsumerGroup) {
	if consumerGroup.consumerCancel != nil {
		consumerGroup.consumerCancel()
		consumerGroup.consumerCancel = nil
		k.consumerWG.Wait()
	}

	if len(consumerGroup.subscribeTopics) == 0 || k.closed.Load() {
		return
	}

	topics := consumerGroup.subscribeTopics.TopicList()

	k.logger.Debugf("Subscribed and listening to topics: %s", topics)

	consumer := &consumer{k: k, consumerGroup: consumerGroup}

	ctx, cancel := context.WithCancel(context.Background())
	consumerGroup.consumerCancel = cancel

	k.consumerWG.Add(1)
	go func() {
		defer k.consumerWG.Done()
		k.consume(ctx, topics, consumer)
		k.logger.Debugf("Closing ConsumerGroup for topics: %v", topics)
	}()
}

func (k *Kafka) consume(ctx context.Context, topics []string, consumer *consumer) {
	for {
		err := consumer.consumerGroup.cg.Consume(ctx, topics, consumer)
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
