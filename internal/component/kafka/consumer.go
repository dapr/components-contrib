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
	k        *Kafka
	ready    chan bool
	callback EventHandler
	once     sync.Once
}

func (consumer *consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	if consumer.callback == nil {
		return fmt.Errorf("nil consumer callback")
	}

	b := consumer.k.backOffConfig.NewBackOffWithContext(session.Context())
	for message := range claim.Messages() {
		event := NewEvent{
			Topic: message.Topic,
			Data:  message.Value,
		}
		if err := retry.NotifyRecover(func() error {
			consumer.k.logger.Debugf("Processing Kafka message: %s/%d/%d [key=%s]", message.Topic, message.Partition, message.Offset, asBase64String(message.Key))
			err := consumer.callback(session.Context(), &event)
			if err == nil {
				session.MarkMessage(message, "")
			}

			return err
		}, b, func(err error, d time.Duration) {
			consumer.k.logger.Errorf("Error processing Kafka message: %s/%d/%d [key=%s]. Retrying...", message.Topic, message.Partition, message.Offset, asBase64String(message.Key))
		}, func() {
			consumer.k.logger.Infof("Successfully processed Kafka message after it previously failed: %s/%d/%d [key=%s]", message.Topic, message.Partition, message.Offset, asBase64String(message.Key))
		}); err != nil {
			return err
		}
	}

	return nil
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

// Subscribe to topic in the Kafka cluster
// This call cannot block like its sibling in bindings/kafka because of where this is invoked in runtime.go.
func (k *Kafka) Subscribe(topics []string, _ map[string]string, handler EventHandler) error {
	if k.consumerGroup == "" {
		return errors.New("kafka: consumerGroup must be set to subscribe")
	}

	// Close resources and reset synchronization primitives
	k.closeSubscriptionResources()

	cg, err := sarama.NewConsumerGroup(k.brokers, k.consumerGroup, k.config)
	if err != nil {
		return err
	}

	k.cg = cg

	ctx, cancel := context.WithCancel(context.Background())
	k.cancel = cancel

	ready := make(chan bool)
	k.consumer = consumer{
		k:        k,
		ready:    ready,
		callback: handler,
	}

	go func() {
		defer func() {
			k.logger.Debugf("Closing ConsumerGroup for topics: %v", topics)
			err := k.cg.Close()
			if err != nil {
				k.logger.Errorf("Error closing consumer group: %v", err)
			}
		}()

		k.logger.Debugf("Subscribed and listening to topics: %s", topics)

		for {
			k.logger.Debugf("Starting loop to consume.")

			// Consume the requested topics
			bo := backoff.WithContext(backoff.NewConstantBackOff(k.consumeRetryInterval), ctx)
			innerErr := retry.NotifyRecover(func() error {
				return k.cg.Consume(ctx, topics, &(k.consumer))
			}, bo, func(err error, t time.Duration) {
				k.logger.Errorf("Error consuming %v. Retrying...: %v", topics, err)
			}, func() {
				k.logger.Infof("Recovered consuming %v", topics)
			})
			if innerErr != nil && !errors.Is(innerErr, context.Canceled) {
				k.logger.Errorf("Permanent error consuming %v: %v", topics, innerErr)
			}

			// If the context was cancelled, as is the case when handling SIGINT and SIGTERM below, then this pops
			// us out of the consume loop
			if ctx.Err() != nil {
				return
			}
		}
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
			close(k.consumer.ready)
			k.consumer.once = sync.Once{}
		})
	}
}
