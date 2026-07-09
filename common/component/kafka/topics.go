/*
Copyright 2026 The Dapr Authors
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

	"github.com/IBM/sarama"
)

// ensureTopic creates the topic with the configured number of partitions if
// numPartitions metadata was set. It is a no-op if numPartitions is unset,
// and idempotent if the topic already exists.
func (k *Kafka) ensureTopic(topic string) error {
	if k.numPartitions <= 0 {
		return nil
	}
	if _, done := k.ensuredTopics.Load(topic); done {
		return nil
	}

	clients, err := k.latestClients()
	if err != nil || clients == nil || clients.admin == nil {
		return fmt.Errorf("failed to get kafka admin client: %w", err)
	}

	err = clients.admin.CreateTopic(topic, &sarama.TopicDetail{
		NumPartitions:     k.numPartitions,
		ReplicationFactor: k.replicationFactor,
	}, false)
	if err != nil && !errors.Is(err, sarama.ErrTopicAlreadyExists) {
		return fmt.Errorf("failed to create topic %s: %w", topic, err)
	}

	k.ensuredTopics.Store(topic, struct{}{})
	return nil
}
