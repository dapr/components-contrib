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

package kafka_test

import (
	"fmt"
	"time"

	"github.com/IBM/sarama"

	"github.com/dapr/components-contrib/tests/certification/flow"
	"github.com/dapr/components-contrib/tests/certification/flow/retry"
)

const consumerOffsetsTopic = "__consumer_offsets"

// waitForKafkaReadiness returns a step that waits until the Kafka cluster can
// serve consumer groups. It tries every 10 seconds, for at most 5 minutes.
func waitForKafkaReadiness(brokers []string, clientID string) flow.Runnable {
	return retry.Do(10*time.Second, 30, func(ctx flow.Context) error {
		return kafkaReady(brokers, clientID)
	})
}

// kafkaReady returns nil when the brokers serve a topic partition and each
// partition of __consumer_offsets has a leader.
//
// A consumer group can join only when its group coordinator is available. The
// coordinator is the leader of one partition of __consumer_offsets. The brokers
// create that topic with replication factor 3 when a client first asks for a
// coordinator. On a slow CI runner this can take minutes. A check that only
// reads a topic partition passes before that, and then the consumer groups of
// the sidecars fail with "The coordinator is not available".
func kafkaReady(brokers []string, clientID string) error {
	config := sarama.NewConfig()
	config.ClientID = clientID

	client, err := sarama.NewClient(brokers, config)
	if err != nil {
		return err
	}
	defer client.Close()

	consumer, err := sarama.NewConsumerFromClient(client)
	if err != nil {
		return err
	}
	defer consumer.Close()

	// The brokers serve a topic partition.
	partitionConsumer, err := consumer.ConsumePartition("myTopic", 0, sarama.OffsetOldest)
	if err != nil {
		return err
	}
	if err = partitionConsumer.Close(); err != nil {
		return err
	}

	// A FindCoordinator request makes the brokers create __consumer_offsets
	// if it does not exist yet.
	if _, err = client.Coordinator(clientID); err != nil {
		return fmt.Errorf("group coordinator is not available: %w", err)
	}

	// Each partition of __consumer_offsets has a leader, so the coordinator
	// of each consumer group is available.
	if err = client.RefreshMetadata(consumerOffsetsTopic); err != nil {
		return err
	}
	partitions, err := client.Partitions(consumerOffsetsTopic)
	if err != nil {
		return err
	}
	if len(partitions) == 0 {
		return fmt.Errorf("%s has no partitions", consumerOffsetsTopic)
	}
	for _, partition := range partitions {
		if _, err = client.Leader(consumerOffsetsTopic, partition); err != nil {
			return fmt.Errorf("partition %d of %s has no leader: %w", partition, consumerOffsetsTopic, err)
		}
	}

	return nil
}
