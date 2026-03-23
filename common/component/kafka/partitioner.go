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
	"fmt"

	"github.com/IBM/sarama"
)

// daprPartitioner is a sarama.Partitioner that supports both explicit partition
// selection (via msg.Partition >= 0) and hash-based partitioning (when
// msg.Partition < 0). This allows per-message partition targeting via the
// "partitionNumber" metadata key while preserving the default hash-based
// behavior for messages using "partitionKey" or no key at all.
type daprPartitioner struct {
	hashPartitioner sarama.Partitioner
}

// newDaprPartitioner is a sarama.PartitionerConstructor that creates a
// daprPartitioner wrapping a hash partitioner for the given topic.
func newDaprPartitioner(topic string) sarama.Partitioner {
	return &daprPartitioner{
		hashPartitioner: sarama.NewHashPartitioner(topic),
	}
}

func (p *daprPartitioner) Partition(message *sarama.ProducerMessage, numPartitions int32) (int32, error) {
	if message.Partition >= 0 {
		if message.Partition >= numPartitions {
			return 0, fmt.Errorf("partition number %d is out of range, topic has %d partitions", message.Partition, numPartitions)
		}
		return message.Partition, nil
	}
	return p.hashPartitioner.Partition(message, numPartitions)
}

func (p *daprPartitioner) RequiresConsistency() bool {
	return p.hashPartitioner.RequiresConsistency()
}
