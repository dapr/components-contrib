package kafka

import (
	"fmt"

	"github.com/IBM/sarama"
)

type clients struct {
	consumerGroup sarama.ConsumerGroup
	producer      sarama.SyncProducer
}

func (k *Kafka) latestClients() (*clients, error) {
	switch {
	// case 0: use mock clients for testing
	case k.mockProducer != nil || k.mockConsumerGroup != nil:
		return &clients{
			consumerGroup: k.mockConsumerGroup,
			producer:      k.mockProducer,
		}, nil

	// case 1: use aws clients with refreshable tokens in the cfg
	case k.awsConfig != nil:
		k.clientsLock.RLock()
		if k.clients != nil {
			defer k.clientsLock.RUnlock()
			return k.clients, nil
		}
		k.clientsLock.RUnlock()

		awsKafkaOpts := KafkaOptions{
			Config:          k.config,
			ConsumerGroup:   k.consumerGroup,
			Brokers:         k.brokers,
			MaxMessageBytes: k.maxMessageBytes,
		}

		awsKafkaClients := InitAwsClients(awsKafkaOpts)
		err := awsKafkaClients.New(k.awsConfig)
		if err != nil {
			return nil, fmt.Errorf("failed to get AWS IAM Kafka clients: %w", err)
		}

		k.clientsLock.Lock()
		// Double-check: another goroutine may have created clients while we
		// were waiting for the write lock.
		if k.clients != nil {
			k.clientsLock.Unlock()
			return k.clients, nil
		}
		k.clients = &clients{
			consumerGroup: awsKafkaClients.ConsumerGroup,
			producer:      awsKafkaClients.Producer,
		}
		k.clientsLock.Unlock()
		return k.clients, nil

	// case 2: normal static auth profile clients
	default:
		k.clientsLock.RLock()
		if k.clients != nil {
			defer k.clientsLock.RUnlock()
			return k.clients, nil
		}
		k.clientsLock.RUnlock()

		cg, err := sarama.NewConsumerGroup(k.brokers, k.consumerGroup, k.config)
		if err != nil {
			return nil, err
		}

		p, err := GetSyncProducer(*k.config, k.brokers, k.maxMessageBytes)
		if err != nil {
			return nil, err
		}

		newStaticClients := clients{
			consumerGroup: cg,
			producer:      p,
		}
		k.clientsLock.Lock()
		// Double-check: another goroutine may have created clients while we
		// were waiting for the write lock.
		if k.clients != nil {
			k.clientsLock.Unlock()
			return k.clients, nil
		}
		k.clients = &newStaticClients
		k.clientsLock.Unlock()
		return k.clients, nil
	}
}

// invalidateClients closes and clears the cached clients, forcing
// re-creation on the next call to latestClients().
func (k *Kafka) invalidateClients() {
	k.clientsLock.Lock()
	old := k.clients
	k.clients = nil
	k.clientsLock.Unlock()

	// Close old clients outside the lock to avoid holding it during I/O.
	if old != nil {
		if old.producer != nil {
			if err := old.producer.Close(); err != nil {
				k.logger.Warnf("Error closing old Kafka producer during client invalidation: %v", err)
			}
		}
		if old.consumerGroup != nil {
			if err := old.consumerGroup.Close(); err != nil {
				k.logger.Warnf("Error closing old Kafka consumer group during client invalidation: %v", err)
			}
		}
	}
}
