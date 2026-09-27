package kafka

import (
	"errors"
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
		k.clientsLock.Lock()
		defer k.clientsLock.Unlock()
		if k.clients != nil {
			return k.clients.snapshot(), nil
		}

		awsKafkaOpts := KafkaOptions{
			Config:         k.config,
			ConsumerGroup:  k.consumerGroup,
			Brokers:        k.brokers,
			ProducerConfig: k.producerConfig,
		}

		awsKafkaClients := InitAwsClients(awsKafkaOpts)
		err := awsKafkaClients.New(k.awsConfig)
		if err != nil {
			return nil, fmt.Errorf("failed to get AWS IAM Kafka clients: %w", err)
		}

		k.awsClients = awsKafkaClients
		k.clients = &clients{
			consumerGroup: awsKafkaClients.ConsumerGroup,
			producer:      awsKafkaClients.Producer,
		}
		return k.clients.snapshot(), nil

	// case 2: normal static auth profile clients
	default:
		k.clientsLock.Lock()
		defer k.clientsLock.Unlock()
		if k.clients != nil {
			return k.clients.snapshot(), nil
		}
		cg, err := sarama.NewConsumerGroup(k.brokers, k.consumerGroup, k.config)
		if err != nil {
			return nil, err
		}

		p, err := GetSyncProducer(*k.config, k.brokers, k.producerConfig)
		if err != nil {
			return nil, err
		}

		newStaticClients := clients{
			consumerGroup: cg,
			producer:      p,
		}
		k.clients = &newStaticClients
		return k.clients.snapshot(), nil
	}
}

// snapshot returns a copy of the client pair taken under clientsLock. Callers
// read the fields after the lock is released, and invalidateProducer /
// transactionalProducer mutate the canonical struct's producer field
// under the lock — handing out the live struct would be a data race.
func (c *clients) snapshot() *clients {
	return &clients{
		consumerGroup: c.consumerGroup,
		producer:      c.producer,
	}
}

// transactionalProducer returns the shared producer for a transactional
// publish, lazily recreating it when a previous fatal transaction error
// dropped it. Recreation lives here — on the publish path only — so a
// producer-side failure can never starve the consume loop, which does not
// use the shared producer.
//
// Callers must hold txnMu. Every invalidation of the shared producer happens
// under txnMu (endTxnWithError runs inside withPublishTxn, and Close either
// wins a TryLock on txnMu or hands the close to closeProducerWhenIdle, which
// takes txnMu itself), so a producer returned here cannot be closed for the
// duration of the caller's critical section.
func (k *Kafka) transactionalProducer() (sarama.SyncProducer, error) {
	// Checked before the mock short-circuit so the closed contract is
	// uniform between mock-backed tests and real clients.
	if k.closed.Load() {
		return nil, errors.New("component is closed")
	}
	if k.mockProducer != nil {
		return k.mockProducer, nil
	}

	k.clientsLock.Lock()
	if k.clients == nil {
		k.clientsLock.Unlock()
		return nil, errors.New("component is not initialized")
	}
	if k.clients.producer != nil {
		p := k.clients.producer
		k.clientsLock.Unlock()
		return p, nil
	}
	awsClients := k.awsClients
	k.clientsLock.Unlock()

	// The recreation dial runs OUTSIDE clientsLock: txnMu (held by the
	// caller) already serializes recreation, and a broker dial can take
	// minutes — holding clientsLock across it would stall Close()'s
	// consumer-group teardown and every publish for the dial duration.
	var (
		p   sarama.SyncProducer
		err error
	)
	if awsClients != nil {
		p, err = awsClients.getSyncProducer()
	} else {
		p, err = GetSyncProducer(*k.config, k.brokers, k.producerConfig)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to recreate Kafka producer: %w", err)
	}

	k.clientsLock.Lock()
	defer k.clientsLock.Unlock()
	// Close() may have run during the dial; its TryLock on txnMu failed (the
	// caller holds it), so closeProducerWhenIdle is parked on txnMu waiting
	// for this publish. Closing the fresh producer here rather than storing
	// it keeps the dial from outliving Close by the length of the caller's
	// critical section.
	if k.closed.Load() {
		_ = p.Close()
		return nil, errors.New("component is closed")
	}
	if awsClients != nil {
		awsClients.Producer = p
	}
	k.clients.producer = p
	return p, nil
}

// closeProducerWhenIdle waits for the in-flight transactional publish to
// release txnMu, then detaches and closes the shared producer. It only runs
// after Close() set k.closed, so transactionalProducer() refuses to hand the
// producer out again and no new transaction can start behind it; the publish
// it waits on is the last transactional use. If that publish never releases
// txnMu the goroutine stays parked, which leaks no more than abandoning the
// producer outright would have.
func (k *Kafka) closeProducerWhenIdle() {
	k.txnMu.Lock()
	defer k.txnMu.Unlock()

	var p sarama.SyncProducer
	k.clientsLock.Lock()
	if k.clients != nil && k.clients.producer != nil {
		p = k.clients.producer
		k.clients.producer = nil
	}
	k.clientsLock.Unlock()
	if p == nil {
		return
	}
	if err := p.Close(); err != nil {
		k.logger.Warnf("Failed to close the Kafka producer once the in-flight transactional publish finished: %v", err)
	}
}

// invalidateProducer closes and drops the cached producer after a fatal
// transaction error; the next publish recreates it with the same
// transactional.id, which bumps the producer epoch and aborts any stale
// transaction broker-side. The consumer group is untouched. The producer
// argument prevents dropping a replacement created by a concurrent caller.
func (k *Kafka) invalidateProducer(producer sarama.SyncProducer) {
	// Detach under the lock, close outside it — same split Close() uses, and
	// for the same reason: sarama's producer shutdown blocks on in-flight
	// work, so closing here would hold clientsLock for that whole drain and
	// stall Close()'s consumer-group teardown, which is what keeps the
	// rebalance prompt (dapr/components-contrib#3907). Detaching first is
	// enough: the slot is already nil to everyone else, and txnMu serializes
	// recreation.
	var detached sarama.SyncProducer
	k.clientsLock.Lock()
	if k.clients != nil && k.clients.producer == producer {
		detached = k.clients.producer
		k.clients.producer = nil
	}
	k.clientsLock.Unlock()
	if detached != nil {
		_ = detached.Close()
	}
}
