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
	"strconv"
	"sync"

	"github.com/IBM/sarama"
	"github.com/google/uuid"
)

// errTxnTokenClosed is returned when a publish carries a transaction token
// that does not match an open delivery transaction. Failing loudly is
// deliberate: silently downgrading to a non-transactional publish would
// betray the atomicity the app asked for.
var errTxnTokenClosed = errors.New("kafka: transaction token does not match an open delivery transaction (the handler may have already returned, or consumerTransactionsEnabled is disabled)")

// txnSession is one open consume transaction, correlated to an in-flight
// delivery by its token. Publishes carrying the token are routed here and
// join the transaction.
type txnSession struct {
	mu       sync.Mutex
	producer sarama.SyncProducer
	open     bool
	// sent records whether the transaction may contain records. sarama skips
	// offset commits for transactions that produced no records, so a
	// record-less delivery must commit its offset outside the transaction.
	sent bool
}

func (s *txnSession) send(send func(sarama.SyncProducer) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.open {
		return errTxnTokenClosed
	}
	// sarama adds the partition to the transaction on the send *attempt*, so
	// the transaction may hold records even when the send returns an error.
	// Mark before sending: a failed publish whose error the app swallows must
	// finish through the transactional path (where the error state aborts)
	// rather than the record-less fallback, which would commit any records
	// that did land plus the offset out-of-band.
	s.sent = true
	return send(s.producer)
}

// hasSent reports whether any publish was attempted against the transaction.
// Only meaningful after end().
func (s *txnSession) hasSent() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sent
}

// end closes the session, waiting out any in-flight send. It must be called
// before the transaction is ended so no late publish can slip into a
// committing transaction — or worse, into the next transaction on the same
// producer.
func (s *txnSession) end() {
	s.mu.Lock()
	s.open = false
	s.mu.Unlock()
}

func (k *Kafka) registerTxnSession(s *txnSession) string {
	token := uuid.NewString()
	k.txnSessionsMu.Lock()
	k.txnSessions[token] = s
	k.txnSessionsMu.Unlock()
	return token
}

func (k *Kafka) deregisterTxnSession(token string) {
	k.txnSessionsMu.Lock()
	delete(k.txnSessions, token)
	k.txnSessionsMu.Unlock()
}

// publishInConsumeTxn routes a publish carrying a transaction token into the
// correlated delivery's open transaction. It never begins or commits
// anything: the consume side owns the transaction lifecycle.
func (k *Kafka) publishInConsumeTxn(token string, send func(sarama.SyncProducer) error) error {
	k.txnSessionsMu.RLock()
	sess := k.txnSessions[token]
	k.txnSessionsMu.RUnlock()
	if sess == nil {
		return errTxnTokenClosed
	}
	return sess.send(send)
}

// claimTxn manages the transactional producer of one consumer-group claim
// (topic-partition). Deliveries within a claim are serial, so the producer
// has at most one open transaction by construction and no lock is ever held
// across the app handler. Its transactional.id is stable per claim: after a
// rebalance the new owner's producer epoch-fences a zombie's, and a producer
// recreated after a fatal error aborts its own stale transaction the same
// way.
type claimTxn struct {
	k         *Kafka
	topic     string
	partition int32
	producer  sarama.SyncProducer
}

func (k *Kafka) newClaimTxn(topic string, partition int32) *claimTxn {
	return &claimTxn{k: k, topic: topic, partition: partition}
}

func (ct *claimTxn) transactionalID() string {
	return ct.k.txnIDPrefix + "-" + ct.k.consumerGroup + "-" + ct.topic + "-" + strconv.Itoa(int(ct.partition))
}

// getProducer lazily creates the claim's transactional producer on the first
// transactional delivery.
func (ct *claimTxn) getProducer() (sarama.SyncProducer, error) {
	if ct.producer != nil {
		return ct.producer, nil
	}

	pc := ct.k.producerConfig
	pc.TransactionsEnabled = true
	pc.TransactionalID = ct.transactionalID()

	var (
		p   sarama.SyncProducer
		err error
	)
	if ct.k.claimProducerFactory != nil {
		p, err = ct.k.claimProducerFactory(pc)
	} else {
		p, err = GetSyncProducer(*ct.k.config, ct.k.brokers, pc)
	}
	if err != nil {
		return nil, fmt.Errorf("kafka: failed to create transactional producer for %s/%d: %w", ct.topic, ct.partition, err)
	}

	ct.producer = p
	return p, nil
}

// invalidate drops the claim producer after a fatal transaction error; the
// next delivery recreates it.
func (ct *claimTxn) invalidate() {
	if ct.producer != nil {
		_ = ct.producer.Close()
		ct.producer = nil
	}
}

// close releases the claim producer when the claim ends (rebalance or
// shutdown).
func (ct *claimTxn) close() {
	ct.invalidate()
}
