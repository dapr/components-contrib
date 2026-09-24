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
	"net/url"
	"strconv"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"github.com/cenkalti/backoff/v4"

	"github.com/dapr/kit/retry"
)

type consumer struct {
	k     *Kafka
	mutex sync.Mutex
}

func (consumer *consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	b := consumer.k.backOffConfig.NewBackOffWithContext(session.Context())
	isBulkSubscribe := consumer.k.checkBulkSubscribe(claim.Topic())

	handlerConfig, err := consumer.k.GetTopicHandlerConfig(claim.Topic())
	if err != nil {
		return fmt.Errorf("error getting bulk handler config for topic %s: %w", claim.Topic(), err)
	}

	// When consumer transactions are enabled, this claim gets its own
	// transactional producer (created lazily on the first delivery) so its
	// transactions never contend with other claims or with the app handler.
	var ct *claimTxn
	if consumer.k.consumerTxnEnabled {
		ct = consumer.k.newClaimTxn(claim.Topic(), claim.Partition())
		defer ct.close()
	}

	if isBulkSubscribe {
		ticker := time.NewTicker(time.Duration(handlerConfig.SubscribeConfig.MaxAwaitDurationMs) * time.Millisecond)
		defer ticker.Stop()
		messages := make([]*sarama.ConsumerMessage, 0, handlerConfig.SubscribeConfig.MaxMessagesCount)
		for {
			select {
			case <-session.Context().Done():
				// Flushing after the session is cancelled is pre-existing
				// behaviour, and it stays safe under transactions because the
				// broker rejects what a revoked member tries to commit: the
				// offset carries this session's generation and member id, so
				// a commit from a generation the coordinator has moved past
				// comes back ILLEGAL_GENERATION / UNKNOWN_MEMBER_ID and the
				// delivery aborts. The buffered records are then redelivered
				// to whoever owns the partition, which is what should happen.
				//
				// The one cost is that this flush may create the claim's
				// producer for the first time, and creating it is an
				// InitProducerId on a transactional.id the new owner may
				// already hold — bumping the epoch under them. That costs the
				// new owner one aborted transaction and one redelivered
				// batch, which it recovers from on its next delivery. Not
				// worth skipping the flush over: in an ordinary rebalance
				// sarama waits for this call to return before the member
				// rejoins, so the new owner initializes last and it is this
				// producer that gets fenced, which is the intended direction.
				return consumer.flushBulkMessages(claim, messages, session, handlerConfig.BulkHandler, b, ct)
			case message := <-claim.Messages():
				consumer.mutex.Lock()
				if message != nil {
					messages = append(messages, message)
					if len(messages) >= handlerConfig.SubscribeConfig.MaxMessagesCount {
						_ = consumer.flushBulkMessages(claim, messages, session, handlerConfig.BulkHandler, b, ct) //nolint:errcheck // legacy behavior preserved
						messages = messages[:0]
						ticker.Reset(time.Duration(handlerConfig.SubscribeConfig.MaxAwaitDurationMs) * time.Millisecond)
					}
				}
				consumer.mutex.Unlock()
			case <-ticker.C:
				consumer.mutex.Lock()
				_ = consumer.flushBulkMessages(claim, messages, session, handlerConfig.BulkHandler, b, ct) //nolint:errcheck // legacy behavior preserved
				messages = messages[:0]
				consumer.mutex.Unlock()
			}
		}
	} else {
		for {
			select {
			// Should return when `session.Context()` is done.
			// If not, will raise `ErrRebalanceInProgress` or `read tcp <ip>:<port>: i/o timeout` when kafka rebalance. see:
			// https://github.com/IBM/sarama/issues/1192
			// Make sure the check for session context done happens before the next message is processed.
			// There is a possibility that the pod takes some time to shutdown and in case of a poison pill message, the `retry` would get interrupted (as expected),
			// but the next message would be processed as a result,
			// therefore dropping the poison pill message regardless of resiliency policy.
			case <-session.Context().Done():
				return nil
			case message, ok := <-claim.Messages():
				if !ok {
					return nil
				}

				if consumer.k.consumeRetryEnabled {
					if err := retry.NotifyRecover(func() error {
						return consumer.dispatchCallback(session, message, ct)
					}, b, func(err error, d time.Duration) {
						consumer.k.logger.Warnf("Error processing Kafka message: %s/%d/%d [key=%s]. Error: %v. Retrying...", message.Topic, message.Partition, message.Offset, asBase64String(message.Key), err)
					}, func() {
						consumer.k.logger.Infof("Successfully processed Kafka message after it previously failed: %s/%d/%d [key=%s]", message.Topic, message.Partition, message.Offset, asBase64String(message.Key))
					}); err != nil {
						// When the session context is canceled (graceful
						// shutdown / rebalance), the retry exits with the last
						// observed error rather than a real "exhausted"
						// outcome. The message is left unmarked and will be
						// redelivered to whichever consumer takes over the
						// partition, so this is not a processing failure —
						// just shutdown noise. Demote the log accordingly.
						if errors.Is(session.Context().Err(), context.Canceled) || errors.Is(err, context.Canceled) {
							consumer.k.logger.Debugf("Kafka message processing aborted due to shutdown; will be redelivered: %s/%d/%d [key=%s]", message.Topic, message.Partition, message.Offset, asBase64String(message.Key))
							return nil
						}
						consumer.k.logger.Errorf("Too many failed attempts at processing Kafka message: %s/%d/%d [key=%s]. Error: %v.", message.Topic, message.Partition, message.Offset, asBase64String(message.Key), err)
					}
				} else {
					err := consumer.dispatchCallback(session, message, ct)
					if err != nil {
						consumer.k.logger.Errorf("Error processing Kafka message: %s/%d/%d [key=%s]. Error: %v.", message.Topic, message.Partition, message.Offset, asBase64String(message.Key), err)
					}
				}
			}
		}
	}
}

func (consumer *consumer) flushBulkMessages(claim sarama.ConsumerGroupClaim,
	messages []*sarama.ConsumerMessage, session sarama.ConsumerGroupSession,
	handler BulkEventHandler, b backoff.BackOff, ct *claimTxn,
) error {
	if len(messages) > 0 {
		if consumer.k.consumeRetryEnabled {
			if err := retry.NotifyRecover(func() error {
				return consumer.dispatchBulkCallback(session, messages, handler, claim.Topic(), ct)
			}, b, func(err error, d time.Duration) {
				consumer.k.logger.Warnf("Error processing Kafka bulk messages: %s. Error: %v. Retrying...", claim.Topic(), err)
			}, func() {
				consumer.k.logger.Infof("Successfully processed Kafka message after it previously failed: %s", claim.Topic())
			}); err != nil {
				// Same shutdown-vs-exhausted distinction as the singular
				// path: demote when session ctx is canceled.
				if errors.Is(session.Context().Err(), context.Canceled) || errors.Is(err, context.Canceled) {
					consumer.k.logger.Debugf("Kafka bulk message processing aborted due to shutdown; will be redelivered: %s", claim.Topic())
				} else {
					consumer.k.logger.Errorf("Too many failed attempts at processing Kafka message: %s. Error: %v.", claim.Topic(), err)
				}
			}
		} else {
			err := consumer.dispatchBulkCallback(session, messages, handler, claim.Topic(), ct)
			if err != nil {
				consumer.k.logger.Errorf("Error processing Kafka message: %s. Error: %v.", claim.Topic(), err)
			}
			return err
		}
	}
	return nil
}

// dispatchCallback routes a delivery to the transactional or the plain
// callback, depending on whether this claim processes transactionally.
func (consumer *consumer) dispatchCallback(session sarama.ConsumerGroupSession, message *sarama.ConsumerMessage, ct *claimTxn) error {
	if ct != nil {
		return consumer.doCallbackTxn(session, message, ct)
	}
	return consumer.doCallback(session, message)
}

// dispatchBulkCallback is dispatchCallback for bulk deliveries.
func (consumer *consumer) dispatchBulkCallback(session sarama.ConsumerGroupSession,
	messages []*sarama.ConsumerMessage, handler BulkEventHandler, topic string, ct *claimTxn,
) error {
	if ct != nil {
		return consumer.doBulkCallbackTxn(session, messages, handler, topic, ct)
	}
	return consumer.doBulkCallback(session, messages, handler, topic)
}

func (consumer *consumer) doBulkCallback(session sarama.ConsumerGroupSession,
	messages []*sarama.ConsumerMessage, handler BulkEventHandler, topic string,
) error {
	consumer.k.logger.Debugf("Processing Kafka bulk message: %s", topic)
	messageValues := make([]KafkaBulkMessageEntry, len(messages))

	for i, message := range messages {
		if message != nil {
			metadata := GetEventMetadata(message, consumer.k)
			handlerConfig, err := consumer.k.GetTopicHandlerConfig(message.Topic)
			if err != nil {
				return err
			}
			messageVal, err := consumer.k.DeserializeValue(message, handlerConfig)
			if err != nil {
				return err
			}
			childMessage := KafkaBulkMessageEntry{
				EntryId:  strconv.Itoa(i),
				Event:    messageVal,
				Metadata: metadata,
			}
			messageValues[i] = childMessage
		}
	}
	event := KafkaBulkMessage{
		Topic:   topic,
		Entries: messageValues,
	}
	responses, err := handler(session.Context(), &event)

	if err != nil {
		for i, resp := range responses {
			// An extra check to confirm that runtime returned responses are in order
			if resp.EntryId != messageValues[i].EntryId {
				return errors.New("entry id mismatch while processing bulk messages")
			}
			if resp.Error != nil {
				break
			}
			session.MarkMessage(messages[i], "")
		}
	} else {
		for _, message := range messages {
			session.MarkMessage(message, "")
		}
	}
	return err
}

func (consumer *consumer) doCallback(session sarama.ConsumerGroupSession, message *sarama.ConsumerMessage) error {
	consumer.k.logger.Debugf("Processing Kafka message: %s/%d/%d [key=%s]", message.Topic, message.Partition, message.Offset, asBase64String(message.Key))
	handlerConfig, err := consumer.k.GetTopicHandlerConfig(message.Topic)
	if err != nil {
		return err
	}
	if !handlerConfig.IsBulkSubscribe && handlerConfig.Handler == nil {
		return errors.New("invalid handler config for subscribe call")
	}

	messageVal, err := consumer.k.DeserializeValue(message, handlerConfig)
	if err != nil {
		return err
	}
	event := NewEvent{
		Topic: message.Topic,
		Data:  messageVal,
	}
	event.Metadata = GetEventMetadata(message, consumer.k)

	err = handlerConfig.Handler(session.Context(), &event)
	if err == nil {
		session.MarkMessage(message, "")
	}
	return err
}

// doCallbackTxn processes one delivery inside a Kafka transaction on the
// claim's producer: publishes made by the handler that carry the delivery's
// transaction token join the transaction, and on success the consumer offset
// commits with them atomically. On any failure the transaction aborts and
// the existing retry/redelivery path takes over with a fresh transaction per
// attempt.
func (consumer *consumer) doCallbackTxn(session sarama.ConsumerGroupSession, message *sarama.ConsumerMessage, ct *claimTxn) error {
	k := consumer.k
	k.logger.Debugf("Processing Kafka message transactionally: %s/%d/%d [key=%s]", message.Topic, message.Partition, message.Offset, asBase64String(message.Key))
	handlerConfig, err := k.GetTopicHandlerConfig(message.Topic)
	if err != nil {
		return err
	}
	if !handlerConfig.IsBulkSubscribe && handlerConfig.Handler == nil {
		return errors.New("invalid handler config for subscribe call")
	}

	messageVal, err := k.DeserializeValue(message, handlerConfig)
	if err != nil {
		return err
	}

	producer, err := ct.getProducer()
	if err != nil {
		return err
	}
	if err := producer.BeginTxn(); err != nil {
		return k.endProducerTxnWithError(producer, fmt.Errorf("kafka: begin transaction: %w", err), ct.invalidate)
	}

	sess := &txnSession{producer: producer, open: true}
	token := k.registerTxnSession(sess)
	// Panic hygiene only: end/deregister run explicitly before the
	// transaction is ended below; both are idempotent.
	defer func() {
		sess.end()
		k.deregisterTxnSession(token)
	}()

	event := NewEvent{
		Topic: message.Topic,
		Data:  messageVal,
	}
	event.Metadata = GetEventMetadata(message, k)
	event.Metadata[txnTokenMetadataKey] = token

	handlerErr := handlerConfig.Handler(session.Context(), &event)

	// Close the token before ending the transaction so no late publish can
	// slip into the commit — or into the next transaction on this producer.
	sess.end()
	k.deregisterTxnSession(token)

	if handlerErr != nil {
		return k.endProducerTxnWithError(producer, handlerErr, ct.invalidate)
	}

	return consumer.commitTxnWithOffset(session, producer, message, ct, sess.hasSent())
}

// doBulkCallbackTxn processes a bulk delivery inside a Kafka transaction.
// The batch is all-or-nothing: a handler error or any per-entry error aborts
// the whole transaction and the batch is redelivered whole. Partial success
// cannot coexist with an atomic transaction.
func (consumer *consumer) doBulkCallbackTxn(session sarama.ConsumerGroupSession,
	messages []*sarama.ConsumerMessage, handler BulkEventHandler, topic string, ct *claimTxn,
) error {
	k := consumer.k
	k.logger.Debugf("Processing Kafka bulk message transactionally: %s", topic)

	messageValues := make([]KafkaBulkMessageEntry, len(messages))
	var lastMessage *sarama.ConsumerMessage
	for i, message := range messages {
		if message != nil {
			metadata := GetEventMetadata(message, k)
			handlerConfig, err := k.GetTopicHandlerConfig(message.Topic)
			if err != nil {
				return err
			}
			messageVal, err := k.DeserializeValue(message, handlerConfig)
			if err != nil {
				return err
			}
			messageValues[i] = KafkaBulkMessageEntry{
				EntryId:  strconv.Itoa(i),
				Event:    messageVal,
				Metadata: metadata,
			}
			lastMessage = message
		}
	}
	if lastMessage == nil {
		return nil
	}

	producer, err := ct.getProducer()
	if err != nil {
		return err
	}
	if err := producer.BeginTxn(); err != nil {
		return k.endProducerTxnWithError(producer, fmt.Errorf("kafka: begin transaction: %w", err), ct.invalidate)
	}

	sess := &txnSession{producer: producer, open: true}
	token := k.registerTxnSession(sess)
	// Panic hygiene only: end/deregister run explicitly before the
	// transaction is ended below; both are idempotent.
	defer func() {
		sess.end()
		k.deregisterTxnSession(token)
	}()

	event := KafkaBulkMessage{
		Topic:    topic,
		Entries:  messageValues,
		Metadata: map[string]string{txnTokenMetadataKey: token},
	}
	responses, handlerErr := handler(session.Context(), &event)

	sess.end()
	k.deregisterTxnSession(token)

	if handlerErr == nil {
		for _, resp := range responses {
			if resp.Error != nil {
				handlerErr = fmt.Errorf("kafka: bulk entry %s failed, aborting the batch transaction: %w", resp.EntryId, resp.Error)
				break
			}
		}
	}
	if handlerErr != nil {
		return k.endProducerTxnWithError(producer, handlerErr, ct.invalidate)
	}

	// A claim is a single partition, so the last message carries the batch's
	// highest offset.
	return consumer.commitTxnWithOffset(session, producer, lastMessage, ct, sess.hasSent())
}

// commitTxnWithOffset finishes a successful delivery. When the handler
// published into the transaction, the offset joins it (with the consumer
// group member metadata so the broker fences stale members, KIP-447) and
// both commit atomically. When nothing was published, sarama would silently
// skip the offset commit of a record-less transaction — and with no records
// there is nothing for the offset to be atomic with — so the empty
// transaction is ended and the offset commits synchronously instead.
//
// Neither path touches the session's offset manager, which stays unused in
// transactional mode. session.Commit() flushes every dirty partition of the
// session at once, so one partition's record-less commit would also write
// whatever the offset manager last held for the others — a value that lags
// the offsets the transactional path commits inside its transactions, which
// would redeliver messages whose output is already on the topic. Committing
// only this partition keeps the two paths from writing each other's
// offsets.
func (consumer *consumer) commitTxnWithOffset(session sarama.ConsumerGroupSession, producer sarama.SyncProducer, message *sarama.ConsumerMessage, ct *claimTxn, sent bool) error {
	k := consumer.k

	if !sent {
		if err := producer.CommitTxn(); err != nil {
			return k.endProducerTxnWithError(producer, fmt.Errorf("kafka: commit transaction: %w", err), ct.invalidate)
		}
		return consumer.commitOffset(session, producer, message)
	}

	groupMetadata := &sarama.ConsumerGroupMetadata{
		GroupID:      k.consumerGroup,
		GenerationID: session.GenerationID(),
		MemberID:     session.MemberID(),
	}
	if err := producer.AddMessageToTxnWithGroupMetadata(message, groupMetadata, nil); err != nil {
		return k.endProducerTxnWithError(producer, fmt.Errorf("kafka: add offsets to transaction: %w", err), ct.invalidate)
	}
	if err := producer.CommitTxn(); err != nil {
		// A commit that exhausted sarama's EndTxn retries leaves the producer
		// in CommittingTransaction with no answer from the broker: the
		// transaction may still be open, or already committing. Retrying the
		// delivery blind republishes its outputs if it committed, and epoch
		// fencing cannot take a committed transaction back — fencing aborts
		// open transactions only.
		ambiguous := producer.TxnStatus()&sarama.ProducerTxnFlagCommittingTransaction != 0
		cleanupErr := k.endProducerTxnWithError(producer, fmt.Errorf("kafka: commit transaction: %w", err), ct.invalidate)
		if !ambiguous || !consumer.resolveAmbiguousCommit(session, ct, message) {
			return cleanupErr
		}
		k.logger.Warnf("Kafka transaction commit returned %v but the broker had committed it; not reprocessing %s/%d/%d", err, message.Topic, message.Partition, message.Offset)
	}
	// The offset is already at the broker, committed inside the transaction.
	return nil
}

// commitOffset commits this partition's offset on its own, without going
// through the session's offset manager. sarama's session.Commit() flushes
// every dirty partition of the session in one request, which would let a
// record-less delivery on one partition write a stale offset for the others;
// a single-partition request cannot. The group generation and member id go
// with it so the broker fences a commit from a revoked member, the same
// fencing the transactional path gets from KIP-447.
func (consumer *consumer) commitOffset(session sarama.ConsumerGroupSession, producer sarama.SyncProducer, message *sarama.ConsumerMessage) error {
	k := consumer.k

	commit := k.offsetCommitter
	if commit == nil {
		commit = commitOffsetToCoordinator
	}
	if err := commit(producer, k.consumerGroup, session.GenerationID(), session.MemberID(), message); err != nil {
		return fmt.Errorf("kafka: commit offset for %s/%d/%d: %w", message.Topic, message.Partition, message.Offset, err)
	}
	return nil
}

// commitOffsetToCoordinator sends a single-partition OffsetCommit to the
// group coordinator over the client the producer already owns.
func commitOffsetToCoordinator(producer sarama.SyncProducer, group string, generation int32, memberID string, message *sarama.ConsumerMessage) error {
	owner, ok := producer.(clientOwner)
	if !ok {
		return errors.New("kafka: producer does not carry a client")
	}
	client := owner.Client()

	coordinator, err := client.Coordinator(group)
	if err != nil {
		return fmt.Errorf("locate group coordinator: %w", err)
	}

	req := &sarama.OffsetCommitRequest{
		// consumerTransactionsEnabled enforces Kafka >= 2.5, so the flexible
		// v8 request is always supported.
		Version:                 8,
		ConsumerGroup:           group,
		ConsumerGroupGeneration: generation,
		ConsumerID:              memberID,
	}
	// The committed offset is the next one to read, not the one just handled.
	req.AddBlock(message.Topic, message.Partition, message.Offset+1, 0, "")

	resp, err := coordinator.CommitOffset(req)
	if err != nil {
		return fmt.Errorf("commit offset: %w", err)
	}
	kerr, ok := resp.Errors[message.Topic][message.Partition]
	if !ok {
		return errors.New("commit offset: no result for the partition")
	}
	if kerr != sarama.ErrNoError {
		dropStaleCoordinator(client, group, kerr)
		return fmt.Errorf("commit offset: %w", kerr)
	}
	return nil
}

// How often resolveAmbiguousCommit re-asks the coordinator whether a pending
// transaction has become terminal. A commit already writing its markers
// finishes in milliseconds; the wait is bounded by the session, not by a
// retry count.
const ambiguousCommitFenceBackoff = 200 * time.Millisecond

// dropStaleCoordinator clears the client's cached group coordinator when the
// broker answers that it is not the coordinator any more. sarama looks the
// coordinator up once and caches it until something asks for a refresh, so
// without this every redelivery reuses the claim producer, hence the same
// client, hence the same migrated-away broker: the offset stops advancing
// and the handler re-runs on each attempt until the session dies.
func dropStaleCoordinator(client sarama.Client, group string, kerr sarama.KError) {
	switch kerr { //nolint:exhaustive // only the coordinator-moved errors matter
	case sarama.ErrNotCoordinatorForConsumer, sarama.ErrConsumerCoordinatorNotAvailable:
		_ = client.RefreshCoordinator(group)
	}
}

// clientOwner is implemented by the producers this component builds.
type clientOwner interface {
	Client() sarama.Client
}

// resolveAmbiguousCommit settles a transaction whose commit outcome is
// unknown and reports whether it committed.
//
// The order is load-bearing. Reading the committed offset straight away
// cannot answer the question: until the transaction is terminal the group
// coordinator still shows the pre-transaction offset whether it is about to
// commit or can still abort. Recreating the claim's producer first is what
// settles it — the new producer reuses the same transactional.id, so the
// coordinator aborts a transaction that was still open, and answers
// CONCURRENT_TRANSACTIONS (which sarama surfaces as a failure to build the
// producer) until one already committing becomes terminal. Once the producer
// exists, the offset the coordinator reports is final.
//
// It keeps trying until the session ends. Giving up early would buy nothing:
// the delivery would go back to the retry loop, which re-enters through the
// same getProducer and blocks on the same pending transaction — so the wait
// happens either way, and stopping here only throws away the offset read that
// tells a committed transaction from an aborted one. Session cancellation
// answers false, which reprocesses the delivery on whoever takes the
// partition over: a duplicate rather than a lost message.
func (consumer *consumer) resolveAmbiguousCommit(session sarama.ConsumerGroupSession, ct *claimTxn, message *sarama.ConsumerMessage) bool {
	k := consumer.k

	for attempt := 0; ; attempt++ {
		if attempt > 0 {
			select {
			case <-session.Context().Done():
				k.logger.Warnf("Session ended before the transaction of %s/%d/%d settled; the delivery will be reprocessed by the next owner", message.Topic, message.Partition, message.Offset)
				return false
			case <-time.After(ambiguousCommitFenceBackoff):
			}
		}
		producer, err := ct.getProducer()
		if err == nil {
			return consumer.commitLanded(producer, message)
		}
		k.logger.Debugf("Waiting for the transaction of %s/%d/%d to settle before reading the committed offset: %v", message.Topic, message.Partition, message.Offset, err)
	}
}

// commitLanded reports whether the broker committed the transaction whose
// CommitTxn returned an unknown outcome, by reading the consumer group's
// committed offset. A transactional commit writes message.Offset+1, so any
// committed offset past this message means the transaction landed.
//
// Every failure to find out answers false, which reprocesses the delivery —
// the pre-existing behaviour, and the safe side of the guess for a
// transaction that most likely did not commit.
func (consumer *consumer) commitLanded(producer sarama.SyncProducer, message *sarama.ConsumerMessage) bool {
	k := consumer.k

	fetch := k.committedOffsetFetcher
	if fetch == nil {
		fetch = fetchCommittedOffset
	}
	committed, err := fetch(producer, k.consumerGroup, message.Topic, message.Partition)
	if err != nil {
		k.logger.Warnf("Could not read the committed offset after an unknown-outcome transaction commit for %s/%d/%d, reprocessing the delivery: %v", message.Topic, message.Partition, message.Offset, err)
		return false
	}
	return committed > message.Offset
}

// fetchCommittedOffset asks the group coordinator for the consumer group's
// committed offset on one partition, over the client the producer already
// owns.
func fetchCommittedOffset(producer sarama.SyncProducer, group, topic string, partition int32) (int64, error) {
	owner, ok := producer.(clientOwner)
	if !ok {
		return 0, errors.New("kafka: producer does not carry a client")
	}
	client := owner.Client()

	coordinator, err := client.Coordinator(group)
	if err != nil {
		return 0, fmt.Errorf("kafka: locate group coordinator: %w", err)
	}
	// Deliberately without RequireStable. It would answer
	// ErrUnstableOffsetCommit whenever a transactional offset commit for this
	// partition has not materialized, and the group coordinator gives that
	// same answer whether the transaction is about to commit or is still open
	// and can yet abort — it only knows "pending" from "materialized", the
	// distinction lives in the transaction coordinator's log. Reading pending
	// as committed would skip a message that never got published.
	resp, err := coordinator.FetchOffset(sarama.NewOffsetFetchRequest(
		client.Config().Version, group, map[string][]int32{topic: {partition}},
	))
	if err != nil {
		return 0, fmt.Errorf("kafka: fetch committed offset: %w", err)
	}
	block := resp.GetBlock(topic, partition)
	if block == nil {
		return 0, errors.New("kafka: fetch committed offset: no block for the partition")
	}
	if block.Err != sarama.ErrNoError {
		dropStaleCoordinator(client, group, block.Err)
		return 0, fmt.Errorf("kafka: fetch committed offset: %w", block.Err)
	}
	return block.Offset, nil
}

func GetEventMetadata(message *sarama.ConsumerMessage, kafka *Kafka) map[string]string {
	if message != nil {
		metadata := make(map[string]string, len(message.Headers)+5)
		if message.Key != nil {
			if kafka.escapeHeaders {
				metadata[keyMetadataKey] = url.QueryEscape(string(message.Key))
			} else {
				metadata[keyMetadataKey] = string(message.Key)
			}
		}
		metadata[offsetMetadataKey] = strconv.FormatInt(message.Offset, 10)
		metadata[topicMetadataKey] = message.Topic
		metadata[timestampMetadataKey] = strconv.FormatInt(message.Timestamp.UnixMilli(), 10)
		metadata[partitionMetadataKey] = strconv.FormatInt(int64(message.Partition), 10)
		for _, header := range message.Headers {
			// skip headers that are excluded from metadata
			if kafka.excludeHeaderMetaRegex != nil && kafka.excludeHeaderMetaRegex.MatchString(string(header.Key)) {
				kafka.logger.Debugf("Skipping header %v that is excluded from metadata", string(header.Key))
				continue
			}
			if kafka.escapeHeaders {
				metadata[string(header.Key)] = url.QueryEscape(string(header.Value))
			} else {
				metadata[string(header.Key)] = string(header.Value)
			}
		}
		return metadata
	}
	return nil
}

func (consumer *consumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (consumer *consumer) Setup(sarama.ConsumerGroupSession) error {
	return nil
}

// checkBulkSubscribe checks if a bulk handler and config are correctly registered for provided topic
func (k *Kafka) checkBulkSubscribe(topic string) bool {
	if bulkHandlerConfig, ok := k.subscribeTopics[topic]; ok &&
		bulkHandlerConfig.IsBulkSubscribe &&
		bulkHandlerConfig.BulkHandler != nil && (bulkHandlerConfig.SubscribeConfig.MaxMessagesCount > 0) &&
		bulkHandlerConfig.SubscribeConfig.MaxAwaitDurationMs > 0 {
		return true
	}
	return false
}

// GetTopicBulkHandler returns the handlerConfig for a topic
func (k *Kafka) GetTopicHandlerConfig(topic string) (SubscriptionHandlerConfig, error) {
	handlerConfig, ok := k.subscribeTopics[topic]
	if ok && ((handlerConfig.IsBulkSubscribe && handlerConfig.BulkHandler != nil) ||
		(!handlerConfig.IsBulkSubscribe && handlerConfig.Handler != nil)) {
		return handlerConfig, nil
	}
	return SubscriptionHandlerConfig{},
		fmt.Errorf("any handler for messages of topic %s not found", topic)
}
