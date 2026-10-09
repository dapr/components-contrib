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

// Package daprmq is the shared implementation of the DaprMQ pub/sub components. An [Entity] maps a
// Dapr topic to DaprMQ: where a publish goes, and which queue a subscriber consumes. The subscriber
// polls that queue with locked dequeues, renewing each lock until the message is settled: a message
// the handler accepts is acknowledged, one it fails is nacked for redelivery.
package daprmq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cenkalti/backoff/v4"
	mq "github.com/olitomlinson/dapr-mq/sdks/go"
	"go.uber.org/ratelimit"

	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/kit/logger"
	"github.com/dapr/kit/retry"
)

const (
	priorityKey   = "priority"
	settleTimeout = 30 * time.Second
	// The most lock renewals in flight at once.
	maxConcurrentRenewals = 20
)

var errClosed = errors.New("component is closed")

// Entity is the DaprMQ entity a Dapr topic maps to.
type Entity interface {
	// Publish adds items to the entity for the topic.
	Publish(ctx context.Context, client *mq.Client, topic string, items []mq.EnqueueItem) error
	// SubscriberQueue returns the queue the consumer ID consumes the topic from.
	SubscriberQueue(ctx context.Context, client *mq.Client, consumerID, topic string) (string, error)
	// RequiresConsumerID reports whether the consumer ID selects what a subscriber receives.
	RequiresConsumerID() bool
}

// PubSub is a DaprMQ pub/sub component for an [Entity]. The component types embed it.
type PubSub struct {
	entity  Entity
	md      Metadata
	client  *mq.Client
	logger  logger.Logger
	closed  atomic.Bool
	closeCh chan struct{}
	wg      sync.WaitGroup

	// openStream opens a Consume stream in stream mode; tests replace it with a fake.
	openStream func(ctx context.Context, queueID string, options *mq.ConsumeOptions) (queueStream, error)
	// retryLimiter paces failed messages back to the queue, as Service Bus does.
	retryLimiter ratelimit.Limiter
}

// New returns a DaprMQ pub/sub component for the entity.
func New(logger logger.Logger, entity Entity) *PubSub {
	return &PubSub{entity: entity, logger: logger, closeCh: make(chan struct{})}
}

func (d *PubSub) Init(_ context.Context, meta pubsub.Metadata) error {
	md, err := ParseMetadata(meta.Properties, d.entity.RequiresConsumerID())
	if err != nil {
		return fmt.Errorf("daprmq: %w", err)
	}
	client, err := mq.NewClient(md.HTTPEndpoint, md.GRPCEndpoint, nil)
	if err != nil {
		return err
	}
	d.md, d.client = md, client
	d.openStream = func(ctx context.Context, queueID string, options *mq.ConsumeOptions) (queueStream, error) {
		stream, err := client.Consume(ctx, queueID, options)
		if err != nil {
			return nil, err
		}
		return sdkStream{stream}, nil
	}
	d.retryLimiter = ratelimit.NewUnlimited()
	if md.MaxRetriableErrorsPerSec > 0 {
		d.retryLimiter = ratelimit.New(md.MaxRetriableErrorsPerSec)
	}
	return nil
}

func (d *PubSub) Features() []pubsub.Feature {
	return []pubsub.Feature{pubsub.FeatureBulkPublish}
}

func (d *PubSub) Publish(ctx context.Context, req *pubsub.PublishRequest) error {
	if d.closed.Load() {
		return errClosed
	}
	item, err := enqueueItem(req.Data, req.ContentType, req.Metadata)
	if err != nil {
		return err
	}
	return d.entity.Publish(ctx, d.client, req.Topic, []mq.EnqueueItem{item})
}

// BulkPublish publishes every entry in one call, so the entries succeed or fail together.
func (d *PubSub) BulkPublish(ctx context.Context, req *pubsub.BulkPublishRequest) (pubsub.BulkPublishResponse, error) {
	if d.closed.Load() {
		return pubsub.NewBulkPublishResponse(req.Entries, errClosed), errClosed
	}
	items := make([]mq.EnqueueItem, len(req.Entries))
	for i, entry := range req.Entries {
		var contentType *string
		if entry.ContentType != "" {
			contentType = &entry.ContentType
		}
		item, err := enqueueItem(entry.Event, contentType, entry.Metadata)
		if err != nil {
			return pubsub.NewBulkPublishResponse(req.Entries, err), err
		}
		items[i] = item
	}
	if err := d.entity.Publish(ctx, d.client, req.Topic, items); err != nil {
		return pubsub.NewBulkPublishResponse(req.Entries, err), err
	}
	return pubsub.BulkPublishResponse{}, nil
}

func (d *PubSub) Subscribe(ctx context.Context, req pubsub.SubscribeRequest, handler pubsub.Handler) error {
	if d.closed.Load() {
		return errClosed
	}

	queueID, err := d.entity.SubscriberQueue(ctx, d.client, d.md.ConsumerID, req.Topic)
	if err != nil {
		return err
	}

	subscribeCtx, cancel := context.WithCancel(ctx)
	d.wg.Add(2)
	go func() {
		defer d.wg.Done()
		defer cancel()
		select {
		case <-subscribeCtx.Done():
		case <-d.closeCh:
		}
	}()
	go func() {
		defer d.wg.Done()
		defer cancel()
		if d.md.ReceiveMode == ReceiveModePoll {
			d.poll(subscribeCtx, req.Topic, queueID, handler)
		} else {
			d.consume(subscribeCtx, req.Topic, queueID, handler)
		}
	}()
	return nil
}

// newBackOff paces reconnects and failed dequeues: from 0.5s, growing to 30s, never giving up.
func newBackOff() backoff.BackOff {
	config := retry.Config{
		Policy:              retry.PolicyExponential,
		InitialInterval:     500 * time.Millisecond,
		RandomizationFactor: 0.5,
		Multiplier:          1.5,
		MaxInterval:         30 * time.Second,
		MaxRetries:          -1,
	}
	return config.NewBackOff()
}

func (d *PubSub) poll(ctx context.Context, topic, queueID string, handler pubsub.Handler) {
	bo := newBackOff()
	options := &mq.DequeueLockedOptions{
		Count:                   d.md.DequeueCount,
		TTL:                     d.md.LockTTL,
		AllowCompetingConsumers: d.md.AllowCompetingConsumers,
	}

	// Renewal outlives the subscription's context until this loop returns, so the locks of messages
	// still being handled during shutdown stay held until they're settled.
	locks := newActiveLocks()
	if d.md.LockRenewalInterval > 0 {
		renewCtx, stopRenewing := context.WithCancel(context.WithoutCancel(ctx))
		defer stopRenewing()
		d.wg.Add(1)
		go func() {
			defer d.wg.Done()
			d.renewLocks(renewCtx, queueID, locks)
		}()
	}

	for ctx.Err() == nil {
		result, err := d.client.DequeueLocked(ctx, queueID, options)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			d.logger.Warnf("daprmq: dequeue from %s failed: %v", queueID, err)
			wait(ctx, bo.NextBackOff())
			continue
		}
		bo.Reset()

		if len(result.Items) == 0 {
			wait(ctx, d.md.PollInterval)
			continue
		}
		// Every lock is renewed from the moment it's taken, including while it waits for a handler.
		for _, item := range result.Items {
			locks.add(item.LockID, item.LockExpiresAt)
		}
		d.handleBatch(ctx, topic, queueID, result.Items, handler, locks)
	}
}

func (d *PubSub) handleBatch(ctx context.Context, topic, queueID string, items []mq.DequeueLockedItem, handler pubsub.Handler, locks *activeLocks) {
	if d.md.ConcurrencyMode == pubsub.Single {
		for _, item := range items {
			d.handle(ctx, topic, queueID, item, handler, locks)
		}
		return
	}

	limit := d.md.MaxConcurrentHandlers
	if limit == 0 {
		limit = len(items)
	}
	sem := make(chan struct{}, limit)
	var batch sync.WaitGroup
	for _, item := range items {
		sem <- struct{}{}
		batch.Add(1)
		go func() {
			defer batch.Done()
			defer func() { <-sem }()
			d.handle(ctx, topic, queueID, item, handler, locks)
		}()
	}
	batch.Wait()
}

// handle delivers one item and settles its lock. Settling outlives the subscription's context, so
// a message handled during shutdown is still acknowledged; one not yet handled is nacked.
func (d *PubSub) handle(ctx context.Context, topic, queueID string, item mq.DequeueLockedItem, handler pubsub.Handler, locks *activeLocks) {
	settleCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), settleTimeout)
	defer cancel()

	msg, err := unwrap(item.Item, topic)
	if err != nil {
		locks.remove(item.LockID)
		d.logger.Errorf("daprmq: dead-lettering undecodable message from %s: %v", queueID, err)
		if dlErr := d.client.DeadLetter(settleCtx, queueID, item.LockID, nil); dlErr != nil {
			d.logger.Warnf("daprmq: dead-letter %s failed: %v", item.LockID, dlErr)
		}
		return
	}

	handlerErr := ctx.Err()
	if handlerErr == nil {
		handlerErr = handler(ctx, msg)
	}

	// Stop renewing before settling, so a renewal can't race the settle and report a lost lock.
	locks.remove(item.LockID)
	if handlerErr == nil {
		err = d.client.Acknowledge(settleCtx, queueID, item.LockID, nil)
	} else {
		d.paceRetry(ctx)
		_, err = d.client.Nack(settleCtx, queueID, item.LockID, nil)
	}
	if err != nil {
		d.logger.Warnf("daprmq: settling %s failed: %v", item.LockID, err)
	}
}

// renewLocks extends every active lock each interval until ctx ends, back up to about lockTTL
// ahead of now (see renewalExtension).
func (d *PubSub) renewLocks(ctx context.Context, queueID string, locks *activeLocks) {
	t := time.NewTicker(d.md.LockRenewalInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			d.renewOnce(ctx, queueID, locks)
		}
	}
}

func (d *PubSub) renewOnce(ctx context.Context, queueID string, locks *activeLocks) {
	expiries := locks.snapshot()
	if len(expiries) == 0 {
		return
	}

	var (
		mu    sync.Mutex
		errs  []error
		wg    sync.WaitGroup
		limit = make(chan struct{}, maxConcurrentRenewals)
	)
	for lockID, expiresAt := range expiries {
		limit <- struct{}{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-limit }()
			// Skip a lock settled since the snapshot.
			if !locks.has(lockID) {
				return
			}

			callCtx, cancel := context.WithTimeout(ctx, settleTimeout)
			defer cancel()
			extension := renewalExtension(time.Now(), expiresAt, d.md.LockTTL)
			err := d.client.ExtendLock(callCtx, queueID, lockID, extension, nil)
			if err == nil {
				locks.extended(lockID, expiresAt.Add(extension))
				return
			}
			if !locks.has(lockID) {
				return
			}
			var mqErr *mq.Error
			if errors.As(err, &mqErr) && (mqErr.Code == mq.CodeLockNotFound || mqErr.Code == mq.CodeLockExpired) {
				// The lock is lost: the message will be redelivered, so renewing it again is pointless.
				locks.remove(lockID)
			}
			mu.Lock()
			errs = append(errs, fmt.Errorf("lock %s: %w", lockID, err))
			mu.Unlock()
		}()
	}
	wg.Wait()

	if len(errs) > 0 {
		d.logger.Warnf("daprmq: renewing locks on %s failed for %d/%d: %v", queueID, len(errs), len(expiries), errors.Join(errs...))
	}
}

// renewalExtension is how far to extend a lock expiring at expiresAt so it again expires about
// ttl from now. ExtendLock adds to the current expiry, so extending by a fixed interval would let
// a lock fall behind whenever a tick runs late; catching up to now + ttl can't drift. The server
// takes whole seconds, so the result is rounded, and never under one second.
func renewalExtension(now, expiresAt time.Time, ttl time.Duration) time.Duration {
	return max(time.Second, now.Add(ttl).Sub(expiresAt).Round(time.Second))
}

func (d *PubSub) Close() error {
	if d.closed.CompareAndSwap(false, true) {
		close(d.closeCh)
	}
	d.wg.Wait()
	if d.client == nil {
		return nil
	}
	return d.client.Close()
}

// envelope carries a message as a DaprMQ item, which must be JSON: a JSON payload (such as a
// CloudEvent) is kept as JSON in Data, anything else is base64-encoded in DataBase64.
type envelope struct {
	Data        json.RawMessage   `json:"data,omitempty"`
	DataBase64  []byte            `json:"dataBase64,omitempty"`
	ContentType string            `json:"contentType,omitempty"`
	Metadata    map[string]string `json:"metadata,omitempty"`
}

func wrap(data []byte, contentType *string, md map[string]string) envelope {
	e := envelope{Metadata: md}
	if contentType != nil {
		e.ContentType = *contentType
	}
	if json.Valid(data) {
		e.Data = data
	} else {
		e.DataBase64 = data
	}
	return e
}

func unwrap(raw json.RawMessage, topic string) (*pubsub.NewMessage, error) {
	var e envelope
	if err := json.Unmarshal(raw, &e); err != nil {
		return nil, err
	}
	msg := &pubsub.NewMessage{Topic: topic, Data: e.DataBase64, Metadata: e.Metadata}
	if e.Data != nil {
		msg.Data = e.Data
	}
	if e.ContentType != "" {
		msg.ContentType = &e.ContentType
	}
	return msg, nil
}

func enqueueItem(data []byte, contentType *string, md map[string]string) (mq.EnqueueItem, error) {
	item := mq.EnqueueItem{Item: wrap(data, contentType, md)}
	if p, ok := md[priorityKey]; ok {
		priority, err := strconv.Atoi(p)
		if err != nil || priority < 0 {
			return item, fmt.Errorf("daprmq: invalid %s metadata %q: must be a non-negative integer", priorityKey, p)
		}
		item.Priority = &priority
	}
	return item, nil
}

func wait(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}

// activeLocks is the set of locks taken from a subscriber's queue and not yet settled, with the
// expiry each was last given.
type activeLocks struct {
	mu  sync.Mutex
	ids map[string]time.Time
}

func newActiveLocks() *activeLocks {
	return &activeLocks{ids: map[string]time.Time{}}
}

func (a *activeLocks) add(lockID string, expiresAt time.Time) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.ids[lockID] = expiresAt
}

// extended records a lock's new expiry, unless it was settled meanwhile.
func (a *activeLocks) extended(lockID string, expiresAt time.Time) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if _, ok := a.ids[lockID]; ok {
		a.ids[lockID] = expiresAt
	}
}

func (a *activeLocks) remove(lockID string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	delete(a.ids, lockID)
}

func (a *activeLocks) has(lockID string) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	_, ok := a.ids[lockID]
	return ok
}

func (a *activeLocks) snapshot() map[string]time.Time {
	a.mu.Lock()
	defer a.mu.Unlock()
	expiries := make(map[string]time.Time, len(a.ids))
	for id, expiresAt := range a.ids {
		expiries[id] = expiresAt
	}
	return expiries
}

// paceRetry takes a token before a failed message goes back to the queue, so a message that keeps
// failing is redelivered at most maxRetriableErrorsPerSec times a second. Messages returned only
// because the subscription is ending are not paced.
func (d *PubSub) paceRetry(ctx context.Context) {
	if ctx.Err() == nil {
		d.retryLimiter.Take()
	}
}
