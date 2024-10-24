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

package redis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	rediscomponent "github.com/dapr/components-contrib/common/component/redis"
	contribMetadata "github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/kit/logger"
)

const (
	consumerID        = "consumerID"
	enableTLS         = "enableTLS"
	processingTimeout = "processingTimeout"
	redeliverInterval = "redeliverInterval"
	queueDepth        = "queueDepth"
	concurrency       = "concurrency"
	maxLenApprox      = "maxLenApprox"
)

// redisStreams handles consuming from a Redis stream using
// `XREADGROUP` for reading new messages and `XPENDING` and
// `XCLAIM` for redelivering messages that previously failed.
//
// See https://redis.io/topics/streams-intro for more information
// on the mechanics of Redis Streams.
type redisStreams struct {
	client         rediscomponent.RedisClient
	clientSettings *rediscomponent.Settings
	logger         logger.Logger
	wg             sync.WaitGroup
	closed         atomic.Bool
	closeCh        chan struct{}

	queue chan redisMessageWrapper
}

// redisMessageWrapper encapsulates the message identifier,
// pubsub message, and handler to send to the queue channel for processing.
type redisMessageWrapper struct {
	ctx       context.Context
	messageID string
	message   pubsub.NewMessage
	handler   pubsub.Handler
}

// NewRedisStreams returns a new redis streams pub-sub implementation.
func NewRedisStreams(logger logger.Logger) pubsub.PubSub {
	return &redisStreams{
		logger:  logger,
		closeCh: make(chan struct{}),
	}
}

func (r *redisStreams) Init(ctx context.Context, metadata pubsub.Metadata) error {
	var err error
	r.client, r.clientSettings, err = rediscomponent.ParseClientFromProperties(metadata.Properties, contribMetadata.PubSubType, ctx, &r.logger)
	if err != nil {
		return err
	}

	if _, err = r.client.PingResult(ctx); err != nil {
		return fmt.Errorf("redis streams: error connecting to redis at %s: %s", r.clientSettings.Host, err)
	}
	r.queue = make(chan redisMessageWrapper, int(r.clientSettings.QueueDepth)) //nolint:gosec

	for range r.clientSettings.Concurrency {
		r.wg.Add(1)
		go func() {
			defer r.wg.Done()
			r.worker()
		}()
	}

	return nil
}

func (r *redisStreams) Publish(ctx context.Context, req *pubsub.PublishRequest) error {
	if r.closed.Load() {
		return errors.New("component is closed")
	}

	redisPayload := map[string]interface{}{"data": req.Data}

	if req.Metadata != nil {
		serializedMetadata, err := json.Marshal(req.Metadata)
		if err != nil {
			return err
		}
		redisPayload["metadata"] = serializedMetadata
	}

	_, err := r.client.XAdd(ctx, req.Topic, r.clientSettings.MaxLenApprox, redisPayload)
	if err != nil {
		return fmt.Errorf("redis streams: error from publish: %s", err)
	}

	return nil
}

func (r *redisStreams) CreateConsumerGroup(ctx context.Context, stream string) error {
	err := r.client.XGroupCreateMkStream(ctx, stream, r.clientSettings.ConsumerID, "0")
	// Ignore BUSYGROUP errors
	if err != nil && err.Error() != "BUSYGROUP Consumer Group name already exists" {
		r.logger.Errorf("redis streams: %s", err)
		return err
	}
	return nil
}

func (r *redisStreams) Subscribe(ctx context.Context, req pubsub.SubscribeRequest, handler pubsub.Handler) error {
	if r.closed.Load() {
		return errors.New("component is closed")
	}

	if err := r.CreateConsumerGroup(ctx, req.Topic); err != nil {
		return err
	}

	loopCtx, cancel := context.WithCancel(ctx)
	r.wg.Add(3)
	go func() {
		// Add a context which catches the close signal to account for situations
		// where Close is called, but the context is not cancelled.
		defer r.wg.Done()
		defer cancel()
		select {
		case <-loopCtx.Done():
		case <-r.closeCh:
		}
	}()
	go func() {
		defer r.wg.Done()
		r.pollNewMessagesLoop(loopCtx, req.Topic, handler)
	}()
	go func() {
		defer r.wg.Done()
		r.reclaimPendingMessagesLoop(loopCtx, req.Topic, handler)
	}()

	return nil
}

// enqueueMessages is a shared function that funnels new messages (via polling)
// and redelivered messages (via reclaiming) to a channel where workers can
// pick them up for processing.
func (r *redisStreams) enqueueMessages(ctx context.Context, stream string, handler pubsub.Handler, msgs []rediscomponent.RedisXMessage) {
	for _, msg := range msgs {
		rmsg := r.createRedisMessageWrapper(ctx, stream, handler, msg)

		select {
		// Might block if the queue is full so we need the ctx.Done below.
		case r.queue <- rmsg:
			// Noop
		// Handle cancelation
		case <-ctx.Done():
			return
		}
	}
}

// createRedisMessageWrapper encapsulates the Redis message, message identifier, and handler
// in `redisMessage` for processing.
func (r *redisStreams) createRedisMessageWrapper(ctx context.Context, stream string, handler pubsub.Handler, msg rediscomponent.RedisXMessage) redisMessageWrapper {
	var data []byte
	if dataValue, exists := msg.Values["data"]; exists && dataValue != nil {
		switch v := dataValue.(type) {
		case string:
			data = []byte(v)
		case []byte:
			data = v
		}
	}

	var metadata map[string]string
	if metadataValue, exists := msg.Values["metadata"]; exists && metadataValue != nil {
		metadataStr := metadataValue.(string)
		err := json.Unmarshal([]byte(metadataStr), &metadata)
		if err != nil {
			r.logger.Warnf("Redis PubSub: Could not extract metadata for Redis message %s: %v", msg.ID, err)
		}
	}

	return redisMessageWrapper{
		ctx: ctx,
		message: pubsub.NewMessage{
			Topic:    stream,
			Data:     data,
			Metadata: metadata,
		},
		messageID: msg.ID,
		handler:   handler,
	}
}

// worker runs in separate goroutine(s) and pull messages from a channel for processing.
// The number of workers is controlled by the `concurrency` setting.
func (r *redisStreams) worker() {
	for {
		select {
		// Handle closing
		case <-r.closeCh:
			return

		case msg := <-r.queue:
			r.processMessage(msg)
		}
	}
}

// processMessage attempts to process a single Redis message by invoking
// its handler. If the message processed successfully, then it is Ack'ed.
// Otherwise, it remains in the pending list and will be redelivered
// by `reclaimPendingMessagesLoop`.
func (r *redisStreams) processMessage(msg redisMessageWrapper) error {
	r.logger.Debugf("Processing Redis message %s", msg.messageID)
	ctx := msg.ctx
	var cancel context.CancelFunc
	if r.clientSettings.ProcessingTimeout != 0 && r.clientSettings.RedeliverInterval != 0 {
		ctx, cancel = context.WithTimeout(ctx, r.clientSettings.ProcessingTimeout)
		defer cancel()
	}
	if err := msg.handler(ctx, &msg.message); err != nil {
		r.logger.Errorf("Error processing Redis message %s: %v", msg.messageID, err)

		return err
	}

	// Use the background context in case subscriptionCtx is already closed.
	if err := r.client.XAck(context.Background(), msg.message.Topic, r.clientSettings.ConsumerID, msg.messageID); err != nil {
		r.logger.Errorf("Error acknowledging Redis message %s: %v", msg.messageID, err)

		return err
	}

	return nil
}

// pollMessagesLoop calls `XReadGroup` for new messages and funnels them to the message channel
// by calling `enqueueMessages`.
func (r *redisStreams) pollNewMessagesLoop(ctx context.Context, stream string, handler pubsub.Handler) {
	for {
		// Return on cancelation
		if ctx.Err() != nil {
			return
		}

		// Read messages
		//nolint:gosec
		streams, err := r.client.XReadGroupResult(ctx, r.clientSettings.ConsumerID, r.clientSettings.ConsumerID, []string{stream, ">"}, int64(r.clientSettings.QueueDepth), time.Duration(r.clientSettings.ReadTimeout))
		if err != nil {
			if !errors.Is(err, r.client.GetNilValueError()) && err != context.Canceled {
				if strings.Contains(err.Error(), "NOGROUP") {
					r.logger.Warnf("redis streams: consumer group %s does not exist for stream %s. This could mean the server experienced data loss, or the group/stream was deleted.", r.clientSettings.ConsumerID, stream)
					r.logger.Warnf("redis streams: recreating group %s for stream %s", r.clientSettings.ConsumerID, stream)
					r.CreateConsumerGroup(ctx, stream)
				}
				r.logger.Errorf("redis streams: error reading from stream %s: %s", stream, err)
			}
			continue
		}

		// Enqueue messages for the returned streams
		for _, s := range streams {
			r.enqueueMessages(ctx, s.Stream, handler, s.Messages)
		}
	}
}

// reclaimPendingMessagesLoop periodically reclaims pending messages
// based on the `redeliverInterval` setting.
func (r *redisStreams) reclaimPendingMessagesLoop(ctx context.Context, stream string, handler pubsub.Handler) {
	// Having a `processingTimeout` or `redeliverInterval` means that
	// redelivery is disabled so we just return out of the goroutine.
	if r.clientSettings.ProcessingTimeout == 0 || r.clientSettings.RedeliverInterval == 0 {
		return
	}

	// Do an initial reclaim call
	r.reclaimPendingMessages(ctx, stream, handler)

	reclaimTicker := time.NewTicker(r.clientSettings.RedeliverInterval)

	for {
		select {
		case <-ctx.Done():
			return

		case <-reclaimTicker.C:
			r.reclaimPendingMessages(ctx, stream, handler)
		}
	}
}

// reclaimPendingMessages handles reclaiming messages that previously failed to process and
// funneling them to the message channel by calling `enqueueMessages`.
func (r *redisStreams) reclaimPendingMessages(ctx context.Context, stream string, handler pubsub.Handler) {
	for {
		// Retrieve pending messages for this stream and consumer
		pendingResult, err := r.client.XPendingExtResult(ctx,
			stream,
			r.clientSettings.ConsumerID,
			"-",
			"+",
			int64(r.clientSettings.QueueDepth), //nolint:gosec
		)
		if err != nil && !errors.Is(err, r.client.GetNilValueError()) {
			r.logger.Errorf("error retrieving pending Redis messages: %v", err)

			break
		}

		// Filter out messages that have not timed out yet
		msgIDs := make([]string, 0, len(pendingResult))
		for _, msg := range pendingResult {
			if msg.Idle >= r.clientSettings.ProcessingTimeout {
				msgIDs = append(msgIDs, msg.ID)
			}
		}

		// Nothing to claim
		if len(msgIDs) == 0 {
			break
		}

		// Attempt to claim the messages for the filtered IDs
		claimResult, err := r.client.XClaimResult(ctx,
			stream,
			r.clientSettings.ConsumerID,
			r.clientSettings.ConsumerID,
			r.clientSettings.ProcessingTimeout,
			msgIDs,
		)
		if err != nil && !errors.Is(err, r.client.GetNilValueError()) {
			r.logger.Errorf("error claiming pending Redis messages: %v", err)

			break
		}

		// Enqueue claimed messages
		r.enqueueMessages(ctx, stream, handler, claimResult)

		// If the Redis nil error is returned, it means somes message in the pending
		// state no longer exist. We need to acknowledge these messages to
		// remove them from the pending list.
		if errors.Is(err, r.client.GetNilValueError()) {
			// Build a set of message IDs that were not returned
			// that potentially no longer exist.
			expectedMsgIDs := make(map[string]struct{}, len(msgIDs))
			for _, id := range msgIDs {
				expectedMsgIDs[id] = struct{}{}
			}
			for _, claimed := range claimResult {
				delete(expectedMsgIDs, claimed.ID)
			}

			r.removeMessagesThatNoLongerExistFromPending(ctx, stream, expectedMsgIDs, handler)
		}
	}
}

// removeMessagesThatNoLongerExistFromPending attempts to claim messages individually so that messages in the pending list
// that no longer exist can be removed from the pending list. This is done by calling `XACK`.
func (r *redisStreams) removeMessagesThatNoLongerExistFromPending(ctx context.Context, stream string, messageIDs map[string]struct{}, handler pubsub.Handler) {
	// Check each message ID individually.
	for pendingID := range messageIDs {
		claimResultSingleMsg, err := r.client.XClaimResult(ctx,
			stream,
			r.clientSettings.ConsumerID,
			r.clientSettings.ConsumerID,
			0,
			[]string{pendingID},
		)
		if err != nil && !errors.Is(err, r.client.GetNilValueError()) {
			r.logger.Errorf("error claiming pending Redis message %s: %v", pendingID, err)

			continue
		}

		// Ack the message to remove it from the pending list.
		if errors.Is(err, r.client.GetNilValueError()) {
			// Use the background context in case subscriptionCtx is already closed.
			if err = r.client.XAck(context.Background(), stream, r.clientSettings.ConsumerID, pendingID); err != nil {
				r.logger.Errorf("error acknowledging Redis message %s after failed claim for %s: %v", pendingID, stream, err)
			}
		} else {
			// This should not happen but if it does the message should be processed.
			r.enqueueMessages(ctx, stream, handler, claimResultSingleMsg)
		}
	}
}

func (r *redisStreams) Close() error {
	defer r.wg.Wait()
	if r.closed.CompareAndSwap(false, true) {
		close(r.closeCh)
	}

	if r.client == nil {
		return nil
	}
	return r.client.Close()
}

func (r *redisStreams) Features() []pubsub.Feature {
	return nil
}

func (r *redisStreams) Ping(ctx context.Context) error {
	if _, err := r.client.PingResult(ctx); err != nil {
		return fmt.Errorf("redis pubsub: error connecting to redis at %s: %s", r.clientSettings.Host, err)
	}

	return nil
}

func (r *redisStreams) GetComponentMetadata() (metadataInfo contribMetadata.MetadataMap) {
	metadataStruct := rediscomponent.Settings{}
	contribMetadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, contribMetadata.PubSubType)
	return
}
