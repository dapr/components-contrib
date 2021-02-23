// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package redis

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/go-redis/redis/v7"

	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/dapr/pkg/logger"
)

const (
	host              = "redisHost"
	password          = "redisPassword"
	consumerID        = "consumerID"
	enableTLS         = "enableTLS"
	processingTimeout = "processingTimeout"
	redeliverInterval = "redeliverInterval"
	queueDepth        = "queueDepth"
	concurrency       = "concurrency"
)

// redisStreams handles consuming from a Redis stream using
// `XREADGROUP` for reading new messages and `XPENDING` and
// `XCLAIM` for redelivering messages that previously failed.
//
// See https://redis.io/topics/streams-intro for more information
// on the mechanics of Redis Streams.
type redisStreams struct {
	metadata metadata
	client   *redis.Client

	logger logger.Logger

	queue chan redisMessageWrapper

	ctx    context.Context
	cancel context.CancelFunc
}

// redisMessageWrapper encapsulates the message identifier,
// pubsub message, and handler to send to the queue channel for processing.
type redisMessageWrapper struct {
	messageID string
	message   pubsub.NewMessage
	handler   func(msg *pubsub.NewMessage) error
}

// NewRedisStreams returns a new redis streams pub-sub implementation
func NewRedisStreams(logger logger.Logger) pubsub.PubSub {
	return &redisStreams{logger: logger}
}

func parseRedisMetadata(meta pubsub.Metadata) (metadata, error) {
	// Default values
	m := metadata{
		processingTimeout: 60 * time.Second,
		redeliverInterval: 15 * time.Second,
		queueDepth:        100,
		concurrency:       10,
	}
	if val, ok := meta.Properties[host]; ok && val != "" {
		m.host = val
	} else {
		return m, errors.New("redis streams error: missing host address")
	}

	if val, ok := meta.Properties[password]; ok && val != "" {
		m.password = val
	}

	if val, ok := meta.Properties[enableTLS]; ok && val != "" {
		tls, err := strconv.ParseBool(val)
		if err != nil {
			return m, fmt.Errorf("redis streams error: can't parse enableTLS field: %s", err)
		}
		m.enableTLS = tls
	}

	if val, ok := meta.Properties[consumerID]; ok && val != "" {
		m.consumerID = val
	} else {
		return m, errors.New("redis streams error: missing consumerID")
	}

	if val, ok := meta.Properties[processingTimeout]; ok && val != "" {
		if processingTimeoutMs, err := strconv.ParseUint(val, 10, 64); err == nil {
			m.processingTimeout = time.Duration(processingTimeoutMs) * time.Millisecond
		} else if d, err := time.ParseDuration(val); err == nil {
			m.processingTimeout = d
		} else {
			return m, fmt.Errorf("redis streams error: can't parse processingTimeout field: %s", err)
		}
	}

	if val, ok := meta.Properties[redeliverInterval]; ok && val != "" {
		if redeliverIntervalMs, err := strconv.ParseUint(val, 10, 64); err == nil {
			m.redeliverInterval = time.Duration(redeliverIntervalMs) * time.Millisecond
		} else if d, err := time.ParseDuration(val); err == nil {
			m.redeliverInterval = d
		} else {
			return m, fmt.Errorf("redis streams error: can't parse processingTimeout field: %s", err)
		}
	}

	if val, ok := meta.Properties[queueDepth]; ok && val != "" {
		queueDepth, err := strconv.ParseUint(val, 10, 64)
		if err != nil {
			return m, fmt.Errorf("redis streams error: can't parse queueDepth field: %s", err)
		}
		m.queueDepth = uint(queueDepth)
	}

	if val, ok := meta.Properties[concurrency]; ok && val != "" {
		concurrency, err := strconv.ParseUint(val, 10, 64)
		if err != nil {
			return m, fmt.Errorf("redis streams error: can't parse concurrency field: %s", err)
		}
		m.concurrency = uint(concurrency)
	}

	return m, nil
}

func (r *redisStreams) Init(metadata pubsub.Metadata) error {
	m, err := parseRedisMetadata(metadata)
	if err != nil {
		return err
	}
	r.metadata = m

	options := &redis.Options{
		Addr:            m.host,
		Password:        m.password,
		DB:              0,
		MaxRetries:      3,
		MaxRetryBackoff: time.Second * 2,
	}

	/* #nosec */
	if r.metadata.enableTLS {
		options.TLSConfig = &tls.Config{
			InsecureSkipVerify: r.metadata.enableTLS,
		}
	}

	client := redis.NewClient(options)
	if _, err = client.Ping().Result(); err != nil {
		return fmt.Errorf("redis streams: error connecting to redis at %s: %s", m.host, err)
	}

	r.ctx, r.cancel = context.WithCancel(context.Background())

	r.queue = make(chan redisMessageWrapper, int(r.metadata.queueDepth))
	r.client = client

	for i := uint(0); i < r.metadata.concurrency; i++ {
		go r.worker()
	}

	return nil
}

func (r *redisStreams) Publish(req *pubsub.PublishRequest) error {
	_, err := r.client.XAdd(&redis.XAddArgs{
		Stream: req.Topic,
		Values: map[string]interface{}{"data": req.Data},
	}).Result()
	if err != nil {
		return fmt.Errorf("redis streams: error from publish: %s", err)
	}

	return nil
}

func (r *redisStreams) Subscribe(req pubsub.SubscribeRequest, handler func(msg *pubsub.NewMessage) error) error {
	err := r.client.XGroupCreateMkStream(req.Topic, r.metadata.consumerID, "0").Err()
	// Ignore BUSYGROUP errors
	if err != nil && err.Error() != "BUSYGROUP Consumer Group name already exists" {
		r.logger.Errorf("redis streams: %s", err)

		return err
	}

	go r.pollNewMessagesLoop(req.Topic, handler)
	go r.reclaimPendingMessagesLoop(req.Topic, handler)

	return nil
}

// enqueueMessages is a shared function that funnels new messages (via polling)
// and redelivered messages (via reclaiming) to a channel where workers can
// pick them up for processing.
func (r *redisStreams) enqueueMessages(stream string, handler func(msg *pubsub.NewMessage) error, msgs []redis.XMessage) {
	for _, msg := range msgs {
		rmsg := createRedisMessageWrapper(stream, handler, msg)

		select {
		// Might block if the queue is full so we need the r.ctx.Done below.
		case r.queue <- rmsg:

		// Handle cancelation
		case <-r.ctx.Done():
			return
		}
	}
}

// createRedisMessageWrapper encapsulates the Redis message, message identifier, and handler
// in `redisMessage` for processing.
func createRedisMessageWrapper(stream string, handler func(msg *pubsub.NewMessage) error, msg redis.XMessage) redisMessageWrapper {
	var data []byte
	if dataValue, exists := msg.Values["data"]; exists && dataValue != nil {
		switch v := dataValue.(type) {
		case string:
			data = []byte(v)
		case []byte:
			data = v
		}
	}

	return redisMessageWrapper{
		message: pubsub.NewMessage{
			Topic: stream,
			Data:  data,
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
		// Handle cancelation
		case <-r.ctx.Done():
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
	if err := msg.handler(&msg.message); err != nil {
		r.logger.Errorf("Error processing Redis message %s: %v", msg.messageID, err)

		return err
	}

	if err := r.client.XAck(msg.message.Topic, r.metadata.consumerID, msg.messageID).Err(); err != nil {
		r.logger.Errorf("Error acknowledging Redis message %s: %v", msg.messageID, err)

		return err
	}

	return nil
}

// pollMessagesLoop calls `XReadGroup` for new messages and funnels them to the message channel
// by calling `enqueueMessages`.
func (r *redisStreams) pollNewMessagesLoop(stream string, handler func(msg *pubsub.NewMessage) error) {
	for {
		// Read messages
		streams, err := r.client.XReadGroup(&redis.XReadGroupArgs{
			Group:    r.metadata.consumerID,
			Consumer: r.metadata.consumerID,
			Streams:  []string{stream, ">"},
			Count:    int64(r.metadata.queueDepth),
			Block:    0,
		}).Result()
		if err != nil {
			r.logger.Errorf("redis streams: error reading from stream %s: %s", stream, err)

			continue
		}

		// Enqueue messages for the returned streams
		for _, s := range streams {
			r.enqueueMessages(s.Stream, handler, s.Messages)
		}

		// Return on cancelation
		if r.ctx.Err() != nil {
			return
		}
	}
}

// reclaimPendingMessagesLoop periodically reclaims pending messages
// based on the `redeliverInterval` setting.
func (r *redisStreams) reclaimPendingMessagesLoop(stream string, handler func(msg *pubsub.NewMessage) error) {
	if r.metadata.processingTimeout == 0 || r.metadata.redeliverInterval == 0 {
		return
	}

	// Do an initial reclaim call
	r.reclaimPendingMessages(stream, handler)

	reclaimTicker := time.NewTicker(r.metadata.redeliverInterval)

	for {
		select {
		case <-r.ctx.Done():
			return

		case <-reclaimTicker.C:
			r.reclaimPendingMessages(stream, handler)
		}
	}
}

// reclaimPendingMessages handles reclaiming messages that previously failed to process and
// funneling them to the message channel by calling `enqueueMessages`.
func (r *redisStreams) reclaimPendingMessages(stream string, handler func(msg *pubsub.NewMessage) error) {
	for {
		// Retrieve pending messages for this stream and consumer
		pendingResult, err := r.client.XPendingExt(&redis.XPendingExtArgs{
			Stream: stream,
			Group:  r.metadata.consumerID,
			Start:  "-",
			End:    "+",
			Count:  int64(r.metadata.queueDepth),
		}).Result()
		if err != nil && !errors.Is(err, redis.Nil) {
			r.logger.Errorf("error retrieving pending Redis messages: %v", err)

			break
		}

		// Filter out messages that have not timed out yet
		msgIDs := make([]string, 0, len(pendingResult))
		for _, msg := range pendingResult {
			if msg.Idle >= r.metadata.processingTimeout {
				msgIDs = append(msgIDs, msg.ID)
			}
		}

		// Nothing to claim
		if len(msgIDs) == 0 {
			break
		}

		// Attempt to claim the messages for the filtered IDs
		claimResult, err := r.client.XClaim(&redis.XClaimArgs{
			Stream:   stream,
			Group:    r.metadata.consumerID,
			Consumer: r.metadata.consumerID,
			MinIdle:  r.metadata.processingTimeout,
			Messages: msgIDs,
		}).Result()
		if err != nil && !errors.Is(err, redis.Nil) {
			r.logger.Errorf("error claiming pending Redis messages: %v", err)

			break
		}

		// Enqueue claimed messages
		r.enqueueMessages(stream, handler, claimResult)

		// If the Redis nil error is returned, it means somes message in the pending
		// state no longer exist. We need to acknowledge these messages to
		// remove them from the pending list.
		if errors.Is(err, redis.Nil) {
			// Build a set of message IDs that were not returned
			// that potentially no longer exist.
			expectedMsgIDs := make(map[string]struct{}, len(msgIDs))
			for _, id := range msgIDs {
				expectedMsgIDs[id] = struct{}{}
			}
			for _, claimed := range claimResult {
				delete(expectedMsgIDs, claimed.ID)
			}

			r.removeMessagesThatNoLongerExistFromPending(stream, expectedMsgIDs, handler)
		}
	}
}

// removeMessagesThatNoLongerExistFromPending attempts to claim messages individually so that messages in the pending list
// that no longer exist can be removed from the pending list. This is done by calling `XACK`.
func (r *redisStreams) removeMessagesThatNoLongerExistFromPending(stream string, messageIDs map[string]struct{}, handler func(msg *pubsub.NewMessage) error) {
	// Check each message ID individually.
	for pendingID := range messageIDs {
		claimResultSingleMsg, err := r.client.XClaim(&redis.XClaimArgs{
			Stream:   stream,
			Group:    r.metadata.consumerID,
			Consumer: r.metadata.consumerID,
			MinIdle:  r.metadata.processingTimeout,
			Messages: []string{pendingID},
		}).Result()
		if err != nil && !errors.Is(err, redis.Nil) {
			r.logger.Errorf("error claiming pending Redis message %s: %v", pendingID, err)

			return
		}

		// Ack the message to remove it from the pending list.
		if errors.Is(err, redis.Nil) {
			if err = r.client.XAck(stream, r.metadata.consumerID, pendingID).Err(); err != nil {
				r.logger.Errorf("error acknowledging Redis message %s after failed claim for %s: %v", pendingID, stream, err)
			}
		} else {
			// This should not happen but if it does the message should be processed.
			r.enqueueMessages(stream, handler, claimResultSingleMsg)
		}
	}
}

func (r *redisStreams) Close() error {
	r.cancel()

	return r.client.Close()
}

func (r *redisStreams) Features() []pubsub.Feature {
	return nil
}
