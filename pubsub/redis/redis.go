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
	"github.com/dapr/kit/logger"
)

const (
	host                  = "redisHost"
	password              = "redisPassword"
	db                    = "redisDB"
	redisMaxRetries       = "redisMaxRetries"
	redisMinRetryInterval = "redisMinRetryInterval"
	redisMaxRetryInterval = "redisMaxRetryInterval"
	dialTimeout           = "dialTimeout"
	readTimeout           = "readTimeout"
	writeTimeout          = "writeTimeout"
	poolSize              = "poolSize"
	minIdleConns          = "minIdleConns"
	poolTimeout           = "poolTimeout"
	idleTimeout           = "idleTimeout"
	idleCheckFrequency    = "idleCheckFrequency"
	maxConnAge            = "maxConnAge"
	consumerID            = "consumerID"
	enableTLS             = "enableTLS"
	processingTimeout     = "processingTimeout"
	redeliverInterval     = "redeliverInterval"
	queueDepth            = "queueDepth"
	concurrency           = "concurrency"
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
	handler   pubsub.Handler
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

	if val, ok := meta.Properties[db]; ok && val != "" {
		db, err := strconv.Atoi(val)
		if err != nil {
			return m, fmt.Errorf("redis streams error: can't parse db field: %s", err)
		}
		m.db = db
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
			return m, fmt.Errorf("redis streams error: can't parse redeliverInterval field: %s", err)
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

	if val, ok := meta.Properties[redisMaxRetries]; ok && val != "" {
		redisMaxRetries, err := strconv.Atoi(val)
		if err != nil {
			return m, fmt.Errorf("redis streams error: can't parse redisMaxRetries field: %s", err)
		}
		m.redisMaxRetries = redisMaxRetries
	}

	if val, ok := meta.Properties[redisMinRetryInterval]; ok && val != "" {
		if val == "-1" {
			m.redisMinRetryInterval = -1
		} else if redisMinRetryIntervalMs, err := strconv.ParseUint(val, 10, 64); err == nil {
			m.redisMinRetryInterval = time.Duration(redisMinRetryIntervalMs) * time.Millisecond
		} else if d, err := time.ParseDuration(val); err == nil {
			m.redisMinRetryInterval = d
		} else {
			return m, fmt.Errorf("redis streams error: invalid redisMinRetryInterval %s, %s", val, err)
		}
	}

	if val, ok := meta.Properties[redisMaxRetryInterval]; ok && val != "" {
		if val == "-1" {
			m.redisMaxRetryInterval = -1
		} else if redisMaxRetryIntervalMs, err := strconv.ParseUint(val, 10, 64); err == nil {
			m.redisMaxRetryInterval = time.Duration(redisMaxRetryIntervalMs) * time.Millisecond
		} else if d, err := time.ParseDuration(val); err == nil {
			m.redisMaxRetryInterval = d
		} else {
			return m, fmt.Errorf("redis streams error: invalid redisMaxRetryInterval %s, %s", val, err)
		}
	}

	if val, ok := meta.Properties[dialTimeout]; ok && val != "" {
		if dialTimeoutMs, err := strconv.ParseUint(val, 10, 64); err == nil {
			m.dialTimeout = time.Duration(dialTimeoutMs) * time.Millisecond
		} else if d, err := time.ParseDuration(val); err == nil {
			m.dialTimeout = d
		} else {
			return m, fmt.Errorf("redis streams error: invalid dialTimeout %s, %s", val, err)
		}
	}

	if val, ok := meta.Properties[readTimeout]; ok && val != "" {
		if val == "-1" {
			m.readTimeout = -1
		} else if readTimeoutMs, err := strconv.ParseUint(val, 10, 64); err == nil {
			m.readTimeout = time.Duration(readTimeoutMs) * time.Millisecond
		} else if d, err := time.ParseDuration(val); err == nil {
			m.readTimeout = d
		} else {
			return m, fmt.Errorf("redis streams error: invalid readTimeout %s, %s", val, err)
		}
	}

	if val, ok := meta.Properties[writeTimeout]; ok && val != "" {
		if writeTimeoutMs, err := strconv.ParseUint(val, 10, 64); err == nil {
			m.writeTimeout = time.Duration(writeTimeoutMs) * time.Millisecond
		} else if d, err := time.ParseDuration(val); err == nil {
			m.writeTimeout = d
		} else {
			return m, fmt.Errorf("redis streams error: invalid writeTimeout %s, %s", val, err)
		}
	}

	if val, ok := meta.Properties[poolSize]; ok && val != "" {
		var err error
		m.poolSize, err = strconv.Atoi(val)
		if err != nil {
			return m, fmt.Errorf("redis streams error: invalid poolSize %s, %s", val, err)
		}
	}

	if val, ok := meta.Properties[maxConnAge]; ok && val != "" {
		if maxConnAgeMs, err := strconv.ParseUint(val, 10, 64); err == nil {
			m.maxConnAge = time.Duration(maxConnAgeMs) * time.Millisecond
		} else if d, err := time.ParseDuration(val); err == nil {
			m.maxConnAge = d
		} else {
			return m, fmt.Errorf("redis streams error: invalid maxConnAge %s, %s", val, err)
		}
	}

	if val, ok := meta.Properties[minIdleConns]; ok && val != "" {
		minIdleConns, err := strconv.Atoi(val)
		if err != nil {
			return m, fmt.Errorf("redis streams error: can't parse minIdleConns field: %s", err)
		}
		m.minIdleConns = minIdleConns
	}

	if val, ok := meta.Properties[poolTimeout]; ok && val != "" {
		if poolTimeoutMs, err := strconv.ParseUint(val, 10, 64); err == nil {
			m.poolTimeout = time.Duration(poolTimeoutMs) * time.Millisecond
		} else if d, err := time.ParseDuration(val); err == nil {
			m.poolTimeout = d
		} else {
			return m, fmt.Errorf("redis streams error: invalid poolTimeout %s, %s", val, err)
		}
	}

	if val, ok := meta.Properties[idleTimeout]; ok && val != "" {
		if val == "-1" {
			m.idleTimeout = -1
		} else if idleTimeoutMs, err := strconv.ParseUint(val, 10, 64); err == nil {
			m.idleTimeout = time.Duration(idleTimeoutMs) * time.Millisecond
		} else if d, err := time.ParseDuration(val); err == nil {
			m.idleTimeout = d
		} else {
			return m, fmt.Errorf("redis streams error: invalid idleTimeout %s, %s", val, err)
		}
	}

	if val, ok := meta.Properties[idleCheckFrequency]; ok && val != "" {
		if val == "-1" {
			m.idleCheckFrequency = -1
		} else if idleCheckFrequencyMs, err := strconv.ParseUint(val, 10, 64); err == nil {
			m.idleCheckFrequency = time.Duration(idleCheckFrequencyMs) * time.Millisecond
		} else if d, err := time.ParseDuration(val); err == nil {
			m.idleCheckFrequency = d
		} else {
			return m, fmt.Errorf("redis streams error: invalid idleCheckFrequency %s, %s", val, err)
		}
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
		Addr:               m.host,
		Password:           m.password,
		DB:                 m.db,
		MaxRetries:         m.redisMaxRetries,
		MaxRetryBackoff:    m.redisMaxRetryInterval,
		MinRetryBackoff:    m.redisMinRetryInterval,
		DialTimeout:        m.dialTimeout,
		ReadTimeout:        m.readTimeout,
		WriteTimeout:       m.writeTimeout,
		PoolSize:           m.poolSize,
		MaxConnAge:         m.maxConnAge,
		MinIdleConns:       m.minIdleConns,
		PoolTimeout:        m.poolTimeout,
		IdleCheckFrequency: m.idleCheckFrequency,
		IdleTimeout:        m.idleTimeout,
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

func (r *redisStreams) Subscribe(req pubsub.SubscribeRequest, handler pubsub.Handler) error {
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
func (r *redisStreams) enqueueMessages(stream string, handler pubsub.Handler, msgs []redis.XMessage) {
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
func createRedisMessageWrapper(stream string, handler pubsub.Handler, msg redis.XMessage) redisMessageWrapper {
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
	if err := msg.handler(r.ctx, &msg.message); err != nil {
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
func (r *redisStreams) pollNewMessagesLoop(stream string, handler pubsub.Handler) {
	for {
		// Return on cancelation
		if r.ctx.Err() != nil {
			return
		}

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
	}
}

// reclaimPendingMessagesLoop periodically reclaims pending messages
// based on the `redeliverInterval` setting.
func (r *redisStreams) reclaimPendingMessagesLoop(stream string, handler pubsub.Handler) {
	// Having a `processingTimeout` or `redeliverInterval` means that
	// redelivery is disabled so we just return out of the goroutine.
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
func (r *redisStreams) reclaimPendingMessages(stream string, handler pubsub.Handler) {
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
func (r *redisStreams) removeMessagesThatNoLongerExistFromPending(stream string, messageIDs map[string]struct{}, handler pubsub.Handler) {
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

			continue
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
