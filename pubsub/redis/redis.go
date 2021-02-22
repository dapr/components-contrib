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
	reclaimInterval   = "reclaimInterval"
	queueDepth        = "queueDepth"
	concurreny        = "concurrency"
)

type redisStreams struct {
	metadata metadata
	client   *redis.Client

	logger logger.Logger

	queue chan redisMessage

	ctx    context.Context
	cancel context.CancelFunc
}

// redisMessage encapsulates the pubsub message and handler to
// send to the queue channel for processing.
type redisMessage struct {
	message pubsub.NewMessage

	messageID string
	handler   func(msg *pubsub.NewMessage) error
}

// NewRedisStreams returns a new redis streams pub-sub implementation
func NewRedisStreams(logger logger.Logger) pubsub.PubSub {
	return &redisStreams{logger: logger}
}

func parseRedisMetadata(meta pubsub.Metadata) (metadata, error) {
	// Default values
	m := metadata{
		processingTimeout: 5 * time.Second,
		reclaimInterval:   1 * time.Second,
		queueDepth:        10,
		concurrency:       1,
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
		if processingTimeoutMS, err := strconv.ParseUint(val, 10, 64); err == nil {
			m.processingTimeout = time.Duration(processingTimeoutMS) * time.Millisecond
		} else if d, err := time.ParseDuration(val); err == nil {
			m.processingTimeout = d
		} else {
			return m, fmt.Errorf("redis streams error: can't parse processingTimeout field: %s", err)
		}
	}

	if val, ok := meta.Properties[reclaimInterval]; ok && val != "" {
		if reclaimIntervalMS, err := strconv.ParseUint(val, 10, 64); err == nil {
			m.reclaimInterval = time.Duration(reclaimIntervalMS) * time.Millisecond
		} else if d, err := time.ParseDuration(val); err == nil {
			m.reclaimInterval = d
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

	if val, ok := meta.Properties[concurreny]; ok && val != "" {
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

	r.queue = make(chan redisMessage, int(r.metadata.queueDepth))
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

	go r.pollMessagesLoop(req.Topic, handler)
	go r.reclaimMessagesLoop(req.Topic, handler)

	return nil
}

func (r *redisStreams) enqueue(stream, consumerID string, handler func(msg *pubsub.NewMessage) error, msgs []redis.XMessage) {
	for _, msg := range msgs {
		var data []byte
		if dataValue, exists := msg.Values["data"]; exists && dataValue != nil {
			switch v := dataValue.(type) {
			case string:
				data = []byte(v)
			case []byte:
				data = v
			}
		}

		rmsg := redisMessage{
			message: pubsub.NewMessage{
				Topic: stream,
				Data:  data,
			},
			messageID: msg.ID,
			handler:   handler,
		}

		select {
		// Might block if the queue is full so we need the r.ctx.Done below.
		case r.queue <- rmsg:

		// Handle cancelation
		case <-r.ctx.Done():
			return
		}
	}
}

func (r *redisStreams) worker() {
	for {
		select {
		// Handle cancelation
		case <-r.ctx.Done():
			return

		case msg := <-r.queue:
			r.logger.Debugf("Processing Redis message %s", msg.messageID)
			if err := msg.handler(&msg.message); err == nil {
				if err = r.client.XAck(msg.message.Topic, r.metadata.consumerID, msg.messageID).Err(); err != nil {
					r.logger.Errorf("Error acknowledging Redis message %s: %v", msg.messageID, err)
				}
			} else {
				r.logger.Errorf("Error processing Redis message %s: %v", msg.messageID, err)
			}
		}
	}
}

func (r *redisStreams) pollMessagesLoop(stream string, handler func(msg *pubsub.NewMessage) error) {
	// first read pending items in case of recovering from crash
	start := "0"

	for {
		// Read messages
		streams, err := r.client.XReadGroup(&redis.XReadGroupArgs{
			Group:    r.metadata.consumerID,
			Consumer: r.metadata.consumerID,
			Streams:  []string{stream, start},
			Block:    0,
		}).Result()
		if err != nil {
			r.logger.Errorf("redis streams: error reading from stream %s: %s", stream, err)

			continue
		}

		// Enqueue messages for the returned streams
		for _, s := range streams {
			r.enqueue(s.Stream, consumerID, handler, s.Messages)
		}

		// continue with new non received items
		start = ">"

		// Return on cancelation
		if r.ctx.Err() != nil {
			return
		}
	}
}

func (r *redisStreams) reclaimMessagesLoop(stream string, handler func(msg *pubsub.NewMessage) error) {
	if r.metadata.processingTimeout == 0 {
		return
	}

	reclaimTicker := time.NewTicker(r.metadata.reclaimInterval)

	for {
		select {
		case <-r.ctx.Done():
			return

		case <-reclaimTicker.C:
			for {
				// Retrieve pending messages for this stream and consumer
				pendingResult, err := r.client.XPendingExt(&redis.XPendingExtArgs{
					Stream: stream,
					Group:  r.metadata.consumerID,
					Start:  "-",
					End:    "+",
					Count:  int64(r.metadata.queueDepth),
				}).Result()
				if err != nil && err != redis.Nil {
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
				if err != nil && err != redis.Nil {
					r.logger.Errorf("error claiming pending Redis messages: %v", err)

					break
				}

				// Enqueue claimed messages
				r.enqueue(stream, consumerID, handler, claimResult)

				// If the Redis nil error is returned, it means somes message in the pending
				// state no longer exist. We need to acknowledge these messages to
				// remove them from the pending list.
				if err == redis.Nil {
					// Build a set of message IDs that were not returned
					// that potentially no longer exist.
					expectedMsgIDs := make(map[string]struct{}, len(msgIDs))
					for _, id := range msgIDs {
						expectedMsgIDs[id] = struct{}{}
					}
					for _, claimed := range claimResult {
						delete(expectedMsgIDs, claimed.ID)
					}

					r.handlePendingWithoutMessages(stream, expectedMsgIDs, handler)
				}
			}
		}
	}
}

func (r *redisStreams) handlePendingWithoutMessages(stream string, messageIDs map[string]struct{}, handler func(msg *pubsub.NewMessage) error) {
	// Check each message ID individually.
	for pendingID := range messageIDs {
		claimResultSingleMsg, err := r.client.XClaim(&redis.XClaimArgs{
			Stream:   stream,
			Group:    r.metadata.consumerID,
			Consumer: r.metadata.consumerID,
			MinIdle:  r.metadata.processingTimeout,
			Messages: []string{pendingID},
		}).Result()
		if err != nil && err != redis.Nil {
			r.logger.Errorf("error claiming pending Redis message %s: %v", pendingID, err)

			return
		}

		// Ack the message to remove it from the pending list.
		if err == redis.Nil {
			err = r.client.XAck(stream, r.metadata.consumerID, pendingID).Err()
			if err != nil {
				r.logger.Errorf("error acknowledging Redis message %s after failed claim for %s: %v", pendingID, stream, err)
			}
		} else {
			// This should not happen but if it does the message should be processed.
			r.enqueue(stream, consumerID, handler, claimResultSingleMsg)
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
