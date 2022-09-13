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

package pubsub

import (
	"context"
	"fmt"

	"github.com/google/uuid"

	"github.com/dapr/components-contrib/health"
	"github.com/dapr/components-contrib/internal/utils"
)

// PubSub is the interface for message buses.
type PubSub interface {
	BulkMessager
	Init(metadata Metadata) error
	Features() []Feature
	Publish(req *PublishRequest) error
	Subscribe(ctx context.Context, req SubscribeRequest, handler Handler) error
	Close() error
}

// BulkMessager is the interface defining BulkPublish and BulkSubscribe definitions for message buses
type BulkMessager interface {
	BulkPublish(req *BulkPublishRequest) (BulkPublishResponse, error)
	BulkSubscribe(ctx context.Context, req SubscribeRequest, handler BulkHandler) error
}

// Handler is the handler used to invoke the app handler.
type Handler func(ctx context.Context, msg *NewMessage) error

// BulkHandler is the handler used to invoke the app handler.
// It returns first type as []BulkSubscribeResponseEntry which represents status per message - if not nil,
// broker can take appropriate action accordingly.
// Second return type is error which if not nil, reflects that there was an issue with
// the whole bulk event and nothing could be sent ahead.
type BulkHandler func(ctx context.Context, msg *BulkMessage) ([]BulkSubscribeResponseEntry, error)

func Ping(pubsub PubSub) error {
	// checks if this pubsub has the ping option then executes
	if pubsubWithPing, ok := pubsub.(health.Pinger); ok {
		return pubsubWithPing.Ping()
	} else {
		return fmt.Errorf("ping is not implemented by this pubsub")
	}
}

// DefaultBulkMessager is default implementation for BukMessager
type DefaultBulkMessager struct {
	p PubSub
}

// NewDefaultBulkMessager to create new DefaultBulkMessager for a PubSub
func NewDefaultBulkMessager(pubsub PubSub) DefaultBulkMessager {
	return DefaultBulkMessager{
		p: pubsub,
	}
}

// BulkPublish publishes messages to a broker that does not support bulk publish.
// If 'bulkPublishKeepOrder' metadata is set to true, the messages are published serially
// in the same order, otherwise they are published in parallel.
func (p *DefaultBulkMessager) BulkPublish(req *BulkPublishRequest) (BulkPublishResponse, error) {
	if req.Metadata[bulkPublishKeepOrderKey] == "true" {
		return p.bulkPublishSerial(req)
	} else {
		return p.bulkPublishParallel(req)
	}
}

// BulkSubscribe subscribes to a topic using a BulkHandler.
// Dapr buffers messages in memory and calls the handler with a list of messages
// when the buffer is full or max await duration is reached.
func (p *DefaultBulkMessager) BulkSubscribe(ctx context.Context, req SubscribeRequest, handler BulkHandler) error {
	cfg := BulkSubscribeConfig{
		MaxBulkCount:           utils.GetIntOrDefault(req.Metadata, bulkSubscribeMaxCountKey, defaultMaxBulkCount),
		MaxBulkAwaitDurationMs: utils.GetIntOrDefault(req.Metadata, bulkSubscribeMaxAwaitDurationMsKey, defaultMaxBulkAwaitDurationMs),
	}

	msgCbChan := make(chan msgWithCallback, cfg.MaxBulkCount)
	go processBulkMessages(ctx, req, msgCbChan, cfg, handler)

	return p.p.Subscribe(ctx, req, func(ctx context.Context, msg *NewMessage) error {
		var err error
		done := make(chan struct{})

		msgCbChan <- msgWithCallback{
			msg: BulkMessageEntry{
				EntryID:     uuid.NewString(),
				Event:       msg.Data,
				ContentType: *msg.ContentType,
				Metadata:    msg.Metadata,
			},
			cb: func(ierr error) {
				err = ierr
				done <- struct{}{}
			},
		}

		<-done
		return err
	})
}
