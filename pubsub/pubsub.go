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
	"errors"
	"io"

	"github.com/dapr/components-contrib/health"
	"github.com/dapr/components-contrib/metadata"
)

// PubSub is the interface for message buses.
type PubSub interface {
	metadata.ComponentWithMetadata

	Init(ctx context.Context, metadata Metadata) error
	Features() []Feature
	Publish(ctx context.Context, req *PublishRequest) error
	Subscribe(ctx context.Context, req SubscribeRequest, handler Handler) error
	io.Closer
}

// BulkPublisher is the interface that wraps the BulkPublish method.

// BulkPublish publishes a collection of entries/messages in a BulkPublishRequest to a
// message bus topic and returns a BulkPublishResponse with failed entries for any failed messages.
// Error is returned on partial or complete failures. If there are no failures, error is nil.
type BulkPublisher interface {
	BulkPublish(ctx context.Context, req *BulkPublishRequest) (BulkPublishResponse, error)
}

// BulkSubscriber is the interface defining BulkSubscribe definition for message buses
type BulkSubscriber interface {
	// BulkSubscribe is used to subscribe to a topic and receive collection of entries/ messages
	// from a message bus topic.
	// The bulkHandler will be called with a list of messages.
	BulkSubscribe(ctx context.Context, req SubscribeRequest, bulkHandler BulkHandler) error
}

// Handler is the handler used to invoke the app handler.
type Handler func(ctx context.Context, msg *NewMessage) error

// BulkHandler is the handler used to invoke the app handler in a bulk fashion.

// If second return type error is not nil, and []BulkSubscribeResponseEntry is nil,
// it represents some issue and that none of the message could be sent.

// If second return type error is not nil, and []BulkSubscribeResponseEntry is also not nil,
// []BulkSubscribeResponseEntry can be checked for each message's response status.

// If second return type error is nil, that reflects all items were sent successfully
// and []BulkSubscribeResponseEntry doesn't matter

// []BulkSubscribeResponseEntry represents individual statuses for each message in an
// orderly fashion.
type BulkHandler func(ctx context.Context, msg *BulkMessage) ([]BulkSubscribeResponseEntry, error)

func Ping(ctx context.Context, pubsub PubSub) error {
	// checks if this pubsub has the ping option then executes
	if pubsubWithPing, ok := pubsub.(health.Pinger); ok {
		return pubsubWithPing.Ping(ctx)
	} else {
		return errors.New("ping is not implemented by this pubsub")
	}
}
