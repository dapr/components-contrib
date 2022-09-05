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

	"github.com/dapr/components-contrib/health"
)

// PubSub is the interface for message buses.
type PubSub interface {
	Batcher
	Init(metadata Metadata) error
	Features() []Feature
	Publish(req *PublishRequest) error
	Subscribe(ctx context.Context, req SubscribeRequest, handler Handler) error
	Close() error
}

// Batcher is the interface for batching functionality in message buses.
type Batcher interface {
	BatchPublish(req *BatchPublishRequest) BatchPublishResponse
	BatchSubscribe(ctx context.Context, req SubscribeRequest, handler BatchHandler) error
}

// Handler is the handler used to invoke the app handler.
type Handler func(ctx context.Context, msg *NewMessage) error

// BatchHandler is the handler used to invoke the app handler for batch messages.
type BatchHandler func(ctx context.Context, msg *NewBatchMessage) (error, []BatchMessageResponse)

func Ping(pubsub PubSub) error {
	// checks if this pubsub has the ping option then executes
	if pubsubWithPing, ok := pubsub.(health.Pinger); ok {
		return pubsubWithPing.Ping()
	} else {
		return fmt.Errorf("Ping is not implemented by this pubsub")
	}
}

// DefaultBatcher is a default batching implementation.
// This is used when the pubsub broker does not implement batching.
type DefaultBatcher struct {
	p PubSub
}

// NewDefaultBatcher builds a new DefaultBatcher from a PubSub.
func NewDefaultBatcher(pubsub PubSub) DefaultBatcher {
	return DefaultBatcher{
		p: pubsub,
	}
}

// BatchPublish publishes a batch of messages.
// TODO: implement
func (b *DefaultBatcher) BatchPublish(req *BatchPublishRequest) BatchPublishResponse {
	return BatchPublishResponse{}
}

// BatchSubscribe subscribes to a topic using a batch handler.
// TODO: implement
func (b *DefaultBatcher) BatchSubscribe(ctx context.Context, req SubscribeRequest, handler BatchHandler) error {
	return nil
}
