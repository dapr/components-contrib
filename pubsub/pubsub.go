// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package pubsub

import "context"

// PubSub is the interface for message buses
type PubSub interface {
	Init(metadata Metadata) error
	Features() []Feature
	Publish(req *PublishRequest) error
	Subscribe(req SubscribeRequest, handler Handler) error
	Close() error
}

// Handler is the handler used to invoke the app handler
type Handler func(ctx context.Context, msg *NewMessage) error

// SuspendableSub is the interface that allows pubsub to suspend/resume its subscription independent to the publishing
// It is used in the lifecycle operations of dapr
type SuspendableSub interface {
	SuspendSubscription() error
	ResumeSubscription(handler Handler) error
}
