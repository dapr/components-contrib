// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package pubsubadapter

import (
	"context"
	"fmt"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/components-contrib/pubsub"
)

const (
	defaultReadTopicAttribute   = "readTopic"
	defaultInvokeTopicAttribute = "invokeTopic"
	defaultInvokeTopicMetadata  = "topic"
)

// Ensure Adapter satifies the input and output binding interfaces.
var (
	_ bindings.InputBinding  = (*Adapter)(nil)
	_ bindings.OutputBinding = (*Adapter)(nil)
)

// Adapter allows any PubSub component to be used as an input
// and output binding.
type Adapter struct {
	broker               pubsub.PubSub
	readTopicAttribute   string
	readTopic            string
	invokeTopicAttribute string
	invokeTopicMetadata  string
	invokeTopic          string
}

// Option is passed into `New` and overrides a default value.
type Option func(a *Adapter)

// WithReadTopicAttribute overrides the default read topic attribute
// that determines the topic to subscribe to when Read is called.
func WithReadTopicAttribute(readTopicAttribute string) Option {
	return func(a *Adapter) {
		a.readTopicAttribute = readTopicAttribute
	}
}

// WithInvokeTopicAttribute overrides the default invoke topic attribute
// that determines the topic to publish to when Invoke is called and
// the topic is not overriden in the request metadata.
func WithInvokeTopicAttribute(invokeTopicAttribute string) Option {
	return func(a *Adapter) {
		a.invokeTopicAttribute = invokeTopicAttribute
	}
}

// WithInvokeTopicMetadata overrides the default invoke topic metadata
// name that determines the metadata property that overrides the topic
// to publish to when Invoke is called.
func WithInvokeTopicMetadata(invokeTopicMetadata string) Option {
	return func(a *Adapter) {
		a.invokeTopicMetadata = invokeTopicMetadata
	}
}

// New returns an input/output binding adapter for `broker`
func New(broker pubsub.PubSub, options ...Option) *Adapter {
	a := Adapter{
		broker:               broker,
		readTopicAttribute:   defaultReadTopicAttribute,
		invokeTopicAttribute: defaultInvokeTopicAttribute,
		invokeTopicMetadata:  defaultInvokeTopicMetadata,
	}

	for _, opt := range options {
		opt(&a)
	}

	return &a
}

// Init reads the topic property and passes the properties
// to the underlying PubSub component.
func (a *Adapter) Init(metadata bindings.Metadata) error {
	if t, ok := metadata.Properties[a.readTopicAttribute]; ok && t != "" {
		a.readTopic = t
	} else {
		return fmt.Errorf("pubsub adapter: %q attribute was missing", a.readTopicAttribute)
	}

	if t, ok := metadata.Properties[a.invokeTopicAttribute]; ok && t != "" {
		a.invokeTopic = t
	} else {
		return fmt.Errorf("pubsub adapter: %q attribute was missing", a.invokeTopicAttribute)
	}

	return a.broker.Init(pubsub.Metadata{
		Properties: metadata.Properties,
	})
}

// Read subscribes to the configured topic and invokes `handler`
// when a message is received.
func (a *Adapter) Read(handler func(*bindings.ReadResponse) ([]byte, error)) error {
	return a.broker.Subscribe(pubsub.SubscribeRequest{
		Topic: a.invokeTopic,
	}, func(ctx context.Context, msg *pubsub.NewMessage) error {
		_, err := handler(&bindings.ReadResponse{
			Data:     msg.Data,
			Metadata: msg.Metadata,
		})

		return err
	})
}

// Invoke publishes a message to the configured topic or
// optionally the topic indicated in `req.Metadata`.
func (a *Adapter) Invoke(req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	topic := a.invokeTopic
	if t, ok := req.Metadata[a.invokeTopicMetadata]; ok && t != "" {
		topic = t
	}

	err := a.broker.Publish(&pubsub.PublishRequest{
		Data:     req.Data,
		Topic:    topic,
		Metadata: req.Metadata,
	})

	return nil, err
}

// Operations returns "create" and "publish" as an operations
// but operation is ignored by `Invoke`.
func (a *Adapter) Operations() []bindings.OperationKind {
	return []bindings.OperationKind{bindings.CreateOperation, bindings.OperationKind("publish")}
}

// Close closes the underlying component.
func (a *Adapter) Close() error {
	return a.broker.Close()
}
