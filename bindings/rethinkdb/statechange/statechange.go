// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package statechange

import (
	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/components-contrib/state"
	db "github.com/dapr/components-contrib/state/rethinkdb"
	"github.com/dapr/dapr/pkg/logger"
	"github.com/pkg/errors"
)

// Binding represents RethinkDB change change state binding
type Binding struct {
	logger logger.Logger
	state  *db.RethinkDB
}

var _ = bindings.InputBinding(&Binding{})

// NewRethinkDBStateChangeBinding returns a new RethinkDB actor event input binding
func NewRethinkDBStateChangeBinding(logger logger.Logger) *Binding {
	return &Binding{
		logger: logger,
	}
}

// Init initializes the RethinkDB binding
func (b *Binding) Init(metadata bindings.Metadata) error {
	b.state = db.NewRethinkDBStateStore(b.logger)
	if err := b.state.Init(state.Metadata{Properties: metadata.Properties}); err != nil {
		return errors.Wrap(err, "error initializing RethinkDB state store")
	}
	return nil
}

// Read triggers the RethinkDB scheduler
func (b *Binding) Read(handler func(*bindings.ReadResponse) error) error {
	b.logger.Info("subscribing to state changes...")
	if err := b.state.SubscribeToStateChanges(func(data []byte) error {
		resp := &bindings.ReadResponse{
			Data: data,
			Metadata: map[string]string{
				"provider": "RethinkDB",
			},
		}

		if err := handler(resp); err != nil {
			b.logger.Errorf("error invoking handler: %v", err)
		}
		return nil
	}); err != nil {
		return errors.Wrap(err, "error subscribing to state changes")
	}
	return nil
}
