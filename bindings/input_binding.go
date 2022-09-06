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

package bindings

import (
	"context"
	"errors"
	"io"

	"github.com/dapr/components-contrib/health"
	"github.com/dapr/components-contrib/metadata"
)

// InputBinding is the interface to define a binding that triggers on incoming events.
type InputBinding interface {
	metadata.ComponentWithMetadata

	// Init passes connection and properties metadata to the binding implementation.
	Init(ctx context.Context, metadata Metadata) error
	// Read is a method that runs in background and triggers the callback function whenever an event arrives.
	Read(ctx context.Context, handler Handler) error
	// Close is a method that closes the connection to the binding. Must be
	// called when the binding is no longer needed to free up resources.
	io.Closer
}

// Handler is the handler used to invoke the app handler.
type Handler func(context.Context, *ReadResponse) ([]byte, error)

func PingInpBinding(ctx context.Context, inputBinding InputBinding) error {
	// checks if this input binding has the ping option then executes
	if inputBindingWithPing, ok := inputBinding.(health.Pinger); ok {
		return inputBindingWithPing.Ping(ctx)
	} else {
		return errors.New("ping is not implemented by this input binding")
	}
}
