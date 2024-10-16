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

package configuration

import (
	"context"
	"io"

	"github.com/dapr/components-contrib/metadata"
)

// Store is an interface to perform operations on store.
type Store interface {
	metadata.ComponentWithMetadata

	// Init configuration store.
	Init(ctx context.Context, metadata Metadata) error

	// Get configuration.
	Get(ctx context.Context, req *GetRequest) (*GetResponse, error)

	// Subscribe configuration by update event.
	Subscribe(ctx context.Context, req *SubscribeRequest, handler UpdateHandler) (string, error)

	// Unsubscribe configuration with keys
	Unsubscribe(ctx context.Context, req *UnsubscribeRequest) error

	io.Closer
}

// UpdateHandler is the handler used to send event to daprd.
type UpdateHandler func(ctx context.Context, e *UpdateEvent) error
