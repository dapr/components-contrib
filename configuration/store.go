// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package configuration

import "context"

// Store is an interface to perform operations on store
type Store interface {
	// Init configuration store
	Init(metadata Metadata) error

	// Get configuration and subscribe update event.
	Get(ctx context.Context, req *GetRequest, handler func(e *UpdateEvent) error) (*GetResponse, error)

	// Save configuration
	Save(ctx context.Context, req *SaveRequest) error

	// Delete configuration
	Delete(ctx context.Context, req *DeleteRequest) error
}


