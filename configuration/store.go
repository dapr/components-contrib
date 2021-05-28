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

	// Get configuration
	Get(ctx context.Context, req *GetRequest) (*GetResponse, error)

	// Subscribe configuration by update event.
	Subscribe(ctx context.Context, req *SubscribeRequest, handler UpdateHandler) error

	// Save configuration
	// Only used for integration test now.
	Save(ctx context.Context, req *SaveRequest) error

	// Delete configuration
	// Only used for integration test now.
	Delete(ctx context.Context, req *DeleteRequest) error
}

// Handler is the handler used to invoke the app handler
type UpdateHandler func(ctx context.Context, e *UpdateEvent) error

// Configuration is the whole configuration with all the configuration items for specified application.
type Configuration struct {
	AppID     string  `json:"appID"`
	StoreName string  `json:"store_name"`
	Revision  string  `json:"revision"`
	Items     []*Item `json:"items"`
}
