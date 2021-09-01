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
}

// UpdateHandler is the handler used to send event to daprd
type UpdateHandler func(ctx context.Context, e *UpdateEvent) error

// Configuration is the whole configuration with all the configuration items for specified application.
type Configuration struct {
	AppID     string  `json:"app_id"`
	StoreName string  `json:"store_name"`
	Version   string  `json:"version"`
	Items     []*Item `json:"items"`
}
