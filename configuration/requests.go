// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package configuration

// Item represents a configuration item with name, content and other information.
type Item struct {
	Key      string            `json:"key"`
	Value    string            `json:"value,omitempty"`
	Version  string            `json:"version,omitempty"`
	Metadata map[string]string `json:"metadata,omitempty"`
}

// GetRequest is the object describing a request to get configuration.
type GetRequest struct {
	Keys     []string          `json:"keys"`
	Metadata map[string]string `json:"metadata"`
}

// SubscribeRequest is the object describing a request to subscribe configuration.
type SubscribeRequest struct {
	Keys     []string          `json:"keys"`
	Metadata map[string]string `json:"metadata"`
}

// UpdateEvent is the object describing a configuration update event.
type UpdateEvent struct {
	Items []*Item `json:"items"`
}
