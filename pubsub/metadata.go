// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package pubsub

// Metadata represents a set of message-bus specific properties.
type Metadata struct {
	Properties map[string]string `json:"properties"`
}
