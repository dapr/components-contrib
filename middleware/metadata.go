// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package middleware

// Metadata represents a set of middleware specific properties.
type Metadata struct {
	Properties map[string]string `json:"properties"`
}
