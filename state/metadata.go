// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package state

// Metadata contains a state store specific set of metadata properties.
type Metadata struct {
	Properties map[string]string `json:"properties"`
}
