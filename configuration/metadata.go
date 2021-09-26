// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package configuration

// Metadata contains a configuration store specific set of metadata property.
type Metadata struct {
	Name       string            `json:"name"`
	Properties map[string]string `json:"properties"`
}
