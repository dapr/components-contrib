// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package nameresolution

// Resolver is the interface of name resolver.
type Resolver interface {
	// Init initializes name resolver.
	Init(metadata Metadata) error
	// ResolveID resolves name to address.
	ResolveID(req ResolveRequest) (string, error)
}
