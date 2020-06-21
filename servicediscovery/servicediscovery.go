// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package servicediscovery

// Resolver is the interface of service discovery resolver.
type Resolver interface {
	ResolveID(req ResolveRequest) (string, error)
}
