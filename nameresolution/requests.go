// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package nameresolution

// DefaultNamespace is the default kubernetes namespace.
const DefaultNamespace = "default"

// ResolveRequest represents service discovery resolver request.
type ResolveRequest struct {
	ID        string
	Namespace string
	Port      int
	Data      map[string]string
}

// NewResolveRequest creates ResolveRequest with the default namespace.
func NewResolveRequest() *ResolveRequest {
	return &ResolveRequest{Namespace: DefaultNamespace}
}
