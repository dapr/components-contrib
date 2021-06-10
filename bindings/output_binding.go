// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package bindings

// OutputBinding is the interface for an output binding, allowing users to invoke remote systems with optional payloads.
type OutputBinding interface {
	Init(metadata Metadata) error
	Invoke(req *InvokeRequest) (*InvokeResponse, error)
	Operations() []OperationKind
}
