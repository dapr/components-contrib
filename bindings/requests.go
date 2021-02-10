// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package bindings

// InvokeRequest is the object given to a dapr output binding
type InvokeRequest struct {
	Data      []byte            `json:"data"`
	Metadata  map[string]string `json:"metadata"`
	Operation OperationKind     `json:"operation"`
}

// OperationKind defines an output binding operation
type OperationKind string

// Non exhaustive list of operations. A binding can add operations that are not in this list.
const (
	GetOperation    OperationKind = "get"
	CreateOperation OperationKind = "create"
	DeleteOperation OperationKind = "delete"
	ListOperation   OperationKind = "list"
)
