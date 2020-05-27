// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package bindings

// InvokeRequest is the object given to a dapr output binding
type InvokeRequest struct {
	Data      []byte            `json:"data"`
	Metadata  map[string]string `json:"metadata"`
	Operation string            `json:"operation"`
}

// Non exhaustive list of operations. A binding can add operations that are not in this list.
const (
	GetOperation    = "get"
	CreateOperation = "create"
	DeleteOperation = "delete"
	ListOperation   = "list"
)
