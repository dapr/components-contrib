// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package bindings

import (
	"github.com/dapr/components-contrib/state"
)

// ReadResponse is the return object from an dapr input binding.
type ReadResponse struct {
	Data        []byte            `json:"data"`
	Metadata    map[string]string `json:"metadata"`
	ContentType *string           `json:"contentType,omitempty"`
}

// AppResponse is the object describing the response from user code after a bindings event.
type AppResponse struct {
	Data        interface{}        `json:"data"`
	To          []string           `json:"to"`
	StoreName   string             `json:"storeName"`
	State       []state.SetRequest `json:"state"`
	Concurrency string             `json:"concurrency"`
}

// InvokeResponse is the response object returned from an output binding.
type InvokeResponse struct {
	Data        []byte            `json:"data"`
	Metadata    map[string]string `json:"metadata"`
	ContentType *string           `json:"contentType,omitempty"`
}
