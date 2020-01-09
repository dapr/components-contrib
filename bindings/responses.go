// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package bindings

import (
	"github.com/dapr/components-contrib/state"
)

// ReadResponse is an the return object from an dapr input binding
type ReadResponse struct {
	Data     []byte            `json:"data"`
	Metadata map[string]string `json:"metadata"`
}

// AppResponse is the object describing the response from user code after a bindings event
type AppResponse struct {
	Data        interface{}  `json:"data"`
	To          []string     `json:"to"`
	State       StateRequest `json:"state"`
	Concurrency string       `json:"concurrency"`
}

// StateRequest is the object describing the state request in response from user code after a bindings event
type StateRequest struct {
	StoreName string             `json:"storeName"`
	Requests  []state.SetRequest `json:"requests"`
}
