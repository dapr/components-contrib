/*
Copyright 2021 The Dapr Authors
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

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
