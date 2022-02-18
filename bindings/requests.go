/*
Copyright 2022 The Dapr Authors
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
	"fmt"
	"strconv"
)

// InvokeRequest is the object given to a dapr output binding.
type InvokeRequest struct {
	Data      []byte            `json:"data"`
	Metadata  map[string]string `json:"metadata"`
	Operation OperationKind     `json:"operation"`
}

// OperationKind defines an output binding operation.
type OperationKind string

// Non exhaustive list of operations. A binding can add operations that are not in this list.
const (
	GetOperation    OperationKind = "get"
	CreateOperation OperationKind = "create"
	DeleteOperation OperationKind = "delete"
	ListOperation   OperationKind = "list"
)

// GetMetadataAsBool parses metadata as bool.
func (r *InvokeRequest) GetMetadataAsBool(key string) (bool, error) {
	if val, ok := r.Metadata[key]; ok {
		boolVal, err := strconv.ParseBool(val)
		if err != nil {
			return false, fmt.Errorf("error parsing metadata `%s` with value `%s` as bool: %w", key, val, err)
		}

		return boolVal, nil
	}

	return false, nil
}

// GetMetadataAsInt64 parses metadata as int64.
func (r *InvokeRequest) GetMetadataAsInt64(key string, bitSize int) (int64, error) {
	if val, ok := r.Metadata[key]; ok {
		intVal, err := strconv.ParseInt(val, 10, bitSize)
		if err != nil {
			return 0, fmt.Errorf("error parsing metadata `%s` with value `%s` as int%d: %w", key, val, bitSize, err)
		}

		return intVal, nil
	}

	return 0, nil
}
