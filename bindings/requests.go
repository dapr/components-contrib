// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package bindings

import (
	"fmt"
	"strconv"
)

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

// GetMetadataAsBool parses metadata as bool
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

// GetMetadataAsBool parses metadata as int64
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
