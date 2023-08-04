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

package metadata

import (
	"fmt"
	"reflect"

	"github.com/mitchellh/mapstructure"
	"github.com/spf13/cast"
	"k8s.io/apimachinery/pkg/api/resource"
)

// ByteSize contains a quantity for a resource that is measured in bytes.
// This extends the resource.Quantity struct from k8s.io/apimachinery to add some utility methods specific for Dapr.
// Although the library from K8s supports other kinds of resource quantities, our focus is on sizes in bytes.
type ByteSize struct {
	resource.Quantity
}

// NewByteSize returns a new ByteSize with a default value in bytes.
func NewByteSize(defaultValue int64) ByteSize {
	return ByteSize{
		Quantity: *resource.NewQuantity(defaultValue, resource.BinarySI),
	}
}

// GetBytes returns the number of bytes in the quantity.
// Note: this operation is expensive, so it's recommended to cache the returned value.
func (q *ByteSize) GetBytes() (int64, error) {
	if q == nil || q.IsZero() {
		return 0, nil
	}

	val, ok := q.AsInt64()
	if !ok {
		return 0, fmt.Errorf("cannot get bytes from resource quantity value '%v'", q)
	}

	return val, nil
}

func toByteSizeHookFunc() mapstructure.DecodeHookFunc {
	bytesizeType := reflect.TypeOf(ByteSize{})
	bytesizePtrType := reflect.TypeOf(&ByteSize{})

	return func(
		f reflect.Type,
		t reflect.Type,
		data any,
	) (any, error) {
		var isPtr bool
		switch t {
		case bytesizeType:
			// Nop
		case bytesizePtrType:
			isPtr = true
		default:
			// Not a type we support with this hook
			return data, nil
		}

		// First, cast to string
		str, err := cast.ToStringE(data)
		if err != nil {
			return nil, fmt.Errorf("failed to cast value to string: %w", err)
		}

		// Parse as quantity
		q, err := resource.ParseQuantity(str)
		if err != nil {
			return nil, fmt.Errorf("value is not a valid quantity: %w", err)
		}

		// Return a pointer if desired
		res := ByteSize{Quantity: q}
		if isPtr {
			return &res, nil
		}
		return res, nil
	}
}
