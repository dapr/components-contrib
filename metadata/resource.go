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

func toResourceHookFunc() mapstructure.DecodeHookFunc {
	quantityType := reflect.TypeOf(resource.Quantity{})
	quantityPtrType := reflect.TypeOf(&resource.Quantity{})

	return func(
		f reflect.Type,
		t reflect.Type,
		data any,
	) (any, error) {
		var isPtr bool
		switch t {
		case quantityType:
			// Nop
		case quantityPtrType:
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
		if isPtr {
			return &q, nil
		}
		return q, nil
	}
}
