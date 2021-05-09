// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package config

import (
	"fmt"
)

// Normalize converts map[interface{}]interface{} to map[string]interface{} to normalize
// for JSON and usage in component initialization.
// nolint:cyclop
func Normalize(i interface{}) (interface{}, error) {
	var err error
	switch x := i.(type) {
	case map[interface{}]interface{}:
		m2 := map[string]interface{}{}
		for k, v := range x {
			if strKey, ok := k.(string); ok {
				if m2[strKey], err = Normalize(v); err != nil {
					return nil, err
				}
			} else {
				return nil, fmt.Errorf("error parsing config field: %v", k)
			}
		}

		return m2, nil
	case map[string]interface{}:
		m2 := map[string]interface{}{}
		for k, v := range x {
			if m2[k], err = Normalize(v); err != nil {
				return nil, err
			}
		}

		return m2, nil
	case []interface{}:
		for i, v := range x {
			if x[i], err = Normalize(v); err != nil {
				return nil, err
			}
		}
	}

	return i, nil
}
