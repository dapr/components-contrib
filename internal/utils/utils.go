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

package utils

import (
	"encoding/json"
	"strconv"
	"strings"

	"github.com/spf13/cast"
)

// IsTruthy returns true if a string is a truthy value.
// Truthy values are "y", "yes", "true", "t", "on", "1" (case-insensitive); everything else is false.
func IsTruthy(val string) bool {
	switch strings.ToLower(strings.TrimSpace(val)) {
	case "y", "yes", "true", "t", "on", "1":
		return true
	default:
		return false
	}
}

// GetElemOrDefaultFromMap returns the value of a key from a map, or a default value
// if the key does not exist or the value is not of the expected type.
func GetElemOrDefaultFromMap[T int | uint64](m map[string]string, key string, def T) T {
	if val, ok := m[key]; ok {
		switch any(def).(type) {
		case int:
			if ival, err := strconv.ParseInt(val, 10, 64); err == nil {
				return T(ival)
			}
		case uint64:
			if uval, err := strconv.ParseUint(val, 10, 64); err == nil {
				return T(uval)
			}
		}
	}
	return def
}

// GetIntValOrDefault returns an int value if greater than 0 OR default value.
func GetIntValOrDefault(val int, defaultValue int) int {
	if val > 0 {
		return val
	}
	return defaultValue
}

// GetIntValFromString returns an int value if the string is not empty and is a correct integer OR default value.
func GetIntValFromString(val string, defaultValue int) int {
	if val != "" {
		if i, err := strconv.Atoi(val); err == nil {
			return i
		}
	}
	return defaultValue
}

// Unquote parses a request data that may be quoted due to JSON encoding, and removes the quotes.
func Unquote(data []byte) (res string) {
	var dataObj any
	err := json.Unmarshal(data, &dataObj)
	if err != nil {
		// If data can't be un-marshalled, keep as-is
		res = string(data)
	} else {
		// Try casting to string
		res, err = cast.ToStringE(dataObj)
		if err != nil {
			// If dataObj can't be cast to string, keep as-is
			res = string(data)
		}
	}
	return res
}
