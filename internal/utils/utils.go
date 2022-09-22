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
	"strconv"
	"strings"
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
