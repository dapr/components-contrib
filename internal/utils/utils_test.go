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

import "testing"

func TestGetElemOrDefaultFromMap(t *testing.T) {
	t.Run("test int", func(t *testing.T) {
		testcases := []struct {
			name     string
			m        map[string]string
			key      string
			def      int
			expected int
		}{
			{
				name:     "Get an int value from map that exists",
				m:        map[string]string{"key": "1"},
				key:      "key",
				def:      0,
				expected: 1,
			},
			{
				name:     "Get an int value from map that does not exist",
				m:        map[string]string{"key": "1"},
				key:      "key2",
				def:      0,
				expected: 0,
			},
			{
				name:     "Get an int value from map that exists but is not an int",
				m:        map[string]string{"key": "a"},
				key:      "key",
				def:      0,
				expected: 0,
			},
		}
		for _, tc := range testcases {
			t.Run(tc.name, func(t *testing.T) {
				actual := GetElemOrDefaultFromMap(tc.m, tc.key, tc.def)
				if actual != tc.expected {
					t.Errorf("expected %v, got %v", tc.expected, actual)
				}
			})
		}
	})
	t.Run("test uint64", func(t *testing.T) {
		testcases := []struct {
			name     string
			m        map[string]string
			key      string
			def      uint64
			expected uint64
		}{
			{
				name:     "Get an uint64 value from map that exists",
				m:        map[string]string{"key": "1"},
				key:      "key",
				def:      uint64(0),
				expected: uint64(1),
			},
			{
				name:     "Get an uint64 value from map that does not exist",
				m:        map[string]string{"key": "1"},
				key:      "key2",
				def:      uint64(0),
				expected: uint64(0),
			},
			{
				name:     "Get an int value from map that exists but is not an uint64",
				m:        map[string]string{"key": "-1"},
				key:      "key",
				def:      0,
				expected: 0,
			},
		}

		for _, tc := range testcases {
			t.Run(tc.name, func(t *testing.T) {
				actual := GetElemOrDefaultFromMap(tc.m, tc.key, tc.def)
				if actual != tc.expected {
					t.Errorf("expected %v, got %v", tc.expected, actual)
				}
			})
		}
	})
}
