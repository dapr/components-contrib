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

func TestGetIntOrDefault(t *testing.T) {
	testMap := map[string]string{"key1": "1", "key2": "2", "key3": "3"}
	tcs := []struct {
		name     string
		m        map[string]string
		key      string
		def      int
		expected int
	}{
		{
			name:     "key exists in the map",
			m:        testMap,
			key:      "key2",
			def:      0,
			expected: 2,
		},
		{
			name:     "key does not exist in the map, default value is used",
			m:        testMap,
			key:      "key4",
			def:      4,
			expected: 4,
		},
		{
			name:     "empty map, default value is used",
			m:        map[string]string{},
			key:      "key1",
			def:      100,
			expected: 100,
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			actual := GetIntOrDefault(tc.m, tc.key, tc.def)
			if actual != tc.expected {
				t.Errorf("expected %d, actual %d", tc.expected, actual)
			}
		})
	}
}
