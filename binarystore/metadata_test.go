/*
Copyright 2026 The Dapr Authors
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

package binarystore

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestObjectPath(t *testing.T) {
	tests := []struct {
		name     string
		prefix   string
		fileName string
		expected string
	}{
		{name: "without prefix", fileName: "file.bin", expected: "file.bin"},
		{name: "prefix without separator", prefix: "objects", fileName: "file.bin", expected: "objects/file.bin"},
		{name: "prefix with separator", prefix: "objects/", fileName: "file.bin", expected: "objects/file.bin"},
		{name: "file name with leading separator", prefix: "objects", fileName: "/file.bin", expected: "objects/file.bin"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, ObjectPath(tt.prefix, tt.fileName))
		})
	}
}
