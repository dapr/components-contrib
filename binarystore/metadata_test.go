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
	"github.com/stretchr/testify/require"
)

func TestObjectPath(t *testing.T) {
	tests := []struct {
		name        string
		prefix      string
		fileName    string
		expected    string
		expectedErr error
	}{
		{name: "without prefix", fileName: "file.bin", expected: "file.bin"},
		{name: "prefix without separator", prefix: "objects", fileName: "file.bin", expected: "objects/file.bin"},
		{name: "nested prefix", prefix: "tenant/objects", fileName: "file.bin", expected: "tenant/objects/file.bin"},
		{name: "nested file name", prefix: "objects", fileName: "images/file.bin", expected: "objects/images/file.bin"},
		{name: "empty file name", prefix: "objects", expectedErr: ErrMissingFileName},
		{name: "prefix with leading separator", prefix: "/objects", fileName: "file.bin", expectedErr: ErrInvalidPrefix},
		{name: "prefix with trailing separator", prefix: "objects/", fileName: "file.bin", expectedErr: ErrInvalidPrefix},
		{name: "file name with leading separator", prefix: "objects", fileName: "/file.bin", expectedErr: ErrInvalidFileName},
		{name: "file name with trailing separator", prefix: "objects", fileName: "file.bin/", expectedErr: ErrInvalidFileName},
		{name: "file name with leading separator without prefix", fileName: "/file.bin", expectedErr: ErrInvalidFileName},
		{name: "file name with trailing separator without prefix", fileName: "file.bin/", expectedErr: ErrInvalidFileName},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path, err := ObjectPath(tt.prefix, tt.fileName)
			if tt.expectedErr != nil {
				require.ErrorIs(t, err, tt.expectedErr)
				assert.Empty(t, path)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.expected, path)
		})
	}
}
