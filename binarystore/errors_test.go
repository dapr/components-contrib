/*
Copyright 2025 The Dapr Authors
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
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The sentinel errors and the SetRequest defaults are shared by every
// provider, so they are asserted once here rather than repeated per component.

func TestSentinelErrorsWrap(t *testing.T) {
	for name, err := range map[string]error{
		"ErrFileAlreadyExists":   ErrFileAlreadyExists,
		"ErrFileNotFound":        ErrFileNotFound,
		"ErrMissingFileName":     ErrMissingFileName,
		"ErrReaderNotResettable": ErrReaderNotResettable,
	} {
		t.Run(name, func(t *testing.T) {
			require.ErrorIs(t, fmt.Errorf("provider context: %w", err), err)
		})
	}
}

func TestSentinelErrorsAreDistinct(t *testing.T) {
	all := []error{ErrFileAlreadyExists, ErrFileNotFound, ErrMissingFileName, ErrReaderNotResettable}
	for i, outer := range all {
		for j, inner := range all {
			if i == j {
				continue
			}
			assert.NotEqual(t, outer, inner)
			assert.False(t, errors.Is(outer, inner))
		}
	}
}

func TestSetRequestOverwriteDefaultsToCreateOnly(t *testing.T) {
	req := &SetRequest{FileName: "test.bin", Data: strings.NewReader("hello")}
	assert.False(t, req.Overwrite, "zero value of Overwrite must be false (create-only semantics)")

	req.Overwrite = true
	assert.True(t, req.Overwrite)
}

func TestDefaultUploadTuning(t *testing.T) {
	// Providers pass these to their SDKs explicitly, so a change here changes
	// every component's buffering behaviour at once. The part size must stay
	// at or above OCI Object Storage's 10 MiB minimum for non-final multipart
	// parts, otherwise uploads larger than one part fail on that provider.
	assert.Equal(t, int64(16*1024*1024), DefaultUploadPartSize)
	assert.Equal(t, 2, DefaultUploadConcurrency)
	assert.GreaterOrEqual(t, DefaultUploadPartSize, int64(10*1024*1024),
		"part size must satisfy the largest provider minimum part size (OCI, 10 MiB)")
}
