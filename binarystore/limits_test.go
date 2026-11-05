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
	"bytes"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSizeLimitReader(t *testing.T) {
	t.Run("body under the limit passes through", func(t *testing.T) {
		data, err := io.ReadAll(NewSizeLimitReader(strings.NewReader("payload"), 8))
		require.NoError(t, err)
		assert.Equal(t, []byte("payload"), data)
	})

	t.Run("body of exactly the limit passes through", func(t *testing.T) {
		data, err := io.ReadAll(NewSizeLimitReader(strings.NewReader("payload!"), 8))
		require.NoError(t, err)
		assert.Equal(t, []byte("payload!"), data)
	})

	t.Run("empty body passes through", func(t *testing.T) {
		data, err := io.ReadAll(NewSizeLimitReader(strings.NewReader(""), 8))
		require.NoError(t, err)
		assert.Empty(t, data)
	})

	t.Run("nil body is treated as empty", func(t *testing.T) {
		data, err := io.ReadAll(NewSizeLimitReader(nil, 8))
		require.NoError(t, err)
		assert.Empty(t, data)
	})

	t.Run("body over the limit fails with ErrFileTooLarge", func(t *testing.T) {
		data, err := io.ReadAll(NewSizeLimitReader(strings.NewReader("too large"), 8))
		require.ErrorIs(t, err, ErrFileTooLarge)
		// Only the allowed bytes are passed through before the failure.
		assert.Equal(t, []byte("too larg"), data)
	})

	t.Run("limit is enforced regardless of read buffer size", func(t *testing.T) {
		r := NewSizeLimitReader(bytes.NewReader(bytes.Repeat([]byte("x"), 100)), 10)
		buf := make([]byte, 3)
		var total int
		for {
			n, err := r.Read(buf)
			total += n
			if err != nil {
				require.ErrorIs(t, err, ErrFileTooLarge)
				break
			}
		}
		assert.Equal(t, 10, total)
	})

	t.Run("the error is sticky", func(t *testing.T) {
		r := NewSizeLimitReader(strings.NewReader("too large"), 8)
		_, err := io.ReadAll(r)
		require.ErrorIs(t, err, ErrFileTooLarge)

		n, err := r.Read(make([]byte, 1))
		assert.Zero(t, n)
		require.ErrorIs(t, err, ErrFileTooLarge)
	})

	t.Run("source errors are propagated", func(t *testing.T) {
		r := NewSizeLimitReader(&failingReader{}, 8)
		_, err := io.ReadAll(r)
		require.ErrorIs(t, err, errSourceFailure)
		require.NotErrorIs(t, err, ErrFileTooLarge)
	})

	t.Run("source errors after the limit is exhausted are propagated", func(t *testing.T) {
		// The probe read after the allowance is exhausted must surface a
		// genuine source error rather than report ErrFileTooLarge.
		r := NewSizeLimitReader(&failingReader{}, 0)
		_, err := io.ReadAll(r)
		require.ErrorIs(t, err, errSourceFailure)
		require.NotErrorIs(t, err, ErrFileTooLarge)
	})
}

var errSourceFailure = io.ErrUnexpectedEOF

type failingReader struct{}

func (*failingReader) Read([]byte) (int, error) {
	return 0, errSourceFailure
}
