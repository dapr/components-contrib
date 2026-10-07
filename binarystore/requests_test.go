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
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSetRequestResetReader(t *testing.T) {
	t.Run("seeker", func(t *testing.T) {
		req := &SetRequest{Data: bytes.NewReader([]byte("payload"))}
		_, err := req.Data.Read(make([]byte, 3))
		require.NoError(t, err)
		require.NoError(t, req.ResetReader())

		got, err := io.ReadAll(req.Data)
		require.NoError(t, err)
		require.Equal(t, "payload", string(got))
	})

	t.Run("callback", func(t *testing.T) {
		called := false
		req := &SetRequest{
			Data: strings.NewReader("payload"),
			Reset: func() error {
				called = true
				return nil
			},
		}
		require.NoError(t, req.ResetReader())
		require.True(t, called)
	})

	t.Run("non-seekable", func(t *testing.T) {
		req := &SetRequest{Data: io.NopCloser(strings.NewReader("payload"))}
		require.ErrorIs(t, req.ResetReader(), ErrReaderNotResettable)
	})

	t.Run("callback error", func(t *testing.T) {
		errExpected := errors.New("reset failed")
		req := &SetRequest{
			Data: strings.NewReader("payload"),
			Reset: func() error {
				return errExpected
			},
		}
		require.ErrorIs(t, req.ResetReader(), errExpected)
	})
}
