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

package pubsub

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestNewRetriableError(t *testing.T) {
	t.Run("nil error returns nil", func(t *testing.T) {
		assert.NoError(t, NewRetriableError(nil))
	})

	t.Run("carries codes.Unavailable and is readable via status.FromError", func(t *testing.T) {
		base := errors.New("broker is down")
		err := NewRetriableError(base)

		require.Error(t, err)
		assert.Equal(t, "broker is down", err.Error())

		st, ok := status.FromError(err)
		require.True(t, ok, "status.FromError must extract the gRPC status")
		assert.Equal(t, codes.Unavailable, st.Code())
		assert.Equal(t, "broker is down", st.Message())
	})

	t.Run("Unwrap preserves the underlying error for errors.Is", func(t *testing.T) {
		base := errors.New("broker is down")
		err := NewRetriableError(fmt.Errorf("publish failed: %w", base))

		assert.ErrorIs(t, err, base)
	})
}

func TestNewTerminalError(t *testing.T) {
	t.Run("nil error returns nil", func(t *testing.T) {
		assert.NoError(t, NewTerminalError(nil))
	})

	t.Run("carries codes.FailedPrecondition and is readable via status.FromError", func(t *testing.T) {
		base := errors.New("invalid topic")
		err := NewTerminalError(base)

		require.Error(t, err)
		assert.Equal(t, "invalid topic", err.Error())

		st, ok := status.FromError(err)
		require.True(t, ok)
		assert.Equal(t, codes.FailedPrecondition, st.Code())
	})

	t.Run("Unwrap preserves the underlying error for errors.As", func(t *testing.T) {
		base := &customError{msg: "bad request"}
		err := NewTerminalError(fmt.Errorf("publish failed: %w", base))

		var target *customError
		require.ErrorAs(t, err, &target)
		assert.Equal(t, "bad request", target.msg)
	})
}

func TestHelpersPreserveExistingStatus(t *testing.T) {
	// An error that already carries a gRPC status (as gRPC-native SDKs like GCP return)
	// must keep its precise code rather than being overridden with the helper's code.
	t.Run("retriable preserves existing code", func(t *testing.T) {
		orig := status.Error(codes.PermissionDenied, "denied")

		err := NewRetriableError(orig)

		st, ok := status.FromError(err)
		require.True(t, ok)
		assert.Equal(t, codes.PermissionDenied, st.Code(), "must not override the existing code with Unavailable")
	})

	t.Run("terminal preserves existing code", func(t *testing.T) {
		orig := status.Error(codes.Unavailable, "try later")

		err := NewTerminalError(orig)

		st, ok := status.FromError(err)
		require.True(t, ok)
		assert.Equal(t, codes.Unavailable, st.Code(), "must not override the existing code with FailedPrecondition")
	})

	t.Run("existing code is found through a wrap", func(t *testing.T) {
		orig := status.Error(codes.NotFound, "missing")

		err := NewRetriableError(fmt.Errorf("publish failed: %w", orig))

		st, ok := status.FromError(err)
		require.True(t, ok)
		assert.Equal(t, codes.NotFound, st.Code())
	})
}

func TestCodeErrorCode(t *testing.T) {
	var ce *CodeError
	require.ErrorAs(t, NewRetriableError(errors.New("x")), &ce)
	assert.Equal(t, codes.Unavailable, ce.Code())

	require.ErrorAs(t, NewTerminalError(errors.New("y")), &ce)
	assert.Equal(t, codes.FailedPrecondition, ce.Code())
}

type customError struct {
	msg string
}

func (e *customError) Error() string {
	return e.msg
}
