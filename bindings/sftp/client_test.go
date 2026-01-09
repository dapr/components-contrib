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

package sftp

import (
	"errors"
	"fmt"
	"testing"

	sftpClient "github.com/pkg/sftp"
	"github.com/stretchr/testify/assert"
)

func TestShouldReconnect(t *testing.T) {
	c := &Client{}

	t.Run("nil error should not reconnect", func(t *testing.T) {
		assert.False(t, c.shouldReconnect(nil))
	})

	t.Run("StatusError should not reconnect", func(t *testing.T) {
		// StatusError with permission denied code (3)
		statusErr := &sftpClient.StatusError{
			Code: 3, // SSH_FX_PERMISSION_DENIED
		}
		assert.False(t, c.shouldReconnect(statusErr))
	})

	t.Run("wrapped StatusError should not reconnect", func(t *testing.T) {
		// StatusError with no such file code (2)
		statusErr := &sftpClient.StatusError{
			Code: 2, // SSH_FX_NO_SUCH_FILE
		}
		wrappedErr := fmt.Errorf("operation failed: %w", statusErr)
		assert.False(t, c.shouldReconnect(wrappedErr))
	})

	t.Run("ErrSSHFxPermissionDenied should not reconnect", func(t *testing.T) {
		assert.False(t, c.shouldReconnect(sftpClient.ErrSSHFxPermissionDenied))
	})

	t.Run("ErrSSHFxNoSuchFile should not reconnect", func(t *testing.T) {
		assert.False(t, c.shouldReconnect(sftpClient.ErrSSHFxNoSuchFile))
	})

	t.Run("ErrSSHFxOpUnsupported should not reconnect", func(t *testing.T) {
		assert.False(t, c.shouldReconnect(sftpClient.ErrSSHFxOpUnsupported))
	})

	t.Run("ErrSSHFxFailure should not reconnect", func(t *testing.T) {
		assert.False(t, c.shouldReconnect(sftpClient.ErrSSHFxFailure))
	})

	t.Run("ErrSSHFxBadMessage should not reconnect", func(t *testing.T) {
		assert.False(t, c.shouldReconnect(sftpClient.ErrSSHFxBadMessage))
	})

	t.Run("ErrSSHFxEOF should not reconnect", func(t *testing.T) {
		assert.False(t, c.shouldReconnect(sftpClient.ErrSSHFxEOF))
	})

	t.Run("permission denied string should not reconnect", func(t *testing.T) {
		err := errors.New("sftp: permission denied")
		assert.False(t, c.shouldReconnect(err))
	})

	t.Run("no such file string should not reconnect", func(t *testing.T) {
		err := errors.New("sftp: no such file or directory")
		assert.False(t, c.shouldReconnect(err))
	})

	t.Run("not a directory string should not reconnect", func(t *testing.T) {
		err := errors.New("mkdir upload/: not a directory")
		assert.False(t, c.shouldReconnect(err))
	})

	t.Run("file exists string should not reconnect", func(t *testing.T) {
		err := errors.New("sftp: file exists error")
		assert.False(t, c.shouldReconnect(err))
	})

	t.Run("bad message string should not reconnect", func(t *testing.T) {
		err := errors.New("sftp: bad message")
		assert.False(t, c.shouldReconnect(err))
	})

	t.Run("connection reset should reconnect", func(t *testing.T) {
		err := errors.New("connection reset by peer")
		assert.True(t, c.shouldReconnect(err))
	})

	t.Run("EOF should reconnect", func(t *testing.T) {
		err := errors.New("EOF")
		assert.True(t, c.shouldReconnect(err))
	})

	t.Run("broken pipe should reconnect", func(t *testing.T) {
		err := errors.New("write: broken pipe")
		assert.True(t, c.shouldReconnect(err))
	})

	t.Run("connection refused should reconnect", func(t *testing.T) {
		err := errors.New("connection refused")
		assert.True(t, c.shouldReconnect(err))
	})

	t.Run("timeout should reconnect", func(t *testing.T) {
		err := errors.New("i/o timeout")
		assert.True(t, c.shouldReconnect(err))
	})

	t.Run("unknown error should reconnect", func(t *testing.T) {
		err := errors.New("unknown transport error")
		assert.True(t, c.shouldReconnect(err))
	})
}

