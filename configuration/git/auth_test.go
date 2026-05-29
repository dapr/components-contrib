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

package git

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	githttp "github.com/go-git/go-git/v5/plumbing/transport/http"

	"github.com/dapr/kit/logger"
)

// TestSelectAuth verifies that selectAuth (which wires metadata into the
// auth sub-package) picks the correct strategy and produces the expected
// AuthMethod. Tests for the strategies themselves live in
// configuration/git/auth.
func TestSelectAuth(t *testing.T) {
	t.Run("none", func(t *testing.T) {
		m := &metadata{RemoteURL: "https://example.com/repo.git"}
		s, err := selectAuth(m, logger.NewLogger("test"))
		require.NoError(t, err)
		am, err := s.AuthMethod(t.Context())
		require.NoError(t, err)
		assert.Nil(t, am)
	})

	t.Run("pat classic token", func(t *testing.T) {
		m := &metadata{RemoteURL: "https://example.com/repo.git", Token: "ghp_classic1234567890abcdef"}
		s, err := selectAuth(m, logger.NewLogger("test"))
		require.NoError(t, err)
		am, err := s.AuthMethod(t.Context())
		require.NoError(t, err)
		ba, ok := am.(*githttp.BasicAuth)
		require.True(t, ok)
		assert.Equal(t, "x-access-token", ba.Username)
		assert.Equal(t, "ghp_classic1234567890abcdef", ba.Password)
	})

	t.Run("pat fine-grained token", func(t *testing.T) {
		// Fine-grained PATs use the `github_pat_` prefix. They go through
		// the same HTTP Basic Auth path and must produce an identically
		// shaped AuthMethod.
		m := &metadata{
			RemoteURL: "https://example.com/repo.git",
			Token:     "github_pat_finegrained_abcdef1234567890",
		}
		s, err := selectAuth(m, logger.NewLogger("test"))
		require.NoError(t, err)
		am, err := s.AuthMethod(t.Context())
		require.NoError(t, err)
		ba, ok := am.(*githttp.BasicAuth)
		require.True(t, ok)
		assert.Equal(t, "x-access-token", ba.Username)
		assert.Equal(t, "github_pat_finegrained_abcdef1234567890", ba.Password)
	})

	t.Run("pat with explicit username", func(t *testing.T) {
		user := "alice"
		m := &metadata{RemoteURL: "https://example.com/repo.git", Username: &user, Token: "ghp_abc"}
		s, err := selectAuth(m, logger.NewLogger("test"))
		require.NoError(t, err)
		am, err := s.AuthMethod(t.Context())
		require.NoError(t, err)
		ba := am.(*githttp.BasicAuth)
		assert.Equal(t, "alice", ba.Username)
	})

	t.Run("auto-detect ssh from url", func(t *testing.T) {
		key := generateTestSSHPrivateKey(t)
		insecure := true
		m := &metadata{RemoteURL: "git@example.com:org/repo.git", PrivateKey: key, InsecureIgnoreHostKey: &insecure}
		_, err := selectAuth(m, logger.NewLogger("test"))
		require.NoError(t, err)
	})

	t.Run("ssh missing known_hosts when not insecure", func(t *testing.T) {
		key := generateTestSSHPrivateKey(t)
		m := &metadata{RemoteURL: "git@example.com:org/repo.git", PrivateKey: key}
		_, err := selectAuth(m, logger.NewLogger("test"))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "knownHosts")
	})
}

// generateTestSSHPrivateKey returns a PEM-encoded RSA private key suitable
// for the SSH auth strategy in tests. Used here to keep TestSelectAuth
// self-contained.
func generateTestSSHPrivateKey(t *testing.T) string {
	t.Helper()
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	der := x509.MarshalPKCS1PrivateKey(priv)
	block := &pem.Block{Type: "RSA PRIVATE KEY", Bytes: der}
	return string(pem.EncodeToMemory(block))
}
