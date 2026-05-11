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
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"net/http"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	githttp "github.com/go-git/go-git/v5/plumbing/transport/http"
	xssh "golang.org/x/crypto/ssh"

	"github.com/dapr/kit/logger"
)

func signerFromPrivateKey(priv *rsa.PrivateKey) (xssh.Signer, error) {
	return xssh.NewSignerFromKey(priv)
}

func base64Encode(b []byte) string { return base64.StdEncoding.EncodeToString(b) }

func TestSelectAuth(t *testing.T) {
	t.Run("none", func(t *testing.T) {
		m := &metadata{URL: "https://example.com/repo.git"}
		s, err := selectAuth(m, logger.NewLogger("test"))
		require.NoError(t, err)
		_, ok := s.(*noneAuth)
		assert.True(t, ok)
		am, err := s.AuthMethod(t.Context())
		require.NoError(t, err)
		assert.Nil(t, am)
	})

	t.Run("pat with default username", func(t *testing.T) {
		m := &metadata{URL: "https://example.com/repo.git", Token: "ghp_abc"}
		s, err := selectAuth(m, logger.NewLogger("test"))
		require.NoError(t, err)
		am, err := s.AuthMethod(t.Context())
		require.NoError(t, err)
		ba, ok := am.(*githttp.BasicAuth)
		require.True(t, ok)
		assert.Equal(t, "x-access-token", ba.Username)
		assert.Equal(t, "ghp_abc", ba.Password)
	})

	t.Run("pat with explicit username", func(t *testing.T) {
		user := "alice"
		m := &metadata{URL: "https://example.com/repo.git", Username: &user, Token: "ghp_abc"}
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
		m := &metadata{URL: "git@example.com:org/repo.git", SSHPrivateKey: key, SSHInsecureIgnoreHostKey: &insecure}
		s, err := selectAuth(m, logger.NewLogger("test"))
		require.NoError(t, err)
		_, ok := s.(*sshAuth)
		assert.True(t, ok)
	})

	t.Run("ssh missing known_hosts when not insecure", func(t *testing.T) {
		key := generateTestSSHPrivateKey(t)
		m := &metadata{URL: "git@example.com:org/repo.git", SSHPrivateKey: key}
		_, err := selectAuth(m, logger.NewLogger("test"))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "sshKnownHosts")
	})
}

func TestGitHubAppAuth_RefreshCadence(t *testing.T) {
	keyPEM := generateTestRSAPEM(t)
	appID := int64(123)
	installID := int64(456)
	skew := 5 * time.Minute

	m := &metadata{
		URL:                     "https://github.com/org/repo.git",
		GithubAppID:             &appID,
		GithubAppInstallationID: &installID,
		GithubAppPrivateKey:     keyPEM,
		GithubAppRefreshSkew:    &skew,
	}

	var calls atomic.Int32
	expiry := time.Now().Add(1 * time.Hour)
	fetcher := func(_ context.Context, _ *http.Client, _ string, _ int64, _ string) (*installationToken, error) {
		calls.Add(1)
		return &installationToken{Token: "ghs_abc", ExpiresAt: expiry}, nil
	}

	a, err := newGitHubAppAuth(m, logger.NewLogger("test"), fetcher)
	require.NoError(t, err)

	// First call mints JWT and fetches.
	for range 5 {
		am, methodErr := a.AuthMethod(t.Context())
		require.NoError(t, methodErr)
		ba := am.(*githttp.BasicAuth)
		assert.Equal(t, "x-access-token", ba.Username)
		assert.Equal(t, "ghs_abc", ba.Password)
	}
	assert.Equal(t, int32(1), calls.Load(), "should reuse cached token until skew window")

	// Force expiry within skew → should refresh.
	a.mu.Lock()
	a.cached.ExpiresAt = time.Now().Add(skew - time.Minute)
	a.mu.Unlock()

	_, err = a.AuthMethod(t.Context())
	require.NoError(t, err)
	assert.Equal(t, int32(2), calls.Load(), "should refresh once expiry is within skew")
}

// generateTestRSAPEM produces a fresh PEM-encoded RSA private key. Used to
// keep the test self-contained and avoid committing test keys.
func generateTestRSAPEM(t *testing.T) string {
	t.Helper()
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	der := x509.MarshalPKCS1PrivateKey(priv)
	block := &pem.Block{Type: "RSA PRIVATE KEY", Bytes: der}
	return string(pem.EncodeToMemory(block))
}

// generateTestSSHPrivateKey returns a PEM-encoded private key suitable for
// gitssh.NewPublicKeys.
func generateTestSSHPrivateKey(t *testing.T) string {
	t.Helper()
	return generateTestRSAPEM(t)
}

func TestSSHAuth_LoadKeyFromPath(t *testing.T) {
	dir := t.TempDir()
	keyPath := dir + "/key"
	require.NoError(t, os.WriteFile(keyPath, []byte(generateTestSSHPrivateKey(t)), 0o600))

	insecure := true
	m := &metadata{
		URL:                      "git@example.com:org/repo.git",
		SSHPrivateKeyPath:        &keyPath,
		SSHInsecureIgnoreHostKey: &insecure,
	}
	s, err := selectAuth(m, logger.NewLogger("test"))
	require.NoError(t, err)
	require.IsType(t, &sshAuth{}, s)
}

func TestSSHAuth_KnownHostsFromInlineString(t *testing.T) {
	// Build a known_hosts entry using a freshly generated key and verify the
	// SSH auth strategy successfully constructs a HostKeyCallback from it.
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	signer, err := signerFromPrivateKey(priv)
	require.NoError(t, err)
	known := "github.com " + signer.PublicKey().Type() + " " + base64Encode(signer.PublicKey().Marshal()) + "\n"

	keyPEM := generateTestRSAPEM(t)
	m := &metadata{
		URL:           "git@github.com:org/repo.git",
		SSHPrivateKey: keyPEM,
		SSHKnownHosts: &known,
	}
	s, err := selectAuth(m, logger.NewLogger("test"))
	require.NoError(t, err)
	require.IsType(t, &sshAuth{}, s)
}

func TestGitHubAppAuth_LoadKeyFromPath(t *testing.T) {
	dir := t.TempDir()
	keyPath := dir + "/app.pem"
	require.NoError(t, os.WriteFile(keyPath, []byte(generateTestRSAPEM(t)), 0o600))

	appID := int64(1)
	installID := int64(2)
	m := &metadata{
		URL:                     "https://github.com/org/repo.git",
		GithubAppID:             &appID,
		GithubAppInstallationID: &installID,
		GithubAppPrivateKeyPath: &keyPath,
	}
	a, err := newGitHubAppAuth(m, logger.NewLogger("test"), func(_ context.Context, _ *http.Client, _ string, _ int64, _ string) (*installationToken, error) {
		return &installationToken{Token: "tok", ExpiresAt: time.Now().Add(time.Hour)}, nil
	})
	require.NoError(t, err)
	_, err = a.AuthMethod(t.Context())
	require.NoError(t, err)
}
