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
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
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
		m := &metadata{RemoteURL: "https://example.com/repo.git"}
		s, err := selectAuth(m, logger.NewLogger("test"))
		require.NoError(t, err)
		_, ok := s.(*noneAuth)
		assert.True(t, ok)
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
		s, err := selectAuth(m, logger.NewLogger("test"))
		require.NoError(t, err)
		_, ok := s.(*sshAuth)
		assert.True(t, ok)
	})

	t.Run("ssh missing known_hosts when not insecure", func(t *testing.T) {
		key := generateTestSSHPrivateKey(t)
		m := &metadata{RemoteURL: "git@example.com:org/repo.git", PrivateKey: key}
		_, err := selectAuth(m, logger.NewLogger("test"))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "knownHosts")
	})
}

func TestGitHubAppAuth_RefreshCadence(t *testing.T) {
	keyPEM := generateTestRSAPEM(t)
	appID := int64(123)
	installID := int64(456)
	skew := 5 * time.Minute

	m := &metadata{
		RemoteURL:      "https://github.com/org/repo.git",
		AppID:          &appID,
		InstallationID: &installID,
		PrivateKey:     keyPEM,
		RefreshSkew:    &skew,
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
		RemoteURL:             "git@example.com:org/repo.git",
		PrivateKeyPath:        &keyPath,
		InsecureIgnoreHostKey: &insecure,
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
		RemoteURL:  "git@github.com:org/repo.git",
		PrivateKey: keyPEM,
		KnownHosts: &known,
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
		RemoteURL:      "https://github.com/org/repo.git",
		AppID:          &appID,
		InstallationID: &installID,
		PrivateKeyPath: &keyPath,
	}
	a, err := newGitHubAppAuth(m, logger.NewLogger("test"), func(_ context.Context, _ *http.Client, _ string, _ int64, _ string) (*installationToken, error) {
		return &installationToken{Token: "tok", ExpiresAt: time.Now().Add(time.Hour)}, nil
	})
	require.NoError(t, err)
	_, err = a.AuthMethod(t.Context())
	require.NoError(t, err)
}

// TestInstallationTokenFetcher_RateLimitRetry exercises the 429 / Retry-After
// path: the first response 429s with a short Retry-After; the second
// succeeds. The fetcher must wait, retry, and return the success token.
func TestInstallationTokenFetcher_RateLimitRetry(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		n := hits.Add(1)
		if n == 1 {
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"token":"ghs_recovered","expires_at":"2030-01-01T00:00:00Z"}`))
	}))
	t.Cleanup(srv.Close)

	tok, err := defaultInstallationTokenFetcher(t.Context(), srv.Client(), srv.URL, 42, "jwt-stub")
	require.NoError(t, err)
	require.NotNil(t, tok)
	assert.Equal(t, "ghs_recovered", tok.Token)
	assert.Equal(t, int32(2), hits.Load(), "fetcher must retry exactly once after 429")
}

// TestInstallationTokenFetcher_RateLimitTwice asserts that two consecutive
// 429s surface as a rateLimitError to the caller (used by the poll loop to
// trigger back-off).
func TestInstallationTokenFetcher_RateLimitTwice(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		w.Header().Set("Retry-After", "0")
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	t.Cleanup(srv.Close)

	_, err := defaultInstallationTokenFetcher(t.Context(), srv.Client(), srv.URL, 42, "jwt-stub")
	require.Error(t, err)
	var rl *rateLimitError
	assert.ErrorAs(t, err, &rl, "second 429 must produce a rateLimitError")
}

func TestParseRetryAfter(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		assert.Equal(t, time.Duration(0), parseRetryAfter("", time.Now()))
	})
	t.Run("zero", func(t *testing.T) {
		assert.Equal(t, time.Duration(0), parseRetryAfter("0", time.Now()))
	})
	t.Run("seconds", func(t *testing.T) {
		assert.Equal(t, 30*time.Second, parseRetryAfter("30", time.Now()))
	})
	t.Run("http date in future", func(t *testing.T) {
		future := time.Now().Add(2 * time.Minute).UTC()
		got := parseRetryAfter(future.Format(http.TimeFormat), time.Now())
		// HTTP-date is second-resolution; allow ±2s for the round-trip through
		// http.ParseTime + the wall-clock advance between formatting and parsing.
		diff := got - 2*time.Minute
		if diff < 0 {
			diff = -diff
		}
		assert.LessOrEqual(t, diff, 2*time.Second)
	})
	t.Run("http date in past", func(t *testing.T) {
		past := time.Now().Add(-2 * time.Minute).UTC()
		assert.Equal(t, time.Duration(0), parseRetryAfter(past.Format(http.TimeFormat), time.Now()))
	})
	t.Run("unparseable", func(t *testing.T) {
		assert.Equal(t, time.Duration(0), parseRetryAfter("soon", time.Now()))
	})
}

// TestIsRateLimited covers the 403-with-rate-limit-headers case used for
// GitHub secondary rate limits.
func TestIsRateLimited(t *testing.T) {
	cases := []struct {
		name string
		code int
		hdr  http.Header
		want bool
	}{
		{"429", http.StatusTooManyRequests, http.Header{}, true},
		{"403 with remaining 0", http.StatusForbidden, http.Header{"X-Ratelimit-Remaining": {"0"}}, true},
		{"403 with retry-after", http.StatusForbidden, http.Header{"Retry-After": {"60"}}, true},
		{"403 without rate-limit headers", http.StatusForbidden, http.Header{}, false},
		{"200", http.StatusOK, http.Header{}, false},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			r := &http.Response{StatusCode: tt.code, Header: tt.hdr}
			assert.Equal(t, tt.want, isRateLimited(r))
		})
	}
}

// TestIsTransportRateLimit covers the go-git transport-error pattern check.
func TestIsTransportRateLimit(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"unrelated", errors.New("connection reset"), false},
		{"429 in message", errors.New("unexpected status 429"), true},
		{"rate limit text", errors.New("API rate limit exceeded"), true},
		{"secondary rate limit", errors.New("you have exceeded a secondary rate limit"), true},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, isTransportRateLimit(tt.err))
		})
	}
}

// ensure strconv is referenced (imports list normalised by goimports later).
var _ = strconv.Itoa
