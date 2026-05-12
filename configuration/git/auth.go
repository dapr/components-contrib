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
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-git/go-git/v5/plumbing/transport"
	githttp "github.com/go-git/go-git/v5/plumbing/transport/http"
	gitssh "github.com/go-git/go-git/v5/plumbing/transport/ssh"
	"github.com/lestrrat-go/jwx/v2/jwa"
	"github.com/lestrrat-go/jwx/v2/jwt"
	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/knownhosts"

	"github.com/dapr/kit/logger"
)

// authStrategy resolves a go-git AuthMethod, refreshing internal credentials
// over time if needed (GitHub App installation tokens have a 1h TTL).
type authStrategy interface {
	AuthMethod(ctx context.Context) (transport.AuthMethod, error)
	Close() error
}

// selectAuth resolves the active auth strategy from metadata. log is
// guaranteed non-nil by callers (it threads through from the Store's
// constructor, which always supplies a logger).
func selectAuth(m *metadata, log logger.Logger) (authStrategy, error) {
	switch m.resolveAuthMode() {
	case authModeNone:
		return &noneAuth{}, nil
	case authModePAT:
		return newPATAuth(m), nil
	case authModeSSH:
		return newSSHAuth(m, log)
	case authModeGithubApp:
		return newGitHubAppAuth(m, log, defaultInstallationTokenFetcher)
	}
	return nil, fmt.Errorf("unsupported auth mode %q", m.resolveAuthMode())
}

// --------------- none ---------------

type noneAuth struct{}

func (*noneAuth) AuthMethod(context.Context) (transport.AuthMethod, error) { return nil, nil }
func (*noneAuth) Close() error                                             { return nil }

// --------------- pat ---------------

type patAuth struct {
	username string
	token    string
}

func newPATAuth(m *metadata) *patAuth {
	user := "x-access-token"
	if m.Username != nil && *m.Username != "" {
		user = *m.Username
	}
	return &patAuth{username: user, token: m.Token}
}

func (a *patAuth) AuthMethod(context.Context) (transport.AuthMethod, error) {
	return &githttp.BasicAuth{Username: a.username, Password: a.token}, nil
}

func (a *patAuth) Close() error { return nil }

// --------------- ssh ---------------

type sshAuth struct {
	user       string
	keys       *gitssh.PublicKeys
	knownHosts ssh.HostKeyCallback
	insecure   bool
}

func newSSHAuth(m *metadata, log logger.Logger) (*sshAuth, error) {
	keyBytes, err := loadSSHKey(m)
	if err != nil {
		return nil, fmt.Errorf("ssh: %w", err)
	}
	pk, err := gitssh.NewPublicKeys(m.user(), keyBytes, m.Passphrase)
	if err != nil {
		return nil, fmt.Errorf("ssh: parse private key: %w", err)
	}
	a := &sshAuth{user: m.user(), keys: pk}

	if m.insecureIgnoreHostKey() {
		log.Warnf("git ssh: insecureIgnoreHostKey=true — host key verification is DISABLED. Do not use in production.")
		a.insecure = true
		// Explicitly opting in to disabled host-key verification because the
		// operator set insecureIgnoreHostKey=true. Loud-logged above.
		pk.HostKeyCallback = ssh.InsecureIgnoreHostKey() //nolint:gosec // G106: documented opt-in via metadata
		return a, nil
	}

	cb, err := buildKnownHostsCallback(m)
	if err != nil {
		return nil, fmt.Errorf("ssh: known_hosts: %w", err)
	}
	pk.HostKeyCallback = cb
	a.knownHosts = cb
	return a, nil
}

func (a *sshAuth) AuthMethod(context.Context) (transport.AuthMethod, error) {
	return a.keys, nil
}

func (a *sshAuth) Close() error { return nil }

func loadSSHKey(m *metadata) ([]byte, error) {
	return loadKeyMaterial("ssh", m.PrivateKey, m.PrivateKeyPath)
}

func buildKnownHostsCallback(m *metadata) (ssh.HostKeyCallback, error) {
	if m.KnownHostsPath != nil && *m.KnownHostsPath != "" {
		return knownhosts.New(*m.KnownHostsPath)
	}
	if m.KnownHosts != nil && *m.KnownHosts != "" {
		// `knownhosts.New` requires a file path. Persist the inline data to a
		// temp file just long enough to hand it off; the file is removed
		// before we return regardless of outcome.
		f, err := os.CreateTemp("", "dapr-knownhosts-*")
		if err != nil {
			return nil, fmt.Errorf("create knownHosts temp file: %w", err)
		}
		path := f.Name()
		defer func() {
			_ = f.Close()
			_ = os.Remove(path)
		}()
		if _, err := f.WriteString(*m.KnownHosts); err != nil {
			return nil, fmt.Errorf("write knownHosts: %w", err)
		}
		if err := f.Close(); err != nil {
			return nil, fmt.Errorf("close knownHosts: %w", err)
		}
		return knownhosts.New(path)
	}
	return nil, errors.New("no knownHosts/knownHostsPath configured and insecureIgnoreHostKey is false")
}

// --------------- githubApp ---------------

type installationToken struct {
	Token     string
	ExpiresAt time.Time
}

// installationTokenFetcher exchanges a JWT for a GitHub App installation
// token. It is injectable so tests can avoid real HTTP. The `client`
// argument is supplied by the caller so connection pooling is reused across
// refreshes; tests pass nil and use a stub fetcher.
type installationTokenFetcher func(ctx context.Context, client *http.Client, apiBase string, installationID int64, jwtToken string) (*installationToken, error)

type githubAppAuth struct {
	appID          int64
	installationID int64
	apiBase        string
	privateKey     *rsa.PrivateKey
	skew           time.Duration
	fetcher        installationTokenFetcher
	httpClient     *http.Client
	log            logger.Logger

	mu     sync.Mutex
	cached *installationToken
}

func newGitHubAppAuth(m *metadata, log logger.Logger, fetcher installationTokenFetcher) (*githubAppAuth, error) {
	keyBytes, err := loadGithubAppKey(m)
	if err != nil {
		return nil, fmt.Errorf("githubApp: %w", err)
	}
	priv, err := parseRSAPrivateKey(keyBytes)
	if err != nil {
		return nil, fmt.Errorf("githubApp: parse private key: %w", err)
	}
	return &githubAppAuth{
		appID:          *m.AppID,
		installationID: *m.InstallationID,
		apiBase:        m.apiBase(),
		privateKey:     priv,
		skew:           m.refreshSkew(),
		fetcher:        fetcher,
		httpClient:     &http.Client{Timeout: 10 * time.Second},
		log:            log,
	}, nil
}

func (a *githubAppAuth) AuthMethod(ctx context.Context) (transport.AuthMethod, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.cached != nil && time.Until(a.cached.ExpiresAt) > a.skew {
		return basicAuthFromInstallation(a.cached.Token), nil
	}
	jwtToken, err := a.mintJWT()
	if err != nil {
		return nil, fmt.Errorf("githubApp: mint JWT: %w", err)
	}
	tok, err := a.fetcher(ctx, a.httpClient, a.apiBase, a.installationID, jwtToken)
	if err != nil {
		return nil, fmt.Errorf("githubApp: exchange installation token: %w", err)
	}
	a.cached = tok
	return basicAuthFromInstallation(tok.Token), nil
}

func (a *githubAppAuth) Close() error { return nil }

func (a *githubAppAuth) mintJWT() (string, error) {
	now := time.Now()
	tok, err := jwt.NewBuilder().
		Issuer(strconv.FormatInt(a.appID, 10)).
		IssuedAt(now.Add(-30 * time.Second)).
		Expiration(now.Add(10 * time.Minute)).
		Build()
	if err != nil {
		return "", fmt.Errorf("build jwt: %w", err)
	}
	signed, err := jwt.Sign(tok, jwt.WithKey(jwa.RS256, a.privateKey))
	if err != nil {
		return "", fmt.Errorf("sign jwt: %w", err)
	}
	return string(signed), nil
}

// parseRSAPrivateKey decodes a PEM-encoded RSA private key. Accepts both
// PKCS#1 ("RSA PRIVATE KEY") and PKCS#8 ("PRIVATE KEY") encodings — GitHub
// Apps can be downloaded in either form.
func parseRSAPrivateKey(pemBytes []byte) (*rsa.PrivateKey, error) {
	block, _ := pem.Decode(pemBytes)
	if block == nil {
		return nil, errors.New("no PEM block found")
	}
	if key, err := x509.ParsePKCS1PrivateKey(block.Bytes); err == nil {
		return key, nil
	}
	parsed, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("parse private key: %w", err)
	}
	rsaKey, ok := parsed.(*rsa.PrivateKey)
	if !ok {
		return nil, fmt.Errorf("expected RSA private key, got %T", parsed)
	}
	return rsaKey, nil
}

func basicAuthFromInstallation(token string) *githttp.BasicAuth {
	return &githttp.BasicAuth{Username: "x-access-token", Password: token}
}

func loadGithubAppKey(m *metadata) ([]byte, error) {
	return loadKeyMaterial("githubApp", m.PrivateKey, m.PrivateKeyPath)
}

// loadKeyMaterial returns inline PEM bytes if non-empty, else reads them
// from the supplied path. Returns an error if neither is provided.
func loadKeyMaterial(kind, inline string, path *string) ([]byte, error) {
	if inline != "" {
		return []byte(inline), nil
	}
	if path != nil && *path != "" {
		return os.ReadFile(*path)
	}
	return nil, fmt.Errorf("%s: inline key or path is required", kind)
}

// rateLimitError indicates the GitHub API rejected the request with a
// 429-style rate-limit status. RetryAfter conveys the duration the server
// asked us to wait (zero if no Retry-After header was present, in which
// case the caller should fall back to its configured retry-after default).
type rateLimitError struct {
	RetryAfter time.Duration
	Err        error
}

func (e *rateLimitError) Error() string {
	if e.RetryAfter > 0 {
		return fmt.Sprintf("github api rate-limited (retry-after %s): %v", e.RetryAfter, e.Err)
	}
	return fmt.Sprintf("github api rate-limited: %v", e.Err)
}

func (e *rateLimitError) Unwrap() error { return e.Err }

// defaultInstallationTokenFetcher exchanges a JWT for an installation token
// against the real GitHub API. Used unless replaced for testing.
//
// On a 429 response (or 403 with rate-limit headers — GitHub uses 403 for
// secondary rate limits), Retry-After is parsed and the call is retried
// once. If the second attempt also fails with a rate-limit response, a
// `*rateLimitError` is returned so the poll loop can back off.
//
// Error messages exclude the response body — a misconfigured or compromised
// apiBase could echo sensitive content back. The 64 KiB LimitReader caps
// memory in case the body is unexpectedly large.
func defaultInstallationTokenFetcher(ctx context.Context, client *http.Client, apiBase string, installationID int64, jwtToken string) (*installationToken, error) {
	if client == nil {
		client = &http.Client{Timeout: 10 * time.Second}
	}
	tok, err := doInstallationTokenRequest(ctx, client, apiBase, installationID, jwtToken)
	var rateErr *rateLimitError
	if errors.As(err, &rateErr) && rateErr.RetryAfter > 0 {
		// Wait for the server-suggested duration, then retry once.
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(rateErr.RetryAfter):
		}
		tok, err = doInstallationTokenRequest(ctx, client, apiBase, installationID, jwtToken)
	}
	return tok, err
}

func doInstallationTokenRequest(ctx context.Context, client *http.Client, apiBase string, installationID int64, jwtToken string) (*installationToken, error) {
	url := fmt.Sprintf("%s/app/installations/%d/access_tokens", apiBase, installationID)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+jwtToken)
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("X-GitHub-Api-Version", "2022-11-28")

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 64*1024))
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}

	if isRateLimited(resp) {
		return nil, &rateLimitError{
			RetryAfter: parseRetryAfter(resp.Header.Get("Retry-After"), time.Now()),
			Err:        fmt.Errorf("github api: status %d (installation %d)", resp.StatusCode, installationID),
		}
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("github api: status %d (installation %d)", resp.StatusCode, installationID)
	}

	var payload struct {
		Token     string    `json:"token"`
		ExpiresAt time.Time `json:"expires_at"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}
	if payload.Token == "" {
		return nil, errors.New("github api returned empty token")
	}
	return &installationToken{Token: payload.Token, ExpiresAt: payload.ExpiresAt}, nil
}

// isRateLimited returns true if the response indicates the request was
// rate-limited by GitHub. GitHub uses both 429 and 403 (with the
// X-RateLimit-Remaining: 0 header) for primary and secondary rate limits.
func isRateLimited(resp *http.Response) bool {
	if resp.StatusCode == http.StatusTooManyRequests {
		return true
	}
	if resp.StatusCode == http.StatusForbidden &&
		(resp.Header.Get("X-RateLimit-Remaining") == "0" ||
			resp.Header.Get("Retry-After") != "") {
		return true
	}
	return false
}

// parseRetryAfter interprets an HTTP Retry-After header value, which may be
// either an integer number of seconds or an HTTP-date. Returns 0 when the
// header is empty or unparseable.
func parseRetryAfter(v string, now time.Time) time.Duration {
	if v == "" {
		return 0
	}
	if secs, err := strconv.Atoi(v); err == nil && secs > 0 {
		return time.Duration(secs) * time.Second
	}
	if when, err := http.ParseTime(v); err == nil {
		if d := time.Until(when); d > 0 {
			return d
		}
		// HTTP-date already in the past — fall through to "no wait".
		_ = now
	}
	return 0
}

// isTransportRateLimit returns true if err appears to be a rate-limit
// response from a go-git HTTP/HTTPS transport. go-git doesn't expose
// structured response status, so this is a string-pattern check —
// best-effort but it covers GitHub's typical error shapes.
func isTransportRateLimit(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "429") ||
		strings.Contains(msg, "rate limit") ||
		strings.Contains(msg, "secondary rate limit")
}
