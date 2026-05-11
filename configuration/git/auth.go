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

// selectAuth resolves the configured auth strategy. log may be nil in tests.
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
	return nil, fmt.Errorf("unsupported authMode %q", m.resolveAuthMode())
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
	pk, err := gitssh.NewPublicKeys(m.sshUser(), keyBytes, m.SSHPassphrase)
	if err != nil {
		return nil, fmt.Errorf("ssh: parse private key: %w", err)
	}
	a := &sshAuth{user: m.sshUser(), keys: pk}

	if m.sshInsecureIgnoreHostKey() {
		if log != nil {
			log.Warnf("git ssh: sshInsecureIgnoreHostKey=true — host key verification is DISABLED. Do not use in production.")
		}
		a.insecure = true
		// Explicitly opting in to disabled host-key verification because the
		// operator set sshInsecureIgnoreHostKey=true. Loud-logged above.
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
	return loadKeyMaterial("ssh", m.SSHPrivateKey, m.SSHPrivateKeyPath)
}

func buildKnownHostsCallback(m *metadata) (ssh.HostKeyCallback, error) {
	if m.SSHKnownHostsPath != nil && *m.SSHKnownHostsPath != "" {
		return knownhosts.New(*m.SSHKnownHostsPath)
	}
	if m.SSHKnownHosts != nil && *m.SSHKnownHosts != "" {
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
		if _, err := f.WriteString(*m.SSHKnownHosts); err != nil {
			return nil, fmt.Errorf("write knownHosts: %w", err)
		}
		if err := f.Close(); err != nil {
			return nil, fmt.Errorf("close knownHosts: %w", err)
		}
		return knownhosts.New(path)
	}
	return nil, errors.New("no sshKnownHosts/sshKnownHostsPath configured and sshInsecureIgnoreHostKey is false")
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
		appID:          *m.GithubAppID,
		installationID: *m.GithubAppInstallationID,
		apiBase:        m.githubAppAPIBase(),
		privateKey:     priv,
		skew:           m.githubAppRefreshSkew(),
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
	return loadKeyMaterial("githubApp", m.GithubAppPrivateKey, m.GithubAppPrivateKeyPath)
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

// defaultInstallationTokenFetcher exchanges a JWT for an installation token
// against the real GitHub API. Used unless replaced for testing.
//
// Error messages deliberately exclude the response body — a misconfigured or
// compromised `githubAppApiBase` could echo sensitive content back, and the
// fetcher's errors propagate through to operator-visible logs. The 64 KiB
// LimitReader protects against an oversized body exhausting sidecar memory.
func defaultInstallationTokenFetcher(ctx context.Context, client *http.Client, apiBase string, installationID int64, jwtToken string) (*installationToken, error) {
	url := fmt.Sprintf("%s/app/installations/%d/access_tokens", apiBase, installationID)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+jwtToken)
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("X-GitHub-Api-Version", "2022-11-28")

	if client == nil {
		client = &http.Client{Timeout: 10 * time.Second}
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 64*1024))
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
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
