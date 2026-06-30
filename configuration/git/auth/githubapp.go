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

package auth

import (
	"context"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/go-git/go-git/v5/plumbing/transport"
	githttp "github.com/go-git/go-git/v5/plumbing/transport/http"
	"github.com/lestrrat-go/jwx/v2/jwa"
	"github.com/lestrrat-go/jwx/v2/jwt"

	"github.com/dapr/kit/logger"
)

// GitHubAppConfig collects the parameters required to construct a GitHub
// App auth strategy. Exactly one of PrivateKey or PrivateKeyPath must be
// non-empty.
type GitHubAppConfig struct {
	AppID          int64
	InstallationID int64
	PrivateKey     string        // Inline PEM-encoded RSA private key.
	PrivateKeyPath string        // Path to a PEM-encoded RSA private key on disk.
	APIBase        string        // GitHub API base URL (default "https://api.github.com").
	RefreshSkew    time.Duration // Token refreshes when remaining lifetime is less than this.
}

type githubAppStrategy struct {
	appID          int64
	installationID int64
	apiBase        string
	privateKey     *rsa.PrivateKey
	skew           time.Duration
	fetcher        InstallationTokenFetcher
	httpClient     *http.Client
	log            logger.Logger

	mu     sync.Mutex
	cached *InstallationToken
}

// NewGitHubApp returns a GitHub App auth Strategy. log must be non-nil.
// fetcher is the installation-token exchange function; pass
// DefaultInstallationTokenFetcher unless overriding for tests.
func NewGitHubApp(cfg GitHubAppConfig, log logger.Logger, fetcher InstallationTokenFetcher) (Strategy, error) {
	keyBytes, err := loadKeyMaterial("githubApp", cfg.PrivateKey, cfg.PrivateKeyPath)
	if err != nil {
		return nil, fmt.Errorf("githubApp: %w", err)
	}
	priv, err := parseRSAPrivateKey(keyBytes)
	if err != nil {
		return nil, fmt.Errorf("githubApp: parse private key: %w", err)
	}
	apiBase := cfg.APIBase
	if apiBase == "" {
		apiBase = "https://api.github.com"
	}
	return &githubAppStrategy{
		appID:          cfg.AppID,
		installationID: cfg.InstallationID,
		apiBase:        apiBase,
		privateKey:     priv,
		skew:           cfg.RefreshSkew,
		fetcher:        fetcher,
		httpClient:     &http.Client{Timeout: 10 * time.Second},
		log:            log,
	}, nil
}

func (a *githubAppStrategy) AuthMethod(ctx context.Context) (transport.AuthMethod, error) {
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

func (a *githubAppStrategy) Close() error { return nil }

func (a *githubAppStrategy) mintJWT() (string, error) {
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
