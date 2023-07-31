/*
Copyright 2021 The Dapr Authors
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

package oidc

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/oauth2"
	ccreds "golang.org/x/oauth2/clientcredentials"
	"k8s.io/utils/clock"

	"github.com/dapr/kit/logger"
)

type ClientCredentialsOptions struct {
	Logger       logger.Logger
	TokenURL     string
	ClientID     string
	ClientSecret string
	Scopes       []string
	Audiences    []string
	CAPEM        []byte
}

// ClientCredentials is an OAuth2 Token Source that uses the client_credentials
// grant type to fetch a token.
type ClientCredentials struct {
	log          logger.Logger
	currentToken *oauth2.Token
	httpClient   *http.Client
	fetchTokenFn func(context.Context) (*oauth2.Token, error)

	lock    sync.RWMutex
	wg      sync.WaitGroup
	closeCh chan struct{}
	closed  atomic.Bool
	clock   clock.Clock
}

func NewClientCredentials(ctx context.Context, opts ClientCredentialsOptions) (*ClientCredentials, error) {
	conf, httpClient, err := toConfig(opts)
	if err != nil {
		return nil, err
	}

	token, err := conf.Token(context.WithValue(ctx, oauth2.HTTPClient, httpClient))
	if err != nil {
		return nil, fmt.Errorf("error fetching initial oidc client_credentials token: %w", err)
	}

	opts.Logger.Info("Fetched initial oidc client_credentials token")

	return &ClientCredentials{
		log:          opts.Logger,
		currentToken: token,
		httpClient:   httpClient,
		closeCh:      make(chan struct{}),
		clock:        clock.RealClock{},
		fetchTokenFn: conf.Token,
	}, nil
}

func (c *ClientCredentials) Run(ctx context.Context) {
	c.log.Info("Running oidc client_credentials token renewer")
	renewDuration := c.tokenRenewDuration()

	c.wg.Add(1)
	go func() {
		defer func() {
			c.log.Info("Stopped oidc client_credentials token renewer")
			c.wg.Done()
		}()

		for {
			select {
			case <-c.closeCh:
				return
			case <-ctx.Done():
				return
			case <-c.clock.After(renewDuration):
			}

			c.log.Info("Renewing client credentials token")

			token, err := c.fetchTokenFn(context.WithValue(ctx, oauth2.HTTPClient, c.httpClient))
			if err != nil {
				c.log.Errorf("Error fetching renewed oidc client_credentials token, retrying in 30 seconds: %s", err)
				renewDuration = time.Second * 30
				continue
			}

			c.lock.Lock()
			c.currentToken = token
			c.lock.Unlock()
			renewDuration = c.tokenRenewDuration()
		}
	}()
}

func toConfig(opts ClientCredentialsOptions) (*ccreds.Config, *http.Client, error) {
	scopes := opts.Scopes
	if len(scopes) == 0 {
		// If no scopes are provided, then the default is to use the 'openid' scope
		// since that is always required for OIDC so implicitly add it.
		scopes = []string{"openid"}
	}

	var oidcScopeFound bool
	for _, scope := range scopes {
		if scope == "openid" {
			oidcScopeFound = true
			break
		}
	}
	if !oidcScopeFound {
		return nil, nil, errors.New("oidc client_credentials token source requires the 'openid' scope")
	}

	tokenURL, err := url.Parse(opts.TokenURL)
	if err != nil {
		return nil, nil, fmt.Errorf("error parsing token URL: %w", err)
	}
	if tokenURL.Scheme != "https" {
		return nil, nil, fmt.Errorf("OIDC token provider URL requires 'https' scheme: %q", tokenURL)
	}

	conf := &ccreds.Config{
		ClientID:     opts.ClientID,
		ClientSecret: opts.ClientSecret,
		TokenURL:     opts.TokenURL,
		Scopes:       scopes,
	}

	if len(opts.Audiences) == 0 {
		return nil, nil, errors.New("oidc client_credentials token source requires at least one audience")
	}

	conf.EndpointParams = url.Values{"audience": opts.Audiences}

	// If caPool is nil, then the Go TLS library will use the system's root CA.
	var caPool *x509.CertPool
	if len(opts.CAPEM) > 0 {
		caPool = x509.NewCertPool()
		if !caPool.AppendCertsFromPEM(opts.CAPEM) {
			return nil, nil, errors.New("failed to parse CA PEM")
		}
	}

	return conf, &http.Client{
		Timeout: time.Second * 30,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				MinVersion: tls.VersionTLS12,
				RootCAs:    caPool,
			},
		},
	}, nil
}

func (c *ClientCredentials) Close() {
	defer c.wg.Wait()

	if c.closed.CompareAndSwap(false, true) {
		close(c.closeCh)
	}
}

func (c *ClientCredentials) Token() (string, error) {
	c.lock.RLock()
	defer c.lock.RUnlock()

	if c.closed.Load() {
		return "", errors.New("client_credentials token source is closed")
	}

	if !c.currentToken.Valid() {
		return "", errors.New("client_credentials token source is invalid")
	}

	return c.currentToken.AccessToken, nil
}

// tokenRenewTime returns the duration when the token should be renewed, which is
// half of the token's lifetime.
func (c *ClientCredentials) tokenRenewDuration() time.Duration {
	c.lock.RLock()
	defer c.lock.RUnlock()
	return c.currentToken.Expiry.Sub(c.clock.Now()) / 2
}
