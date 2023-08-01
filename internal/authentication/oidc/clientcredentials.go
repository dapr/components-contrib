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
	"time"

	"golang.org/x/oauth2"
	ccreds "golang.org/x/oauth2/clientcredentials"

	"github.com/dapr/kit/logger"
)

const (
	oidcScopeOpenID = "openid"
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

	lock sync.RWMutex
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
		fetchTokenFn: conf.Token,
	}, nil
}

func toConfig(opts ClientCredentialsOptions) (*ccreds.Config, *http.Client, error) {
	scopes := opts.Scopes
	if len(scopes) == 0 {
		// If no scopes are provided, then the default is to use the 'openid' scope
		// since that is always required for OIDC so implicitly add it.
		scopes = []string{oidcScopeOpenID}
	}

	var oidcScopeFound bool
	for _, scope := range scopes {
		if scope == oidcScopeOpenID {
			oidcScopeFound = true
			break
		}
	}
	if !oidcScopeFound {
		return nil, nil, fmt.Errorf("oidc client_credentials token source requires the %q scope", oidcScopeOpenID)
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

func (c *ClientCredentials) Token() (string, error) {
	c.lock.RLock()
	defer c.lock.RUnlock()

	if !c.currentToken.Valid() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
		defer cancel()
		if err := c.renewToken(ctx); err != nil {
			return "", err
		}
	}

	return c.currentToken.AccessToken, nil
}

func (c *ClientCredentials) renewToken(ctx context.Context) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	// We need to check if the current token is valid because we might have lost
	// the mutex lock race from the caller and we don't want to double-fetch a
	// token unnecessarily!
	if c.currentToken.Valid() {
		return nil
	}

	token, err := c.fetchTokenFn(context.WithValue(ctx, oauth2.HTTPClient, c.httpClient))
	if err != nil {
		return err
	}

	c.currentToken = token
	return nil
}
