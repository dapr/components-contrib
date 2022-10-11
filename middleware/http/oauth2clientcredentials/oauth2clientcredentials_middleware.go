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

package oauth2clientcredentials

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/patrickmn/go-cache"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/clientcredentials"

	mdutils "github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/middleware"
	"github.com/dapr/kit/logger"
)

// Metadata is the oAuth clientcredentials middleware config.
type oAuth2ClientCredentialsMiddlewareMetadata struct {
	ClientID            string `json:"clientID"`
	ClientSecret        string `json:"clientSecret"`
	Scopes              string `json:"scopes"`
	TokenURL            string `json:"tokenURL"`
	HeaderName          string `json:"headerName"`
	EndpointParamsQuery string `json:"endpointParamsQuery,omitempty"`
	AuthStyle           int    `json:"authStyle"`
}

// TokenProviderInterface provides a common interface to Mock the Token retrieval in unit tests.
type TokenProviderInterface interface {
	GetToken(conf *clientcredentials.Config) (*oauth2.Token, error)
}

// NewOAuth2ClientCredentialsMiddleware returns a new oAuth2 middleware.
func NewOAuth2ClientCredentialsMiddleware(logger logger.Logger) middleware.Middleware {
	m := &Middleware{
		log:        logger,
		tokenCache: cache.New(1*time.Hour, 10*time.Minute),
	}
	// Default: set Token Provider to real implementation (we will overwrite it for unit testing)
	m.SetTokenProvider(m)

	return m
}

// Middleware is an oAuth2 authentication middleware.
type Middleware struct {
	log           logger.Logger
	tokenCache    *cache.Cache
	tokenProvider TokenProviderInterface
}

// GetHandler retruns the HTTP handler provided by the middleware.
func (m *Middleware) GetHandler(metadata middleware.Metadata) (func(next http.Handler) http.Handler, error) {
	meta, err := m.getNativeMetadata(metadata)
	if err != nil {
		m.log.Errorf("getNativeMetadata error: %s", err)
		return nil, err
	}

	endpointParams, err := url.ParseQuery(meta.EndpointParamsQuery)
	if err != nil {
		m.log.Warnf("Error parsing endpoint parameters: %s", err)
		endpointParams, _ = url.ParseQuery("")
	}

	conf := &clientcredentials.Config{
		ClientID:       meta.ClientID,
		ClientSecret:   meta.ClientSecret,
		Scopes:         strings.Split(meta.Scopes, ","),
		TokenURL:       meta.TokenURL,
		EndpointParams: endpointParams,
		AuthStyle:      oauth2.AuthStyle(meta.AuthStyle),
	}

	cacheKey := m.getCacheKey(meta)

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			var headerValue string

			// Check if valid token is in the cache
			cachedToken, found := m.tokenCache.Get(cacheKey)
			if !found {
				m.log.Debugf("Cached token not found, try get one")

				token, err := m.tokenProvider.GetToken(conf)
				if err != nil {
					m.log.Errorf("Error acquiring token: %s", err)
					return
				}

				tokenExpirationDuration := token.Expiry.Sub(time.Now())
				m.log.Debugf("Token expires at %s (%s from now)", token.Expiry, tokenExpirationDuration)

				headerValue = token.Type() + " " + token.AccessToken
				m.tokenCache.Set(cacheKey, headerValue, tokenExpirationDuration)
			} else {
				m.log.Debugf("Cached token found for key %s", cacheKey)
				headerValue = cachedToken.(string)
			}

			w.Header().Add(meta.HeaderName, headerValue)
			next.ServeHTTP(w, r)
		})
	}, nil
}

func (m *Middleware) getNativeMetadata(metadata middleware.Metadata) (*oAuth2ClientCredentialsMiddlewareMetadata, error) {
	var middlewareMetadata oAuth2ClientCredentialsMiddlewareMetadata
	err := mdutils.DecodeMetadata(metadata.Properties, &middlewareMetadata)
	if err != nil {
		return nil, fmt.Errorf("metadata errors: %w", err)
	}

	// Do input validation checks
	errorString := ""

	// Check if values are present
	m.checkMetadataValueExists(&errorString, &middlewareMetadata.HeaderName, "headerName")
	m.checkMetadataValueExists(&errorString, &middlewareMetadata.ClientID, "clientID")
	m.checkMetadataValueExists(&errorString, &middlewareMetadata.ClientSecret, "clientSecret")
	m.checkMetadataValueExists(&errorString, &middlewareMetadata.Scopes, "scopes")
	m.checkMetadataValueExists(&errorString, &middlewareMetadata.TokenURL, "tokenURL")

	// Value-check AuthStyle
	if middlewareMetadata.AuthStyle < 0 || middlewareMetadata.AuthStyle > 2 {
		errorString += fmt.Sprintf("Parameter 'authStyle' can only have the values 0,1,2. Received: '%d'. ", middlewareMetadata.AuthStyle)
	}

	// Return errors if any found
	if errorString != "" {
		return nil, fmt.Errorf("metadata errors: %s", errorString)
	}

	return &middlewareMetadata, nil
}

func (m *Middleware) checkMetadataValueExists(errorString *string, metadataValue *string, metadataName string) {
	if *metadataValue == "" {
		*errorString += fmt.Sprintf("Parameter '%s' needs to be set. ", metadataName)
	}
}

func (m *Middleware) getCacheKey(meta *oAuth2ClientCredentialsMiddlewareMetadata) string {
	// we will hash the key components ClientID + Scopes is a unique composite key/identifier for a token
	hashedKey := sha256.Sum224([]byte(meta.ClientID + meta.Scopes))
	return hex.EncodeToString(hashedKey[:])
}

// SetTokenProvider will enable to change the tokenProvider used after instanciation (needed for mocking).
func (m *Middleware) SetTokenProvider(tokenProvider TokenProviderInterface) {
	m.tokenProvider = tokenProvider
}

// GetToken returns a token from the current OAuth2 ClientCredentials Configuration.
func (m *Middleware) GetToken(conf *clientcredentials.Config) (*oauth2.Token, error) {
	tokenSource := conf.TokenSource(context.Background())

	return tokenSource.Token()
}
