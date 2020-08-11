// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package oauth2clientcredentials

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/dapr/components-contrib/middleware"
	"github.com/dapr/dapr/pkg/logger"
	"github.com/patrickmn/go-cache"
	"github.com/valyala/fasthttp"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/clientcredentials"
)

// Metadata is the oAuth clientcredentials middleware config
type oAuth2ClientCredentialsMiddlewareMetadata struct {
	ClientID            string `json:"clientID"`
	ClientSecret        string `json:"clientSecret"`
	Scopes              string `json:"scopes"`
	TokenURL            string `json:"tokenURL"`
	AuthHeaderName      string `json:"authHeaderName"`
	EndpointParamsQuery string `json:"endpointParamsQuery"`
	AuthStyleString     string `json:"authStyle"`
	AuthStyle           int
}

// dapr logger
var log = logger.NewLogger("dapr.components-contrib.middleware.http.oauth2clientcredentials")

// TokenProviderInterface provides a common interface to Mock the Token retrieval in unit tests
type TokenProviderInterface interface {
	GetToken(conf *clientcredentials.Config) (*oauth2.Token, error)
}

// NewOAuth2ClientCredentialsMiddleware returns a new oAuth2 middleware
func NewOAuth2ClientCredentialsMiddleware() *Middleware {
	m := &Middleware{
		tokenCache: cache.New(1*time.Hour, 10*time.Minute),
	}
	m.SetTokenProvider(m)
	return m
}

// Middleware is an oAuth2 authentication middleware
type Middleware struct {
	// Token Cache
	tokenCache *cache.Cache
	// Token provider
	tokenProvider TokenProviderInterface
}

// GetHandler retruns the HTTP handler provided by the middleware
func (m *Middleware) GetHandler(metadata middleware.Metadata) (func(h fasthttp.RequestHandler) fasthttp.RequestHandler, error) {
	meta, err := m.getNativeMetadata(metadata)
	if err != nil {
		log.Errorf("getNativeMetadata error, %s", err)
		return nil, err
	}

	return func(h fasthttp.RequestHandler) fasthttp.RequestHandler {
		return func(ctx *fasthttp.RequestCtx) {

			var cacheKey = m.getCacheKey(meta)
			var headerValue string
			cachedToken, found := m.tokenCache.Get(cacheKey)
			if !found {
				log.Debugf("Cached token not found, try get one")
				var endpointParams, err = url.ParseQuery(meta.EndpointParamsQuery)
				if err != nil {
					log.Errorf("Error parsing endpoint parameters, %s", err)
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

				token, tokenError := m.tokenProvider.GetToken(conf)
				if tokenError != nil {
					log.Errorf("Error acquiring token, %s", tokenError)
					return
				}

				tokenExpirationDuration := token.Expiry.Sub(time.Now().In(time.UTC))
				log.Debugf("Duration in seconds %s, Expiry Time %s", tokenExpirationDuration, token.Expiry)
				if err != nil {
					log.Errorf("Error parsing duration string, %s", fmt.Sprintf("%ss", token.Expiry))
					return
				}

				headerValue = token.Type() + " " + token.AccessToken
				m.tokenCache.Set(cacheKey, headerValue, tokenExpirationDuration)
			} else {
				log.Debugf("Cached token found for key %s", cacheKey)
				headerValue = cachedToken.(string)
			}

			ctx.Request.Header.Add(meta.AuthHeaderName, headerValue)
			h(ctx)
		}
	}, nil
}

func (m *Middleware) getNativeMetadata(metadata middleware.Metadata) (*oAuth2ClientCredentialsMiddlewareMetadata, error) {
	b, err := json.Marshal(metadata.Properties)
	if err != nil {
		return nil, err
	}
	var middlewareMetadata oAuth2ClientCredentialsMiddlewareMetadata
	err = json.Unmarshal(b, &middlewareMetadata)
	if err != nil {
		return nil, err
	}

	// Converting AuthStyle to int and do a value check
	authStyle, err := strconv.Atoi(middlewareMetadata.AuthStyleString)
	if err != nil {
		return nil, fmt.Errorf("AuthStyle can only have the values 0,1,2. Received: '%s'", middlewareMetadata.AuthStyleString)
	}
	if authStyle < 0 || authStyle > 2 {
		return nil, fmt.Errorf("AuthStyle can only have the values 0,1,2. Received: '%d'", authStyle)
	}
	middlewareMetadata.AuthStyle = authStyle

	return &middlewareMetadata, nil
}

func (m *Middleware) getCacheKey(meta *oAuth2ClientCredentialsMiddlewareMetadata) string {
	// we will hash the key components
	hashedKey := sha256.New()
	// ClientID + Scopes is a unique composite key/identifier for a token
	key := strings.Join([]string{meta.ClientID, meta.Scopes}, "")
	// Return the hashed key as string
	hashedKey.Write([]byte(key))
	return fmt.Sprintf("%x", hashedKey.Sum(nil))
}

// SetTokenProvider will enable to change the tokenProvider used after instanciation (needed for mocking)
func (m *Middleware) SetTokenProvider(tokenProvider TokenProviderInterface) {
	m.tokenProvider = tokenProvider
}

// GetToken returns a token from the current OAuth2 ClientCredentials Configuration
func (m *Middleware) GetToken(conf *clientcredentials.Config) (*oauth2.Token, error) {
	tokenSource := conf.TokenSource(context.Background())
	return tokenSource.Token()
}
