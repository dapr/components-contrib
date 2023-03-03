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

package oauth2

import (
	"context"
	"net/http"
	"net/url"
	"strings"

	"github.com/fasthttp-contrib/sessions"
	"github.com/google/uuid"
	"golang.org/x/oauth2"

	"github.com/dapr/components-contrib/internal/httputils"
	"github.com/dapr/components-contrib/internal/utils"
	mdutils "github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/middleware"
	"github.com/dapr/kit/logger"
)

// Metadata is the oAuth middleware config.
type oAuth2MiddlewareMetadata struct {
	ClientID       string `json:"clientID"`
	ClientSecret   string `json:"clientSecret"`
	Scopes         string `json:"scopes"`
	AuthURL        string `json:"authURL"`
	TokenURL       string `json:"tokenURL"`
	AuthHeaderName string `json:"authHeaderName"`
	RedirectURL    string `json:"redirectURL"`
	ForceHTTPS     string `json:"forceHTTPS"`
}

// NewOAuth2Middleware returns a new oAuth2 middleware.
func NewOAuth2Middleware(log logger.Logger) middleware.Middleware {
	m := &Middleware{logger: log}
	m.SetTokenProvider(m)
	return m
}

// Middleware is an oAuth2 authentication middleware.
type Middleware struct {
	logger        logger.Logger
	tokenProvider TokenProviderInterface
}

const (
	stateParam   = "state"
	savedState   = "auth-state"
	redirectPath = "redirect-url"
	codeParam    = "code"
)

// TokenProviderInterface provides a common interface to Mock the Token retrieval in unit tests.
type TokenProviderInterface interface {
	AuthCodeURL(conf *oauth2.Config, state string, opts ...oauth2.AuthCodeOption) string
	Exchange(conf *oauth2.Config, ctx context.Context, code string, opts ...oauth2.AuthCodeOption) (*oauth2.Token, error)
}

// GetHandler retruns the HTTP handler provided by the middleware.
func (m *Middleware) GetHandler(ctx context.Context, metadata middleware.Metadata) (func(next http.Handler) http.Handler, error) {
	meta, err := m.getNativeMetadata(metadata)
	if err != nil {
		return nil, err
	}

	forceHTTPS := utils.IsTruthy(meta.ForceHTTPS)
	conf := &oauth2.Config{
		ClientID:     meta.ClientID,
		ClientSecret: meta.ClientSecret,
		Scopes:       strings.Split(meta.Scopes, ","),
		RedirectURL:  meta.RedirectURL,
		Endpoint: oauth2.Endpoint{
			AuthURL:  meta.AuthURL,
			TokenURL: meta.TokenURL,
		},
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			session := sessions.Start(w, r)

			if session.GetString(meta.AuthHeaderName) != "" {
				r.Header.Add(meta.AuthHeaderName, session.GetString(meta.AuthHeaderName))
				next.ServeHTTP(w, r)
				return
			}

			state := r.URL.Query().Get(stateParam)
			if state == "" {
				id, err := uuid.NewRandom()
				if err != nil {
					httputils.RespondWithError(w, http.StatusInternalServerError)
					m.logger.Errorf("Failed to generate UUID: %v", err)
					return
				}
				idStr := id.String()

				session.Set(savedState, idStr)
				session.Set(redirectPath, r.URL)

				url := m.tokenProvider.AuthCodeURL(conf, idStr, oauth2.AccessTypeOffline)
				httputils.RespondWithRedirect(w, http.StatusFound, url)
			} else {
				authState := session.GetString(savedState)
				redirectURL, ok := session.Get(redirectPath).(*url.URL)
				if !ok {
					httputils.RespondWithError(w, http.StatusInternalServerError)
					m.logger.Errorf("Value saved in state key '%s' is not a *url.URL", redirectPath)
					return
				}

				if forceHTTPS {
					redirectURL.Scheme = "https"
				}

				if state != authState {
					httputils.RespondWithErrorAndMessage(w, http.StatusBadRequest, "invalid state")
					return
				}

				code := r.URL.Query().Get(codeParam)
				if code == "" {
					httputils.RespondWithErrorAndMessage(w, http.StatusBadRequest, "code not found")
					return
				}

				token, err := m.tokenProvider.Exchange(conf, r.Context(), code)
				if err != nil {
					httputils.RespondWithError(w, http.StatusInternalServerError)
					m.logger.Error("Failed to exchange token")
					return
				}

				authHeader := token.Type() + " " + token.AccessToken
				session.Set(meta.AuthHeaderName, authHeader)
				r.Header.Add(meta.AuthHeaderName, authHeader)
				httputils.RespondWithRedirect(w, http.StatusFound, redirectURL.String())
			}
		})
	}, nil
}

func (m *Middleware) getNativeMetadata(metadata middleware.Metadata) (*oAuth2MiddlewareMetadata, error) {
	var middlewareMetadata oAuth2MiddlewareMetadata
	err := mdutils.DecodeMetadata(metadata.Properties, &middlewareMetadata)
	if err != nil {
		return nil, err
	}
	return &middlewareMetadata, nil
}

// SetTokenProvider will enable to change the tokenProvider used after instanciation (needed for mocking).
func (m *Middleware) SetTokenProvider(tokenProvider TokenProviderInterface) {
	m.tokenProvider = tokenProvider
}

// AuthCodeURL returns a URL to OAuth 2.0 provider's consent page that asks for permissions
func (m *Middleware) AuthCodeURL(conf *oauth2.Config, state string, opts ...oauth2.AuthCodeOption) string {
	return conf.AuthCodeURL(state, opts...)
}

// Exchange returns a token from the current OAuth2 ClientCredentials Configuration.
func (m *Middleware) Exchange(conf *oauth2.Config, ctx context.Context, code string, opts ...oauth2.AuthCodeOption) (*oauth2.Token, error) {
	return conf.Exchange(ctx, code, opts...)
}
