/*
Copyright 2023 The Dapr Authors
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
	"fmt"
	"net/http"
	"net/url"
	"reflect"
	"strings"

	"github.com/fasthttp-contrib/sessions"
	"github.com/google/uuid"
	"golang.org/x/oauth2"

	"github.com/dapr/components-contrib/internal/httputils"
	mdutils "github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/middleware"
	"github.com/dapr/kit/logger"
)

// NewOAuth2Middleware returns a new oAuth2 middleware.
func NewOAuth2Middleware(log logger.Logger) middleware.Middleware {
	return &Middleware{
		logger: log,
	}
}

// Middleware is an oAuth2 authentication middleware.
type Middleware struct {
	logger logger.Logger
	meta   oAuth2MiddlewareMetadata
}

const (
	savedState   = "auth-state"
	redirectPath = "redirect-url"
)

// GetHandler retruns the HTTP handler provided by the middleware.
func (m *Middleware) GetHandler(ctx context.Context, metadata middleware.Metadata) (func(next http.Handler) http.Handler, error) {
	err := m.meta.fromMetadata(metadata, m.logger)
	if err != nil {
		return nil, fmt.Errorf("invalid metadata: %w", err)
	}

	return m.getHandler, nil
}

func (m *Middleware) getHandler(next http.Handler) http.Handler {
	conf := oauth2.Config{
		ClientID:     m.meta.ClientID,
		ClientSecret: m.meta.ClientSecret,
		Scopes:       strings.Split(m.meta.Scopes, ","),
		RedirectURL:  m.meta.RedirectURL,
		Endpoint: oauth2.Endpoint{
			AuthURL:  m.meta.AuthURL,
			TokenURL: m.meta.TokenURL,
		},
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		session := sessions.Start(w, r)

		if session.GetString(m.meta.AuthHeaderName) != "" {
			r.Header.Add(m.meta.AuthHeaderName, session.GetString(m.meta.AuthHeaderName))
			next.ServeHTTP(w, r)
			return
		}

		// Redirect to the auth server
		state := r.URL.Query().Get("state")
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

			url := conf.AuthCodeURL(idStr, oauth2.AccessTypeOffline)
			httputils.RespondWithRedirect(w, http.StatusFound, url)
		} else {
			authState := session.GetString(savedState)
			redirectURL, ok := session.Get(redirectPath).(*url.URL)
			if !ok {
				httputils.RespondWithError(w, http.StatusInternalServerError)
				m.logger.Errorf("Value saved in state key '%s' is not a *url.URL", redirectPath)
				return
			}

			if m.meta.ForceHTTPS {
				redirectURL.Scheme = "https"
			}

			if state != authState {
				httputils.RespondWithErrorAndMessage(w, http.StatusBadRequest, "invalid state")
				return
			}

			code := r.URL.Query().Get("code")
			if code == "" {
				httputils.RespondWithErrorAndMessage(w, http.StatusBadRequest, "code not found")
				return
			}

			token, err := conf.Exchange(r.Context(), code)
			if err != nil {
				httputils.RespondWithError(w, http.StatusInternalServerError)
				m.logger.Error("Failed to exchange token")
				return
			}

			authHeader := token.Type() + " " + token.AccessToken
			session.Set(m.meta.AuthHeaderName, authHeader)
			httputils.RespondWithRedirect(w, http.StatusFound, redirectURL.String())
		}
	})
}

func (m *Middleware) getNativeMetadata(metadata middleware.Metadata) (*oAuth2MiddlewareMetadata, error) {
	var middlewareMetadata oAuth2MiddlewareMetadata
	err := mdutils.DecodeMetadata(metadata.Properties, &middlewareMetadata)
	if err != nil {
		return nil, err
	}
	return &middlewareMetadata, nil
}

func (m *Middleware) GetComponentMetadata() map[string]string {
	metadataStruct := oAuth2MiddlewareMetadata{}
	metadataInfo := map[string]string{}
	mdutils.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, mdutils.MiddlewareType)
	return metadataInfo
}
