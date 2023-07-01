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
}

const (
	savedState   = "auth-state"
	redirectPath = "redirect-url"
)

// GetHandler retruns the HTTP handler provided by the middleware.
func (m *Middleware) GetHandler(ctx context.Context, metadata middleware.Metadata) (func(next http.Handler) http.Handler, error) {
	meta := oAuth2MiddlewareMetadata{}
	err := meta.fromMetadata(metadata)
	if err != nil {
		return nil, err
	}

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

				if meta.ForceHTTPS {
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
				session.Set(meta.AuthHeaderName, authHeader)
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

func (m *Middleware) GetComponentMetadata() map[string]string {
	metadataStruct := oAuth2MiddlewareMetadata{}
	metadataInfo := map[string]string{}
	mdutils.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, mdutils.MiddlewareType)
	return metadataInfo
}
