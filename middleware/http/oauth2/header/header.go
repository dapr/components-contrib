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

// Package oauth2header is a concrete implementation of the oauth2 middleware that expects the token to be present in the Authorization header.
package oauth2header

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"reflect"
	"strings"
	"time"

	mdutils "github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/middleware"
	"github.com/dapr/components-contrib/middleware/http/oauth2/impl"
	"github.com/dapr/kit/logger"
)

const (
	headerName   = "Authorization"
	bearerPrefix = "bearer "
)

type OAuth2HeaderMiddlewareMetadata struct {
	impl.OAuth2MiddlewareMetadata `mapstructure:",squash"`
}

// NewOAuth2HeaderMiddleware returns a new OAuth2 middleware.
func NewOAuth2HeaderMiddleware(log logger.Logger) middleware.Middleware {
	mw := &OAuth2HeaderMiddleware{
		OAuth2Middleware: impl.OAuth2Middleware{
			Logger: log,
		},
	}
	mw.OAuth2Middleware.GetTokenFn = mw.getClaimsFromHeader
	mw.OAuth2Middleware.SetTokenFn = mw.setTokenResponse
	return mw
}

// OAuth2HeaderMiddleware is an OAuth2 authentication middleware that stores session data in a cookie.
type OAuth2HeaderMiddleware struct {
	impl.OAuth2Middleware

	logger logger.Logger
	meta   OAuth2HeaderMiddlewareMetadata
}

// GetHandler retruns the HTTP handler provided by the middleware.
func (m *OAuth2HeaderMiddleware) GetHandler(ctx context.Context, metadata middleware.Metadata) (func(next http.Handler) http.Handler, error) {
	err := m.meta.FromMetadata(metadata, m.logger)
	if err != nil {
		return nil, fmt.Errorf("invalid metadata: %w", err)
	}
	m.OAuth2Middleware.SetMetadata(m.meta.OAuth2MiddlewareMetadata)

	return m.OAuth2Middleware.GetHandler(ctx)
}

func (m *OAuth2HeaderMiddleware) getClaimsFromHeader(r *http.Request) (map[string]string, error) {
	// Get the header which should contain the JWE
	// The "Bearer " prefix is optional
	header := r.Header.Get(headerName)
	if header == "" {
		return nil, nil
	}
	if len(header) > len(bearerPrefix) && strings.ToLower(header[0:len(bearerPrefix)]) == bearerPrefix {
		header = header[len(bearerPrefix):]
	}

	return m.ParseToken(header)
}

func (m *OAuth2HeaderMiddleware) setTokenResponse(w http.ResponseWriter, r *http.Request, reqClaims map[string]string, token string, exp time.Duration) {
	// Delete the state token cookie
	m.UnsetCookie(w, impl.IsRequestSecure(r))

	// Set the response in the body
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	enc.Encode(map[string]string{
		headerName: token,
	})
}

func (m *OAuth2HeaderMiddleware) GetComponentMetadata() map[string]string {
	metadataStruct := OAuth2HeaderMiddlewareMetadata{}
	metadataInfo := map[string]string{}
	mdutils.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, mdutils.MiddlewareType)
	return metadataInfo
}
