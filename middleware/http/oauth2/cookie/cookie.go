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

// Package oauth2cookie is a concrete implementation of the oauth2 middleware that stores the token in a cookie in the client.
package oauth2cookie

import (
	"context"
	"fmt"
	"net/http"
	"reflect"

	mdutils "github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/middleware"
	"github.com/dapr/components-contrib/middleware/http/oauth2/impl"
	"github.com/dapr/kit/logger"
)

type OAuth2CookieMiddlewareMetadata struct {
	impl.OAuth2MiddlewareMetadata `mapstructure:",squash"`
}

// NewOAuth2CookieMiddleware returns a new oAuth2 middleware.
func NewOAuth2CookieMiddleware(log logger.Logger) middleware.Middleware {
	mw := &OAuth2CookieMiddleware{
		OAuth2Middleware: impl.OAuth2Middleware{},
		logger:           log,
	}
	mw.OAuth2Middleware.GetClaimsFn = mw.GetClaimsFromCookie
	mw.OAuth2Middleware.SetClaimsFn = mw.SetSecureCookie
	return mw
}

// OAuth2CookieMiddleware is an OAuth2 authentication middleware that stores session data in a cookie.
type OAuth2CookieMiddleware struct {
	impl.OAuth2Middleware

	logger logger.Logger
	meta   OAuth2CookieMiddlewareMetadata
}

// GetHandler retruns the HTTP handler provided by the middleware.
func (m *OAuth2CookieMiddleware) GetHandler(ctx context.Context, metadata middleware.Metadata) (func(next http.Handler) http.Handler, error) {
	err := m.meta.FromMetadata(metadata, m.logger)
	if err != nil {
		return nil, fmt.Errorf("invalid metadata: %w", err)
	}
	m.OAuth2Middleware.SetMetadata(m.meta.OAuth2MiddlewareMetadata)

	return m.OAuth2Middleware.GetHandler(ctx)
}

func (m *OAuth2CookieMiddleware) GetComponentMetadata() map[string]string {
	metadataStruct := OAuth2CookieMiddlewareMetadata{}
	metadataInfo := map[string]string{}
	mdutils.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, mdutils.MiddlewareType)
	return metadataInfo
}
