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

package bearer

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/lestrrat-go/httprc"
	"github.com/lestrrat-go/jwx/v2/jwk"
	"github.com/lestrrat-go/jwx/v2/jwt"

	"github.com/dapr/components-contrib/internal/httputils"
	mdutils "github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/middleware"
	"github.com/dapr/kit/logger"
)

type bearerMiddlewareMetadata struct {
	IssuerURL string `json:"issuerURL"`
	ClientID  string `json:"clientID"`
}

const (
	// Prefix for the authorization header (case-insensitive)
	bearerPrefix = "bearer "
	// Minimum interval before refreshing the JWKS cache
	minRefreshInterval = 10 * time.Minute
	// Allowed clock skew
	allowedClockSkew = 5 * time.Minute
)

// NewBearerMiddleware returns a new OAuth2 middleware.
func NewBearerMiddleware(logger logger.Logger) middleware.Middleware {
	return &Middleware{
		logger: logger,
	}
}

// Middleware is an OAuth2 authentication middleware.
type Middleware struct {
	logger logger.Logger
}

// GetHandler retruns the HTTP handler provided by the middleware.
func (m *Middleware) GetHandler(ctx context.Context, metadata middleware.Metadata) (func(next http.Handler) http.Handler, error) {
	meta, err := m.getNativeMetadata(metadata)
	if err != nil {
		return nil, err
	}

	// Create a JWKS cache that is refreshed automatically
	cache := jwk.NewCache(ctx,
		jwk.WithErrSink(httprc.ErrSinkFunc(func(err error) {
			m.logger.Warnf("Error while refreshing JWKS cache: %v", err)
		})),
	)
	err = cache.Register(meta.IssuerURL,
		jwk.WithMinRefreshInterval(minRefreshInterval),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to register JWKS cache: %w", err)
	}

	// Fetch the JWKS right away to start, so we can check it's valid and populate the cache
	_, err = cache.Refresh(ctx, meta.IssuerURL)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch JWKS: %w", err)
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			authHeader := r.Header.Get("authorization")
			if strings.ToLower(authHeader[0:len(bearerPrefix)]) != bearerPrefix {
				httputils.RespondWithError(w, http.StatusUnauthorized)
				return
			}
			rawToken := authHeader[len(bearerPrefix):]
			if len(rawToken) < 10 {
				httputils.RespondWithError(w, http.StatusUnauthorized)
				return
			}

			keyset, err := cache.Get(r.Context(), meta.IssuerURL)
			if err != nil {
				m.logger.Errorf("Failed to retrieve JWKS cache: %v", err)
				httputils.RespondWithError(w, http.StatusInternalServerError)
				return
			}

			_, err = jwt.Parse([]byte(rawToken),
				jwt.WithContext(r.Context()),
				jwt.WithAcceptableSkew(allowedClockSkew),
				jwt.WithKeySet(keyset),
				jwt.WithAudience(meta.ClientID),
			)
			if err != nil {
				httputils.RespondWithError(w, http.StatusUnauthorized)
				return
			}

			next.ServeHTTP(w, r)
		})
	}, nil
}

func (m *Middleware) getNativeMetadata(metadata middleware.Metadata) (*bearerMiddlewareMetadata, error) {
	var middlewareMetadata bearerMiddlewareMetadata
	err := mdutils.DecodeMetadata(metadata.Properties, &middlewareMetadata)
	if err != nil {
		return nil, err
	}
	return &middlewareMetadata, nil
}
