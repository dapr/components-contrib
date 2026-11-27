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

package bearer

import (
	"context"
	"fmt"
	"net/http"
	"reflect"
	"strings"
	"time"

	"github.com/lestrrat-go/httprc/v3"
	"github.com/lestrrat-go/httprc/v3/errsink"
	"github.com/lestrrat-go/jwx/v3/jwk"
	"github.com/lestrrat-go/jwx/v3/jws"
	"github.com/lestrrat-go/jwx/v3/jwt"

	"github.com/dapr/components-contrib/common/httputils"
	contribMetadata "github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/middleware"
	"github.com/dapr/kit/logger"
)

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
	meta := &bearerMiddlewareMetadata{
		logger: m.logger,
	}
	err := meta.fromMetadata(metadata)
	if err != nil {
		return nil, err
	}

	// Retrieve the OpenID Configuration document if needed
	getCtx, getCancel := context.WithTimeout(ctx, 30*time.Second)
	defer getCancel()
	err = meta.retrieveOpenIDConfigurationDocument(getCtx)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve OpenID Configuration document: %w", err)
	}

	// Create a JWKS cache that is refreshed automatically.
	// The cache runs goroutines until its context is canceled: if init fails, stop them right away.
	cacheCtx, cacheCancel := context.WithCancel(ctx)
	var initDone bool
	defer func() {
		if !initDone {
			cacheCancel()
		}
	}()
	cache, err := jwk.NewCache(cacheCtx, httprc.NewClient(
		httprc.WithErrorSink(errsink.NewFunc(func(_ context.Context, err error) {
			m.logger.Warnf("Error while refreshing JWKS cache: %v", err)
		})),
	))
	if err != nil {
		return nil, fmt.Errorf("failed to create JWKS cache: %w", err)
	}
	// Don't wait for the first fetch here: Register would block until it succeeds, so a failing endpoint would hang
	// until ctx is canceled. The Refresh call below does the first fetch and returns its error.
	err = cache.Register(ctx, meta.JWKSURL,
		jwk.WithMinInterval(minRefreshInterval),
		jwk.WithWaitReady(false),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to register JWKS cache: %w", err)
	}

	// Fetch the JWKS right away to start, so we can check it's valid and populate the cache
	_, err = cache.Refresh(ctx, meta.JWKSURL)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch JWKS: %w", err)
	}

	// Look the resource up once: cache.Lookup goes through the cache's single controller goroutine,
	// while resource.Resource() is a lock-free read of the last fetched set.
	resource, err := cache.LookupResource(ctx, meta.JWKSURL)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve JWKS cache: %w", err)
	}
	initDone = true

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			authHeader := r.Header.Get("authorization")
			if (len(authHeader) < len(bearerPrefix)+1) || strings.ToLower(authHeader[0:len(bearerPrefix)]) != bearerPrefix {
				httputils.RespondWithError(w, http.StatusUnauthorized)
				return
			}
			rawToken := authHeader[len(bearerPrefix):]
			if len(rawToken) < 10 {
				httputils.RespondWithError(w, http.StatusUnauthorized)
				return
			}

			keyset := resource.Resource()
			if keyset == nil {
				m.logger.Error("Failed to retrieve JWKS cache: no key set fetched")
				httputils.RespondWithError(w, http.StatusInternalServerError)
				return
			}

			// jwx v3's jwt.Parse verifies the compact serialization only, so a JWS JSON envelope around a once-signed
			// token can't be used to substitute the claims.
			_, err := jwt.Parse([]byte(rawToken),
				jwt.WithContext(r.Context()),
				jwt.WithAcceptableSkew(allowedClockSkew),
				jwt.WithKeySet(keyset, jws.WithInferAlgorithmFromKey(true)),
				jwt.WithAudience(meta.Audience),
				jwt.WithIssuer(meta.Issuer),
			)
			if err != nil {
				httputils.RespondWithError(w, http.StatusUnauthorized)
				return
			}

			next.ServeHTTP(w, r)
		})
	}, nil
}

func (m *Middleware) GetComponentMetadata() (metadataInfo contribMetadata.MetadataMap) {
	metadataStruct := bearerMiddlewareMetadata{}
	_ = contribMetadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, contribMetadata.MiddlewareType)
	return
}
