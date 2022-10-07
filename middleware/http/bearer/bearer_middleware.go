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
	"net/http"
	"strings"

	oidc "github.com/coreos/go-oidc"

	"github.com/dapr/components-contrib/internal/httputils"
	mdutils "github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/middleware"
	"github.com/dapr/kit/logger"
)

type bearerMiddlewareMetadata struct {
	IssuerURL string `json:"issuerURL"`
	ClientID  string `json:"clientID"`
}

// NewBearerMiddleware returns a new oAuth2 middleware.
func NewBearerMiddleware(_ logger.Logger) middleware.Middleware {
	return &Middleware{}
}

// Middleware is an oAuth2 authentication middleware.
type Middleware struct{}

const (
	bearerPrefix       = "bearer "
	bearerPrefixLength = len(bearerPrefix)
)

// GetHandler retruns the HTTP handler provided by the middleware.
func (m *Middleware) GetHandler(metadata middleware.Metadata) (func(next http.Handler) http.Handler, error) {
	meta, err := m.getNativeMetadata(metadata)
	if err != nil {
		return nil, err
	}

	provider, err := oidc.NewProvider(context.Background(), meta.IssuerURL)
	if err != nil {
		return nil, err
	}

	verifier := provider.Verifier(&oidc.Config{
		ClientID: meta.ClientID,
	})

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			authHeader := r.Header.Get("authorization")
			if !strings.HasPrefix(strings.ToLower(authHeader), bearerPrefix) {
				httputils.RespondWithError(w, http.StatusUnauthorized)
				return
			}
			rawToken := authHeader[bearerPrefixLength:]
			_, err := verifier.Verify(r.Context(), rawToken)
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
