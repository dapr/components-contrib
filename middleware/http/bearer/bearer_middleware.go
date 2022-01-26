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
	"encoding/json"
	"fmt"
	"strings"

	oidc "github.com/coreos/go-oidc"
	"github.com/valyala/fasthttp"

	"github.com/dapr/components-contrib/middleware"
	"github.com/dapr/kit/logger"
)

type bearerMiddlewareMetadata struct {
	IssuerURL          string `json:"issuerURL"`
	ClientID           string `json:"clientID"`
	WhiteList          string `json:"whiteList"`
	WhiteListMatchType string `json:"whiteListMatchType"`
}

// NewBearerMiddleware returns a new oAuth2 middleware.
func NewBearerMiddleware(logger logger.Logger) *Middleware {
	return &Middleware{logger: logger}
}

// Middleware is an oAuth2 authentication middleware.
type Middleware struct {
	logger logger.Logger
	w      WhiteListMatcher
}

const (
	bearerPrefix       = "bearer "
	bearerPrefixLength = len(bearerPrefix)
	defaultWhiteList   = "/v1.0/healthz,/v1.0/healthz/outbound"
	whitelistSeparator = ","

	matchTypeExact = "exact"
	matchTypeRegex = "regex"
)

// GetHandler retruns the HTTP handler provided by the middleware.
func (m *Middleware) GetHandler(metadata middleware.Metadata) (func(h fasthttp.RequestHandler) fasthttp.RequestHandler, error) {
	meta, err := m.getNativeMetadata(metadata)
	if err != nil {
		return nil, err
	}
	m.w, err = NewMatcher(meta.WhiteListMatchType, meta.WhiteList)
	if err != nil {
		return nil, fmt.Errorf("bearer middleware err: %w", err)
	}

	provider, err := oidc.NewProvider(context.Background(), meta.IssuerURL)
	if err != nil {
		return nil, err
	}

	verifier := provider.Verifier(&oidc.Config{
		ClientID: meta.ClientID,
	})

	return func(h fasthttp.RequestHandler) fasthttp.RequestHandler {
		return func(ctx *fasthttp.RequestCtx) {
			// bypass white list
			if m.w.Match(string(ctx.Path())) {
				h(ctx)

				return
			}
			authHeader := string(ctx.Request.Header.Peek(fasthttp.HeaderAuthorization))
			if !strings.HasPrefix(strings.ToLower(authHeader), bearerPrefix) {
				ctx.Error(fasthttp.StatusMessage(fasthttp.StatusUnauthorized), fasthttp.StatusUnauthorized)

				return
			}
			rawToken := authHeader[bearerPrefixLength:]
			_, err := verifier.Verify(ctx, rawToken)
			if err != nil {
				ctx.Error(fasthttp.StatusMessage(fasthttp.StatusUnauthorized), fasthttp.StatusUnauthorized)

				return
			}

			h(ctx)
		}
	}, nil
}

func (m *Middleware) getNativeMetadata(metadata middleware.Metadata) (*bearerMiddlewareMetadata, error) {
	b, err := json.Marshal(metadata.Properties)
	if err != nil {
		return nil, err
	}

	middlewareMetadata := bearerMiddlewareMetadata{
		WhiteList:          defaultWhiteList,
		WhiteListMatchType: matchTypeExact,
	}
	err = json.Unmarshal(b, &middlewareMetadata)
	if err != nil {
		return nil, err
	}

	return &middlewareMetadata, nil
}
