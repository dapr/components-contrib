// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package bearer

import (
	"context"
	"encoding/json"
	"strings"

	oidc "github.com/coreos/go-oidc"
	"github.com/valyala/fasthttp"

	"github.com/dapr/components-contrib/middleware"
	"github.com/dapr/kit/logger"
)

type bearerMiddlewareMetadata struct {
	IssuerURL string `json:"issuerURL"`
	ClientID  string `json:"clientID"`
}

// NewBearerMiddleware returns a new oAuth2 middleware.
func NewBearerMiddleware(logger logger.Logger) *Middleware {
	return &Middleware{logger: logger}
}

// Middleware is an oAuth2 authentication middleware.
type Middleware struct {
	logger logger.Logger
}

const (
	bearerPrefix       = "bearer "
	bearerPrefixLength = len(bearerPrefix)
)

// GetHandler retruns the HTTP handler provided by the middleware.
func (m *Middleware) GetHandler(metadata middleware.Metadata) (func(h fasthttp.RequestHandler) fasthttp.RequestHandler, error) {
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

	return func(h fasthttp.RequestHandler) fasthttp.RequestHandler {
		return func(ctx *fasthttp.RequestCtx) {
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

	var middlewareMetadata bearerMiddlewareMetadata
	err = json.Unmarshal(b, &middlewareMetadata)
	if err != nil {
		return nil, err
	}

	return &middlewareMetadata, nil
}
