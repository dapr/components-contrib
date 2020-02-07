// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package ratelimit

import (
	"fmt"
	"strconv"

	"github.com/dapr/components-contrib/middleware"
	"github.com/dapr/components-contrib/middleware/http/nethttpadaptor"
	"github.com/didip/tollbooth"
	"github.com/valyala/fasthttp"
	"github.com/valyala/fasthttp/fasthttpadaptor"
)

// Metadata is the ratelimit middleware config
type rateLimitMiddlewareMetadata struct {
	MaxRequestsPerSecond float64 `json:"maxRequestsPerSecond"`
}

const (
	maxRequestsPerSecondKey = "maxRequestsPerSecond"

	// Defaults
	defaultMaxRequestsPerSecond = 100
)

// NewRateLimitMiddleware returns a new oAuth2 middleware
func NewRateLimitMiddleware() *Middleware {
	return &Middleware{}
}

// Middleware is an oAuth2 authentication middleware
type Middleware struct{}

// GetHandler returns the HTTP handler provided by the middleware
func (m *Middleware) GetHandler(metadata middleware.Metadata) (func(h fasthttp.RequestHandler) fasthttp.RequestHandler, error) {
	meta, err := m.getNativeMetadata(metadata)
	if err != nil {
		return nil, err
	}

	limiter := tollbooth.NewLimiter(meta.MaxRequestsPerSecond, nil)

	return func(h fasthttp.RequestHandler) fasthttp.RequestHandler {
		limitHandler := tollbooth.LimitFuncHandler(limiter, nethttpadaptor.NewNetHTTPHandlerFunc(h))
		wrappedHandler := fasthttpadaptor.NewFastHTTPHandlerFunc(limitHandler.ServeHTTP)
		return func(ctx *fasthttp.RequestCtx) {
			wrappedHandler(ctx)
		}
	}, nil
}

func (m *Middleware) getNativeMetadata(metadata middleware.Metadata) (*rateLimitMiddlewareMetadata, error) {
	var middlewareMetadata rateLimitMiddlewareMetadata

	middlewareMetadata.MaxRequestsPerSecond = defaultMaxRequestsPerSecond
	if val, ok := metadata.Properties[maxRequestsPerSecondKey]; ok {
		f, err := strconv.ParseFloat(val, 64)
		if err != nil {
			return nil, fmt.Errorf("error parsing ratelimit middelware property %s: %+v", maxRequestsPerSecondKey, err)
		}
		if f <= 0 {
			return nil, fmt.Errorf("ratelimit middelware property %s must be a positive value", maxRequestsPerSecondKey)
		}
		middlewareMetadata.MaxRequestsPerSecond = f
	}

	return &middlewareMetadata, nil
}