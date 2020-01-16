// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package ratelimit

import (
	"fmt"
	"strconv"

	"github.com/dapr/components-contrib/middleware"
	"github.com/juju/ratelimit"
	"github.com/valyala/fasthttp"
)

// Metadata is the oAuth middleware config
type rateLimitMiddlewareMetadata struct {
	Block                bool    `json:"block"`
	MaxRequestsPerSecond float64 `json:"maxRequestsPerSecond"`
}

const (
	blockKey                = "block"
	maxRequestsPerSecondKey = "maxRequestsPerSecond"
	numInvokesPerCall       = 4
	httpTooManyRequestsErr  = "too many requests"

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

	rps := meta.MaxRequestsPerSecond * numInvokesPerCall
	var cap int64
	if meta.MaxRequestsPerSecond < numInvokesPerCall {
		cap = numInvokesPerCall
	} else {
		cap = int64(meta.MaxRequestsPerSecond) // WARN: data loss
	}
	bucket := ratelimit.NewBucketWithRate(rps, cap)

	return func(h fasthttp.RequestHandler) fasthttp.RequestHandler {
		return func(ctx *fasthttp.RequestCtx) {
			if meta.Block {
				// server side blocks
				bucket.Wait(1)
			} else if bucket.TakeAvailable(1) == 0 {
				// error and expect client to handle retries
				ctx.Error(httpTooManyRequestsErr, fasthttp.StatusTooManyRequests)
				return
			}
			h(ctx)
		}
	}, nil
}

func (m *Middleware) getNativeMetadata(metadata middleware.Metadata) (*rateLimitMiddlewareMetadata, error) {
	var middlewareMetadata rateLimitMiddlewareMetadata

	middlewareMetadata.Block = false
	if val, ok := metadata.Properties[blockKey]; ok {
		b, err := strconv.ParseBool(val)
		if err != nil {
			return nil, fmt.Errorf("error parsing ratelimit middelware property %s: %+v", blockKey, err)
		}
		middlewareMetadata.Block = b
	}

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
