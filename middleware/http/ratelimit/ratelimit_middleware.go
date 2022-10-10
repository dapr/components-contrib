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

package ratelimit

import (
	"fmt"
	"net/http"
	"strconv"

	"github.com/didip/tollbooth"

	"github.com/dapr/components-contrib/middleware"
	"github.com/dapr/kit/logger"
)

// Metadata is the ratelimit middleware config.
type rateLimitMiddlewareMetadata struct {
	MaxRequestsPerSecond float64 `json:"maxRequestsPerSecond"`
}

const (
	maxRequestsPerSecondKey = "maxRequestsPerSecond"

	// Defaults.
	defaultMaxRequestsPerSecond = 100
)

// NewRateLimitMiddleware returns a new ratelimit middleware.
func NewRateLimitMiddleware(_ logger.Logger) middleware.Middleware {
	return &Middleware{}
}

// Middleware is an ratelimit middleware.
type Middleware struct{}

// GetHandler returns the HTTP handler provided by the middleware.
func (m *Middleware) GetHandler(metadata middleware.Metadata) (func(next http.Handler) http.Handler, error) {
	meta, err := m.getNativeMetadata(metadata)
	if err != nil {
		return nil, err
	}

	limiter := tollbooth.NewLimiter(meta.MaxRequestsPerSecond, nil)

	return func(next http.Handler) http.Handler {
		return tollbooth.LimitHandler(limiter, next)
	}, nil
}

func (m *Middleware) getNativeMetadata(metadata middleware.Metadata) (*rateLimitMiddlewareMetadata, error) {
	var middlewareMetadata rateLimitMiddlewareMetadata

	middlewareMetadata.MaxRequestsPerSecond = defaultMaxRequestsPerSecond
	if val, ok := metadata.Properties[maxRequestsPerSecondKey]; ok {
		f, err := strconv.ParseFloat(val, 64)
		if err != nil {
			return nil, fmt.Errorf("error parsing ratelimit middleware property %s: %w", maxRequestsPerSecondKey, err)
		}
		if f <= 0 {
			return nil, fmt.Errorf("ratelimit middleware property %s must be a positive value", maxRequestsPerSecondKey)
		}
		middlewareMetadata.MaxRequestsPerSecond = f
	}

	return &middlewareMetadata, nil
}
