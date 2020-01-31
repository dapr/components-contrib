// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package ratelimit

import (
	"fmt"
	"strconv"
	"net/http"
	"io/ioutil"

	"github.com/dapr/components-contrib/middleware"
	"github.com/didip/tollbooth"
	"github.com/valyala/fasthttp"
	log "github.com/sirupsen/logrus"
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
		limitHandler := tollbooth.LimitFuncHandler(limiter, newHTTPHandlerFunc(h))
		return func(ctx *fasthttp.RequestCtx) {
			fasthttpadaptor.NewFastHTTPHandlerFunc(limitHandler.ServeHTTP)(ctx)
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

// TODO: This is also used in https://github.com/dapr/dapr/pull/962.
//       move to common location when needed. Also needs additional
//       support for multipartform data etc.
func newHTTPHandlerFunc(h fasthttp.RequestHandler) http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c := fasthttp.RequestCtx{
			Request: fasthttp.Request{},
			Response: fasthttp.Response{},
		}

		reqBody, err := ioutil.ReadAll(r.Body)
		if err != nil {
			log.Errorf("error reading request body, %+v", err)
			return
		}
		c.Request.SetBody(reqBody)
		c.Request.SetRequestURI(r.URL.RequestURI())
		c.Request.SetHost(r.Host)
		c.Request.Header.SetMethod(r.Method)
		c.Request.Header.Set("Proto", r.Proto)
		c.Request.Header.Set("ProtoMajor", string(r.ProtoMajor))
		c.Request.Header.Set("ProtoMinor", string(r.ProtoMinor))
		for _, cookie := range r.Cookies() {
			c.Request.Header.SetCookie(cookie.Name, cookie.Value)
		}
		for k, v := range r.Header {
			for _, i := range v { // TODO: Check this works for Transfer-Encoding
				c.Request.Header.Add(k, i)
			}
		}
		
		h(&c)

		c.Response.Header.VisitAll(func(k []byte, v []byte) {
			w.Header().Add(string(k), string(v))
		})
		c.Response.BodyWriteTo(w)
	})
}