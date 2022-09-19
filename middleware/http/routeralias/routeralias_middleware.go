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

package routeralias

import (
	"fmt"
	"net/http"
	"net/url"

	"github.com/dapr/components-contrib/middleware"
	"github.com/dapr/kit/logger"

	"github.com/julienschmidt/httprouter"
	"github.com/valyala/fasthttp"
)

// Middleware is an routeralias middleware.
type Middleware struct {
	logger logger.Logger
	router *httprouter.Router
}

// NewMiddleware returns a new routerchecker middleware.
func NewMiddleware(logger logger.Logger) middleware.Middleware {
	return &Middleware{logger: logger}
}

// GetHandler retruns the HTTP handler provided by the middleware.
func (m *Middleware) GetHandler(metadata middleware.Metadata) (
	func(h fasthttp.RequestHandler) fasthttp.RequestHandler, error,
) {
	if err := m.getNativeMetadata(metadata); err != nil {
		return nil, err
	}
	return func(h fasthttp.RequestHandler) fasthttp.RequestHandler {
		return func(ctx *fasthttp.RequestCtx) {
			uri := ctx.Request.URI()

			handle, params, _ := m.router.Lookup("", string(uri.Path()))
			if handle != nil {
				req := &http.Request{
					URL: new(url.URL),
				}
				req.URL.RawQuery = string(uri.QueryString())
				handle(nil, req, params)
				ctx.Request.SetRequestURI(req.URL.RequestURI())
			}
			h(ctx)
		}
	}, nil
}

func (m *Middleware) getNativeMetadata(metadata middleware.Metadata) error {
	m.router = httprouter.New()
	for key, value := range metadata.Properties {
		m.router.Handle("", key, m.routerConvert(value))
	}

	return nil
}

func (m *Middleware) routerConvert(daprRouter string) func(resp http.ResponseWriter, req *http.Request, params httprouter.Params) {
	return func(resp http.ResponseWriter, req *http.Request, params httprouter.Params) {
		vals := req.URL.Query()
		for _, param := range params {
			vals.Add(param.Key, param.Value)
		}
		req.URL.RawQuery = vals.Encode()
		req.URL.Path = daprRouter
		req.RequestURI = fmt.Sprintf("%s?%s", daprRouter, req.URL.RawQuery)
	}
}
