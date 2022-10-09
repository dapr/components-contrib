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
	"context"
	"fmt"
	"net/http"

	"github.com/dapr/components-contrib/middleware"
	"github.com/dapr/kit/logger"

	"github.com/gorilla/mux"
)

type contextKey int

const varsKey contextKey = iota

// Middleware is an routeralias middleware.
type Middleware struct {
	logger logger.Logger
	router *mux.Router
}

// NewMiddleware returns a new routerchecker middleware.
func NewMiddleware(logger logger.Logger) middleware.Middleware {
	return &Middleware{logger: logger}
}

// GetHandler retruns the HTTP handler provided by the middleware.
func (m *Middleware) GetHandler(metadata middleware.Metadata) (
	func(next http.Handler) http.Handler, error,
) {
	if err := m.getNativeMetadata(metadata); err != nil {
		return nil, err
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			var match mux.RouteMatch
			if m.router.Match(r, &match) {
				ctx := context.WithValue(r.Context(), varsKey, match.Vars)
				r = r.WithContext(ctx)
				match.Handler.ServeHTTP(w, r)
			}
			next.ServeHTTP(w, r)
		})
	}, nil
}

func (m *Middleware) getNativeMetadata(metadata middleware.Metadata) error {
	m.router = mux.NewRouter()
	for key, value := range metadata.Properties {
		m.router.Handle(key, m.routerConvert(value))
	}

	return nil
}

func (m *Middleware) routerConvert(daprRouter string) http.Handler {
	return http.HandlerFunc(func(resp http.ResponseWriter, req *http.Request) {
		params := vars(req)
		vals := req.URL.Query()
		for key, val := range params {
			vals.Add(key, val)
		}
		req.URL.RawQuery = vals.Encode()
		req.URL.Path = daprRouter
		req.RequestURI = fmt.Sprintf("%s?%s", daprRouter, req.URL.RawQuery)
	})
}

// vars returns the route variables for the current request, if any.
func vars(r *http.Request) map[string]string {
	if rv := r.Context().Value(varsKey); rv != nil {
		return rv.(map[string]string)
	}
	return nil
}
