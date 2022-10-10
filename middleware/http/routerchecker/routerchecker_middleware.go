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

package routerchecker

import (
	"fmt"
	"net/http"
	"regexp"

	"github.com/dapr/components-contrib/internal/httputils"
	mdutils "github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/middleware"
	"github.com/dapr/kit/logger"
)

// Metadata is the routerchecker middleware config.
type Metadata struct {
	Rule string `json:"rule"`
}

// NewRouterCheckerMiddleware returns a new routerchecker middleware.
func NewMiddleware(logger logger.Logger) middleware.Middleware {
	return &Middleware{logger: logger}
}

// Middleware is an routerchecker middleware.
type Middleware struct {
	logger logger.Logger
}

// GetHandler retruns the HTTP handler provided by the middleware.
func (m *Middleware) GetHandler(metadata middleware.Metadata) (func(next http.Handler) http.Handler, error) {
	meta, err := m.getNativeMetadata(metadata)
	if err != nil {
		return nil, err
	}

	re, err := regexp.Compile(meta.Rule)
	if err != nil {
		return nil, fmt.Errorf("failed to compile rule regexp: %w", err)
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			isPass := re.MatchString(httputils.RequestURI(r))
			if !isPass {
				httputils.RespondWithErrorAndMessage(w, http.StatusBadRequest, "invalid router")
				return
			}
			next.ServeHTTP(w, r)
		})
	}, nil
}

func (m *Middleware) getNativeMetadata(metadata middleware.Metadata) (*Metadata, error) {
	var middlewareMetadata Metadata
	err := mdutils.DecodeMetadata(metadata.Properties, &middlewareMetadata)
	if err != nil {
		return nil, err
	}
	return &middlewareMetadata, nil
}
