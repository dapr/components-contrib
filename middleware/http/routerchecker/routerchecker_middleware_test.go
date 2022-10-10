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
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/middleware"
	"github.com/dapr/kit/logger"
)

// mockedRequestHandler acts like an upstream service returns success status code 200 and a fixed response body.
func mockedRequestHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("from mock"))
}

func TestRequestHandlerWithIllegalRouterRule(t *testing.T) {
	meta := middleware.Metadata{Base: metadata.Base{Properties: map[string]string{
		"rule": "^[A-Za-z0-9/._-]+$",
	}}}
	log := logger.NewLogger("routerchecker.test")
	rchecker := NewMiddleware(log)
	handler, err := rchecker.GetHandler(meta)
	assert.Nil(t, err)

	r := httptest.NewRequest(http.MethodGet, "http://localhost:5001/v1.0/invoke/qcg.default/method/%20cat%20password", nil)
	w := httptest.NewRecorder()
	handler(http.HandlerFunc(mockedRequestHandler)).ServeHTTP(w, r)

	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestRequestHandlerWithLegalRouterRule(t *testing.T) {
	meta := middleware.Metadata{Base: metadata.Base{Properties: map[string]string{
		"rule": "^[A-Za-z0-9/._-]+$",
	}}}

	log := logger.NewLogger("routerchecker.test")
	rchecker := NewMiddleware(log)
	handler, err := rchecker.GetHandler(meta)
	assert.Nil(t, err)

	r := httptest.NewRequest(http.MethodGet, "http://localhost:5001/v1.0/invoke/qcg.default/method", nil)
	w := httptest.NewRecorder()
	handler(http.HandlerFunc(mockedRequestHandler)).ServeHTTP(w, r)

	assert.Equal(t, http.StatusOK, w.Code)
}
