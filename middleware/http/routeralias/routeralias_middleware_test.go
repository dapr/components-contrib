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
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/dapr/components-contrib/internal/httputils"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/middleware"
	"github.com/dapr/kit/logger"

	"github.com/stretchr/testify/assert"
)

// mockedRequestHandler acts like an upstream service returns success status code 200 and a fixed response body.
func mockedRequestHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	uri := httputils.RequestURI(r)
	w.Write([]byte(uri))
}

func TestRequestHandlerWithIllegalRouterRule(t *testing.T) {
	meta := middleware.Metadata{
		Base: metadata.Base{
			Properties: map[string]string{
				"/v1.0/mall/activity/info":       "/v1.0/invoke/srv.default/method/mall/activity/info",
				"/v1.0/hello/activity/{id}/info": "/v1.0/invoke/srv.default/method/hello/activity/info",
				"/v1.0/hello/activity/{id}/user": "/v1.0/invoke/srv.default/method/hello/activity/user",
			},
		},
	}
	log := logger.NewLogger("routeralias.test")
	ralias := NewMiddleware(log)
	handler, err := ralias.GetHandler(meta)
	assert.Nil(t, err)

	t.Run("hit: change router with common request", func(t *testing.T) {
		r := httptest.NewRequest(http.MethodGet,
			"http://localhost:5001/v1.0/mall/activity/info?id=123", nil)
		w := httptest.NewRecorder()

		handler(http.HandlerFunc(mockedRequestHandler)).ServeHTTP(w, r)
		msg, err := io.ReadAll(w.Body)
		assert.Nil(t, err)
		result := w.Result()
		assert.Equal(t, http.StatusOK, result.StatusCode)
		assert.Equal(t,
			"/v1.0/invoke/srv.default/method/mall/activity/info?id=123",
			string(msg))
		result.Body.Close()
	})

	t.Run("hit: change router with restful request", func(t *testing.T) {
		r := httptest.NewRequest(http.MethodGet,
			"http://localhost:5001/v1.0/hello/activity/1/info", nil)
		w := httptest.NewRecorder()

		handler(http.HandlerFunc(mockedRequestHandler)).ServeHTTP(w, r)
		msg, err := io.ReadAll(w.Body)
		assert.Nil(t, err)
		result := w.Result()
		assert.Equal(t, http.StatusOK, result.StatusCode)
		assert.Equal(t,
			"/v1.0/invoke/srv.default/method/hello/activity/info?id=1",
			string(msg))
		result.Body.Close()
	})

	t.Run("hit: change router with restful request and query string", func(t *testing.T) {
		r := httptest.NewRequest(http.MethodGet,
			"http://localhost:5001/v1.0/hello/activity/1/user?userid=123", nil)
		w := httptest.NewRecorder()

		handler(http.HandlerFunc(mockedRequestHandler)).ServeHTTP(w, r)
		msg, err := io.ReadAll(w.Body)
		assert.Nil(t, err)
		result := w.Result()
		assert.Equal(t, http.StatusOK, result.StatusCode)
		assert.Equal(t,
			"/v1.0/invoke/srv.default/method/hello/activity/user?id=1&userid=123",
			string(msg))
		result.Body.Close()
	})

	t.Run("miss: no change router", func(t *testing.T) {
		r := httptest.NewRequest(http.MethodGet,
			"http://localhost:5001/v1.0/invoke/srv.default", nil)
		w := httptest.NewRecorder()
		handler(http.HandlerFunc(mockedRequestHandler)).ServeHTTP(w, r)
		msg, err := io.ReadAll(w.Body)
		assert.Nil(t, err)
		result := w.Result()
		assert.Equal(t, http.StatusOK, result.StatusCode)
		assert.Equal(t,
			"/v1.0/invoke/srv.default",
			string(msg))
		result.Body.Close()
	})
}
