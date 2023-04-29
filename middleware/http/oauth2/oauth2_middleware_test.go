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

package oauth2

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/fasthttp-contrib/sessions"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/middleware"
	"github.com/dapr/kit/logger"
)

// mockedRequestHandler acts like an upstream service returns success status code 200 and a fixed response body.
func mockedRequestHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("access_token=12345"))
}

// TestOAuth2RedirectURL checks if
// 1. the redirect URL is set to the fixed value from metadata if the redirect param is not set in the request
// 2. the redirect URL is set to the value from the redirect param if the redirect param is set in the request
func TestOAuth2RedirectURL(t *testing.T) {
	// initialize test server
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mockedRequestHandler(w, r)
	}))
	defer ts.Close()

	// Specify components metadata
	var metadata middleware.Metadata
	metadata.Properties = map[string]string{
		"clientID":             "testId",
		"clientSecret":         "testSecret",
		"scopes":               "ascope",
		"authURL":              ts.URL,
		"tokenURL":             ts.URL,
		"authHeaderName":       "someHeader",
		"redirectURL":          "https://127.0.0.1:8080",
		"redirectParamName":    "redirectPath",
		"redirectURLWhitelist": `https://127.0.0.1:8080,https://192.168.0.1:8080,https://dapr.io, https://*.dapr.io`,
	}

	// Initialize middleware component
	log := logger.NewLogger("oauth2.test")
	oauth2Middleware, _ := NewOAuth2Middleware(log).(*Middleware)
	handler, err := oauth2Middleware.GetHandler(context.Background(), metadata)
	require.NoError(t, err)

	// Test redirect url whitelist
	t.Run("redirect url exact match whitelist", func(t *testing.T) {
		r := httptest.NewRequest(http.MethodGet, "http://dapr.io?redirectPath=https://dapr.io", nil)
		w := httptest.NewRecorder()
		handler(http.HandlerFunc(mockedRequestHandler)).ServeHTTP(w, r)

		assert.Equal(t, http.StatusFound, w.Code)
	})
	t.Run("redirect url with query parameter", func(t *testing.T) {
		r := httptest.NewRequest(http.MethodGet, "http://dapr.io?redirectPath=https://dapr.io?a=123", nil)
		w := httptest.NewRecorder()
		handler(http.HandlerFunc(mockedRequestHandler)).ServeHTTP(w, r)

		assert.Equal(t, http.StatusFound, w.Code)
	})
	t.Run("redirect url matches wildcard", func(t *testing.T) {
		r := httptest.NewRequest(http.MethodGet, "http://dapr.io?redirectPath=https://api.dapr.io?a=123", nil)
		w := httptest.NewRecorder()
		handler(http.HandlerFunc(mockedRequestHandler)).ServeHTTP(w, r)

		assert.Equal(t, http.StatusFound, w.Code)
	})
	t.Run("invalid redirect url", func(t *testing.T) {
		r := httptest.NewRequest(http.MethodGet, "http://dapr.io?redirectPath=https://daprs.io?a=123", nil)
		w := httptest.NewRecorder()
		handler(http.HandlerFunc(mockedRequestHandler)).ServeHTTP(w, r)

		assert.Equal(t, http.StatusBadRequest, w.Code)

		r = httptest.NewRequest(http.MethodGet, "http://dapr.io?redirectPath=http://dapr.io?a=123", nil)
		w = httptest.NewRecorder()
		handler(http.HandlerFunc(mockedRequestHandler)).ServeHTTP(w, r)

		assert.Equal(t, http.StatusBadRequest, w.Code)

		r = httptest.NewRequest(http.MethodGet, "https://dapr.io?redirectPath=https://dapr.io.io?a=123", nil)
		w = httptest.NewRecorder()
		handler(http.HandlerFunc(mockedRequestHandler)).ServeHTTP(w, r)

		assert.Equal(t, http.StatusBadRequest, w.Code)
	})
	t.Run("empty whitelist", func(t *testing.T) {
		metadata.Properties = map[string]string{
			"clientID":          "testId",
			"clientSecret":      "testSecret",
			"scopes":            "ascope",
			"authURL":           ts.URL,
			"tokenURL":          ts.URL,
			"authHeaderName":    "someHeader",
			"redirectURL":       "https://127.0.0.1:8080",
			"redirectParamName": "redirectPath",
		}
		handler, err = oauth2Middleware.GetHandler(context.Background(), metadata)
		r := httptest.NewRequest(http.MethodGet, "http://dapr.io?redirectPath=https://dapr.io.io?a=123", nil)
		w := httptest.NewRecorder()
		handler(http.HandlerFunc(mockedRequestHandler)).ServeHTTP(w, r)

		assert.Equal(t, http.StatusFound, w.Code)
	})

	// Test redirect url metadata and redirect param
	t.Run("request without redirect param", func(t *testing.T) {
		r := httptest.NewRequest(http.MethodGet, "http://dapr.io", nil)
		w := httptest.NewRecorder()
		handler(http.HandlerFunc(mockedRequestHandler)).ServeHTTP(w, r)

		authURLStr := w.Header().Get("location")
		authURL, err := url.Parse(authURLStr)
		assert.NoError(t, err)

		resp := w.Result()
		defer resp.Body.Close()
		cookies := resp.Cookies()
		state := authURL.Query().Get(stateParam)
		r1 := httptest.NewRequest(http.MethodGet, fmt.Sprintf(
			"http://dapr.io?%s=%s&%s=%s",
			stateParam,
			state,
			codeParam,
			"someCode",
		), nil)
		for _, cookie := range cookies {
			r1.AddCookie(cookie)
		}
		w1 := httptest.NewRecorder()
		handler(http.HandlerFunc(mockedRequestHandler)).ServeHTTP(w1, r1)

		assert.Equal(t, "https://127.0.0.1:8080", w1.Header().Get("location"))
	})
	t.Run("request with redirect param", func(t *testing.T) {
		r := httptest.NewRequest(http.MethodGet, "http://dapr.io?redirectPath=https://192.168.0.1:8080", nil)
		w := httptest.NewRecorder()
		handler(http.HandlerFunc(mockedRequestHandler)).ServeHTTP(w, r)

		authURLStr := w.Header().Get("location")
		authURL, err := url.Parse(authURLStr)
		assert.NoError(t, err)

		resp := w.Result()
		defer resp.Body.Close()
		cookies := resp.Cookies()
		state := authURL.Query().Get(stateParam)
		r1 := httptest.NewRequest(http.MethodGet, fmt.Sprintf(
			"http://dapr.io?%s=%s&%s=%s",
			stateParam,
			state,
			codeParam,
			"someCode",
		), nil)
		for _, cookie := range cookies {
			r1.AddCookie(cookie)
		}
		w1 := httptest.NewRecorder()
		handler(http.HandlerFunc(mockedRequestHandler)).ServeHTTP(w1, r1)

		assert.Equal(t, "https://192.168.0.1:8080", w1.Header().Get("location"))
	})
}

func TestOAuth2CreatesAuthorizationHeaderWhenInSessionState(t *testing.T) {
	var metadata middleware.Metadata
	metadata.Properties = map[string]string{
		"clientID":       "testId",
		"clientSecret":   "testSecret",
		"scopes":         "ascope",
		"authURL":        "https://idp:9999",
		"tokenURL":       "https://idp:9999",
		"redirectUrl":    "https://localhost:9999",
		"authHeaderName": "someHeader",
	}

	log := logger.NewLogger("oauth2.test")
	handler, err := NewOAuth2Middleware(log).GetHandler(context.Background(), metadata)
	require.NoError(t, err)

	// Create request and recorder
	r := httptest.NewRequest(http.MethodGet, "http://dapr.io", nil)
	w := httptest.NewRecorder()
	session := sessions.Start(w, r)
	session.Set("someHeader", "Bearer abcd")

	// Copy the session cookie to the request
	cookie := w.Header().Get("Set-Cookie")
	r.Header.Add("Cookie", cookie)

	handler(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("from mock"))
		}),
	).ServeHTTP(w, r)

	assert.Equal(t, "Bearer abcd", r.Header.Get("someHeader"))
}
