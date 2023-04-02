package oauth2

import (
	"fmt"
	"github.com/dapr/components-contrib/middleware"
	"github.com/dapr/kit/logger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
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
		"clientID":          "testId",
		"clientSecret":      "testSecret",
		"scopes":            "ascope",
		"authURL":           ts.URL,
		"tokenURL":          ts.URL,
		"authHeaderName":    "someHeader",
		"redirectURL":       "https://127.0.0.1:8080",
		"redirectParamName": "redirectPath",
	}

	// Initialize middleware component
	log := logger.NewLogger("oauth2.test")
	oauth2Middleware, _ := NewOAuth2Middleware(log).(*Middleware)
	handler, err := oauth2Middleware.GetHandler(metadata)
	require.NoError(t, err)

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
