/*
Copyright 2023 The Dapr Authors
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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/oauth2"
	"google.golang.org/grpc/test/bufconn"

	"github.com/dapr/kit/logger"
)

const testAuthHeaderName = "x-dapr-token"

var (
	log            logger.Logger
	testHTTPClient *http.Client
)

func TestMain(m *testing.M) {
	log = logger.NewLogger("test")

	// Start an internal server for returning tokens
	ln := bufconn.Listen(1 << 20) // 1MB buffer size

	mux := chi.NewMux()
	mux.Post("/token", func(w http.ResponseWriter, r *http.Request) {
		reqBody := map[string]string{}

		switch r.Header.Get("Content-Type") {
		case "application/x-www-form-urlencoded":
			body, err := io.ReadAll(r.Body)
			if err != nil {
				log.Error("Failed to parse request body ", err)
				http.Error(w, "Failed to parse request body", http.StatusBadRequest)
				return
			}

			vals, err := url.ParseQuery(string(body))
			if err != nil {
				log.Error("Failed to parse request body ", err)
				http.Error(w, "Failed to parse request body", http.StatusBadRequest)
				return
			}
			for k, v := range vals {
				reqBody[k] = v[0]
			}

		case "application/json":
			err := json.NewDecoder(r.Body).Decode(&reqBody)
			if err != nil {
				log.Error("Failed to parse request body ", err)
				http.Error(w, "Failed to parse request body", http.StatusBadRequest)
				return
			}
		default:
			log.Error("Invalid content type ", r.Header.Get("Content-Type"))
			http.Error(w, "Invalid content type", http.StatusBadRequest)
			return
		}

		if reqBody["client_id"] != "client-id" || reqBody["client_secret"] != "client-secret" ||
			reqBody["code"] == "" || reqBody["grant_type"] != "authorization_code" {
			log.Error("Invalid request body ", reqBody)
			http.Error(w, "Invalid request body", http.StatusBadRequest)
			return
		}

		switch reqBody["code"] {
		case "bad":
			http.Error(w, "Simulated error", http.StatusUnauthorized)
			return
		default:
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			res := map[string]any{
				"access_token": "good-access-token",
				"expires_in":   1800,
				"token_type":   "Bearer",
			}
			json.NewEncoder(w).Encode(res)
			return
		}
	})
	srv := http.Server{
		Handler: mux,
	}
	go srv.Serve(ln)
	defer srv.Close()

	testHTTPClient = &http.Client{
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
				return ln.DialContext(ctx)
			},
		},
	}

	os.Exit(m.Run())
}

func TestMiddlewareHeader(t *testing.T) {
	md := OAuth2MiddlewareMetadata{
		Mode:               modeHeader,
		ClientID:           "client-id",
		ClientSecret:       "client-secret",
		Scopes:             "profile,email",
		AuthURL:            "http://localhost:8000/auth",
		TokenURL:           "http://localhost:8000/token",
		RedirectURL:        "http://localhost:4000/",
		AuthHeaderName:     testAuthHeaderName,
		TokenEncryptionKey: "Ciao.Mamma.Guarda.Come.Mi.Diverto",
		CookieName:         defaultCookieName,
	}
	md.setOAuth2Conf()
	err := md.setTokenKeys()
	require.NoError(t, err)

	mw := &OAuth2Middleware{
		logger: log,
		meta:   md,
	}

	// Next handler
	nextCh := make(chan string, 1)
	nextHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, "OK")
		nextCh <- r.Header.Get(testAuthHeaderName)
	})

	// Handler from the middleware component
	handler := mw.getHandler()(nextHandler)

	// Create a context that has the custom HTTP client included, so the oauth2 package uses that
	ctx := context.WithValue(context.Background(), oauth2.HTTPClient, testHTTPClient)

	// Cookies, state token, and authorization token
	var (
		cookies   []*http.Cookie
		state     string
		authToken string
	)

	assertRedirectToAuthEndpoint := func(t *testing.T, res *http.Response) {
		// Should have a redirect to the auth URL
		assert.Equal(t, http.StatusFound, res.StatusCode)
		assert.NotEmpty(t, res.Header.Get("Location"))
		loc, err := url.Parse(res.Header.Get("Location"))
		require.NoError(t, err)
		assert.True(t, strings.HasPrefix(loc.String(), "http://localhost:8000/auth"))
		assert.NotEmpty(t, loc.Query().Get("state"))
		assert.Equal(t, "client-id", loc.Query().Get("client_id"))
		assert.Equal(t, "http://localhost:4000/", loc.Query().Get("redirect_uri"))
		assert.Equal(t, "profile email", loc.Query().Get("scope"))

		// Should have a cookie
		// The cookie is an encrypted JWT so we won't verify it here
		require.NotEmpty(t, res.Header.Get("Set-Cookie"))
		cookies = res.Cookies()
		require.Len(t, cookies, 1)
		state = loc.Query().Get("state")

		// Handler should not have been called
		select {
		case <-time.After(100 * time.Millisecond):
			// All good
		case <-nextCh:
			t.Fatalf("Received a signal on nextCh that was not expected")
		}
	}

	t.Run("missing token redirects to auth endpoint", func(t *testing.T) {
		w := httptest.NewRecorder()
		r := httptest.NewRequest(http.MethodPost, "http://localhost:4000/method", nil)
		r = r.WithContext(ctx)

		handler.ServeHTTP(w, r)

		// Allow other goroutines first
		runtime.Gosched()

		// Should redirect to auth endpoint
		res := w.Result()
		defer res.Body.Close()
		assertRedirectToAuthEndpoint(t, res)
	})

	// Cannot continue if tests so far have failed
	if t.Failed() {
		return
	}

	t.Run("token endpoint returns an error", func(t *testing.T) {
		logDest := &bytes.Buffer{}
		log.SetOutput(logDest)
		defer log.SetOutput(os.Stdout)

		w := httptest.NewRecorder()
		r := httptest.NewRequest(http.MethodPost, "http://localhost:4000/method?state="+state+"&code=bad", nil)
		r.AddCookie(cookies[0])
		r = r.WithContext(ctx)

		handler.ServeHTTP(w, r)

		// Allow other goroutines first
		runtime.Gosched()

		// Should contain a message in the logs
		assert.Contains(t, logDest.String(), "Failed to exchange token")

		// Should have an ISE in response
		res := w.Result()
		defer res.Body.Close()

		require.Equal(t, http.StatusInternalServerError, res.StatusCode)
	})

	t.Run("state in cookie doesn't match URL parameter", func(t *testing.T) {
		w := httptest.NewRecorder()
		r := httptest.NewRequest(http.MethodPost, "http://localhost:4000/method?state=badstate&code=good", nil)
		r.AddCookie(cookies[0])
		r = r.WithContext(ctx)

		handler.ServeHTTP(w, r)

		// Allow other goroutines first
		runtime.Gosched()

		// Response should be BadRequest
		res := w.Result()
		defer res.Body.Close()

		require.Equal(t, http.StatusBadRequest, res.StatusCode)
	})

	// Cannot continue if tests so far have failed
	if t.Failed() {
		return
	}

	t.Run("obtain access token", func(t *testing.T) {
		w := httptest.NewRecorder()
		r := httptest.NewRequest(http.MethodPost, "http://localhost:4000/method?state="+state+"&code=good", nil)
		r.AddCookie(cookies[0])
		r = r.WithContext(ctx)

		handler.ServeHTTP(w, r)

		// Allow other goroutines first
		runtime.Gosched()

		// Response should contain the access token
		res := w.Result()
		defer res.Body.Close()
		assert.Equal(t, http.StatusOK, res.StatusCode)
		assert.Equal(t, "application/json", res.Header.Get("content-type"))

		body := map[string]string{}
		err := json.NewDecoder(res.Body).Decode(&body)
		require.NoError(t, err)

		require.NotEmpty(t, body["Authorization"])
		authToken = body["Authorization"]

		// Should delete the cookie
		require.NotEmpty(t, res.Header.Get("Set-Cookie"))
		cookies = res.Cookies()
		require.Len(t, cookies, 1)
		assert.LessOrEqual(t, cookies[0].MaxAge, 0)

		// Handler should not have been called
		select {
		case <-time.After(100 * time.Millisecond):
			// All good
		case <-nextCh:
			t.Fatalf("Received a signal on nextCh that was not expected")
		}
	})

	t.Run("requests with access token should succeed", func(t *testing.T) {
		w := httptest.NewRecorder()
		r := httptest.NewRequest(http.MethodPost, "http://localhost:4000/method", nil)
		r = r.WithContext(ctx)
		r.Header.Set("Authorization", authToken)

		handler.ServeHTTP(w, r)

		// Next handler should be invoked
		select {
		case <-time.After(500 * time.Millisecond):
			t.Fatalf("Did not receive signal on nextCh within 500ms")
		case header := <-nextCh:
			assert.Equal(t, "Bearer good-access-token", header)
		}
	})
}
