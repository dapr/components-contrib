/*
Copyright 2026 The Dapr Authors
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

package conversation

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/oauth2"
)

func TestBuildBearerHTTPClient(t *testing.T) {
	var got http.Header
	srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		got = r.Header.Clone()
	}))
	t.Cleanup(srv.Close)

	t.Run("sets the bearer token and removes API key headers", func(t *testing.T) {
		client := BuildBearerHTTPClient(oauth2.StaticTokenSource(&oauth2.Token{AccessToken: "tok"}))

		req, err := http.NewRequest(http.MethodGet, srv.URL, nil)
		require.NoError(t, err)
		req.Header.Set("x-api-key", PlaceholderToken)
		req.Header.Set("api-key", PlaceholderToken)
		req.Header.Set("Authorization", "Bearer "+PlaceholderToken)

		resp, err := client.Do(req)
		require.NoError(t, err)
		resp.Body.Close()

		assert.Equal(t, "Bearer tok", got.Get("Authorization"))
		assert.Empty(t, got.Get("x-api-key"))
		assert.Empty(t, got.Get("api-key"))
		assert.Equal(t, "Bearer "+PlaceholderToken, req.Header.Get("Authorization"), "the caller's request must not be modified")
	})

	t.Run("token errors fail the request", func(t *testing.T) {
		client := BuildBearerHTTPClient(errTokenSource{})
		req, err := http.NewRequest(http.MethodGet, srv.URL, nil)
		require.NoError(t, err)
		_, err = client.Do(req) //nolint:bodyclose
		require.ErrorContains(t, err, "exchange failed")
	})
}

func TestBuildBearerHTTPClientRedirects(t *testing.T) {
	var otherHits int
	other := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		otherHits++
	}))
	t.Cleanup(other.Close)

	var sameOriginAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/same":
			http.Redirect(w, r, "/final", http.StatusFound)
		case "/final":
			sameOriginAuth = r.Header.Get("Authorization")
		case "/other":
			http.Redirect(w, r, other.URL, http.StatusFound)
		}
	}))
	t.Cleanup(srv.Close)

	client := BuildBearerHTTPClient(oauth2.StaticTokenSource(&oauth2.Token{AccessToken: "tok"}))

	resp, err := client.Get(srv.URL + "/same")
	require.NoError(t, err)
	resp.Body.Close()
	assert.Equal(t, "Bearer tok", sameOriginAuth)

	_, err = client.Get(srv.URL + "/other") //nolint:bodyclose
	require.ErrorContains(t, err, "another origin")
	assert.Zero(t, otherHits, "the token must not be sent to another origin")
}

func TestParseHeaders(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		headers, err := ParseHeaders("")
		require.NoError(t, err)
		assert.Nil(t, headers)
	})

	t.Run("canonicalizes the header names", func(t *testing.T) {
		headers, err := ParseHeaders(`{"x-tenant-id":"acme","Ocp-Apim-Subscription-Key":"secret"}`)
		require.NoError(t, err)
		assert.Equal(t, http.Header{
			"X-Tenant-Id":               {"acme"},
			"Ocp-Apim-Subscription-Key": {"secret"},
		}, headers)
	})

	for name, test := range map[string]struct {
		raw    string
		expErr string
	}{
		"not a JSON object":   {raw: `x-tenant-id=acme`, expErr: "JSON object"},
		"not a string value":  {raw: `{"x-retries":3}`, expErr: "JSON object"},
		"invalid name":        {raw: `{"x tenant":"acme"}`, expErr: "invalid header name"},
		"invalid value":       {raw: `{"x-key":"secret\nvalue"}`, expErr: "invalid value"},
		"client managed":      {raw: `{"host":"example.com"}`, expErr: "set by the HTTP client"},
		"hop-by-hop":          {raw: `{"Connection":"close"}`, expErr: "set by the HTTP client"},
		"set more than once":  {raw: `{"X-Tenant-Id":"a","x-tenant-id":"b"}`, expErr: "more than once"},
		"invalid JSON syntax": {raw: `{"x-key":"secret"`, expErr: "JSON object"},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := ParseHeaders(test.raw)
			require.ErrorContains(t, err, test.expErr)
			assert.NotContains(t, err.Error(), "secret", "errors must not contain header values")
		})
	}
}

func TestWithHeaders(t *testing.T) {
	var got http.Header
	srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		got = r.Header.Clone()
	}))
	t.Cleanup(srv.Close)

	t.Run("sets the headers and replaces existing values", func(t *testing.T) {
		client := WithHeaders(BuildHTTPClient(), http.Header{
			"X-Tenant-Id":   {"acme"},
			"Authorization": {"Bearer gateway"},
		})

		req, err := http.NewRequest(http.MethodGet, srv.URL, nil)
		require.NoError(t, err)
		req.Header.Set("Authorization", "Bearer key")
		req.Header.Set("X-Request-Id", "1")

		resp, err := client.Do(req)
		require.NoError(t, err)
		resp.Body.Close()

		assert.Equal(t, "acme", got.Get("X-Tenant-Id"))
		assert.Equal(t, "Bearer gateway", got.Get("Authorization"))
		assert.Equal(t, "1", got.Get("X-Request-Id"))
		assert.Equal(t, "Bearer key", req.Header.Get("Authorization"), "the caller's request must not be modified")
	})

	t.Run("the bearer token replaces an Authorization header", func(t *testing.T) {
		client := WithHeaders(BuildBearerHTTPClient(oauth2.StaticTokenSource(&oauth2.Token{AccessToken: "tok"})), http.Header{
			"X-Tenant-Id":   {"acme"},
			"Authorization": {"Bearer gateway"},
		})

		resp, err := client.Get(srv.URL)
		require.NoError(t, err)
		resp.Body.Close()

		assert.Equal(t, "acme", got.Get("X-Tenant-Id"))
		assert.Equal(t, "Bearer tok", got.Get("Authorization"))
	})

	t.Run("no headers returns the client", func(t *testing.T) {
		client := BuildHTTPClient()
		assert.Same(t, client, WithHeaders(client, nil))
	})
}

func TestWithHeadersRedirects(t *testing.T) {
	var otherHits int
	other := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		otherHits++
	}))
	t.Cleanup(other.Close)

	var sameOriginTenant string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/same":
			http.Redirect(w, r, "/final", http.StatusFound)
		case "/final":
			sameOriginTenant = r.Header.Get("X-Tenant-Id")
		case "/other":
			http.Redirect(w, r, other.URL, http.StatusFound)
		}
	}))
	t.Cleanup(srv.Close)

	client := WithHeaders(BuildHTTPClient(), http.Header{"X-Tenant-Id": {"acme"}})

	resp, err := client.Get(srv.URL + "/same")
	require.NoError(t, err)
	resp.Body.Close()
	assert.Equal(t, "acme", sameOriginTenant)

	_, err = client.Get(srv.URL + "/other") //nolint:bodyclose
	require.ErrorContains(t, err, "another origin")
	assert.Zero(t, otherHits, "the headers must not be sent to another origin")
}

type errTokenSource struct{}

func (errTokenSource) Token() (*oauth2.Token, error) {
	return nil, errors.New("exchange failed")
}
