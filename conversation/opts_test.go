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

type errTokenSource struct{}

func (errTokenSource) Token() (*oauth2.Token, error) {
	return nil, errors.New("exchange failed")
}
