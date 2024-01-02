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
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/fasthttp-contrib/sessions"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/middleware"
	"github.com/dapr/kit/logger"
)

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
