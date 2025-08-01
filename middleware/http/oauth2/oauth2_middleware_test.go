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
	handler, err := NewOAuth2Middleware(log).GetHandler(t.Context(), metadata)
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

func TestOAuth2CreatesAuthorizationHeaderGetNativeMetadata(t *testing.T) {
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
	oauth2Middleware, ok := NewOAuth2Middleware(log).(*Middleware)
	require.True(t, ok)

	tc := []struct {
		name       string
		pathFilter string
		wantErr    bool
	}{
		{name: "empty pathFilter", pathFilter: "", wantErr: false},
		{name: "wildcard pathFilter", pathFilter: ".*", wantErr: false},
		{name: "api path pathFilter", pathFilter: "/api/v1/users", wantErr: false},
		{name: "debug endpoint pathFilter", pathFilter: "^/debug/?$", wantErr: false},
		{name: "user id pathFilter", pathFilter: "^/user/[0-9]+$", wantErr: false},
		{name: "invalid wildcard pathFilter", pathFilter: "*invalid", wantErr: true},
		{name: "unclosed parenthesis pathFilter", pathFilter: "invalid(", wantErr: true},
		{name: "unopened parenthesis pathFilter", pathFilter: "invalid)", wantErr: true},
	}

	for _, tt := range tc {
		t.Run(tt.name, func(t *testing.T) {
			metadata.Properties["pathFilter"] = tt.pathFilter
			nativeMetadata, err := oauth2Middleware.getNativeMetadata(metadata)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				if tt.pathFilter != "" {
					require.NotNil(t, nativeMetadata.pathFilterRegex)
				} else {
					require.Nil(t, nativeMetadata.pathFilterRegex)
				}
			}
		})
	}
}
