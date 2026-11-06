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

package googleai

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/spiffe/go-spiffe/v2/svid/jwtsvid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tmc/langchaingo/llms"

	"github.com/dapr/components-contrib/common/authentication/spiffe/spiffetest"
	"github.com/dapr/components-contrib/conversation"
	"github.com/dapr/components-contrib/metadata"
	spiffecontext "github.com/dapr/kit/crypto/spiffe/context"
	"github.com/dapr/kit/logger"
)

func TestInitFederation(t *testing.T) {
	const provider = "//iam.googleapis.com/projects/123/locations/global/workloadIdentityPools/dapr/providers/sentry"

	var exchanges []url.Values
	var authHeaders, tenantHeaders []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/token":
			assert.NoError(t, r.ParseForm())
			exchanges = append(exchanges, r.PostForm)
			assert.Empty(t, r.Header.Get("X-Tenant-ID"), "custom headers must not be sent to the token endpoint")
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"access_token":"google-access-token","issued_token_type":"urn:ietf:params:oauth:token-type:access_token","token_type":"Bearer","expires_in":3600}`)
		case "/chat/completions":
			authHeaders = append(authHeaders, r.Header.Get("Authorization"))
			tenantHeaders = append(tenantHeaders, r.Header.Get("X-Tenant-ID"))
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"id":"c","object":"chat.completion","choices":[{"index":0,"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}]}`)
		default:
			t.Errorf("unexpected request to %s", r.URL.Path)
		}
	}))
	t.Cleanup(srv.Close)

	orig := gcpSTSTokenURL
	gcpSTSTokenURL = srv.URL + "/v1/token"
	t.Cleanup(func() { gcpSTSTokenURL = orig })

	minter := new(spiffetest.Minter)
	g := NewGoogleAI(logger.NewLogger("test"))
	require.NoError(t, g.Init(spiffecontext.WithJWTMinter(t.Context(), minter), conversation.Metadata{Base: metadata.Base{Properties: map[string]string{
		"endpoint":                 srv.URL,
		"workloadIdentityProvider": provider,
		"headers":                  `{"X-Tenant-ID":"acme"}`,
	}}}))

	msgs := []llms.MessageContent{llms.TextParts(llms.ChatMessageTypeHuman, "hi")}
	for range 2 {
		_, err := g.Converse(t.Context(), &conversation.Request{Message: &msgs})
		require.NoError(t, err)
	}

	require.Len(t, exchanges, 1)
	require.Len(t, minter.Tokens(), 1)
	assert.Equal(t, provider, exchanges[0].Get("audience"))
	assert.Equal(t, minter.Tokens()[0], exchanges[0].Get("subject_token"))
	_, err := jwtsvid.ParseInsecure(minter.Tokens()[0], []string{provider})
	require.NoError(t, err, "the JWT SVID audience must be the provider resource name")
	assert.Equal(t, "urn:ietf:params:oauth:token-type:jwt", exchanges[0].Get("subject_token_type"))
	assert.Equal(t, []string{"Bearer google-access-token", "Bearer google-access-token"}, authHeaders)
	assert.Equal(t, []string{"acme", "acme"}, tenantHeaders)
}

func TestInitFederationImpersonation(t *testing.T) {
	for email, path := range map[string]string{
		"dapr-ai@my-project.iam.gserviceaccount.com":  "/v1/projects/-/serviceAccounts/dapr-ai@my-project.iam.gserviceaccount.com:generateAccessToken",
		"../x?y#z@my-project.iam.gserviceaccount.com": "/v1/projects/-/serviceAccounts/..%2Fx%3Fy%23z@my-project.iam.gserviceaccount.com:generateAccessToken",
	} {
		t.Run(email, func(t *testing.T) {
			var impersonations, authHeaders []string
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				switch {
				case r.URL.Path == "/v1/token":
					_, _ = io.WriteString(w, `{"access_token":"sts-token","issued_token_type":"urn:ietf:params:oauth:token-type:access_token","token_type":"Bearer","expires_in":3600}`)
				case r.URL.EscapedPath() == path:
					var req struct {
						Scope []string `json:"scope"`
					}
					assert.NoError(t, json.NewDecoder(r.Body).Decode(&req))
					assert.Equal(t, []string{cloudPlatformScope}, req.Scope)
					impersonations = append(impersonations, r.Header.Get("Authorization"))
					_, _ = io.WriteString(w, `{"accessToken":"impersonated-token","expireTime":"`+time.Now().Add(time.Hour).Format(time.RFC3339)+`"}`)
				case r.URL.Path == "/chat/completions":
					authHeaders = append(authHeaders, r.Header.Get("Authorization"))
					_, _ = io.WriteString(w, `{"id":"c","object":"chat.completion","choices":[{"index":0,"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}]}`)
				default:
					t.Errorf("unexpected request to %s", r.URL.EscapedPath())
				}
			}))
			t.Cleanup(srv.Close)

			origSTS, origIAM := gcpSTSTokenURL, gcpIAMCredentialsURL
			gcpSTSTokenURL, gcpIAMCredentialsURL = srv.URL+"/v1/token", srv.URL+"/v1/"
			t.Cleanup(func() { gcpSTSTokenURL, gcpIAMCredentialsURL = origSTS, origIAM })

			g := NewGoogleAI(logger.NewLogger("test"))
			require.NoError(t, g.Init(spiffecontext.WithJWTMinter(t.Context(), new(spiffetest.Minter)), conversation.Metadata{Base: metadata.Base{Properties: map[string]string{
				"endpoint":                 srv.URL,
				"workloadIdentityProvider": "//iam.googleapis.com/projects/123/locations/global/workloadIdentityPools/dapr/providers/sentry",
				"serviceAccountEmail":      email,
			}}}))

			msgs := []llms.MessageContent{llms.TextParts(llms.ChatMessageTypeHuman, "hi")}
			for range 2 {
				_, err := g.Converse(t.Context(), &conversation.Request{Message: &msgs})
				require.NoError(t, err)
			}

			assert.Equal(t, []string{"Bearer sts-token"}, impersonations)
			assert.Equal(t, []string{"Bearer impersonated-token", "Bearer impersonated-token"}, authHeaders)
		})
	}
}

func TestInitFederationRequiresIdentity(t *testing.T) {
	err := NewGoogleAI(logger.NewLogger("test")).Init(t.Context(), conversation.Metadata{Base: metadata.Base{Properties: map[string]string{
		"workloadIdentityProvider": "//iam.googleapis.com/projects/123/locations/global/workloadIdentityPools/dapr/providers/sentry",
	}}})
	require.ErrorContains(t, err, "enable mTLS")
}
