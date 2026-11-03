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
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

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
	var authHeaders []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/token":
			assert.NoError(t, r.ParseForm())
			exchanges = append(exchanges, r.PostForm)
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"access_token":"google-access-token","issued_token_type":"urn:ietf:params:oauth:token-type:access_token","token_type":"Bearer","expires_in":3600}`)
		case "/chat/completions":
			authHeaders = append(authHeaders, r.Header.Get("Authorization"))
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
}

func TestInitFederationRequiresIdentity(t *testing.T) {
	err := NewGoogleAI(logger.NewLogger("test")).Init(t.Context(), conversation.Metadata{Base: metadata.Base{Properties: map[string]string{
		"workloadIdentityProvider": "//iam.googleapis.com/projects/123/locations/global/workloadIdentityPools/dapr/providers/sentry",
	}}})
	require.ErrorContains(t, err, "enable mTLS")
}
