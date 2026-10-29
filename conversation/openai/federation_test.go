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

package openai

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
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
	for name, test := range map[string]struct {
		props map[string]string
		path  string
		reply string
	}{
		"chat completions": {
			path:  "/chat/completions",
			reply: `{"id":"c","object":"chat.completion","choices":[{"index":0,"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}]}`,
		},
		"responses API": {
			props: map[string]string{"useResponsesAPI": "true"},
			path:  "/responses",
			reply: `{"status":"completed","output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"ok"}]}]}`,
		},
	} {
		t.Run(name, func(t *testing.T) {
			var exchanges []map[string]string
			var authHeaders []string
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/oauth/token":
					var body map[string]string
					assert.NoError(t, json.NewDecoder(r.Body).Decode(&body))
					exchanges = append(exchanges, body)
					_, _ = io.WriteString(w, `{"access_token":"openai-access-token","expires_in":600}`)
				case test.path:
					authHeaders = append(authHeaders, r.Header.Get("Authorization"))
					w.Header().Set("Content-Type", "application/json")
					_, _ = io.WriteString(w, test.reply)
				default:
					t.Errorf("unexpected request to %s", r.URL.Path)
				}
			}))
			t.Cleanup(srv.Close)

			orig := openAITokenURL
			openAITokenURL = srv.URL + "/oauth/token"
			t.Cleanup(func() { openAITokenURL = orig })

			props := map[string]string{
				"endpoint":           srv.URL,
				"identityProviderId": "idp_1",
				"serviceAccountId":   "sa_1",
			}
			for k, v := range test.props {
				props[k] = v
			}

			minter := new(spiffetest.Minter)
			o := NewOpenAI(logger.NewLogger("test"))
			require.NoError(t, o.Init(spiffecontext.WithJWTMinter(t.Context(), minter), conversation.Metadata{Base: metadata.Base{Properties: props}}))

			msgs := []llms.MessageContent{llms.TextParts(llms.ChatMessageTypeHuman, "hi")}
			for range 2 {
				_, err := o.Converse(t.Context(), &conversation.Request{Message: &msgs})
				require.NoError(t, err)
			}

			require.Len(t, exchanges, 1)
			require.Len(t, minter.Tokens(), 1)
			assert.Equal(t, map[string]string{
				"grant_type":           "urn:ietf:params:oauth:grant-type:token-exchange",
				"subject_token":        minter.Tokens()[0],
				"subject_token_type":   "urn:ietf:params:oauth:token-type:jwt",
				"identity_provider_id": "idp_1",
				"service_account_id":   "sa_1",
			}, exchanges[0])
			assert.Equal(t, []string{"Bearer openai-access-token", "Bearer openai-access-token"}, authHeaders)
		})
	}
}

func TestInitFederationErrors(t *testing.T) {
	ctx := spiffecontext.WithJWTMinter(t.Context(), new(spiffetest.Minter))

	for name, test := range map[string]struct {
		ctxHasMinter bool
		props        map[string]string
		expErr       string
	}{
		"no Dapr identity": {
			props:  map[string]string{"identityProviderId": "idp_1", "serviceAccountId": "sa_1"},
			expErr: "enable mTLS",
		},
		"missing serviceAccountId": {
			ctxHasMinter: true,
			props:        map[string]string{"identityProviderId": "idp_1"},
			expErr:       "serviceAccountId",
		},
		"azure": {
			ctxHasMinter: true,
			props:        map[string]string{"identityProviderId": "idp_1", "serviceAccountId": "sa_1", "apiType": "azure", "endpoint": "https://example.com", "apiVersion": "2024-06-01"},
			expErr:       "Microsoft Entra ID",
		},
	} {
		t.Run(name, func(t *testing.T) {
			initCtx := t.Context()
			if test.ctxHasMinter {
				initCtx = ctx
			}
			err := NewOpenAI(logger.NewLogger("test")).Init(initCtx, conversation.Metadata{Base: metadata.Base{Properties: test.props}})
			require.ErrorContains(t, err, test.expErr)
		})
	}
}

type fakeAzureCredential struct {
	scopes    []string
	hadMinter bool
}

func (f *fakeAzureCredential) GetToken(ctx context.Context, opts policy.TokenRequestOptions) (azcore.AccessToken, error) {
	f.scopes = opts.Scopes
	_, f.hadMinter = spiffecontext.JWTMinterFrom(ctx)
	return azcore.AccessToken{Token: "entra-token", ExpiresOn: time.Now().Add(time.Hour)}, nil
}

func TestAzureTokenSource(t *testing.T) {
	t.Run("passes the Dapr identity to the credential", func(t *testing.T) {
		cred := new(fakeAzureCredential)
		tok, err := (&azureTokenSource{cred: cred, minter: new(spiffetest.Minter)}).Token()
		require.NoError(t, err)
		assert.Equal(t, "entra-token", tok.AccessToken)
		assert.Equal(t, "Bearer", tok.Type())
		assert.Equal(t, []string{cognitiveServicesScope}, cred.scopes)
		assert.True(t, cred.hadMinter)
	})

	t.Run("works without a Dapr identity", func(t *testing.T) {
		cred := new(fakeAzureCredential)
		_, err := (&azureTokenSource{cred: cred}).Token()
		require.NoError(t, err)
		assert.False(t, cred.hadMinter)
	})
}

func TestInitAzureWithoutKeyUsesEntraID(t *testing.T) {
	err := NewOpenAI(logger.NewLogger("test")).Init(t.Context(), conversation.Metadata{Base: metadata.Base{Properties: map[string]string{
		"apiType":          "azure",
		"endpoint":         "https://example.openai.azure.com",
		"apiVersion":       "2024-06-01",
		"azureAuthMethods": "none",
	}}})
	require.ErrorContains(t, err, "Microsoft Entra ID")
}
