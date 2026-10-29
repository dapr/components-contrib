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

package anthropic

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

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
	var exchanges []map[string]string
	var messageHeaders []http.Header
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/oauth/token":
			var body map[string]string
			assert.NoError(t, json.NewDecoder(r.Body).Decode(&body))
			exchanges = append(exchanges, body)
			_, _ = io.WriteString(w, `{"access_token":"sk-ant-oat01-test","token_type":"Bearer","expires_in":600}`)
		case "/messages":
			messageHeaders = append(messageHeaders, r.Header.Clone())
			w.Header().Set("content-type", "application/json")
			_, _ = io.WriteString(w, `{"id":"m","type":"message","role":"assistant","model":"x",`+
				`"content":[{"type":"text","text":"ok"}],"stop_reason":"end_turn",`+
				`"usage":{"input_tokens":1,"output_tokens":1}}`)
		default:
			t.Errorf("unexpected request to %s", r.URL.Path)
		}
	}))
	t.Cleanup(srv.Close)

	minter := new(spiffetest.Minter)
	ctx := spiffecontext.WithJWTMinter(t.Context(), minter)

	a := NewAnthropic(logger.NewLogger("test"))
	require.NoError(t, a.Init(ctx, conversation.Metadata{Base: metadata.Base{Properties: map[string]string{
		"endpoint":         srv.URL,
		"federationRuleId": "fdrl_1",
		"organizationId":   "org",
		"serviceAccountId": "svac_1",
		"workspaceId":      "wrkspc_1",
	}}}))

	msgs := []llms.MessageContent{llms.TextParts(llms.ChatMessageTypeHuman, "hi")}
	for range 2 {
		_, err := a.Converse(t.Context(), &conversation.Request{Message: &msgs})
		require.NoError(t, err)
	}

	require.Len(t, exchanges, 1, "the access token must be reused until it nears expiry")
	require.Len(t, minter.Tokens(), 1)
	assert.Equal(t, map[string]string{
		"grant_type":         "urn:ietf:params:oauth:grant-type:jwt-bearer",
		"assertion":          minter.Tokens()[0],
		"federation_rule_id": "fdrl_1",
		"organization_id":    "org",
		"service_account_id": "svac_1",
		"workspace_id":       "wrkspc_1",
	}, exchanges[0])

	require.Len(t, messageHeaders, 2)
	for _, h := range messageHeaders {
		assert.Equal(t, "Bearer sk-ant-oat01-test", h.Get("Authorization"))
		assert.Empty(t, h.Get("x-api-key"))
	}
}

func TestInitFederationErrors(t *testing.T) {
	ctx := spiffecontext.WithJWTMinter(t.Context(), new(spiffetest.Minter))
	valid := map[string]string{
		"federationRuleId": "fdrl_1",
		"organizationId":   "org",
		"serviceAccountId": "svac_1",
	}

	t.Run("no Dapr identity", func(t *testing.T) {
		err := NewAnthropic(logger.NewLogger("test")).Init(t.Context(), conversation.Metadata{Base: metadata.Base{Properties: valid}})
		require.ErrorContains(t, err, "enable mTLS")
	})

	t.Run("missing organizationId", func(t *testing.T) {
		props := map[string]string{"federationRuleId": "fdrl_1", "serviceAccountId": "svac_1"}
		err := NewAnthropic(logger.NewLogger("test")).Init(ctx, conversation.Metadata{Base: metadata.Base{Properties: props}})
		require.ErrorContains(t, err, "organizationId")
	})

	t.Run("foundry", func(t *testing.T) {
		props := map[string]string{"apiType": "foundry", "endpoint": "https://example.com"}
		for k, v := range valid {
			props[k] = v
		}
		err := NewAnthropic(logger.NewLogger("test")).Init(ctx, conversation.Metadata{Base: metadata.Base{Properties: props}})
		require.ErrorContains(t, err, "foundry")
	})
}
