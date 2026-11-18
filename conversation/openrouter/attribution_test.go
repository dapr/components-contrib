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

package openrouter

import (
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tmc/langchaingo/llms"

	"github.com/dapr/components-contrib/conversation"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
)

const chatCompletionsReply = `{"id":"gen-1","object":"chat.completion","model":"openai/gpt-4o-mini","choices":[{"index":0,"message":{"role":"assistant","content":"ok"},"finish_reason":"stop","native_finish_reason":"stop"}],"usage":{"prompt_tokens":3,"completion_tokens":1,"total_tokens":4,"cost":0.0000004}}`

// converseOnce initialises the component against a fake OpenRouter server and
// returns the headers of the single chat completion request it receives.
func converseOnce(t *testing.T, props map[string]string) http.Header {
	t.Helper()

	var got http.Header
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/chat/completions") {
			t.Errorf("unexpected request to %s", r.URL.Path)
			return
		}
		got = r.Header.Clone()
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, chatCompletionsReply)
	}))
	t.Cleanup(srv.Close)

	all := map[string]string{"key": "test-key", "endpoint": srv.URL}
	maps.Copy(all, props)

	o := NewOpenrouter(logger.NewLogger("test"))
	require.NoError(t, o.Init(t.Context(), conversation.Metadata{Base: metadata.Base{Properties: all}}))

	msgs := []llms.MessageContent{llms.TextParts(llms.ChatMessageTypeHuman, "hi")}
	resp, err := o.Converse(t.Context(), &conversation.Request{Message: &msgs})
	require.NoError(t, err)
	require.Len(t, resp.Outputs, 1)
	require.Len(t, resp.Outputs[0].Choices, 1)
	assert.Equal(t, "ok", resp.Outputs[0].Choices[0].Message.Content)
	assert.Equal(t, "stop", resp.Outputs[0].Choices[0].FinishReason)

	require.NotNil(t, got)
	return got
}

func TestAppAttributionHeaders(t *testing.T) {
	t.Run("not sent when unset", func(t *testing.T) {
		got := converseOnce(t, nil)

		assert.Equal(t, "Bearer test-key", got.Get("Authorization"))
		assert.Empty(t, got.Get(headerAppURL))
		assert.Empty(t, got.Get(headerAppTitle))
		assert.Empty(t, got.Get(headerAppCategories))
	})

	t.Run("sent from metadata", func(t *testing.T) {
		got := converseOnce(t, map[string]string{
			"appURL":        "https://myapp.example.com",
			"appTitle":      " My App ",
			"appCategories": "personal-agent,general-chat",
		})

		assert.Equal(t, "Bearer test-key", got.Get("Authorization"))
		assert.Equal(t, "https://myapp.example.com", got.Get("HTTP-Referer"))
		assert.Equal(t, "My App", got.Get("X-OpenRouter-Title"))
		assert.Equal(t, "personal-agent,general-chat", got.Get("X-OpenRouter-Categories"))
	})

	t.Run("headers metadata field replaces attribution headers", func(t *testing.T) {
		got := converseOnce(t, map[string]string{
			"appURL":   "https://myapp.example.com",
			"appTitle": "My App",
			"headers":  `{"x-openrouter-title":"Gateway Title","X-Tenant-ID":"acme"}`,
		})

		assert.Equal(t, "https://myapp.example.com", got.Get("HTTP-Referer"))
		assert.Equal(t, []string{"Gateway Title"}, got.Values("X-OpenRouter-Title"))
		assert.Equal(t, "acme", got.Get("X-Tenant-ID"))
	})

	t.Run("invalid header value is rejected", func(t *testing.T) {
		err := NewOpenrouter(logger.NewLogger("test")).Init(t.Context(), conversation.Metadata{Base: metadata.Base{Properties: map[string]string{
			"key":      "test-key",
			"appTitle": "My\nApp",
		}}})
		require.ErrorContains(t, err, "invalid value for appTitle")
	})
}
