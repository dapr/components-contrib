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

package conversation_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tmc/langchaingo/llms"

	"github.com/dapr/components-contrib/conversation"
	"github.com/dapr/components-contrib/conversation/anthropic"
	"github.com/dapr/components-contrib/conversation/deepseek"
	"github.com/dapr/components-contrib/conversation/googleai"
	"github.com/dapr/components-contrib/conversation/huggingface"
	"github.com/dapr/components-contrib/conversation/mistral"
	"github.com/dapr/components-contrib/conversation/ollama"
	"github.com/dapr/components-contrib/conversation/openai"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
)

const (
	chatCompletionsReply = `{"id":"c","object":"chat.completion","choices":[{"index":0,"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}]}`
	responsesReply       = `{"status":"completed","output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"ok"}]}]}`
	anthropicReply       = `{"id":"m","type":"message","role":"assistant","model":"x","content":[{"type":"text","text":"ok"}],"stop_reason":"end_turn","usage":{"input_tokens":1,"output_tokens":1}}`
)

func TestComponentsSetHeaders(t *testing.T) {
	for name, test := range map[string]struct {
		newComponent func(logger.Logger) conversation.Conversation
		props        map[string]string
		path         string
		reply        string
	}{
		"openai": {
			newComponent: openai.NewOpenAI,
			path:         "/chat/completions",
			reply:        chatCompletionsReply,
		},
		"openai responses API": {
			newComponent: openai.NewOpenAI,
			props:        map[string]string{"useResponsesAPI": "true"},
			path:         "/responses",
			reply:        responsesReply,
		},
		"azure openai": {
			newComponent: openai.NewOpenAI,
			props:        map[string]string{"apiType": "azure", "apiVersion": "2024-06-01", "model": "gpt-4o"},
			path:         "/chat/completions",
			reply:        chatCompletionsReply,
		},
		"deepseek": {
			newComponent: deepseek.NewDeepseek,
			path:         "/chat/completions",
			reply:        chatCompletionsReply,
		},
		"googleai": {
			newComponent: googleai.NewGoogleAI,
			path:         "/chat/completions",
			reply:        chatCompletionsReply,
		},
		"huggingface": {
			newComponent: huggingface.NewHuggingface,
			path:         "/chat/completions",
			reply:        chatCompletionsReply,
		},
		"ollama": {
			newComponent: ollama.NewOllama,
			path:         "/chat/completions",
			reply:        chatCompletionsReply,
		},
		"anthropic": {
			newComponent: anthropic.NewAnthropic,
			path:         "/messages",
			reply:        anthropicReply,
		},
	} {
		t.Run(name, func(t *testing.T) {
			var got http.Header
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if !strings.HasSuffix(r.URL.Path, test.path) {
					t.Errorf("unexpected request to %s", r.URL.Path)
					return
				}
				got = r.Header.Clone()
				w.Header().Set("Content-Type", "application/json")
				_, _ = io.WriteString(w, test.reply)
			}))
			t.Cleanup(srv.Close)

			props := map[string]string{
				"endpoint": srv.URL,
				"key":      "test-key",
				"headers":  `{"X-Tenant-ID":"acme","Ocp-Apim-Subscription-Key":"gateway-key"}`,
			}
			for k, v := range test.props {
				props[k] = v
			}

			c := test.newComponent(logger.NewLogger("test"))
			require.NoError(t, c.Init(t.Context(), conversation.Metadata{Base: metadata.Base{Properties: props}}))

			msgs := []llms.MessageContent{llms.TextParts(llms.ChatMessageTypeHuman, "hi")}
			_, err := c.Converse(t.Context(), &conversation.Request{Message: &msgs})
			require.NoError(t, err)

			require.NotNil(t, got)
			assert.Equal(t, "acme", got.Get("X-Tenant-ID"))
			assert.Equal(t, "gateway-key", got.Get("Ocp-Apim-Subscription-Key"))
		})
	}
}

func TestComponentsRejectInvalidHeaders(t *testing.T) {
	for name, newComponent := range map[string]func(logger.Logger) conversation.Conversation{
		"openai":      openai.NewOpenAI,
		"deepseek":    deepseek.NewDeepseek,
		"googleai":    googleai.NewGoogleAI,
		"huggingface": huggingface.NewHuggingface,
		"ollama":      ollama.NewOllama,
		"anthropic":   anthropic.NewAnthropic,
	} {
		t.Run(name, func(t *testing.T) {
			err := newComponent(logger.NewLogger("test")).Init(t.Context(), conversation.Metadata{Base: metadata.Base{Properties: map[string]string{
				"key":     "test-key",
				"headers": `{"Host":"example.com"}`,
			}}})
			require.ErrorContains(t, err, "set by the HTTP client")
		})
	}

	t.Run("mistral does not support headers", func(t *testing.T) {
		err := mistral.NewMistral(logger.NewLogger("test")).Init(t.Context(), conversation.Metadata{Base: metadata.Base{Properties: map[string]string{
			"key":     "test-key",
			"headers": `{"X-Tenant-ID":"acme"}`,
		}}})
		require.ErrorContains(t, err, "not supported")
		assert.NotContains(t, new(mistral.Mistral).GetComponentMetadata(), "headers")
	})
}
