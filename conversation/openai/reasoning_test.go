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
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tmc/langchaingo/llms"

	"github.com/dapr/components-contrib/conversation"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
)

// TestReasoningEffort sends a tool-calling request to a fake OpenAI endpoint and
// checks the reasoning_effort the component put on the wire.
func TestReasoningEffort(t *testing.T) {
	tests := []struct {
		name  string
		props map[string]string
		want  any
	}{
		{name: "not sent when unset", props: map[string]string{}, want: nil},
		{name: "none", props: map[string]string{"reasoningEffort": "none"}, want: "none"},
		{name: "high", props: map[string]string{"reasoningEffort": "high"}, want: "high"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got map[string]any
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				assert.Equal(t, "/chat/completions", r.URL.Path)
				raw, err := io.ReadAll(r.Body)
				assert.NoError(t, err)
				assert.NoError(t, json.Unmarshal(raw, &got))
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(`{"id":"1","object":"chat.completion","model":"gpt-5.1","choices":[{"index":0,"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}]}`))
			}))
			t.Cleanup(srv.Close)

			props := map[string]string{"key": "test-key", "model": "gpt-5.1", "endpoint": srv.URL}
			for k, v := range tt.props {
				props[k] = v
			}
			o := NewOpenAI(logger.NewLogger("test")).(*OpenAI)
			require.NoError(t, o.Init(t.Context(), conversation.Metadata{Base: metadata.Base{Properties: props}}))

			tools := []llms.Tool{{
				Type: "function",
				Function: &llms.FunctionDefinition{
					Name:       "get_weather",
					Parameters: map[string]any{"type": "object", "properties": map[string]any{}},
				},
			}}
			_, err := o.Converse(t.Context(), &conversation.Request{
				Message: &[]llms.MessageContent{
					{Role: llms.ChatMessageTypeHuman, Parts: []llms.ContentPart{llms.TextContent{Text: "hi"}}},
				},
				Tools: &tools,
			})
			require.NoError(t, err)

			require.NotNil(t, got, "the fake endpoint was never called")
			assert.NotEmpty(t, got["tools"], "the request must carry the tools")
			assert.Equal(t, tt.want, got["reasoning_effort"])
		})
	}
}
