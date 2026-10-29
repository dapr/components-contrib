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
	"github.com/dapr/kit/ptr"
)

// TestReasoningEffortOnTheWire verifies the request-level reasoning effort
// reaches the chat completions body as reasoning_effort, and that the
// reasoning_content the endpoint returns comes back on the result message.
func TestReasoningEffortOnTheWire(t *testing.T) {
	tests := []struct {
		name       string
		effort     *string
		wantEffort any
	}{
		{name: "no effort sends none", wantEffort: nil},
		{name: "low", effort: ptr.Of("low"), wantEffort: "low"},
		{name: "medium", effort: ptr.Of("medium"), wantEffort: "medium"},
		{name: "high", effort: ptr.Of("high"), wantEffort: "high"},
		{name: "an effort langchaingo cannot express is not sent", effort: ptr.Of("xhigh"), wantEffort: nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var body map[string]any
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				raw, err := io.ReadAll(r.Body)
				assert.NoError(t, err)
				assert.NoError(t, json.Unmarshal(raw, &body))
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(`{"id":"1","object":"chat.completion","model":"m","choices":[{"index":0,"finish_reason":"stop",` +
					`"message":{"role":"assistant","content":"answer","reasoning_content":"thought"}}]}`))
			}))
			defer server.Close()

			o := NewOpenAI(logger.NewLogger("test")).(*OpenAI)
			require.NoError(t, o.Init(t.Context(), conversation.Metadata{
				Base: metadata.Base{Properties: map[string]string{"key": "test-key", "endpoint": server.URL, "model": "m"}},
			}))

			resp, err := o.Converse(t.Context(), &conversation.Request{
				ReasoningEffort: tt.effort,
				Message: &[]llms.MessageContent{
					{Role: llms.ChatMessageTypeHuman, Parts: []llms.ContentPart{llms.TextContent{Text: "hi"}}},
				},
			})
			require.NoError(t, err)

			assert.Equal(t, tt.wantEffort, body["reasoning_effort"])
			require.Len(t, resp.Outputs, 1)
			require.Len(t, resp.Outputs[0].Choices, 1)
			assert.Equal(t, "thought", resp.Outputs[0].Choices[0].Message.Reasoning)
		})
	}
}
