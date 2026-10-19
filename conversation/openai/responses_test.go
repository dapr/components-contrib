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
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tmc/langchaingo/llms"

	"github.com/dapr/components-contrib/conversation"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
)

// fakeOpenAI records the last request it received and answers with reply.
type fakeOpenAI struct {
	path string
	body map[string]any
}

func newFakeOpenAI(t *testing.T, status int, reply string) (*fakeOpenAI, string) {
	t.Helper()
	f := &fakeOpenAI{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.path = r.URL.Path
		raw, err := io.ReadAll(r.Body)
		assert.NoError(t, err)
		assert.NoError(t, json.Unmarshal(raw, &f.body))
		assert.Equal(t, "Bearer test-key", r.Header.Get("Authorization"))
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(reply))
	}))
	t.Cleanup(srv.Close)
	return f, srv.URL
}

func initOpenAI(t *testing.T, props map[string]string) *OpenAI {
	t.Helper()
	o := NewOpenAI(logger.NewLogger("test")).(*OpenAI)
	require.NoError(t, o.Init(t.Context(), conversation.Metadata{Base: metadata.Base{Properties: props}}))
	return o
}

func weatherTools() *[]llms.Tool {
	return &[]llms.Tool{{
		Type: "function",
		Function: &llms.FunctionDefinition{
			Name:        "get_weather",
			Description: "Get the weather for a city",
			Parameters:  map[string]any{"type": "object", "properties": map[string]any{"city": map[string]any{"type": "string"}}},
		},
	}}
}

func TestResponsesAPIOffByDefault(t *testing.T) {
	fake, url := newFakeOpenAI(t, http.StatusOK, `{"id":"1","object":"chat.completion","model":"gpt-4o","choices":[{"index":0,"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}]}`)
	o := initOpenAI(t, map[string]string{"key": "test-key", "model": "gpt-4o", "endpoint": url})

	res, err := o.Converse(t.Context(), &conversation.Request{
		Message: &[]llms.MessageContent{llms.TextParts(llms.ChatMessageTypeHuman, "hi")},
	})
	require.NoError(t, err)

	assert.Equal(t, "/chat/completions", fake.path)
	assert.Equal(t, "ok", res.Outputs[0].Choices[0].Message.Content)
}

func TestResponsesAPIToolCall(t *testing.T) {
	fake, url := newFakeOpenAI(t, http.StatusOK, `{
		"status": "completed",
		"output": [
			{"type": "reasoning", "summary": []},
			{"type": "function_call", "call_id": "call_2", "name": "get_weather", "arguments": "{\"city\":\"Madrid\"}"}
		],
		"usage": {"input_tokens": 100, "output_tokens": 40, "total_tokens": 140,
			"input_tokens_details": {"cached_tokens": 64}, "output_tokens_details": {"reasoning_tokens": 32}}
	}`)
	o := initOpenAI(t, map[string]string{
		"key": "test-key", "model": "gpt-6.1-sol", "endpoint": url,
		"useResponsesAPI": "true", "maxTokens": "500",
	})

	toolChoice := "auto"
	res, err := o.Converse(t.Context(), &conversation.Request{
		Message: &[]llms.MessageContent{
			llms.TextParts(llms.ChatMessageTypeSystem, "You are helpful."),
			llms.TextParts(llms.ChatMessageTypeHuman, "Weather in Paris and Madrid?"),
			{Role: llms.ChatMessageTypeAI, Parts: []llms.ContentPart{
				llms.ToolCall{ID: "call_1", Type: "function", FunctionCall: &llms.FunctionCall{Name: "get_weather", Arguments: `{"city":"Paris"}`}},
			}},
			{Role: llms.ChatMessageTypeTool, Parts: []llms.ContentPart{
				llms.ToolCallResponse{ToolCallID: "call_1", Name: "get_weather", Content: "sunny"},
			}},
		},
		Tools:      weatherTools(),
		ToolChoice: &toolChoice,
	})
	require.NoError(t, err)

	t.Run("request", func(t *testing.T) {
		assert.Equal(t, "/responses", fake.path)
		assert.Equal(t, "gpt-6.1-sol", fake.body["model"])
		assert.Equal(t, false, fake.body["store"])
		assert.InDelta(t, 500, fake.body["max_output_tokens"], 0)
		assert.Equal(t, "auto", fake.body["tool_choice"])
		assert.NotContains(t, fake.body, "reasoning_effort")

		want := []any{
			map[string]any{"type": "message", "role": "system", "content": []any{map[string]any{"type": "input_text", "text": "You are helpful."}}},
			map[string]any{"type": "message", "role": "user", "content": []any{map[string]any{"type": "input_text", "text": "Weather in Paris and Madrid?"}}},
			map[string]any{"type": "function_call", "call_id": "call_1", "name": "get_weather", "arguments": `{"city":"Paris"}`},
			map[string]any{"type": "function_call_output", "call_id": "call_1", "output": "sunny"},
		}
		assert.Equal(t, want, fake.body["input"])

		tools, ok := fake.body["tools"].([]any)
		require.True(t, ok)
		require.Len(t, tools, 1)
		tool := tools[0].(map[string]any)
		assert.Equal(t, "function", tool["type"])
		assert.Equal(t, "get_weather", tool["name"])
		assert.Equal(t, "Get the weather for a city", tool["description"])
		assert.Equal(t, false, tool["strict"])
	})

	t.Run("response", func(t *testing.T) {
		require.Len(t, res.Outputs, 1)
		choice := res.Outputs[0].Choices[0]
		assert.Equal(t, "tool_calls", choice.FinishReason)
		require.NotNil(t, choice.Message.ToolCallRequest)
		calls := *choice.Message.ToolCallRequest
		require.Len(t, calls, 1)
		assert.Equal(t, "call_2", calls[0].ID)
		assert.Equal(t, "get_weather", calls[0].FunctionCall.Name)
		assert.JSONEq(t, `{"city":"Madrid"}`, calls[0].FunctionCall.Arguments)

		require.NotNil(t, res.Usage)
		assert.Equal(t, uint64(100), res.Usage.PromptTokens)
		assert.Equal(t, uint64(40), res.Usage.CompletionTokens)
		assert.Equal(t, uint64(140), res.Usage.TotalTokens)
		require.NotNil(t, res.Usage.PromptTokensDetails)
		assert.Equal(t, uint64(64), res.Usage.PromptTokensDetails.CachedTokens)
		require.NotNil(t, res.Usage.CompletionTokensDetails)
		assert.Equal(t, uint64(32), res.Usage.CompletionTokensDetails.ReasoningTokens)
	})
}

func TestResponsesAPIText(t *testing.T) {
	tests := []struct {
		name       string
		reply      string
		wantText   string
		wantReason string
	}{
		{
			name:       "completed",
			reply:      `{"status":"completed","output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"Hello "},{"type":"output_text","text":"there"}]}]}`,
			wantText:   "Hello there",
			wantReason: "stop",
		},
		{
			name:       "cut off by max tokens",
			reply:      `{"status":"incomplete","incomplete_details":{"reason":"max_output_tokens"},"output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"Hel"}]}]}`,
			wantText:   "Hel",
			wantReason: "length",
		},
		{
			name:       "tool call cut off by max tokens",
			reply:      `{"status":"incomplete","incomplete_details":{"reason":"max_output_tokens"},"output":[{"type":"function_call","status":"incomplete","call_id":"call_1","name":"get_weather","arguments":"{\"ci"}]}`,
			wantText:   "",
			wantReason: "length",
		},
		{
			name:       "refusal",
			reply:      `{"status":"completed","output":[{"type":"message","role":"assistant","content":[{"type":"refusal","refusal":"I can't help with that."}]}]}`,
			wantText:   "I can't help with that.",
			wantReason: "stop",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, url := newFakeOpenAI(t, http.StatusOK, tt.reply)
			o := initOpenAI(t, map[string]string{"key": "test-key", "endpoint": url, "useResponsesAPI": "true"})

			res, err := o.Converse(t.Context(), &conversation.Request{
				Message: &[]llms.MessageContent{llms.TextParts(llms.ChatMessageTypeHuman, "hi")},
			})
			require.NoError(t, err)

			choice := res.Outputs[0].Choices[0]
			assert.Equal(t, tt.wantText, choice.Message.Content)
			assert.Equal(t, tt.wantReason, choice.FinishReason)
			assert.Nil(t, choice.Message.ToolCallRequest)
		})
	}
}

func TestResponsesAPIToolChoice(t *testing.T) {
	tests := []struct {
		choice string
		want   any
	}{
		{choice: "required", want: "required"},
		{choice: "any", want: "required"},
		{choice: "none", want: "none"},
		{choice: "", want: nil},
		{choice: "get_weather", want: map[string]any{"type": "function", "name": "get_weather"}},
	}

	for _, tt := range tests {
		t.Run(tt.choice, func(t *testing.T) {
			fake, url := newFakeOpenAI(t, http.StatusOK, `{"status":"completed","output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"ok"}]}]}`)
			o := initOpenAI(t, map[string]string{"key": "test-key", "endpoint": url, "useResponsesAPI": "true"})

			choice := tt.choice
			_, err := o.Converse(t.Context(), &conversation.Request{
				Message:    &[]llms.MessageContent{llms.TextParts(llms.ChatMessageTypeHuman, "hi")},
				Tools:      weatherTools(),
				ToolChoice: &choice,
			})
			require.NoError(t, err)
			assert.Equal(t, tt.want, fake.body["tool_choice"])
		})
	}
}

func TestResponsesAPIStructuredOutput(t *testing.T) {
	fake, url := newFakeOpenAI(t, http.StatusOK, `{"status":"completed","output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"{\"city\":\"Paris\"}"}]}]}`)
	o := initOpenAI(t, map[string]string{"key": "test-key", "endpoint": url, "useResponsesAPI": "true"})

	_, err := o.Converse(t.Context(), &conversation.Request{
		Message: &[]llms.MessageContent{llms.TextParts(llms.ChatMessageTypeHuman, "hi")},
		ResponseFormatAsJSONSchema: map[string]any{
			"type":                 "object",
			"properties":           map[string]any{"city": map[string]any{"type": "string"}},
			"required":             []any{"city"},
			"additionalProperties": false,
		},
	})
	require.NoError(t, err)

	text, ok := fake.body["text"].(map[string]any)
	require.True(t, ok, "the request must carry text.format")
	format := text["format"].(map[string]any)
	assert.Equal(t, "json_schema", format["type"])
	assert.Equal(t, map[string]any{
		"type":                 "object",
		"properties":           map[string]any{"city": map[string]any{"type": "string"}},
		"required":             []any{"city"},
		"additionalProperties": false,
	}, format["schema"])
}

func TestResponsesAPIError(t *testing.T) {
	_, url := newFakeOpenAI(t, http.StatusBadRequest, `{"error":{"message":"Invalid value for 'model'.","type":"invalid_request_error"}}`)
	o := initOpenAI(t, map[string]string{"key": "test-key", "endpoint": url, "useResponsesAPI": "true"})

	_, err := o.Converse(t.Context(), &conversation.Request{
		Message: &[]llms.MessageContent{llms.TextParts(llms.ChatMessageTypeHuman, "hi")},
	})
	require.EqualError(t, err, "API returned unexpected status code: 400: Invalid value for 'model'.")
}

func TestResponsesAPIFailedResponse(t *testing.T) {
	_, url := newFakeOpenAI(t, http.StatusOK, `{"status":"failed","error":{"code":"server_error","message":"The model failed."},"output":[]}`)
	o := initOpenAI(t, map[string]string{"key": "test-key", "endpoint": url, "useResponsesAPI": "true"})

	_, err := o.Converse(t.Context(), &conversation.Request{
		Message: &[]llms.MessageContent{llms.TextParts(llms.ChatMessageTypeHuman, "hi")},
	})
	require.EqualError(t, err, "response failed: server_error: The model failed.")
}

func TestResponsesAPIPromptCacheRetention(t *testing.T) {
	t.Run("24h", func(t *testing.T) {
		fake, url := newFakeOpenAI(t, http.StatusOK, `{"status":"completed","output":[]}`)
		o := initOpenAI(t, map[string]string{"key": "test-key", "endpoint": url, "useResponsesAPI": "true"})

		retention := 24 * time.Hour
		_, err := o.Converse(t.Context(), &conversation.Request{
			Message:              &[]llms.MessageContent{llms.TextParts(llms.ChatMessageTypeHuman, "hi")},
			PromptCacheRetention: &retention,
		})
		require.NoError(t, err)
		assert.Equal(t, "24h", fake.body["prompt_cache_retention"])
		assert.NotContains(t, fake.body, "metadata")
	})

	t.Run("unsupported", func(t *testing.T) {
		_, url := newFakeOpenAI(t, http.StatusOK, `{"status":"completed","output":[]}`)
		o := initOpenAI(t, map[string]string{"key": "test-key", "endpoint": url, "useResponsesAPI": "true"})

		retention := time.Hour
		_, err := o.Converse(t.Context(), &conversation.Request{
			Message:              &[]llms.MessageContent{llms.TextParts(llms.ChatMessageTypeHuman, "hi")},
			PromptCacheRetention: &retention,
		})
		require.ErrorContains(t, err, `unsupported prompt cache retention "1h0m0s"`)
	})
}

func TestResponsesAPIRejectsAzure(t *testing.T) {
	o := NewOpenAI(logger.NewLogger("test")).(*OpenAI)
	err := o.Init(t.Context(), conversation.Metadata{Base: metadata.Base{Properties: map[string]string{
		"key": "test-key", "model": "gpt-4o", "endpoint": "https://example.openai.azure.com/",
		"apiType": "azure", "apiVersion": "2025-01-01-preview", "useResponsesAPI": "true",
	}}})
	require.EqualError(t, err, "useResponsesAPI is not supported when apiType is set to 'azure'")
}
