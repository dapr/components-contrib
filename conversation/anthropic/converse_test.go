/*
Copyright 2025 The Dapr Authors
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

	"github.com/dapr/components-contrib/conversation"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
)

// newTestComponent spins up a component pointed at a test server that records
// the outgoing request body and replies with the supplied message JSON.
func newTestComponent(t *testing.T, response string, captured *map[string]any) *Anthropic {
	t.Helper()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if captured != nil {
			require.NoError(t, json.Unmarshal(body, captured))
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, response)
	}))
	t.Cleanup(srv.Close)

	a := NewAnthropic(logger.NewLogger("anthropic test")).(*Anthropic)
	err := a.Init(t.Context(), conversation.Metadata{
		Base: metadata.Base{
			Properties: map[string]string{
				"key":      "test-key",
				"model":    "claude-sonnet-5",
				"endpoint": srv.URL,
			},
		},
	})
	require.NoError(t, err)
	return a
}

const textResponse = `{
  "id": "msg_1",
  "type": "message",
  "role": "assistant",
  "model": "claude-sonnet-5",
  "content": [{"type": "text", "text": "hello there"}],
  "stop_reason": "end_turn",
  "usage": {"input_tokens": 10, "output_tokens": 5, "cache_creation_input_tokens": 0, "cache_read_input_tokens": 0}
}`

func userMessage(text string) *[]llms.MessageContent {
	return &[]llms.MessageContent{
		{
			Role:  llms.ChatMessageTypeHuman,
			Parts: []llms.ContentPart{llms.TextContent{Text: text}},
		},
	}
}

// TestTemperatureOmittedWhenUnset is the core regression: the pinned langchaingo
// always serialized temperature, which 400s on newer Claude generations. When
// the caller does not set one, no temperature key should reach the wire.
func TestTemperatureOmittedWhenUnset(t *testing.T) {
	var body map[string]any
	a := newTestComponent(t, textResponse, &body)

	_, err := a.Converse(t.Context(), &conversation.Request{Message: userMessage("hi")})
	require.NoError(t, err)

	_, ok := body["temperature"]
	assert.False(t, ok, "temperature must not be sent when the caller did not set one")
}

func TestTemperatureSentWhenProvided(t *testing.T) {
	var body map[string]any
	a := newTestComponent(t, textResponse, &body)

	_, err := a.Converse(t.Context(), &conversation.Request{
		Message:     userMessage("hi"),
		Temperature: 0.5,
	})
	require.NoError(t, err)

	require.Contains(t, body, "temperature")
	assert.InEpsilon(t, 0.5, body["temperature"], 1e-9)
}

// TestToolChoicePassthrough verifies tools and tool_choice actually reach the
// request; the pinned langchaingo dropped tool_choice for Anthropic.
func TestToolChoicePassthrough(t *testing.T) {
	var body map[string]any
	a := newTestComponent(t, textResponse, &body)

	toolChoice := "required"
	tools := []llms.Tool{
		{
			Type: "function",
			Function: &llms.FunctionDefinition{
				Name:        "get_weather",
				Description: "Get the weather",
				Parameters: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"city": map[string]any{"type": "string"},
					},
					"required": []string{"city"},
				},
			},
		},
	}

	_, err := a.Converse(t.Context(), &conversation.Request{
		Message:    userMessage("weather?"),
		Tools:      &tools,
		ToolChoice: &toolChoice,
	})
	require.NoError(t, err)

	tc, ok := body["tool_choice"].(map[string]any)
	require.True(t, ok, "tool_choice must be present")
	assert.Equal(t, "any", tc["type"], "required maps to the Anthropic 'any' choice")

	sentTools, ok := body["tools"].([]any)
	require.True(t, ok)
	require.Len(t, sentTools, 1)
	tool := sentTools[0].(map[string]any)
	assert.Equal(t, "get_weather", tool["name"])
	assert.Equal(t, "Get the weather", tool["description"])
	assert.Contains(t, tool, "input_schema")
}

func TestResponseMapping(t *testing.T) {
	const toolResponse = `{
  "id": "msg_2",
  "type": "message",
  "role": "assistant",
  "model": "claude-sonnet-5",
  "content": [
    {"type": "text", "text": "let me check"},
    {"type": "tool_use", "id": "toolu_1", "name": "get_weather", "input": {"city": "Paris"}}
  ],
  "stop_reason": "tool_use",
  "usage": {"input_tokens": 12, "output_tokens": 7, "cache_creation_input_tokens": 0, "cache_read_input_tokens": 3}
}`

	a := newTestComponent(t, toolResponse, nil)

	resp, err := a.Converse(t.Context(), &conversation.Request{Message: userMessage("weather?")})
	require.NoError(t, err)

	require.Len(t, resp.Outputs, 1)
	out := resp.Outputs[0]
	assert.Equal(t, "tool_use", out.StopReason)
	require.Len(t, out.Choices, 1)

	choice := out.Choices[0]
	assert.Equal(t, "let me check", choice.Message.Content)
	require.NotNil(t, choice.Message.ToolCallRequest)
	require.Len(t, *choice.Message.ToolCallRequest, 1)

	call := (*choice.Message.ToolCallRequest)[0]
	assert.Equal(t, "toolu_1", call.ID)
	require.NotNil(t, call.FunctionCall)
	assert.Equal(t, "get_weather", call.FunctionCall.Name)
	assert.JSONEq(t, `{"city":"Paris"}`, call.FunctionCall.Arguments)

	require.NotNil(t, resp.Usage)
	assert.Equal(t, uint64(12), resp.Usage.PromptTokens)
	assert.Equal(t, uint64(7), resp.Usage.CompletionTokens)
	assert.Equal(t, uint64(19), resp.Usage.TotalTokens)
	require.NotNil(t, resp.Usage.PromptTokensDetails)
	assert.Equal(t, uint64(3), resp.Usage.PromptTokensDetails.CachedTokens)
}

// TestRequiredToolChoiceEmptyResponseErrors mirrors the shared langchaingokit
// contract: tool_choice=required with tools but an empty model reply is an error.
func TestRequiredToolChoiceEmptyResponseErrors(t *testing.T) {
	const emptyResponse = `{
  "id": "msg_3",
  "type": "message",
  "role": "assistant",
  "model": "claude-sonnet-5",
  "content": [],
  "stop_reason": "end_turn",
  "usage": {"input_tokens": 4, "output_tokens": 0, "cache_creation_input_tokens": 0, "cache_read_input_tokens": 0}
}`

	a := newTestComponent(t, emptyResponse, nil)

	toolChoice := "required"
	tools := []llms.Tool{
		{Type: "function", Function: &llms.FunctionDefinition{Name: "noop"}},
	}

	_, err := a.Converse(t.Context(), &conversation.Request{
		Message:    userMessage("do it"),
		Tools:      &tools,
		ToolChoice: &toolChoice,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "empty response with no tool calls")
}

func TestFoundryRequiresEndpoint(t *testing.T) {
	a := NewAnthropic(logger.NewLogger("anthropic test")).(*Anthropic)
	err := a.Init(t.Context(), conversation.Metadata{
		Base: metadata.Base{
			Properties: map[string]string{
				"key":     "test-key",
				"apiType": "foundry",
			},
		},
	})
	require.Error(t, err)
	assert.EqualError(t, err, "endpoint must be provided when apiType is set to 'foundry'")
}
