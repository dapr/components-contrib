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
	"strings"
	"testing"

	"github.com/dapr/components-contrib/conversation"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tmc/langchaingo/llms"
)

func TestInit(t *testing.T) {
	testCases := []struct {
		name          string
		metadata      map[string]string
		expectedModel string
		testFn        func(*testing.T, *Anthropic, error)
	}{
		{
			name: "with default settings",
			metadata: map[string]string{
				"key":   "test-key",
				"model": conversation.DefaultAnthropicModel,
			},
			expectedModel: conversation.DefaultAnthropicModel,
			testFn: func(t *testing.T, a *Anthropic, err error) {
				require.NoError(t, err)
				assert.NotNil(t, a.LLM)
			},
		},
		{
			name: "with custom model name",
			metadata: map[string]string{
				"key":   "test-key",
				"model": "claude-opus-4-7",
			},
			expectedModel: "claude-opus-4-7",
			testFn: func(t *testing.T, a *Anthropic, err error) {
				require.NoError(t, err)
				assert.NotNil(t, a.LLM)
			},
		},
		{
			name: "with custom endpoint",
			metadata: map[string]string{
				"key":      "test-key",
				"model":    conversation.DefaultAnthropicModel,
				"endpoint": "https://api.anthropic.com/v1",
			},
			expectedModel: conversation.DefaultAnthropicModel,
			testFn: func(t *testing.T, a *Anthropic, err error) {
				require.NoError(t, err)
				assert.NotNil(t, a.LLM)
			},
		},
		{
			name: "with apiType foundry and endpoint",
			metadata: map[string]string{
				"key":      "test-key",
				"model":    "claude-opus-4-7",
				"apiType":  "foundry",
				"endpoint": "https://my-resource.services.ai.azure.com/anthropic/v1",
			},
			expectedModel: "claude-opus-4-7",
			testFn: func(t *testing.T, a *Anthropic, err error) {
				require.NoError(t, err)
				assert.NotNil(t, a.LLM)
			},
		},
		{
			name: "with apiType foundry but missing endpoint",
			metadata: map[string]string{
				"key":     "test-key",
				"model":   "claude-opus-4-7",
				"apiType": "foundry",
			},
			testFn: func(t *testing.T, a *Anthropic, err error) {
				require.Error(t, err)
				assert.EqualError(t, err, "endpoint must be provided when apiType is set to 'foundry'")
			},
		},
		{
			name: "with apiType foundry case-insensitive",
			metadata: map[string]string{
				"key":      "test-key",
				"apiType":  "Foundry",
				"endpoint": "https://my-resource.services.ai.azure.com/anthropic/v1",
			},
			expectedModel: conversation.DefaultAnthropicModel,
			testFn: func(t *testing.T, a *Anthropic, err error) {
				require.NoError(t, err)
				assert.NotNil(t, a.LLM)
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			a := NewAnthropic(logger.NewLogger("anthropic test"))
			err := a.Init(t.Context(), conversation.Metadata{
				Base: metadata.Base{
					Properties: tc.metadata,
				},
			})
			inner := a.(*Anthropic)
			tc.testFn(t, inner, err)
			if tc.expectedModel != "" {
				assert.Equal(t, tc.expectedModel, inner.GetModel(), "LLM model name must be set on the response after Init")
			}
		})
	}
}

func TestEndpointInMetadata(t *testing.T) {
	a := &Anthropic{}

	md := a.GetComponentMetadata()
	if len(md) == 0 {
		t.Skip("Metadata is not enabled, skipping test")
	}

	_, exists := md["endpoint"]
	assert.True(t, exists, "endpoint field should exist in metadata")

	_, exists = md["apiType"]
	assert.True(t, exists, "apiType field should exist in metadata")
}

// TestConverseKeepsEveryToolCall sends messages shaped the way daprd builds
// them (an assistant turn as text, then one ToolCall part per call) and checks
// that every tool_use reaches Anthropic. Otherwise the following tool_result
// blocks have nothing to match and Anthropic rejects the request.
func TestConverseKeepsEveryToolCall(t *testing.T) {
	var body struct {
		Messages []struct {
			Role    string           `json:"role"`
			Content []map[string]any `json:"content"`
		} `json:"messages"`
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		w.Header().Set("content-type", "application/json")
		_, _ = io.WriteString(w, `{"id":"m","type":"message","role":"assistant","model":"x",`+
			`"content":[{"type":"text","text":"ok"}],"stop_reason":"end_turn",`+
			`"usage":{"input_tokens":1,"output_tokens":1}}`)
	}))
	defer srv.Close()

	a := NewAnthropic(logger.NewLogger("test"))
	require.NoError(t, a.Init(t.Context(), conversation.Metadata{Base: metadata.Base{
		Properties: map[string]string{"key": "x", "endpoint": srv.URL},
	}}))

	call := func(id, path string) llms.ToolCall {
		return llms.ToolCall{ID: id, Type: "function", FunctionCall: &llms.FunctionCall{
			Name: "read", Arguments: `{"path":"` + path + `"}`,
		}}
	}
	msgs := []llms.MessageContent{
		llms.TextParts(llms.ChatMessageTypeHuman, "read a and b"),
		{Role: llms.ChatMessageTypeAI, Parts: []llms.ContentPart{
			llms.TextContent{Text: "Reading both."}, call("t1", "a"), call("t2", "b"),
		}},
		{Role: llms.ChatMessageTypeTool, Parts: []llms.ContentPart{
			llms.ToolCallResponse{ToolCallID: "t1", Name: "read", Content: "A"},
		}},
		{Role: llms.ChatMessageTypeTool, Parts: []llms.ContentPart{
			llms.ToolCallResponse{ToolCallID: "t2", Name: "read", Content: "B"},
		}},
	}
	tools := []llms.Tool{{Type: "function", Function: &llms.FunctionDefinition{
		Name: "read", Parameters: map[string]any{"type": "object"},
	}}}
	auto := "auto" // daprd sets this whenever tools are present
	_, err := a.Converse(t.Context(), &conversation.Request{Message: &msgs, Tools: &tools, ToolChoice: &auto})
	require.NoError(t, err)

	// One entry per content block, so this holds whether the blocks of a turn
	// are sent as one message or as consecutive same-role messages.
	var blocks []string
	for _, m := range body.Messages {
		for _, block := range m.Content {
			id, _ := block["id"].(string)
			if id == "" {
				id, _ = block["tool_use_id"].(string)
			}
			blocks = append(blocks, strings.TrimSpace(m.Role+" "+block["type"].(string)+" "+id))
		}
	}
	assert.Equal(t, []string{
		"user text",
		"assistant text",
		"assistant tool_use t1",
		"assistant tool_use t2",
		"user tool_result t1",
		"user tool_result t2",
	}, blocks)
}
