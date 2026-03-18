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
package langchaingokit

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tmc/langchaingo/llms"

	"github.com/dapr/components-contrib/conversation"
	"github.com/dapr/kit/logger"
)

// stubModel implements llms.Model for testing purposes, returning a controlled ContentResponse.
type stubModel struct {
	generateFn func(ctx context.Context, messages []llms.MessageContent, options ...llms.CallOption) (*llms.ContentResponse, error)
}

func (s *stubModel) GenerateContent(ctx context.Context, messages []llms.MessageContent, options ...llms.CallOption) (*llms.ContentResponse, error) {
	return s.generateFn(ctx, messages, options...)
}

func (s *stubModel) Call(ctx context.Context, prompt string, options ...llms.CallOption) (string, error) {
	return "", nil
}

func newLLMWithStub(fn func(ctx context.Context, messages []llms.MessageContent, options ...llms.CallOption) (*llms.ContentResponse, error)) *LLM {
	return &LLM{
		Model:  &stubModel{generateFn: fn},
		logger: logger.NewLogger("test"),
	}
}

func strPtr(s string) *string { return &s }

// TestConverseEmptyResponseWithTools verifies that when tool_choice=required is set and the LLM
// returns an empty response (no content, no tool calls), Converse returns an error. This ensures
// the resiliency runner in dapr/dapr can retry rather than silently completing the workflow.
func TestConverseEmptyResponseWithTools(t *testing.T) {
	tools := []llms.Tool{
		{
			Type: "function",
			Function: &llms.FunctionDefinition{
				Name:        "get-orders-at-risk-count",
				Description: "Get count of at-risk orders",
			},
		},
	}

	tests := []struct {
		name       string
		choices    []*llms.ContentChoice
		tools      *[]llms.Tool
		toolChoice *string
		wantErr    bool
		errSubstr  string
	}{
		{
			name: "empty content no tool calls with tools provided and tool_choice=required - returns error",
			choices: []*llms.ContentChoice{
				{Content: "", StopReason: "stop"},
			},
			tools:      &tools,
			toolChoice: strPtr("required"),
			wantErr:    true,
			errSubstr:  "LLM returned empty response with no tool calls",
		},
		{
			name: "multiple choices all empty with tools provided and tool_choice=required - returns error",
			choices: []*llms.ContentChoice{
				{Content: "", StopReason: "stop"},
				{Content: "", StopReason: "stop"},
			},
			tools:      &tools,
			toolChoice: strPtr("required"),
			wantErr:    true,
			errSubstr:  "LLM returned empty response with no tool calls",
		},
		{
			name: "empty content no tool calls without tools - no error",
			choices: []*llms.ContentChoice{
				{Content: "", StopReason: "stop"},
			},
			tools:   nil,
			wantErr: false,
		},
		{
			name: "content present with tools - no error",
			choices: []*llms.ContentChoice{
				{Content: "Here are the at-risk orders.", StopReason: "stop"},
			},
			tools:   &tools,
			wantErr: false,
		},
		{
			name: "tool calls present with tools - no error",
			choices: []*llms.ContentChoice{
				{
					Content:    "",
					StopReason: "tool_calls",
					ToolCalls: []llms.ToolCall{
						{
							ID:   "call_1",
							Type: "function",
							FunctionCall: &llms.FunctionCall{
								Name:      "get-orders-at-risk-count",
								Arguments: "{}",
							},
						},
					},
				},
			},
			tools:   &tools,
			wantErr: false,
		},
		{
			name: "multiple choices first empty second has tool calls - no error",
			choices: []*llms.ContentChoice{
				{Content: "", StopReason: "stop"},
				{
					Content:    "",
					StopReason: "tool_calls",
					ToolCalls: []llms.ToolCall{
						{ID: "call_1", Type: "function", FunctionCall: &llms.FunctionCall{Name: "get-orders-at-risk-count"}},
					},
				},
			},
			tools:   &tools,
			wantErr: false,
		},
		{
			name: "empty tool calls slice with tool_choice=required - returns error",
			choices: []*llms.ContentChoice{
				{
					Content:    "",
					StopReason: "tool_calls",
					ToolCalls:  []llms.ToolCall{},
				},
			},
			tools:      &tools,
			toolChoice: strPtr("required"),
			wantErr:    true,
		},
		{
			name:       "empty choices slice with tool_choice=required - returns error (model returned nothing)",
			choices:    []*llms.ContentChoice{},
			tools:      &tools,
			toolChoice: strPtr("required"),
			wantErr:    true,
			errSubstr:  "LLM returned empty response with no tool calls",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			llm := newLLMWithStub(func(ctx context.Context, messages []llms.MessageContent, options ...llms.CallOption) (*llms.ContentResponse, error) {
				return &llms.ContentResponse{Choices: tt.choices}, nil
			})

			req := &conversation.Request{
				Message: &[]llms.MessageContent{
					{
						Role:  llms.ChatMessageTypeHuman,
						Parts: []llms.ContentPart{llms.TextContent{Text: "identify at-risk orders"}},
					},
				},
				Tools:      tt.tools,
				ToolChoice: tt.toolChoice,
			}

			resp, err := llm.Converse(t.Context(), req)
			if tt.wantErr {
				require.Error(t, err)
				if tt.errSubstr != "" {
					assert.Contains(t, err.Error(), tt.errSubstr)
				}
				assert.Nil(t, resp)
			} else {
				require.NoError(t, err)
				assert.NotNil(t, resp)
			}
		})
	}
}
