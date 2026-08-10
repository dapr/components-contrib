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
	"github.com/tmc/langchaingo/llms/openai"

	"github.com/dapr/components-contrib/conversation"
	"github.com/dapr/kit/logger"
	"github.com/dapr/kit/ptr"
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

func TestConverseResponseModel(t *testing.T) {
	choice := &llms.ContentChoice{Content: "hello", StopReason: "stop"}

	t.Run("model set via SetModel is returned in response", func(t *testing.T) {
		llm := newLLMWithStub(func(_ context.Context, _ []llms.MessageContent, _ ...llms.CallOption) (*llms.ContentResponse, error) {
			return &llms.ContentResponse{Choices: []*llms.ContentChoice{choice}}, nil
		})
		llm.SetModel("my-model-name")

		resp, err := llm.Converse(t.Context(), &conversation.Request{
			Message: &[]llms.MessageContent{
				{Role: llms.ChatMessageTypeHuman, Parts: []llms.ContentPart{llms.TextContent{Text: "hi"}}},
			},
		})
		require.NoError(t, err)
		assert.Equal(t, "my-model-name", resp.Model)
	})

	t.Run("model is empty when SetModel is never called", func(t *testing.T) {
		llm := newLLMWithStub(func(_ context.Context, _ []llms.MessageContent, _ ...llms.CallOption) (*llms.ContentResponse, error) {
			return &llms.ContentResponse{Choices: []*llms.ContentChoice{choice}}, nil
		})

		resp, err := llm.Converse(t.Context(), &conversation.Request{
			Message: &[]llms.MessageContent{
				{Role: llms.ChatMessageTypeHuman, Parts: []llms.ContentPart{llms.TextContent{Text: "hi"}}},
			},
		})
		require.NoError(t, err)
		assert.Empty(t, resp.Model, "Response.Model should be empty when SetModel was not called")
	})
}

// TestConverseMaxTokensDefaults verifies the component-level default cap set via
// SetDefaultMaxTokens and its precedence against the request-level MaxTokens.
func TestConverseMaxTokensDefaults(t *testing.T) {
	choice := &llms.ContentChoice{Content: "hello", StopReason: "stop"}

	tests := []struct {
		name             string
		defaultMaxTokens *int64
		requestMaxTokens *int64
		wantMaxTokens    int
	}{
		{name: "no default and no request value leaves max tokens unset", wantMaxTokens: 0},
		{name: "default applies when request has no value", defaultMaxTokens: ptr.Of(int64(50)), wantMaxTokens: 50},
		{name: "request value overrides default", defaultMaxTokens: ptr.Of(int64(50)), requestMaxTokens: ptr.Of(int64(100)), wantMaxTokens: 100},
		{name: "zero default is ignored", defaultMaxTokens: ptr.Of(int64(0)), wantMaxTokens: 0},
		{name: "negative default is ignored", defaultMaxTokens: ptr.Of(int64(-5)), wantMaxTokens: 0},
		{name: "explicit zero request value falls back to default", defaultMaxTokens: ptr.Of(int64(50)), requestMaxTokens: ptr.Of(int64(0)), wantMaxTokens: 50},
		{name: "huge request value is clamped instead of wrapping", requestMaxTokens: ptr.Of(int64(4294967296)), wantMaxTokens: 2147483647},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got llms.CallOptions
			llm := newLLMWithStub(func(_ context.Context, _ []llms.MessageContent, options ...llms.CallOption) (*llms.ContentResponse, error) {
				got = foldCallOptions(options)
				return &llms.ContentResponse{Choices: []*llms.ContentChoice{choice}}, nil
			})
			llm.SetDefaultMaxTokens(tt.defaultMaxTokens)

			_, err := llm.Converse(t.Context(), &conversation.Request{
				MaxTokens: tt.requestMaxTokens,
				Message: &[]llms.MessageContent{
					{Role: llms.ChatMessageTypeHuman, Parts: []llms.ContentPart{llms.TextContent{Text: "hi"}}},
				},
			})
			require.NoError(t, err)
			assert.Equal(t, tt.wantMaxTokens, got.MaxTokens)
		})
	}
}

// TestSetDefaultMaxTokensDiscardsNonPositive verifies that a non-positive
// component default is dropped by the setter itself rather than merely skipped
// when call options are built, so the stored default is always nil or positive.
func TestSetDefaultMaxTokensDiscardsNonPositive(t *testing.T) {
	tests := []struct {
		name      string
		maxTokens *int64
		want      *int64
	}{
		{name: "nil stays nil", want: nil},
		{name: "zero is discarded", maxTokens: ptr.Of(int64(0)), want: nil},
		{name: "negative is discarded", maxTokens: ptr.Of(int64(-5)), want: nil},
		{name: "positive is retained", maxTokens: ptr.Of(int64(50)), want: ptr.Of(int64(50))},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			llm := New(logger.NewLogger("test"))
			llm.SetDefaultMaxTokens(tt.maxTokens)
			assert.Equal(t, tt.want, llm.defaultMaxTokens)
		})
	}
}

// TestConverseNilResponse verifies that a nil model response yields an error
// instead of a nil-pointer panic, and that the max-tokens cap was still
// applied to the outgoing call: options are folded before the call is made,
// so the cap must be present regardless of what the model returns.
func TestConverseNilResponse(t *testing.T) {
	var got llms.CallOptions
	llm := newLLMWithStub(func(_ context.Context, _ []llms.MessageContent, options ...llms.CallOption) (*llms.ContentResponse, error) {
		got = foldCallOptions(options)
		return nil, nil
	})
	llm.SetDefaultMaxTokens(ptr.Of(int64(50)))

	resp, err := llm.Converse(t.Context(), &conversation.Request{
		MaxTokens: ptr.Of(int64(100)),
		Message: &[]llms.MessageContent{
			{Role: llms.ChatMessageTypeHuman, Parts: []llms.ContentPart{llms.TextContent{Text: "hi"}}},
		},
	})
	require.Error(t, err)
	assert.Nil(t, resp)
	assert.Equal(t, 100, got.MaxTokens, "max tokens must be applied to the outgoing call even when the response is nil")
}

// TestConversePostCallOptionsSurviveRequestMetadata verifies that provider
// flags carried in CallOptions.Metadata (e.g. openai.WithLegacyMaxTokensField)
// survive a request that sets its own Metadata: llms.WithMetadata replaces the
// metadata map wholesale when folded, so post options must be applied last.
func TestConversePostCallOptionsSurviveRequestMetadata(t *testing.T) {
	choice := &llms.ContentChoice{Content: "hello", StopReason: "stop"}

	var got llms.CallOptions
	llm := newLLMWithStub(func(_ context.Context, _ []llms.MessageContent, options ...llms.CallOption) (*llms.ContentResponse, error) {
		got = foldCallOptions(options)
		return &llms.ContentResponse{Choices: []*llms.ContentChoice{choice}}, nil
	})
	llm.SetPostCallOptions(openai.WithLegacyMaxTokensField())

	_, err := llm.Converse(t.Context(), &conversation.Request{
		MaxTokens: ptr.Of(int64(100)),
		Metadata:  map[string]string{"trace": "abc"},
		Message: &[]llms.MessageContent{
			{Role: llms.ChatMessageTypeHuman, Parts: []llms.ContentPart{llms.TextContent{Text: "hi"}}},
		},
	})
	require.NoError(t, err)

	assert.Equal(t, 100, got.MaxTokens)
	assert.Equal(t, "abc", got.Metadata["trace"], "request metadata must still be present")
	assert.Equal(t, true, got.Metadata["openai:use_legacy_max_tokens"], "legacy flag must survive request metadata")
}

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
			toolChoice: ptr.Of("required"),
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
			toolChoice: ptr.Of("required"),
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
			toolChoice: ptr.Of("required"),
			wantErr:    true,
		},
		{
			name:       "empty choices slice with tool_choice=required - returns error (model returned nothing)",
			choices:    []*llms.ContentChoice{},
			tools:      &tools,
			toolChoice: ptr.Of("required"),
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
