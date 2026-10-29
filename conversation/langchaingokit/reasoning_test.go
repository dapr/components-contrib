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
	"github.com/dapr/kit/ptr"
)

// TestNormalizeConverseResultReasoning verifies that the model's reasoning is
// carried into the result message instead of being dropped. OpenAI-compatible
// providers fill ContentChoice.ReasoningContent; Anthropic only reports its
// thinking through GenerationInfo["ThinkingContent"].
func TestNormalizeConverseResultReasoning(t *testing.T) {
	tests := []struct {
		name   string
		choice *llms.ContentChoice
		want   string
	}{
		{
			name:   "no reasoning leaves the field empty",
			choice: &llms.ContentChoice{Content: "answer", StopReason: "stop"},
			want:   "",
		},
		{
			name:   "reasoning content is returned",
			choice: &llms.ContentChoice{Content: "answer", StopReason: "stop", ReasoningContent: "thought"},
			want:   "thought",
		},
		{
			name: "thinking content in generation info is returned when reasoning content is empty",
			choice: &llms.ContentChoice{
				Content: "answer", StopReason: "stop",
				GenerationInfo: map[string]any{"ThinkingContent": "thought"},
			},
			want: "thought",
		},
		{
			name: "reasoning content takes precedence over thinking content",
			choice: &llms.ContentChoice{
				Content: "answer", StopReason: "stop", ReasoningContent: "reasoning",
				GenerationInfo: map[string]any{"ThinkingContent": "thinking"},
			},
			want: "reasoning",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			llm := newLLMWithStub(nil)
			outputs, _, err := llm.NormalizeConverseResult([]*llms.ContentChoice{tt.choice})
			require.NoError(t, err)
			require.Len(t, outputs, 1)
			require.Len(t, outputs[0].Choices, 1)
			assert.Equal(t, tt.want, outputs[0].Choices[0].Message.Reasoning)
			assert.Equal(t, "answer", outputs[0].Choices[0].Message.Content)
		})
	}
}

// TestConverseReasoningEffort verifies that the request-level reasoning effort
// is translated only by the component that opted in: enabling reasoning has
// provider-specific side effects (e.g. Anthropic thinking constrains
// temperature and tool_choice), so the shared kit must never guess.
func TestConverseReasoningEffort(t *testing.T) {
	choice := &llms.ContentChoice{Content: "hello", StopReason: "stop"}
	strength := 0.9
	highStrength := func(effort string) []llms.CallOption {
		if effort != "high" {
			return nil
		}
		return []llms.CallOption{llms.WithReasoningStrength(strength)}
	}

	tests := []struct {
		name         string
		translator   func(string) []llms.CallOption
		effort       *string
		wantStrength *float64
	}{
		{name: "no effort and no translator sends no reasoning option"},
		{name: "effort without a translator is ignored", effort: ptr.Of("high")},
		{name: "translator without an effort is not applied", translator: highStrength},
		{name: "translator maps the effort", translator: highStrength, effort: ptr.Of("high"), wantStrength: &strength},
		{name: "translator may ignore an unsupported effort", translator: highStrength, effort: ptr.Of("xhigh")},
		{name: "empty effort is treated as unset", translator: highStrength, effort: ptr.Of("")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got llms.CallOptions
			llm := newLLMWithStub(func(_ context.Context, _ []llms.MessageContent, options ...llms.CallOption) (*llms.ContentResponse, error) {
				got = foldCallOptions(options)
				return &llms.ContentResponse{Choices: []*llms.ContentChoice{choice}}, nil
			})
			llm.SetReasoningEffortOptions(tt.translator)

			_, err := llm.Converse(t.Context(), &conversation.Request{
				ReasoningEffort: tt.effort,
				Message: &[]llms.MessageContent{
					{Role: llms.ChatMessageTypeHuman, Parts: []llms.ContentPart{llms.TextContent{Text: "hi"}}},
				},
			})
			require.NoError(t, err)
			if tt.wantStrength == nil {
				assert.Nil(t, got.Reasoning)
				return
			}
			require.NotNil(t, got.Reasoning)
			assert.Equal(t, tt.wantStrength, got.Reasoning.Strength)
		})
	}
}

// TestConverseStripsReasoningParts verifies that reasoning sent back on an
// assistant message never reaches the langchaingo model: langchaingo providers
// reject or silently mangle content part types they do not know.
func TestConverseStripsReasoningParts(t *testing.T) {
	choice := &llms.ContentChoice{Content: "hello", StopReason: "stop"}

	var got []llms.MessageContent
	llm := newLLMWithStub(func(_ context.Context, messages []llms.MessageContent, _ ...llms.CallOption) (*llms.ContentResponse, error) {
		got = messages
		return &llms.ContentResponse{Choices: []*llms.ContentChoice{choice}}, nil
	})

	request := &conversation.Request{
		Message: &[]llms.MessageContent{
			{Role: llms.ChatMessageTypeHuman, Parts: []llms.ContentPart{llms.TextContent{Text: "hi"}}},
			{Role: llms.ChatMessageTypeAI, Parts: []llms.ContentPart{
				conversation.ReasoningContent{Text: "earlier thought"},
				llms.TextContent{Text: "earlier answer"},
			}},
			{Role: llms.ChatMessageTypeHuman, Parts: []llms.ContentPart{llms.TextContent{Text: "and now?"}}},
		},
	}
	_, err := llm.Converse(t.Context(), request)
	require.NoError(t, err)

	require.Equal(t, []llms.MessageContent{
		{Role: llms.ChatMessageTypeHuman, Parts: []llms.ContentPart{llms.TextContent{Text: "hi"}}},
		{Role: llms.ChatMessageTypeAI, Parts: []llms.ContentPart{llms.TextContent{Text: "earlier answer"}}},
		{Role: llms.ChatMessageTypeHuman, Parts: []llms.ContentPart{llms.TextContent{Text: "and now?"}}},
	}, got)

	// The caller's request must be left untouched: resiliency retries reuse it.
	require.Len(t, (*request.Message)[1].Parts, 2)
	assert.Equal(t, conversation.ReasoningContent{Text: "earlier thought"}, (*request.Message)[1].Parts[0])
}

// TestSplitReasoning verifies the reasoning sent back on each message is
// extracted by message index, joined in order, while every other part is kept.
func TestSplitReasoning(t *testing.T) {
	messages := []llms.MessageContent{
		{Role: llms.ChatMessageTypeHuman, Parts: []llms.ContentPart{llms.TextContent{Text: "hi"}}},
		{Role: llms.ChatMessageTypeAI, Parts: []llms.ContentPart{
			conversation.ReasoningContent{Text: "first"},
			llms.TextContent{Text: "answer"},
			conversation.ReasoningContent{Text: "second"},
		}},
	}

	cleaned, reasoning := SplitReasoning(messages)
	assert.Equal(t, []llms.MessageContent{
		{Role: llms.ChatMessageTypeHuman, Parts: []llms.ContentPart{llms.TextContent{Text: "hi"}}},
		{Role: llms.ChatMessageTypeAI, Parts: []llms.ContentPart{llms.TextContent{Text: "answer"}}},
	}, cleaned)
	assert.Equal(t, map[int]string{1: "first\nsecond"}, reasoning)

	t.Run("no reasoning returns a nil map", func(t *testing.T) {
		_, reasoning := SplitReasoning(messages[:1])
		assert.Nil(t, reasoning)
	})
}
