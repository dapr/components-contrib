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

package echo

import (
	"testing"

	"github.com/dapr/components-contrib/conversation"
	"github.com/dapr/kit/logger"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestEchoVsOpenAIBehaviorComparison documents the key differences between
// Echo (test helper) and real LLM providers like OpenAI based on our analysis
func TestEchoVsOpenAIBehaviorComparison(t *testing.T) {
	e := NewEcho(logger.NewLogger("echo test"))
	err := e.Init(t.Context(), conversation.Metadata{})
	require.NoError(t, err)

	t.Run("conversation_handling_philosophy", func(t *testing.T) {
		// LEARNING: Real LLMs process ALL inputs as conversation history and respond contextually
		// Echo should echo the last user message for predictable testing

		multiInputRequest := &conversation.ConversationRequest{
			Inputs: []conversation.ConversationInput{
				{Message: "Hello", Role: conversation.RoleUser},
				{Message: "Hi there! How can I help?", Role: conversation.RoleAssistant},
				{Message: "Tell me a joke", Role: conversation.RoleUser},
			},
		}

		resp, err := e.Converse(t.Context(), multiInputRequest)
		require.NoError(t, err)

		// Echo behavior: Should echo the last user message ("Tell me a joke")
		// NOT like OpenAI which would respond contextually to the entire conversation
		assert.Equal(t, "Tell me a joke", resp.Outputs[0].Result,
			"Echo should echo last user message, unlike OpenAI which responds contextually")

		// Document what OpenAI would do:
		t.Logf("📝 LEARNING: OpenAI would respond contextually (e.g., 'Sure! Here's a joke...')")
		t.Logf("📝 LEARNING: Echo echoes last user message for predictable testing")
	})

	t.Run("response_structure_compatibility", func(t *testing.T) {
		// LEARNING: Echo must mimic OpenAI's response structure for test compatibility

		resp, err := e.Converse(t.Context(), &conversation.ConversationRequest{
			Inputs: []conversation.ConversationInput{
				{Message: "Test", Role: conversation.RoleUser},
			},
		})
		require.NoError(t, err)

		// Structure should match OpenAI exactly
		assert.Len(t, resp.Outputs, 1, "Must return exactly 1 output like OpenAI")
		assert.NotNil(t, resp.Usage, "Must provide usage info like OpenAI")
		assert.NotEmpty(t, resp.Outputs[0].FinishReason, "Must set FinishReason like OpenAI")

		t.Logf("✅ Echo mimics OpenAI structure: 1 output, usage info, finishReason")
	})

	t.Run("finish_reason_exact_compatibility", func(t *testing.T) {
		// LEARNING: Echo must use identical FinishReason values as OpenAI

		weatherTool := conversation.Tool{
			Type: "function",
			Function: conversation.ToolFunction{
				Name:        "get_weather",
				Description: "Get weather",
				Parameters:  map[string]interface{}{"type": "object"},
			},
		}

		// Test both scenarios
		tests := []struct {
			name           string
			request        *conversation.ConversationRequest
			expectedFinish string
			expectedTools  int
		}{
			{
				name: "normal_response_like_openai",
				request: &conversation.ConversationRequest{
					Inputs: []conversation.ConversationInput{
						{Message: "Hello", Role: conversation.RoleUser},
					},
				},
				expectedFinish: "stop",
				expectedTools:  0,
			},
			{
				name: "tool_calling_like_openai",
				request: &conversation.ConversationRequest{
					Inputs: []conversation.ConversationInput{
						{
							Message: "What's the weather?",
							Role:    conversation.RoleUser,
							Tools:   []conversation.Tool{weatherTool},
						},
					},
				},
				expectedFinish: "tool_calls",
				expectedTools:  1,
			},
		}

		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				resp, err := e.Converse(t.Context(), test.request)
				require.NoError(t, err)

				assert.Equal(t, test.expectedFinish, resp.Outputs[0].FinishReason,
					"FinishReason must match OpenAI exactly")
				assert.Len(t, resp.Outputs[0].ToolCalls, test.expectedTools,
					"Tool calls count must match OpenAI behavior")
			})
		}
	})

	t.Run("tool_calling_intelligence_vs_echoing", func(t *testing.T) {
		// LEARNING: Echo has intelligence in tool matching but echoing in responses

		tools := []conversation.Tool{
			{
				Type: "function",
				Function: conversation.ToolFunction{
					Name:        "send_email",
					Description: "Send an email",
					Parameters:  map[string]interface{}{"type": "object"},
				},
			},
			{
				Type: "function",
				Function: conversation.ToolFunction{
					Name:        "get_weather",
					Description: "Get weather",
					Parameters:  map[string]interface{}{"type": "object"},
				},
			},
		}

		// Echo should be smart about tool matching (case-agnostic, keyword-based)
		resp, err := e.Converse(t.Context(), &conversation.ConversationRequest{
			Inputs: []conversation.ConversationInput{
				{
					Message: "please send email to the team",
					Role:    conversation.RoleUser,
					Tools:   tools,
				},
			},
		})
		require.NoError(t, err)

		// Should intelligently match tools
		require.Len(t, resp.Outputs[0].ToolCalls, 1)
		assert.Equal(t, "send_email", resp.Outputs[0].ToolCalls[0].Function.Name,
			"Echo should intelligently match tools like OpenAI")

		// But responses should be predictable, not creative like OpenAI
		assert.Equal(t, "I'll help you with that by calling the appropriate tools.",
			resp.Outputs[0].Result, "Echo responses should be predictable, not creative like OpenAI")

		t.Logf("📝 LEARNING: Echo has tool intelligence but predictable responses")
		t.Logf("📝 LEARNING: OpenAI would have creative, contextual responses")
	})

	t.Run("context_and_usage_exactness", func(t *testing.T) {
		// LEARNING: Echo should exactly mimic OpenAI's context and usage behavior

		resp, err := e.Converse(t.Context(), &conversation.ConversationRequest{
			Inputs: []conversation.ConversationInput{
				{Message: "Test message", Role: conversation.RoleUser},
			},
			ConversationContext: "ctx-123",
		})
		require.NoError(t, err)

		// Context handling must match OpenAI exactly
		assert.Equal(t, "ctx-123", resp.ConversationContext,
			"Context handling must match OpenAI exactly")

		// Usage info must be provided like OpenAI
		assert.NotNil(t, resp.Usage, "Must provide usage like OpenAI")
		assert.Greater(t, resp.Usage.TotalTokens, int32(0), "Must count tokens like OpenAI")

		t.Logf("✅ Echo exactly mimics OpenAI's context and usage behavior")
	})
}

// TestEchoAsTestDouble documents Echo's role as a test double for real LLM providers
func TestEchoAsTestDouble(t *testing.T) {
	e := NewEcho(logger.NewLogger("echo test"))
	err := e.Init(t.Context(), conversation.Metadata{})
	require.NoError(t, err)

	t.Run("echo_purpose_and_philosophy", func(t *testing.T) {
		// Document Echo's purpose based on our learnings

		resp, err := e.Converse(t.Context(), &conversation.ConversationRequest{
			Inputs: []conversation.ConversationInput{
				{Message: "Test input", Role: conversation.RoleUser},
			},
		})
		require.NoError(t, err)

		// Echo should be:
		// 1. Predictable (echoes input)
		// 2. Compatible (same structure as real LLMs)
		// 3. Intelligent (for tool calling)
		// 4. Fast (no API calls)

		assert.Equal(t, "Test input", resp.Outputs[0].Result, "1. Predictable echoing")
		assert.Len(t, resp.Outputs, 1, "2. Compatible structure")
		assert.NotNil(t, resp.Usage, "2. Compatible metadata")
		// Intelligence tested in other tests
		// Speed inherent (no network calls)

		t.Logf("📝 Echo serves as a perfect test double:")
		t.Logf("   ✅ Predictable responses (echoing)")
		t.Logf("   ✅ Compatible structure (like OpenAI)")
		t.Logf("   ✅ Intelligent tool matching")
		t.Logf("   ✅ Fast execution (no API calls)")
		t.Logf("   ✅ Comprehensive test coverage")
	})
}
