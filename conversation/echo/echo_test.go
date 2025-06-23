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
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConverse(t *testing.T) {
	e := NewEcho(logger.NewLogger("echo test"))
	e.Init(t.Context(), conversation.Metadata{})

	r, err := e.Converse(t.Context(), &conversation.ConversationRequest{
		Inputs: []conversation.ConversationInput{
			{
				Message: "hello",
			},
		},
	})
	require.NoError(t, err)
	assert.Len(t, r.Outputs, 1)
	assert.Equal(t, "hello", r.Outputs[0].Result)
}

// TestEchoVsRealLLMBehavior validates that Echo mimics real LLM response structure
// while providing predictable echoing behavior for testing
func TestEchoVsRealLLMBehavior(t *testing.T) {
	e := NewEcho(logger.NewLogger("echo test"))
	err := e.Init(t.Context(), conversation.Metadata{
		Base: metadata.Base{
			Properties: map[string]string{},
		},
	})
	require.NoError(t, err)

	t.Run("single_output_like_real_llms", func(t *testing.T) {
		// Real LLMs (like OpenAI) always return exactly 1 output
		resp, err := e.Converse(t.Context(), &conversation.ConversationRequest{
			Inputs: []conversation.ConversationInput{
				{Message: "Test message", Role: conversation.RoleUser},
			},
		})
		require.NoError(t, err)

		// Should behave like real LLMs: exactly 1 output
		assert.Len(t, resp.Outputs, 1, "Should return exactly 1 output like real LLMs")
		assert.Equal(t, "Test message", resp.Outputs[0].Result)
	})

	t.Run("multiple_inputs_echo_last_user_message", func(t *testing.T) {
		// Unlike real LLMs that respond contextually, Echo should echo the last user message
		resp, err := e.Converse(t.Context(), &conversation.ConversationRequest{
			Inputs: []conversation.ConversationInput{
				{Message: "First assistant message", Role: conversation.RoleAssistant},
				{Message: "Second user message", Role: conversation.RoleUser},
				{Message: "Third assistant message", Role: conversation.RoleAssistant},
				{Message: "Final user message", Role: conversation.RoleUser},
			},
		})
		require.NoError(t, err)

		// Echo should return the last USER message, not all messages like real LLMs
		assert.Len(t, resp.Outputs, 1)
		assert.Equal(t, "Final user message", resp.Outputs[0].Result, "Should echo the last user message")
	})

	t.Run("finish_reason_mimics_openai", func(t *testing.T) {
		// Test that FinishReason behavior matches OpenAI exactly

		// Test 1: Normal response should be "stop"
		resp1, err := e.Converse(t.Context(), &conversation.ConversationRequest{
			Inputs: []conversation.ConversationInput{
				{Message: "Hello", Role: conversation.RoleUser},
			},
		})
		require.NoError(t, err)
		assert.Equal(t, "stop", resp1.Outputs[0].FinishReason, "Normal responses should finish with 'stop'")
		assert.Empty(t, resp1.Outputs[0].ToolCalls)

		// Test 2: Tool result should be "stop"
		resp2, err := e.Converse(t.Context(), &conversation.ConversationRequest{
			Inputs: []conversation.ConversationInput{
				{
					Message:    `{"result": "success"}`,
					Role:       conversation.RoleTool,
					ToolCallID: "test_123",
					Name:       "test_tool",
				},
			},
		})
		require.NoError(t, err)
		assert.Equal(t, "stop", resp2.Outputs[0].FinishReason, "Tool results should finish with 'stop'")
		assert.Empty(t, resp2.Outputs[0].ToolCalls)
	})

	t.Run("context_handling_like_real_llms", func(t *testing.T) {
		// Test that context is handled like real LLMs (return exactly what was provided)

		// Test 1: With context
		resp1, err := e.Converse(t.Context(), &conversation.ConversationRequest{
			Inputs: []conversation.ConversationInput{
				{Message: "Test", Role: conversation.RoleUser},
			},
			ConversationContext: "test-context-123",
		})
		require.NoError(t, err)
		assert.Equal(t, "test-context-123", resp1.ConversationContext)

		// Test 2: Without context (empty)
		resp2, err := e.Converse(t.Context(), &conversation.ConversationRequest{
			Inputs: []conversation.ConversationInput{
				{Message: "Test", Role: conversation.RoleUser},
			},
			// No ConversationContext provided
		})
		require.NoError(t, err)
		assert.Equal(t, "", resp2.ConversationContext, "Should return empty context when none provided")
	})

	t.Run("usage_information_like_real_llms", func(t *testing.T) {
		// Test that usage information is provided like real LLMs
		resp, err := e.Converse(t.Context(), &conversation.ConversationRequest{
			Inputs: []conversation.ConversationInput{
				{Message: "Test message for token counting", Role: conversation.RoleUser},
			},
		})
		require.NoError(t, err)

		// Should provide usage info like real LLMs
		require.NotNil(t, resp.Usage, "Should provide usage information like real LLMs")
		assert.Greater(t, resp.Usage.TotalTokens, int32(0), "Should count tokens")
		assert.Greater(t, resp.Usage.PromptTokens, int32(0), "Should count input tokens")
		assert.Greater(t, resp.Usage.CompletionTokens, int32(0), "Should count output tokens")
		assert.Equal(t, resp.Usage.PromptTokens+resp.Usage.CompletionTokens, resp.Usage.TotalTokens)
	})
}

// TestEchoParameterGenerationLearnings validates our learnings about parameter defaults
func TestEchoParameterGenerationLearnings(t *testing.T) {
	e := NewEcho(logger.NewLogger("echo test"))
	err := e.Init(t.Context(), conversation.Metadata{})
	require.NoError(t, err)

	// Based on our learning: count should default to 42, not 10
	tool := conversation.Tool{
		Type: "function",
		Function: conversation.ToolFunction{
			Name:        "test_tool",
			Description: "Test tool with count parameter",
			Parameters: map[string]interface{}{
				"type": "object",
				"properties": map[string]interface{}{
					"count": map[string]interface{}{
						"type":        "integer",
						"description": "A number parameter",
					},
				},
			},
		},
	}

	resp, err := e.Converse(t.Context(), &conversation.ConversationRequest{
		Inputs: []conversation.ConversationInput{
			{
				Message: "Call test_tool",
				Role:    conversation.RoleUser,
				Tools:   []conversation.Tool{tool},
			},
		},
	})
	require.NoError(t, err)
	require.Len(t, resp.Outputs[0].ToolCalls, 1)

	// Verify our learning: count should be 42 (default number), not 10
	args := resp.Outputs[0].ToolCalls[0].Function.Arguments
	assert.Contains(t, args, "42", "Count parameter should default to 42 based on our learnings")
}
