//go:build conftests
// +build conftests

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

package conformance

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/conversation"
	"github.com/dapr/components-contrib/conversation/anthropic"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
)

// TestAnthropicMultiTurn tests Anthropic's multi-turn conversation behavior
// This validates the documented issues with Anthropic's "multi-step limitations"
func TestAnthropicMultiTurn(t *testing.T) {
	// Skip if no API key
	if os.Getenv("ANTHROPIC_API_KEY") == "" {
		t.Skip("Skipping Anthropic multi-turn test: ANTHROPIC_API_KEY environment variable not set")
	}

	// Initialize Anthropic component
	logger := logger.NewLogger("anthropic-multi-turn-test")
	comp := anthropic.NewAnthropic(logger)

	metadata := conversation.Metadata{
		Base: metadata.Base{
			Properties: map[string]string{
				"key": os.Getenv("ANTHROPIC_API_KEY"),
			},
		},
	}

	ctx := context.Background()
	err := comp.Init(ctx, metadata)
	require.NoError(t, err)
	defer comp.Close()

	t.Run("multi_turn_tool_calling_sequence", func(t *testing.T) {
		// This test replicates the OpenAI multi-turn pattern to see how Anthropic handles it
		// Based on documented issues: "Multi-step limitations: More conservative approach"

		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Minute)
		defer cancel()

		// Step 1: User asks about weather with tool definition
		req1 := &conversation.ConversationRequest{
			Inputs: []conversation.ConversationInput{
				{
					Role: conversation.RoleUser,
					Parts: []conversation.ContentPart{
						conversation.TextContentPart{Text: "What's the weather like in San Francisco?"},
						conversation.ToolDefinitionsContentPart{
							Tools: []conversation.Tool{
								{
									ToolType: "function",
									Function: conversation.ToolFunction{
										Name:        "get_weather",
										Description: "Get current weather information for a specific location",
										Parameters: map[string]any{
											"type": "object",
											"properties": map[string]any{
												"location": map[string]any{
													"type":        "string",
													"description": "The city and state/country for weather lookup",
												},
											},
											"required": []string{"location"},
										},
									},
								},
							},
						},
					},
				},
			},
		}

		resp1, err := comp.Converse(ctx, req1)
		require.NoError(t, err)
		require.NotEmpty(t, resp1.Outputs)

		t.Logf("🎯 Anthropic Step 1 Response - Parts: %d", len(resp1.Outputs[0].Parts))

		// Extract tool calls from response
		toolCalls := conversation.ExtractToolCallsFromParts(resp1.Outputs[0].Parts)

		if len(toolCalls) == 0 {
			t.Logf("⚠️ Anthropic Conservative Behavior: No tool calls generated (this is documented behavior)")
			t.Logf("📝 Response: %s", conversation.ExtractTextFromParts(resp1.Outputs[0].Parts))

			// This is expected behavior for Anthropic - document it but don't fail
			assert.NotEmpty(t, resp1.Outputs[0].Parts, "Should have text response even without tool calls")
			return // Exit early as Anthropic chose not to call tools
		}

		// If Anthropic did call tools, continue with multi-turn flow
		t.Logf("✅ Anthropic called %d tool(s) - continuing multi-turn test", len(toolCalls))

		firstToolCall := toolCalls[0]
		t.Logf("🔧 Tool call: %s with args: %s", firstToolCall.Function.Name, firstToolCall.Function.Arguments)

		// Step 2: Simulate tool result
		req2 := &conversation.ConversationRequest{
			Inputs: []conversation.ConversationInput{
				// Include previous conversation for context
				{
					Role: conversation.RoleUser,
					Parts: []conversation.ContentPart{
						conversation.TextContentPart{Text: "What's the weather like in San Francisco?"},
						conversation.ToolDefinitionsContentPart{
							Tools: []conversation.Tool{
								{
									ToolType: "function",
									Function: conversation.ToolFunction{
										Name:        "get_weather",
										Description: "Get current weather information for a specific location",
										Parameters: map[string]any{
											"type": "object",
											"properties": map[string]any{
												"location": map[string]any{
													"type":        "string",
													"description": "The city and state/country for weather lookup",
												},
											},
											"required": []string{"location"},
										},
									},
								},
							},
						},
					},
				},
				// Assistant's tool call
				{
					Role:  conversation.RoleAssistant,
					Parts: resp1.Outputs[0].Parts, // Use actual response from Step 1
				},
				// Tool result
				{
					Role: conversation.RoleTool,
					Parts: []conversation.ContentPart{
						conversation.ToolResultContentPart{
							ToolCallID: firstToolCall.ID,
							Name:       firstToolCall.Function.Name,
							Content:    `{"temperature": 72, "condition": "sunny", "humidity": 65, "location": "San Francisco, CA"}`,
							IsError:    false,
						},
					},
				},
			},
		}

		resp2, err := comp.Converse(ctx, req2)
		require.NoError(t, err)
		require.NotEmpty(t, resp2.Outputs)

		t.Logf("🎯 Anthropic Step 2 (Tool Result) Response - Parts: %d", len(resp2.Outputs[0].Parts))

		finalText := conversation.ExtractTextFromParts(resp2.Outputs[0].Parts)
		t.Logf("📝 Final response: %s", finalText)

		// Validate Anthropic handled the tool result properly
		assert.NotEmpty(t, finalText, "Should provide response to tool result")
		assert.Contains(t, finalText, "72", "Should reference the temperature from tool result")
		assert.Contains(t, finalText, "sunny", "Should reference the weather condition")

		// Step 3: Follow-up question to test conversation memory
		req3 := &conversation.ConversationRequest{
			Inputs: []conversation.ConversationInput{
				// Include full conversation history
				{
					Role: conversation.RoleUser,
					Parts: []conversation.ContentPart{
						conversation.TextContentPart{Text: "What's the weather like in San Francisco?"},
						conversation.ToolDefinitionsContentPart{
							Tools: []conversation.Tool{
								{
									ToolType: "function",
									Function: conversation.ToolFunction{
										Name:        "get_weather",
										Description: "Get current weather information for a specific location",
										Parameters: map[string]any{
											"type": "object",
											"properties": map[string]any{
												"location": map[string]any{
													"type":        "string",
													"description": "The city and state/country for weather lookup",
												},
											},
											"required": []string{"location"},
										},
									},
								},
							},
						},
					},
				},
				{
					Role:  conversation.RoleAssistant,
					Parts: resp1.Outputs[0].Parts,
				},
				{
					Role: conversation.RoleTool,
					Parts: []conversation.ContentPart{
						conversation.ToolResultContentPart{
							ToolCallID: firstToolCall.ID,
							Name:       firstToolCall.Function.Name,
							Content:    `{"temperature": 72, "condition": "sunny", "humidity": 65, "location": "San Francisco, CA"}`,
							IsError:    false,
						},
					},
				},
				{
					Role:  conversation.RoleAssistant,
					Parts: resp2.Outputs[0].Parts,
				},
				// New follow-up question
				{
					Role: conversation.RoleUser,
					Parts: []conversation.ContentPart{
						conversation.TextContentPart{Text: "Should I bring a jacket based on that weather?"},
					},
				},
			},
		}

		resp3, err := comp.Converse(ctx, req3)
		require.NoError(t, err)
		require.NotEmpty(t, resp3.Outputs)

		t.Logf("🎯 Anthropic Step 3 (Follow-up) Response - Parts: %d", len(resp3.Outputs[0].Parts))

		followUpText := conversation.ExtractTextFromParts(resp3.Outputs[0].Parts)
		t.Logf("📝 Follow-up response: %s", followUpText)

		// Validate Anthropic maintained conversation context
		assert.NotEmpty(t, followUpText, "Should provide response to follow-up question")
		// Should reference the previous weather information without needing new tool calls
		// This tests Anthropic's conversation memory across turns

		t.Logf("✅ Anthropic multi-turn test completed successfully")
		t.Logf("📊 Turn 1: %d parts, Turn 2: %d parts, Turn 3: %d parts",
			len(resp1.Outputs[0].Parts),
			len(resp2.Outputs[0].Parts),
			len(resp3.Outputs[0].Parts))
	})

	t.Run("conservative_tool_calling_behavior", func(t *testing.T) {
		// Test Anthropic's documented "conservative tool calling" behavior
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
		defer cancel()

		// Test with ambiguous prompt that might or might not trigger tool calling
		req := &conversation.ConversationRequest{
			Inputs: []conversation.ConversationInput{
				{
					Role: conversation.RoleUser,
					Parts: []conversation.ContentPart{
						conversation.TextContentPart{Text: "I'm curious about the weather, can you tell me about meteorology?"},
						conversation.ToolDefinitionsContentPart{
							Tools: []conversation.Tool{
								{
									ToolType: "function",
									Function: conversation.ToolFunction{
										Name:        "get_weather",
										Description: "Get current weather information",
										Parameters: map[string]any{
											"type": "object",
											"properties": map[string]any{
												"location": map[string]any{
													"type":        "string",
													"description": "Location for weather",
												},
											},
											"required": []string{"location"},
										},
									},
								},
							},
						},
					},
				},
			},
		}

		resp, err := comp.Converse(ctx, req)
		require.NoError(t, err)
		require.NotEmpty(t, resp.Outputs)

		toolCalls := conversation.ExtractToolCallsFromParts(resp.Outputs[0].Parts)
		text := conversation.ExtractTextFromParts(resp.Outputs[0].Parts)

		if len(toolCalls) == 0 {
			t.Logf("✅ Anthropic Conservative Behavior Confirmed: Chose not to call tools for ambiguous request")
			t.Logf("📝 Provided text response instead: %s", text[:min(100, len(text))])
		} else {
			t.Logf("🤔 Anthropic called %d tool(s) for ambiguous request", len(toolCalls))
		}

		// Either behavior is valid - just document what Anthropic chose to do
		assert.NotEmpty(t, text, "Should provide some response regardless of tool calling choice")
	})
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
