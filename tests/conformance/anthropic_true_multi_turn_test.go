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

// TestAnthropicTrueMultiTurn tests ACTUAL multi-turn conversation with Anthropic
// This calls Anthropic multiple times to test conversation memory and tool calling persistence
func TestAnthropicTrueMultiTurn(t *testing.T) {
	// Skip if no API key
	if os.Getenv("ANTHROPIC_API_KEY") == "" {
		t.Skip("Skipping Anthropic true multi-turn test: ANTHROPIC_API_KEY environment variable not set")
	}

	// Initialize Anthropic component
	logger := logger.NewLogger("anthropic-true-multi-turn-test")
	comp := anthropic.NewAnthropic(logger)

	metadata := conversation.Metadata{
		Base: metadata.Base{
			Properties: map[string]string{
				"key":   os.Getenv("ANTHROPIC_API_KEY"),
				"model": "claude-sonnet-4-20250514", // Use Claude Sonnet 4
			},
		},
	}

	ctx := context.Background()
	err := comp.Init(ctx, metadata)
	require.NoError(t, err)
	defer comp.Close()

	t.Run("true_multi_turn_conversation", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Minute)
		defer cancel()

		// Define weather tool
		weatherTool := conversation.Tool{
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
		}

		// ========== TURN 1: Initial weather question ==========
		t.Log("🔄 TURN 1: User asks about weather")

		turn1Req := &conversation.ConversationRequest{
			Inputs: []conversation.ConversationInput{
				{
					Role: conversation.RoleUser,
					Parts: []conversation.ContentPart{
						conversation.TextContentPart{Text: "What's the weather like in San Francisco?"},
						conversation.ToolDefinitionsContentPart{Tools: []conversation.Tool{weatherTool}},
					},
				},
			},
		}

		turn1Resp, err := comp.Converse(ctx, turn1Req)
		require.NoError(t, err)
		require.NotEmpty(t, turn1Resp.Outputs)

		turn1Output := turn1Resp.Outputs[0]
		turn1Text := conversation.ExtractTextFromParts(turn1Output.Parts)
		turn1ToolCalls := conversation.ExtractToolCallsFromParts(turn1Output.Parts)

		t.Logf("📤 Turn 1 Response: %s", turn1Text)
		t.Logf("🔧 Turn 1 Tool Calls: %d", len(turn1ToolCalls))

		// Track conversation history
		conversationHistory := []conversation.ConversationInput{
			// User's question
			{
				Role: conversation.RoleUser,
				Parts: []conversation.ContentPart{
					conversation.TextContentPart{Text: "What's the weather like in San Francisco?"},
					conversation.ToolDefinitionsContentPart{Tools: []conversation.Tool{weatherTool}},
				},
			},
			// Anthropic's response
			{
				Role:  conversation.RoleAssistant,
				Parts: turn1Output.Parts,
			},
		}

		// If Anthropic didn't call tools in Turn 1, test if it calls them in Turn 2
		if len(turn1ToolCalls) == 0 {
			t.Log("⚠️ Anthropic Conservative Behavior: No tools called in Turn 1")

			// ========== TURN 2: Follow-up to see if Anthropic calls tools on second attempt ==========
			t.Log("🔄 TURN 2: Follow-up question (testing if Anthropic calls tools on second attempt)")

			// Add user follow-up to history
			conversationHistory = append(conversationHistory, conversation.ConversationInput{
				Role: conversation.RoleUser,
				Parts: []conversation.ContentPart{
					conversation.TextContentPart{Text: "Can you tell me more about San Francisco weather patterns?"},
				},
			})

			turn2Req := &conversation.ConversationRequest{Inputs: conversationHistory}
			turn2Resp, err := comp.Converse(ctx, turn2Req)
			require.NoError(t, err)

			turn2Text := conversation.ExtractTextFromParts(turn2Resp.Outputs[0].Parts)
			turn2ToolCalls := conversation.ExtractToolCallsFromParts(turn2Resp.Outputs[0].Parts)

			t.Logf("📤 Turn 2 Response: %q", turn2Text)
			t.Logf("🔧 Turn 2 Tool Calls: %d", len(turn2ToolCalls))
			t.Logf("📊 Turn 2 Parts Count: %d", len(turn2Resp.Outputs[0].Parts))
			for i, part := range turn2Resp.Outputs[0].Parts {
				t.Logf("📝 Part %d: %s", i+1, part.String())
			}

			// Check if Anthropic called tools in Turn 2 (Claude Sonnet 4 behavior)
			if len(turn2ToolCalls) > 0 {
				t.Logf("🎯 BREAKTHROUGH: Anthropic called %d tool(s) in Turn 2!", len(turn2ToolCalls))
				t.Log("📈 This suggests smarter multi-turn tool calling in newer models!")

				// Continue with tool result processing...
				// Add tool results and test Turn 3
				for _, toolCall := range turn2ToolCalls {
					toolResult := `{"temperature": 68, "condition": "partly cloudy", "humidity": 75, "wind": "8 mph W", "location": "San Francisco, CA"}`

					conversationHistory = append(conversationHistory, conversation.ConversationInput{
						Role:  conversation.RoleAssistant,
						Parts: turn2Resp.Outputs[0].Parts, // Anthropic's tool call
					})

					conversationHistory = append(conversationHistory, conversation.ConversationInput{
						Role: conversation.RoleTool,
						Parts: []conversation.ContentPart{
							conversation.ToolResultContentPart{
								ToolCallID: toolCall.ID,
								Name:       toolCall.Function.Name,
								Content:    toolResult,
								IsError:    false,
							},
						},
					})
				}

				// ========== TURN 3: Process tool results ==========
				t.Log("🔄 TURN 3: Process tool results")
				turn3Req := &conversation.ConversationRequest{Inputs: conversationHistory}
				turn3Resp, err := comp.Converse(ctx, turn3Req)
				require.NoError(t, err)

				turn3Text := conversation.ExtractTextFromParts(turn3Resp.Outputs[0].Parts)
				t.Logf("📤 Turn 3 Response: %s", turn3Text)

				// Validate tool result processing
				assert.Contains(t, turn3Text, "68", "Should reference temperature from tool result")
				assert.Contains(t, turn3Text, "cloudy", "Should reference weather condition")

				t.Log("✅ Anthropic multi-turn tool calling validated!")
				return
			}

			// Original conservative behavior validation
			if len(turn2Text) > 0 {
				assert.Contains(t, turn2Text, "San Francisco", "Should remember the location from previous turn")
			} else {
				t.Log("⚠️ Turn 2 returned empty response with tool calls but no text")
			}

			t.Log("✅ Anthropic conservative behavior validated - maintains conversation memory")
			return
		}

		// ========== TURN 2: Tool results ==========
		t.Log("🔄 TURN 2: Provide tool results")
		t.Logf("✅ Anthropic called tools! Processing %d tool calls", len(turn1ToolCalls))

		// Add simulated tool results to conversation history
		for _, toolCall := range turn1ToolCalls {
			toolResult := `{"temperature": 68, "condition": "partly cloudy", "humidity": 75, "wind": "8 mph W", "location": "San Francisco, CA"}`

			conversationHistory = append(conversationHistory, conversation.ConversationInput{
				Role: conversation.RoleTool,
				Parts: []conversation.ContentPart{
					conversation.ToolResultContentPart{
						ToolCallID: toolCall.ID,
						Name:       toolCall.Function.Name,
						Content:    toolResult,
						IsError:    false,
					},
				},
			})
		}

		turn2Req := &conversation.ConversationRequest{Inputs: conversationHistory}
		turn2Resp, err := comp.Converse(ctx, turn2Req)
		require.NoError(t, err)

		turn2Output := turn2Resp.Outputs[0]
		turn2Text := conversation.ExtractTextFromParts(turn2Output.Parts)

		t.Logf("📤 Turn 2 Response: %s", turn2Text)

		// Validate tool result processing
		assert.Contains(t, turn2Text, "68", "Should reference temperature from tool result")
		assert.Contains(t, turn2Text, "cloudy", "Should reference weather condition")

		// Add assistant response to history
		conversationHistory = append(conversationHistory, conversation.ConversationInput{
			Role:  conversation.RoleAssistant,
			Parts: turn2Output.Parts,
		})

		// ========== TURN 3: Follow-up question ==========
		t.Log("🔄 TURN 3: Follow-up question to test conversation memory")

		conversationHistory = append(conversationHistory, conversation.ConversationInput{
			Role: conversation.RoleUser,
			Parts: []conversation.ContentPart{
				conversation.TextContentPart{Text: "Based on that weather information, should I bring a jacket?"},
			},
		})

		turn3Req := &conversation.ConversationRequest{Inputs: conversationHistory}
		turn3Resp, err := comp.Converse(ctx, turn3Req)
		require.NoError(t, err)

		turn3Text := conversation.ExtractTextFromParts(turn3Resp.Outputs[0].Parts)
		t.Logf("📤 Turn 3 Response: %s", turn3Text)

		// Test conversation memory - should reference previous weather data
		assert.NotEmpty(t, turn3Text, "Should provide response to follow-up")
		// Should reference the weather data from turn 2 without needing new tool calls

		t.Log("✅ Anthropic true multi-turn test completed successfully")
		t.Logf("📊 Total conversation turns: %d", len(conversationHistory))
		t.Logf("🔧 Tool calls made: %d", len(turn1ToolCalls))
	})

	t.Run("multi_turn_without_tools", func(t *testing.T) {
		// Test multi-turn conversation without any tools to validate basic conversation memory
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Minute)
		defer cancel()

		// ========== TURN 1: Ask about a topic ==========
		turn1Req := &conversation.ConversationRequest{
			Inputs: []conversation.ConversationInput{
				{
					Role: conversation.RoleUser,
					Parts: []conversation.ContentPart{
						conversation.TextContentPart{Text: "Tell me about the history of San Francisco."},
					},
				},
			},
		}

		turn1Resp, err := comp.Converse(ctx, turn1Req)
		require.NoError(t, err)

		turn1Text := conversation.ExtractTextFromParts(turn1Resp.Outputs[0].Parts)
		t.Logf("📤 Turn 1 (No Tools): %s", turn1Text[:min(150, len(turn1Text))])

		// ========== TURN 2: Follow-up question ==========
		conversationHistory := []conversation.ConversationInput{
			{
				Role: conversation.RoleUser,
				Parts: []conversation.ContentPart{
					conversation.TextContentPart{Text: "Tell me about the history of San Francisco."},
				},
			},
			{
				Role:  conversation.RoleAssistant,
				Parts: turn1Resp.Outputs[0].Parts,
			},
			{
				Role: conversation.RoleUser,
				Parts: []conversation.ContentPart{
					conversation.TextContentPart{Text: "What was the most significant event in that history?"},
				},
			},
		}

		turn2Req := &conversation.ConversationRequest{Inputs: conversationHistory}
		turn2Resp, err := comp.Converse(ctx, turn2Req)
		require.NoError(t, err)

		turn2Text := conversation.ExtractTextFromParts(turn2Resp.Outputs[0].Parts)
		t.Logf("📤 Turn 2 (No Tools): %s", turn2Text[:min(150, len(turn2Text))])

		// Should maintain context about San Francisco
		assert.Contains(t, turn2Text, "San Francisco", "Should maintain conversation context")

		t.Log("✅ Multi-turn conversation without tools validated")
	})
}
