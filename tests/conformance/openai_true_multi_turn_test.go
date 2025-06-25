//go:build conftests

/*
Copyright 2024 The Dapr Authors
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
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/conversation"
	"github.com/dapr/components-contrib/conversation/openai"
	"github.com/dapr/components-contrib/metadata"
)

func TestOpenAITrueMultiTurn(t *testing.T) {
	apiKey := os.Getenv("OPENAI_API_KEY")
	if apiKey == "" {
		t.Skip("OPENAI_API_KEY environment variable not set, skipping OpenAI multi-turn test")
	}

	// Test with GPT-4o for best tool calling performance
	metadata := conversation.Metadata{
		Base: metadata.Base{
			Properties: map[string]string{
				"key":   apiKey,
				"model": "gpt-4o", // Use GPT-4o for best tool calling
			},
		},
	}

	comp := openai.NewOpenAI(testLogger)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()

	err := comp.Init(ctx, metadata)
	require.NoError(t, err)

	t.Run("true_multi_turn_conversation", func(t *testing.T) {
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
							"description": "The city and state, e.g. San Francisco, CA",
						},
					},
					"required": []string{"location"},
				},
			},
		}

		// Start conversation history
		var conversationHistory []conversation.ConversationInput

		// ========== TURN 1: User asks about weather with tools available ==========
		t.Log("🔄 TURN 1: User asks about weather")

		conversationHistory = append(conversationHistory, conversation.ConversationInput{
			Role: conversation.RoleUser,
			Parts: []conversation.ContentPart{
				conversation.TextContentPart{Text: "What's the weather like in San Francisco?"},
				conversation.ToolDefinitionsContentPart{Tools: []conversation.Tool{weatherTool}},
			},
		})

		turn1Req := &conversation.ConversationRequest{Inputs: conversationHistory}
		turn1Resp, err := comp.Converse(ctx, turn1Req)
		require.NoError(t, err)

		turn1Text := conversation.ExtractTextFromParts(turn1Resp.Outputs[0].Parts)
		turn1ToolCalls := conversation.ExtractToolCallsFromParts(turn1Resp.Outputs[0].Parts)

		t.Logf("📤 Turn 1 Response: %s", turn1Text)
		t.Logf("🔧 Turn 1 Tool Calls: %d", len(turn1ToolCalls))

		for i, toolCall := range turn1ToolCalls {
			t.Logf("🛠️  Tool Call %d: %s(%s)", i+1, toolCall.Function.Name, toolCall.Function.Arguments)
		}

		// OpenAI typically calls tools immediately on first turn
		if len(turn1ToolCalls) > 0 {
			t.Logf("🎯 OpenAI called %d tool(s) in Turn 1 - typical aggressive behavior", len(turn1ToolCalls))

			// Process tool calls and continue conversation
			for _, toolCall := range turn1ToolCalls {
				// Add assistant's tool call to history
				conversationHistory = append(conversationHistory, conversation.ConversationInput{
					Role:  conversation.RoleAssistant,
					Parts: turn1Resp.Outputs[0].Parts,
				})

				// Simulate tool execution
				toolResult := `{"temperature": 65, "condition": "foggy", "humidity": 85, "wind": "12 mph W", "location": "San Francisco, CA"}`

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

			// ========== TURN 2: Process tool results ==========
			t.Log("🔄 TURN 2: Process tool results")
			turn2Req := &conversation.ConversationRequest{Inputs: conversationHistory}
			turn2Resp, err := comp.Converse(ctx, turn2Req)
			require.NoError(t, err)

			turn2Text := conversation.ExtractTextFromParts(turn2Resp.Outputs[0].Parts)
			t.Logf("📤 Turn 2 Response: %s", turn2Text)

			// Validate tool result processing
			assert.Contains(t, turn2Text, "65", "Should reference temperature from tool result")
			assert.Contains(t, turn2Text, "fog", "Should reference weather condition")

			// ========== TURN 3: Follow-up question ==========
			t.Log("🔄 TURN 3: Follow-up question about clothing recommendations")

			conversationHistory = append(conversationHistory, conversation.ConversationInput{
				Role:  conversation.RoleAssistant,
				Parts: turn2Resp.Outputs[0].Parts,
			})

			conversationHistory = append(conversationHistory, conversation.ConversationInput{
				Role: conversation.RoleUser,
				Parts: []conversation.ContentPart{
					conversation.TextContentPart{Text: "Based on that weather, what should I wear?"},
				},
			})

			turn3Req := &conversation.ConversationRequest{Inputs: conversationHistory}
			turn3Resp, err := comp.Converse(ctx, turn3Req)
			require.NoError(t, err)

			turn3Text := conversation.ExtractTextFromParts(turn3Resp.Outputs[0].Parts)
			t.Logf("📤 Turn 3 Response: %s", turn3Text)

			// Validate conversation memory and logical recommendations
			assert.Contains(t, turn3Text, "fog", "Should reference foggy conditions from previous turn")
			// Should recommend appropriate clothing for foggy SF weather
			containsClothingAdvice := false
			clothingTerms := []string{"layer", "Layer", "jacket", "sweater", "warm", "clothing", "wear", "dress"}
			for _, term := range clothingTerms {
				if len(turn3Text) > 0 && strings.Contains(strings.ToLower(turn3Text), strings.ToLower(term)) {
					containsClothingAdvice = true
					break
				}
			}
			assert.True(t, containsClothingAdvice, "Should provide clothing recommendations")

			t.Log("✅ OpenAI multi-turn tool calling validated!")
			return
		}

		// If OpenAI didn't call tools (unexpected), test follow-up behavior
		t.Log("⚠️ Unexpected: OpenAI didn't call tools in Turn 1")

		// ========== TURN 2: Follow-up to encourage tool usage ==========
		t.Log("🔄 TURN 2: Follow-up question to encourage tool usage")

		conversationHistory = append(conversationHistory, conversation.ConversationInput{
			Role: conversation.RoleUser,
			Parts: []conversation.ContentPart{
				conversation.TextContentPart{Text: "Can you check the current weather conditions for me?"},
			},
		})

		turn2Req := &conversation.ConversationRequest{Inputs: conversationHistory}
		turn2Resp, err := comp.Converse(ctx, turn2Req)
		require.NoError(t, err)

		turn2Text := conversation.ExtractTextFromParts(turn2Resp.Outputs[0].Parts)
		turn2ToolCalls := conversation.ExtractToolCallsFromParts(turn2Resp.Outputs[0].Parts)

		t.Logf("📤 Turn 2 Response: %q", turn2Text)
		t.Logf("🔧 Turn 2 Tool Calls: %d", len(turn2ToolCalls))

		if len(turn2ToolCalls) > 0 {
			t.Logf("🎯 OpenAI called %d tool(s) in Turn 2 after follow-up", len(turn2ToolCalls))
			t.Log("✅ OpenAI responsive tool calling validated!")
		} else {
			t.Log("⚠️ OpenAI still not calling tools - unexpected behavior")
			// Validate conversation memory at least
			assert.Contains(t, turn2Text, "San Francisco", "Should remember the location from previous turn")
		}
	})

	t.Run("multi_turn_without_tools", func(t *testing.T) {
		t.Log("🔄 Testing multi-turn conversation without tools")

		// ========== TURN 1: General question ==========
		req1 := &conversation.ConversationRequest{
			Inputs: []conversation.ConversationInput{
				{
					Role: conversation.RoleUser,
					Parts: []conversation.ContentPart{
						conversation.TextContentPart{Text: "Tell me about the history of San Francisco"},
					},
				},
			},
		}

		resp1, err := comp.Converse(ctx, req1)
		require.NoError(t, err)

		text1 := conversation.ExtractTextFromParts(resp1.Outputs[0].Parts)
		preview1 := text1
		if len(text1) > 200 {
			preview1 = text1[:200] + "..."
		}
		t.Logf("📤 Turn 1 (No Tools): %s", preview1)

		assert.NotEmpty(t, text1, "Should provide historical information")
		assert.Contains(t, text1, "San Francisco", "Should mention San Francisco")

		// ========== TURN 2: Follow-up question ==========
		req2 := &conversation.ConversationRequest{
			Inputs: []conversation.ConversationInput{
				{
					Role: conversation.RoleUser,
					Parts: []conversation.ContentPart{
						conversation.TextContentPart{Text: "Tell me about the history of San Francisco"},
					},
				},
				{
					Role:  conversation.RoleAssistant,
					Parts: resp1.Outputs[0].Parts,
				},
				{
					Role: conversation.RoleUser,
					Parts: []conversation.ContentPart{
						conversation.TextContentPart{Text: "What about the 1906 earthquake?"},
					},
				},
			},
		}

		resp2, err := comp.Converse(ctx, req2)
		require.NoError(t, err)

		text2 := conversation.ExtractTextFromParts(resp2.Outputs[0].Parts)
		preview2 := text2
		if len(text2) > 200 {
			preview2 = text2[:200] + "..."
		}
		t.Logf("📤 Turn 2 (No Tools): %s", preview2)

		assert.NotEmpty(t, text2, "Should provide earthquake information")
		assert.Contains(t, text2, "1906", "Should reference the 1906 earthquake")
		assert.Contains(t, text2, "earthquake", "Should mention earthquake")

		t.Log("✅ Multi-turn conversation without tools validated")
	})
}
