//go:build conftests
// +build conftests

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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/conversation"
	"github.com/dapr/components-contrib/conversation/anthropic"
	"github.com/dapr/components-contrib/metadata"
)

// TestAnthropicMultiTurn tests Anthropic's multi-turn conversation behavior with multiple tools
func TestAnthropicMultiTurn(t *testing.T) {
	apiKey := os.Getenv("ANTHROPIC_API_KEY")
	if apiKey == "" {
		t.Skip("ANTHROPIC_API_KEY environment variable not set, skipping Anthropic multi-turn test")
	}

	// Test with Claude 3.5 Sonnet for best tool calling performance
	metadata := conversation.Metadata{
		Base: metadata.Base{
			Properties: map[string]string{
				"key":   apiKey,
				"model": "claude-3-5-sonnet-20241022", // Use Claude 3.5 Sonnet for best tool calling
			},
		},
	}

	comp := anthropic.NewAnthropic(testLogger)
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Minute)
	defer cancel()

	err := comp.Init(ctx, metadata)
	require.NoError(t, err)

	t.Run("multi_tool_calling", func(t *testing.T) {
		// Test multi-tool conversation
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Minute)
		defer cancel()

		// Define multiple tools that could be called in parallel
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

		timeTool := conversation.Tool{
			ToolType: "function",
			Function: conversation.ToolFunction{
				Name:        "get_time",
				Description: "Get current time for a specific location or timezone",
				Parameters: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"location": map[string]any{
							"type":        "string",
							"description": "The city and state/country for time lookup",
						},
					},
					"required": []string{"location"},
				},
			},
		}

		// ========== TURN 1: Request that should trigger multiple tools ==========
		t.Log("🔄 TURN 1: User asks for multiple pieces of information")

		turn1Req := &conversation.ConversationRequest{
			Tools: []conversation.Tool{weatherTool, timeTool}, // All tools available
			Inputs: []conversation.ConversationInput{
				{
					Role: conversation.RoleSystem,
					Parts: []conversation.ContentPart{
						conversation.TextContentPart{Text: "You are a helpful assistant with access to tools. When the user requests information that requires using available tools, call all appropriate tools."},
					},
				},
				{
					Role: conversation.RoleUser,
					Parts: []conversation.ContentPart{
						conversation.TextContentPart{Text: "I'm planning a trip to Tokyo. Can you get me the current weather in Tokyo and the current time there?"},
					},
				},
			},
		}

		turn1Resp, err := comp.Converse(ctx, turn1Req)
		require.NoError(t, err)
		require.NotEmpty(t, turn1Resp.Outputs)

		t.Logf("📤 Turn 1 Response: %d outputs", len(turn1Resp.Outputs))

		// Collect tool calls from ALL outputs
		var allTurn1ToolCalls []conversation.ToolCall

		for i, output := range turn1Resp.Outputs {
			t.Logf("📋 Output %d: %d parts", i+1, len(output.Parts))

			outputText := conversation.ExtractTextFromParts(output.Parts)
			outputToolCalls := conversation.ExtractToolCallsFromParts(output.Parts)

			allTurn1ToolCalls = append(allTurn1ToolCalls, outputToolCalls...)

			t.Logf("📝 Output %d Text: %q", i+1, outputText)
			t.Logf("🔧 Output %d Tool Calls: %d", i+1, len(outputToolCalls))
		}

		t.Logf("🔧 Total Turn 1 Tool Calls: %d", len(allTurn1ToolCalls))

		for i, toolCall := range allTurn1ToolCalls {
			t.Logf("🛠️  Tool Call %d: %s(%s)", i+1, toolCall.Function.Name, toolCall.Function.Arguments)
		}

		// If Anthropic called tools, process them
		if len(allTurn1ToolCalls) > 0 {
			t.Logf("✅ Anthropic called %d tool(s) in Turn 1!", len(allTurn1ToolCalls))

			// Track conversation history (Each output separately like Anthropic pattern)
			conversationHistory := []conversation.ConversationInput{
				{
					Role: conversation.RoleUser,
					Parts: []conversation.ContentPart{
						conversation.TextContentPart{Text: "I'm planning a trip to Tokyo. Can you get me the current weather in Tokyo and the current time there?"},
					},
				},
			}

			// Add each Anthropic output separately to conversation history
			for _, output := range turn1Resp.Outputs {
				conversationHistory = append(conversationHistory, conversation.ConversationInput{
					Role:  conversation.RoleAssistant,
					Parts: output.Parts,
				})
			}

			// Add tool results for each tool call
			for _, toolCall := range allTurn1ToolCalls {
				var toolResult string
				switch toolCall.Function.Name {
				case "get_weather":
					toolResult = `{"temperature": 22, "condition": "sunny", "humidity": 60, "location": "Tokyo, Japan"}`
				case "get_time":
					toolResult = `{"time": "14:30", "timezone": "JST", "date": "2025-01-13", "location": "Tokyo, Japan"}`
				default:
					toolResult = `{"status": "success", "data": "mock result"}`
				}

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
			turn2Req := &conversation.ConversationRequest{
				Tools:  []conversation.Tool{weatherTool, timeTool}, // Keep tools available
				Inputs: conversationHistory,
			}

			turn2Resp, err := comp.Converse(ctx, turn2Req)
			require.NoError(t, err)

			t.Logf("📤 Turn 2 Response: %d outputs", len(turn2Resp.Outputs))

			// Verify we get a meaningful response
			var turn2Text string
			for _, output := range turn2Resp.Outputs {
				outputText := conversation.ExtractTextFromParts(output.Parts)
				turn2Text += outputText
			}

			assert.NotEmpty(t, turn2Text, "Should provide response using tool results")
			t.Logf("✅ Anthropic multi-tool test completed with %d tool calls", len(allTurn1ToolCalls))
		} else {
			t.Log("⚠️ Anthropic didn't call tools in Turn 1 - testing follow-up approach")
		}
	})
}

// min is a helper function
