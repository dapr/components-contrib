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

package conversation

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/dapr/components-contrib/conversation"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/tests/conformance/utils"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type TestConfig struct {
	utils.CommonConfig
}

func NewTestConfig(componentName string) TestConfig {
	tc := TestConfig{
		utils.CommonConfig{
			ComponentType: "conversation",
			ComponentName: componentName,
		},
	}

	return tc
}

func ConformanceTests(t *testing.T, props map[string]string, conv conversation.Conversation, component string) {
	// Ensure component is initialized for isolated test runs
	ensureComponentInitialized := func() {
		// Check if component needs initialization by trying a simple operation
		// This is a heuristic - if the component fails with "not initialized" error, we initialize it
		testReq := &conversation.ConversationRequest{
			Inputs: []conversation.ConversationInput{
				{Message: "init test"},
			},
		}

		_, err := conv.Converse(t.Context(), testReq)
		if err != nil && (strings.Contains(err.Error(), "LLM Model is nil") ||
			strings.Contains(err.Error(), "not properly initialized") ||
			strings.Contains(err.Error(), "not initialized")) {
			// Component needs initialization
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
			defer cancel()

			initErr := conv.Init(ctx, conversation.Metadata{
				Base: metadata.Base{
					Properties: props,
				},
			})
			if initErr != nil {
				t.Fatalf("Failed to initialize component for isolated test: %v", initErr)
			}
		}
	}

	t.Run("converse", func(t *testing.T) {
		t.Run("get a non-empty response without errors", func(t *testing.T) {
			ensureComponentInitialized()
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
			defer cancel()

			req := &conversation.ConversationRequest{
				Inputs: []conversation.ConversationInput{
					{
						Message: "what is the time?",
					},
				},
			}
			resp, err := conv.Converse(ctx, req)

			require.NoError(t, err)
			assert.Len(t, resp.Outputs, 1)
			assert.NotEmpty(t, conversation.ExtractTextFromParts(resp.Outputs[0].Parts))
		})

		t.Run("returns usage information", func(t *testing.T) {
			ensureComponentInitialized()
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
			defer cancel()

			req := &conversation.ConversationRequest{
				Inputs: []conversation.ConversationInput{
					{
						Message: "Say hello",
					},
				},
			}
			resp, err := conv.Converse(ctx, req)

			require.NoError(t, err)
			require.NotNil(t, resp.Usage, "Usage information should be present")
			assert.GreaterOrEqual(t, resp.Usage.PromptTokens, uint64(0), "Should have non-negative prompt tokens")
			assert.GreaterOrEqual(t, resp.Usage.CompletionTokens, uint64(0), "Should have non-negative completion tokens")
			assert.Equal(t, resp.Usage.TotalTokens, resp.Usage.PromptTokens+resp.Usage.CompletionTokens, "Total should equal sum of prompt and completion tokens")
		})

		t.Run("finish_reason_is_stop_for_text_completion", func(t *testing.T) {
			// This test ensures that a simple text completion returns a 'stop' finish reason.
			req := &conversation.ConversationRequest{
				Inputs: []conversation.ConversationInput{
					{Message: "Hello", Role: conversation.RoleUser},
				},
			}

			resp, err := conv.Converse(t.Context(), req)
			require.NoError(t, err)
			require.NotNil(t, resp)
			require.Len(t, resp.Outputs, 1, "Should have one output")

			// The finish reason for a simple text completion varies by provider
			expectedFinishReasons := []string{"stop"} // Default expectation
			if component == "anthropic" {
				expectedFinishReasons = []string{"end_turn", "stop"} // Anthropic uses 'end_turn'
			}

			actualFinishReason := resp.Outputs[0].FinishReason
			assert.Contains(t, expectedFinishReasons, actualFinishReason,
				"Finish reason for simple text completion should be one of: %v (got: %s)",
				expectedFinishReasons, actualFinishReason)
		})
	})

	t.Run("content parts", func(t *testing.T) {
		t.Run("text content parts", func(t *testing.T) {
			ensureComponentInitialized()
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
			defer cancel()

			req := &conversation.ConversationRequest{
				Inputs: []conversation.ConversationInput{
					{
						Role: conversation.RoleUser,
						Parts: []conversation.ContentPart{
							conversation.TextContentPart{Text: "Hello"},
							conversation.TextContentPart{Text: "World"},
						},
					},
				},
			}
			resp, err := conv.Converse(ctx, req)

			require.NoError(t, err)
			assert.Len(t, resp.Outputs, 1)

			output := resp.Outputs[0]
			assert.NotEmpty(t, output.Parts, "Should have content parts in response")

			// Legacy fields should still be populated for backward compatibility
			assert.NotEmpty(t, output.Result, "Legacy result field should be populated") //nolint:staticcheck
		})

		t.Run("tools in request field (correct approach)", func(t *testing.T) {
			ensureComponentInitialized()
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
			defer cancel()

			// Tool definitions should be in the Tools field of the request, NOT in content parts
			req := &conversation.ConversationRequest{
				Tools: []conversation.Tool{
					{
						ToolType: "function",
						Function: conversation.ToolFunction{
							Name:        "test_tool",
							Description: "A test tool",
							Parameters: map[string]any{
								"type": "object",
								"properties": map[string]any{
									"message": map[string]any{
										"type":        "string",
										"description": "A test message parameter",
									},
								},
								"required": []string{"message"},
							},
						},
					},
				},
				Inputs: []conversation.ConversationInput{
					{
						Role: conversation.RoleUser,
						Parts: []conversation.ContentPart{
							conversation.TextContentPart{Text: "What can you do?"},
						},
					},
				},
			}
			resp, err := conv.Converse(ctx, req)

			require.NoError(t, err)
			assert.Len(t, resp.Outputs, 1)

			output := resp.Outputs[0]
			assert.NotEmpty(t, output.Parts, "Should have content parts in response")
		})

		t.Run("content parts utility functions", func(t *testing.T) {
			// Test the utility functions work correctly with proper content part types
			parts := []conversation.ContentPart{
				conversation.TextContentPart{Text: "Hello"},
				conversation.TextContentPart{Text: "World"},
				conversation.ToolCallContentPart{
					ID:       "test_call_id",
					CallType: "function",
					Function: conversation.ToolCallFunction{
						Name:      "test_tool",
						Arguments: `{"param": "value"}`,
					},
				},
				conversation.ToolResultContentPart{
					ToolCallID: "test_call_id",
					Name:       "test_tool",
					Content:    "Test result",
					IsError:    false,
				},
			}

			// Test text extraction
			text := conversation.ExtractTextFromParts(parts)
			assert.Equal(t, "Hello World", text)

			// Test tool call extraction
			toolCalls := conversation.ExtractToolCallsFromParts(parts)
			assert.Len(t, toolCalls, 1)
			assert.Equal(t, "test_tool", toolCalls[0].Function.Name)
			assert.Equal(t, "test_call_id", toolCalls[0].ID)
		})

		t.Run("tool calling with content parts", func(t *testing.T) {
			// Tool calling support is component-dependent
			// This test will run for all components but expectations vary
			ensureComponentInitialized()
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
			defer cancel()

			req := &conversation.ConversationRequest{
				Tools: []conversation.Tool{
					{
						ToolType: "function",
						Function: conversation.ToolFunction{
							Name:        "get_weather",
							Description: "Get current weather",
							Parameters: map[string]any{
								"type": "object",
								"properties": map[string]any{
									"location": map[string]any{
										"type":        "string",
										"description": "City name",
									},
								},
								"required": []string{"location"},
							},
						},
					},
				},
				Inputs: []conversation.ConversationInput{
					{
						Role: conversation.RoleUser,
						Parts: []conversation.ContentPart{
							conversation.TextContentPart{Text: "What's the weather like?"},
						},
					},
				},
			}
			resp, err := conv.Converse(ctx, req)

			require.NoError(t, err)
			assert.NotEmpty(t, resp.Outputs, "Should have at least one output")

			// Collect all parts from all outputs (some providers may return multiple outputs)
			var allParts []conversation.ContentPart
			for _, output := range resp.Outputs {
				assert.NotEmpty(t, output.Parts, "Each output should have content parts")
				allParts = append(allParts, output.Parts...)
			}

			// Check if tool calls were generated (component-dependent)
			toolCalls := conversation.ExtractToolCallsFromParts(allParts)
			if len(toolCalls) > 0 {
				t.Logf("Component generated %d tool calls across %d outputs", len(toolCalls), len(resp.Outputs))
				for _, tc := range toolCalls {
					assert.NotEmpty(t, tc.ID, "Tool call should have ID")
					assert.NotEmpty(t, tc.Function.Name, "Tool call should have function name")
				}
			}
		})

		t.Run("parallel tool calling with content parts", func(t *testing.T) {
			// Test parallel tool calling - multiple tools called simultaneously
			ensureComponentInitialized()
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
			defer cancel()

			req := &conversation.ConversationRequest{
				Tools: []conversation.Tool{
					{
						ToolType: "function",
						Function: conversation.ToolFunction{
							Name:        "get_weather",
							Description: "Get current weather for a location",
							Parameters: map[string]any{
								"type": "object",
								"properties": map[string]any{
									"location": map[string]any{
										"type":        "string",
										"description": "City name",
									},
								},
								"required": []string{"location"},
							},
						},
					},
					{
						ToolType: "function",
						Function: conversation.ToolFunction{
							Name:        "get_time",
							Description: "Get current time for a location",
							Parameters: map[string]any{
								"type": "object",
								"properties": map[string]any{
									"location": map[string]any{
										"type":        "string",
										"description": "City name",
									},
								},
								"required": []string{"location"},
							},
						},
					},
				},
				Inputs: []conversation.ConversationInput{
					{
						Role: conversation.RoleUser,
						Parts: []conversation.ContentPart{
							conversation.TextContentPart{Text: "I need the weather in New York and the time in London"},
						},
					},
				},
			}
			resp, err := conv.Converse(ctx, req)

			require.NoError(t, err)
			assert.NotEmpty(t, resp.Outputs, "Should have at least one output")

			// Collect all parts from all outputs
			var allParts []conversation.ContentPart
			for _, output := range resp.Outputs {
				assert.NotEmpty(t, output.Parts, "Each output should have content parts")
				allParts = append(allParts, output.Parts...)
			}

			// Check for tool calls (component-dependent behavior)
			toolCalls := conversation.ExtractToolCallsFromParts(allParts)
			if len(toolCalls) > 0 {
				t.Logf("Component generated %d tool calls for parallel request", len(toolCalls))

				// Verify each tool call has required fields
				for i, tc := range toolCalls {
					assert.NotEmpty(t, tc.ID, "Tool call %d should have ID", i)
					assert.NotEmpty(t, tc.Function.Name, "Tool call %d should have function name", i)
					assert.Contains(t, []string{"get_weather", "get_time"}, tc.Function.Name,
						"Tool call %d should be one of the defined tools", i)
				}

				// If multiple tool calls were generated, verify they have unique IDs
				if len(toolCalls) > 1 {
					seenIDs := make(map[string]bool)
					for i, tc := range toolCalls {
						assert.False(t, seenIDs[tc.ID], "Tool call %d ID should be unique", i)
						seenIDs[tc.ID] = true
					}
					t.Logf("Verified %d parallel tool calls have unique IDs", len(toolCalls))
				}
			} else {
				t.Log("Component did not generate tool calls (this is acceptable for some components)")
			}
		})

		t.Run("multi-turn conversation with content parts", func(t *testing.T) {
			// This test was previously failing for DeepSeek due to missing ensureComponentInitialized() call
			// Now fixed and working properly for all providers
			ensureComponentInitialized()
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
			defer cancel()

			req := &conversation.ConversationRequest{
				Inputs: []conversation.ConversationInput{
					// User's initial message
					{
						Role: conversation.RoleUser,
						Parts: []conversation.ContentPart{
							conversation.TextContentPart{Text: "Hello, how are you?"},
						},
					},
					// Assistant's response
					{
						Role: conversation.RoleAssistant,
						Parts: []conversation.ContentPart{
							conversation.TextContentPart{Text: "I'm doing well, thank you!"},
						},
					},
					// User's follow-up
					{
						Role: conversation.RoleUser,
						Parts: []conversation.ContentPart{
							conversation.TextContentPart{Text: "What can you help me with?"},
						},
					},
				},
			}
			resp, err := conv.Converse(ctx, req)

			require.NoError(t, err)
			assert.Len(t, resp.Outputs, 1)

			output := resp.Outputs[0]
			assert.NotEmpty(t, output.Parts, "Should have content parts in response")
			assert.NotEmpty(t, output.Result, "Should have legacy result field") //nolint:staticcheck
		})

		t.Run("multi-turn tool calling", func(t *testing.T) {
			// Test real multi-turn tool calling where the AI generates tool calls
			// and we provide the results for further conversation
			ensureComponentInitialized()
			ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
			defer cancel()

			// Step 1: Initial request with tool definitions - AI should generate tool call
			t.Log("Step 1: Making initial request with tool definitions")
			step1Req := &conversation.ConversationRequest{
				Tools: []conversation.Tool{
					{
						ToolType: "function",
						Function: conversation.ToolFunction{
							Name:        "get_weather",
							Description: "Get current weather for a location",
							Parameters: map[string]any{
								"type": "object",
								"properties": map[string]any{
									"location": map[string]any{
										"type":        "string",
										"description": "City name",
									},
								},
								"required": []string{"location"},
							},
						},
					},
				},
				Inputs: []conversation.ConversationInput{
					{
						Role: conversation.RoleUser,
						Parts: []conversation.ContentPart{
							conversation.TextContentPart{Text: "What's the weather like in San Francisco?"},
						},
					},
				},
			}

			step1Resp, err := conv.Converse(ctx, step1Req)
			require.NoError(t, err)
			require.NotEmpty(t, step1Resp.Outputs, "Should have at least one output")

			// Extract tool calls from the response
			var allStep1Parts []conversation.ContentPart
			for _, output := range step1Resp.Outputs {
				allStep1Parts = append(allStep1Parts, output.Parts...)
			}

			toolCalls := conversation.ExtractToolCallsFromParts(allStep1Parts)
			if len(toolCalls) == 0 {
				t.Skip("Component did not generate tool calls for weather request - skipping multi-turn tool calling test")
				return
			}

			require.GreaterOrEqual(t, len(toolCalls), 1, "Should have generated at least one tool call")
			weatherToolCall := toolCalls[0]
			t.Logf("Step 1 completed: AI generated tool call with ID: %s", weatherToolCall.ID)

			// Step 2: Provide tool result and ask follow-up question
			t.Log("Step 2: Providing tool result and asking follow-up")

			var step2Inputs []conversation.ConversationInput

			// Add the original user request
			step2Inputs = append(step2Inputs, step1Req.Inputs[0])

			// For Anthropic, we need to follow the exact pattern from langchaingo example
			// Create assistant message directly from the tool call, not from converted content parts
			if component == "anthropic" {
				// Create assistant message with tool call using the exact structure from langchaingo example
				step2Inputs = append(step2Inputs, conversation.ConversationInput{
					Role: conversation.RoleAssistant,
					Parts: []conversation.ContentPart{
						conversation.ToolCallContentPart{
							ID:       weatherToolCall.ID,
							CallType: weatherToolCall.CallType,
							Function: conversation.ToolCallFunction{
								Name:      weatherToolCall.Function.Name,
								Arguments: weatherToolCall.Function.Arguments,
							},
						},
					},
				})
			} else {
				// For other providers, use the extracted parts
				step2Inputs = append(step2Inputs, conversation.ConversationInput{
					Role:  conversation.RoleAssistant,
					Parts: allStep1Parts,
				})
			}

			// For all providers, provide formal tool result and add follow-up question
			step2Inputs = append(step2Inputs,
				conversation.ConversationInput{
					Role: conversation.RoleTool,
					Parts: []conversation.ContentPart{
						conversation.ToolResultContentPart{
							ToolCallID: weatherToolCall.ID,
							Name:       weatherToolCall.Function.Name,
							Content:    "Sunny, 72°F in San Francisco",
							IsError:    false,
						},
					},
				},
				conversation.ConversationInput{
					Role: conversation.RoleUser,
					Parts: []conversation.ContentPart{
						conversation.TextContentPart{Text: "What should I wear for this weather?"},
					},
				},
			)

			step2Req := &conversation.ConversationRequest{
				Tools: []conversation.Tool{
					{
						ToolType: "function",
						Function: conversation.ToolFunction{
							Name:        "get_weather",
							Description: "Get current weather for a location",
							Parameters: map[string]any{
								"type": "object",
								"properties": map[string]any{
									"location": map[string]any{
										"type":        "string",
										"description": "City name",
									},
								},
								"required": []string{"location"},
							},
						},
					},
				},
				Inputs: step2Inputs,
			}

			step2Resp, err := conv.Converse(ctx, step2Req)
			require.NoError(t, err)
			require.NotEmpty(t, step2Resp.Outputs, "Should have response to follow-up question")

			// Verify the response acknowledges both the weather and provides clothing advice
			var allStep2Parts []conversation.ContentPart
			for _, output := range step2Resp.Outputs {
				allStep2Parts = append(allStep2Parts, output.Parts...)
			}

			step2Text := strings.ToLower(conversation.ExtractTextFromParts(allStep2Parts))
			assert.NotEmpty(t, step2Text, "Should have text response")

			// The response should reference the weather information and provide clothing advice
			hasWeatherRef := strings.Contains(step2Text, "sunny") || strings.Contains(step2Text, "72") || strings.Contains(step2Text, "weather")
			hasClothingAdvice := strings.Contains(step2Text, "wear") || strings.Contains(step2Text, "clothing") || strings.Contains(step2Text, "shirt") || strings.Contains(step2Text, "jacket")

			assert.True(t, hasWeatherRef, "Response should reference the weather information")
			assert.True(t, hasClothingAdvice, "Response should provide clothing advice")

			t.Logf("Step 2 completed: Multi-turn tool calling successful")
			t.Logf("Final response: %s", step2Text)
		})

		t.Run("sophisticated multi-turn multi-tool", func(t *testing.T) {
			// Test sophisticated multi-turn conversation with multiple tools
			// This uses the same approach as the Anthropic-style tests but works with any provider
			ensureComponentInitialized()
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

			exchangeRateTool := conversation.Tool{
				ToolType: "function",
				Function: conversation.ToolFunction{
					Name:        "get_exchange_rate",
					Description: "Get current exchange rate between two currencies",
					Parameters: map[string]any{
						"type": "object",
						"properties": map[string]any{
							"from_currency": map[string]any{
								"type":        "string",
								"description": "Source currency code (e.g., USD)",
							},
							"to_currency": map[string]any{
								"type":        "string",
								"description": "Target currency code (e.g., EUR)",
							},
						},
						"required": []string{"from_currency", "to_currency"},
					},
				},
			}

			// ========== TURN 1: Request that should trigger multiple tools ==========
			t.Log("🔄 TURN 1: User asks for multiple pieces of information that require different tools")

			turn1Req := &conversation.ConversationRequest{
				Tools: []conversation.Tool{weatherTool, timeTool, exchangeRateTool},
				Inputs: []conversation.ConversationInput{
					{
						Role: conversation.RoleSystem,
						Parts: []conversation.ContentPart{
							conversation.TextContentPart{Text: "You are a helpful assistant with access to tools. " +
								"When the user requests information that requires using available tools, call all appropriate tools (for maximum efficiency, whenever you need to perform multiple independent operations, invoke all relevant tools simultaneously rather than sequentially) " +
								"Do not provide status updates like 'I'll check that for you' or 'Let me get that information'. " +
								"Only provide text responses when: 1) You need clarification from the user, 2) The request is ambiguous and needs more details, or 3) You're presenting the final results after tool execution. Prioritize action over commentary. "},
						},
					},
					{
						Role: conversation.RoleUser,
						Parts: []conversation.ContentPart{
							conversation.TextContentPart{Text: "I'm planning a trip to Tokyo. Can you get me the current weather in Tokyo, the current time there, and the USD to JPY exchange rate?"},
						},
					},
				},
			}

			turn1Resp, err := conv.Converse(ctx, turn1Req)
			require.NoError(t, err)
			require.NotEmpty(t, turn1Resp.Outputs)

			t.Logf("📤 Turn 1 Response: %d outputs", len(turn1Resp.Outputs))

			// Collect tool calls from ALL outputs (handle multiple output structures)
			var allTurn1ToolCalls []conversation.ToolCall
			var allTurn1Text strings.Builder

			for i, output := range turn1Resp.Outputs {
				t.Logf("📋 Output %d: %d parts", i+1, len(output.Parts))

				outputText := conversation.ExtractTextFromParts(output.Parts)
				outputToolCalls := conversation.ExtractToolCallsFromParts(output.Parts)

				if outputText != "" {
					allTurn1Text.WriteString(outputText)
					if i < len(turn1Resp.Outputs)-1 {
						allTurn1Text.WriteString(" ")
					}
				}

				allTurn1ToolCalls = append(allTurn1ToolCalls, outputToolCalls...)

				t.Logf("📝 Output %d Text: %q", i+1, outputText)
				t.Logf("🔧 Output %d Tool Calls: %d", i+1, len(outputToolCalls))
			}

			turn1Text := allTurn1Text.String()
			turn1ToolCalls := allTurn1ToolCalls

			t.Logf("📤 Combined Turn 1 Response: %q", turn1Text)
			t.Logf("🔧 Total Turn 1 Tool Calls: %d", len(turn1ToolCalls))

			for i, toolCall := range turn1ToolCalls {
				t.Logf("🛠️  Tool Call %d: %s(%s)", i+1, toolCall.Function.Name, toolCall.Function.Arguments)
			}

			// Track conversation history (handle each output separately like Anthropic does)
			conversationHistory := []conversation.ConversationInput{
				// User's question
				{
					Role: conversation.RoleUser,
					Parts: []conversation.ContentPart{
						conversation.TextContentPart{Text: "I'm planning a trip to Tokyo. Can you get me the current weather in Tokyo, the current time there, and the USD to JPY exchange rate?"},
					},
				},
			}

			// Add each output separately to conversation history (sophisticated approach)
			for i, output := range turn1Resp.Outputs {
				t.Logf("🔄 Adding assistant output %d with %d parts to conversation history", i+1, len(output.Parts))
				conversationHistory = append(conversationHistory, conversation.ConversationInput{
					Role:  conversation.RoleAssistant,
					Parts: output.Parts, // Each output separately
				})
			}

			// If tools were called, process them
			if len(turn1ToolCalls) > 0 {
				t.Logf("✅ Provider called %d tool(s) in Turn 1!", len(turn1ToolCalls))

				// Add tool results for each tool call
				for _, toolCall := range turn1ToolCalls {
					var toolResult string
					switch toolCall.Function.Name {
					case "get_weather":
						toolResult = `{"temperature": 22, "condition": "sunny", "humidity": 60, "location": "Tokyo, Japan"}`
					case "get_time":
						toolResult = `{"time": "14:30", "timezone": "JST", "date": "2025-01-13", "location": "Tokyo, Japan"}`
					case "get_exchange_rate":
						toolResult = `{"rate": 157.25, "from": "USD", "to": "JPY", "timestamp": "2025-01-13T14:30:00Z"}`
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
					Tools:  []conversation.Tool{weatherTool, timeTool, exchangeRateTool},
					Inputs: conversationHistory,
				}
				turn2Resp, err2 := conv.Converse(ctx, turn2Req)
				require.NoError(t, err2)

				// Process Turn 2 outputs using sophisticated approach
				var allTurn2ToolCalls []conversation.ToolCall
				var allTurn2Text strings.Builder

				for i, output := range turn2Resp.Outputs {
					outputText := conversation.ExtractTextFromParts(output.Parts)
					outputToolCalls := conversation.ExtractToolCallsFromParts(output.Parts)

					if outputText != "" {
						allTurn2Text.WriteString(outputText)
						if i < len(turn2Resp.Outputs)-1 {
							allTurn2Text.WriteString(" ")
						}
					}

					allTurn2ToolCalls = append(allTurn2ToolCalls, outputToolCalls...)

					t.Logf("📝 Turn 2 Output %d Text: %q", i+1, outputText)
					t.Logf("🔧 Turn 2 Output %d Tool Calls: %d", i+1, len(outputToolCalls))

					// Add each assistant output separately to conversation history
					conversationHistory = append(conversationHistory, conversation.ConversationInput{
						Role:  conversation.RoleAssistant,
						Parts: output.Parts,
					})
				}

				turn2Text := allTurn2Text.String()
				turn2ToolCalls := allTurn2ToolCalls

				t.Logf("📤 Combined Turn 2 Response: %q", turn2Text)
				t.Logf("🔧 Total Turn 2 Tool Calls: %d", len(turn2ToolCalls))

				// Check if provider made additional tool calls (sequential behavior)
				if len(turn2ToolCalls) > 0 {
					t.Log("🔄 Provider made additional tool calls - continuing sequential pattern")

					// Process additional tool calls (same pattern)
					for _, toolCall := range turn2ToolCalls {
						var toolResult string
						switch toolCall.Function.Name {
						case "get_weather":
							toolResult = `{"temperature": 22, "condition": "sunny", "humidity": 60, "location": "Tokyo, Japan"}`
						case "get_time":
							toolResult = `{"time": "14:30", "timezone": "JST", "date": "2025-01-13", "location": "Tokyo, Japan"}`
						case "get_exchange_rate":
							toolResult = `{"rate": 157.25, "from": "USD", "to": "JPY", "timestamp": "2025-01-13T14:30:00Z"}`
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

					// ========== TURN 3: Process additional tool results ==========
					t.Log("🔄 TURN 3: Process additional tool results")
					turn3Req := &conversation.ConversationRequest{Inputs: conversationHistory}
					turn3Resp, err3 := conv.Converse(ctx, turn3Req)
					require.NoError(t, err3)

					turn3Text := conversation.ExtractTextFromParts(turn3Resp.Outputs[0].Parts)
					t.Logf("📤 Turn 3 Response: %q", turn3Text)

					// Validate comprehensive response
					assert.NotEmpty(t, turn3Text, "Should provide comprehensive response using all tool results")

					t.Logf("✅ Provider sequential multi-tool test completed with %d + %d tool calls", len(turn1ToolCalls), len(turn2ToolCalls))
				} else {
					// No additional tool calls - validate tool result processing
					if len(turn1ToolCalls) >= 2 {
						// Look for evidence that multiple tool results were used
						hasWeatherData := strings.Contains(turn2Text, "22") || strings.Contains(turn2Text, "sunny") || strings.Contains(turn2Text, "temperature")
						hasTimeData := strings.Contains(turn2Text, "14:30") || strings.Contains(turn2Text, "JST") || strings.Contains(turn2Text, "time")
						hasExchangeData := strings.Contains(turn2Text, "157") || strings.Contains(turn2Text, "JPY") || strings.Contains(turn2Text, "rate")

						toolsUsedCount := 0
						if hasWeatherData {
							toolsUsedCount++
						}
						if hasTimeData {
							toolsUsedCount++
						}
						if hasExchangeData {
							toolsUsedCount++
						}

						t.Logf("🎯 Provider integrated %d/%d tool results in response", toolsUsedCount, len(turn1ToolCalls))

						if toolsUsedCount >= 2 {
							t.Log("🎯 SUCCESS: Provider response incorporates data from multiple tool calls!")
						}
					}

					assert.NotEmpty(t, turn2Text, "Should provide response using tool results")
					t.Logf("✅ Provider parallel multi-tool test completed with %d tool calls", len(turn1ToolCalls))
				}

				return
			}

			// If provider didn't call tools in Turn 1, test follow-up approach
			t.Log("⚠️ Provider didn't call tools in Turn 1 - testing follow-up approach")

			// Add follow-up to encourage tool usage
			conversationHistory = append(conversationHistory, conversation.ConversationInput{
				Role: conversation.RoleUser,
				Parts: []conversation.ContentPart{
					conversation.TextContentPart{Text: "Please get that information for me using the available tools."},
				},
			})

			turn2Req := &conversation.ConversationRequest{Inputs: conversationHistory}
			turn2Resp, err := conv.Converse(ctx, turn2Req)
			require.NoError(t, err)

			turn2ToolCalls := conversation.ExtractToolCallsFromParts(turn2Resp.Outputs[0].Parts)
			turn2Text := conversation.ExtractTextFromParts(turn2Resp.Outputs[0].Parts)

			t.Logf("📤 Turn 2 Response: %q", turn2Text)
			t.Logf("🔧 Turn 2 Tool Calls: %d", len(turn2ToolCalls))

			if len(turn2ToolCalls) > 0 {
				t.Logf("🎯 Provider called %d tool(s) in Turn 2 after follow-up!", len(turn2ToolCalls))
				t.Log("✅ Provider responsive tool calling validated!")
			} else {
				t.Log("⚠️ Provider still not calling tools - conservative behavior")
				// Validate conversation memory at least
				assert.Contains(t, turn2Text, "Tokyo", "Should remember the location from previous turn")
			}

			t.Log("✅ Sophisticated multi-tool calling test completed")
		})

		t.Run("sophisticated streaming multi-turn multi-tool", func(t *testing.T) {
			// Test sophisticated streaming multi-turn conversation with multiple tools
			// Check if component supports streaming
			streamingConv, supportsStreaming := conv.(conversation.StreamingConversation)
			if !supportsStreaming {
				t.Skip("Component does not support streaming")
				return
			}

			// Skip Anthropic streaming tool calling due to langchaingo issues
			if component == "anthropic" {
				t.Skip("Skipping Anthropic streaming tool calling: known issues with langchaingo streaming and tool calls")
				return
			}

			// Skip Huggingface streaming due to model limitations
			if component == "huggingface" {
				t.Skip("Skipping Huggingface streaming: model does not support streaming")
				return
			}

			// Skip Mistral streaming multi-tool due to API limitations with tool call/response matching
			if component == "mistral" {
				t.Skip("Skipping Mistral streaming multi-tool: API requires exact tool call/response matching")
				return
			}

			ensureComponentInitialized()
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

			exchangeRateTool := conversation.Tool{
				ToolType: "function",
				Function: conversation.ToolFunction{
					Name:        "get_exchange_rate",
					Description: "Get current exchange rate between two currencies",
					Parameters: map[string]any{
						"type": "object",
						"properties": map[string]any{
							"from_currency": map[string]any{
								"type":        "string",
								"description": "Source currency code (e.g., USD)",
							},
							"to_currency": map[string]any{
								"type":        "string",
								"description": "Target currency code (e.g., EUR)",
							},
						},
						"required": []string{"from_currency", "to_currency"},
					},
				},
			}

			// ========== TURN 1: Request that should trigger multiple tools (STREAMING) ==========
			t.Log("🔄 TURN 1: User asks for multiple pieces of information (Streaming)")

			turn1Req := &conversation.ConversationRequest{
				Tools: []conversation.Tool{weatherTool, timeTool, exchangeRateTool},
				Inputs: []conversation.ConversationInput{
					{
						Role: conversation.RoleSystem,
						Parts: []conversation.ContentPart{
							conversation.TextContentPart{Text: "You are a helpful assistant with access to tools. " +
								"When the user requests information that requires using available tools, call all appropriate tools (for maximum efficiency, whenever you need to perform multiple independent operations, invoke all relevant tools simultaneously rather than sequentially) " +
								"Do not provide status updates like 'I'll check that for you' or 'Let me get that information'. " +
								"Only provide text responses when: 1) You need clarification from the user, 2) The request is ambiguous and needs more details, or 3) You're presenting the final results after tool execution. Prioritize action over commentary. "},
						},
					},
					{
						Role: conversation.RoleUser,
						Parts: []conversation.ContentPart{
							conversation.TextContentPart{Text: "I'm planning a trip to Tokyo. Can you get me the current weather in Tokyo, the current time there, and the USD to JPY exchange rate?"},
						},
					},
				},
			}

			// Collect streaming chunks
			var turn1Chunks [][]byte
			turn1StreamFunc := func(ctx context.Context, chunk []byte) error {
				turn1Chunks = append(turn1Chunks, chunk)
				t.Logf("📦 Turn 1 Chunk: %s", string(chunk))
				return nil
			}

			turn1Resp, err := streamingConv.ConverseStream(ctx, turn1Req, turn1StreamFunc)
			require.NoError(t, err)
			require.NotEmpty(t, turn1Resp.Outputs)

			t.Logf("📤 Turn 1 Streaming Response: %d outputs, %d chunks received", len(turn1Resp.Outputs), len(turn1Chunks))

			// Collect tool calls from ALL outputs (handle multiple output structures)
			var allTurn1ToolCalls []conversation.ToolCall
			var allTurn1Text strings.Builder

			for i, output := range turn1Resp.Outputs {
				t.Logf("📋 Output %d: %d parts", i+1, len(output.Parts))

				outputText := conversation.ExtractTextFromParts(output.Parts)
				outputToolCalls := conversation.ExtractToolCallsFromParts(output.Parts)

				if outputText != "" {
					allTurn1Text.WriteString(outputText)
					if i < len(turn1Resp.Outputs)-1 {
						allTurn1Text.WriteString(" ")
					}
				}

				allTurn1ToolCalls = append(allTurn1ToolCalls, outputToolCalls...)

				t.Logf("📝 Output %d Text: %q", i+1, outputText)
				t.Logf("🔧 Output %d Tool Calls: %d", i+1, len(outputToolCalls))
			}

			turn1Text := allTurn1Text.String()
			turn1ToolCalls := allTurn1ToolCalls

			t.Logf("📤 Combined Turn 1 Response: %q", turn1Text)
			t.Logf("🔧 Total Turn 1 Tool Calls: %d", len(turn1ToolCalls))

			for i, toolCall := range turn1ToolCalls {
				t.Logf("🛠️  Tool Call %d: %s(%s)", i+1, toolCall.Function.Name, toolCall.Function.Arguments)
			}

			// Verify streaming behavior: only require chunks if there's text content
			// Tool calls are often returned as complete structures without streaming
			if turn1Text != "" && len(turn1ToolCalls) == 0 {
				// Only text content - should have streaming chunks
				assert.NotEmpty(t, turn1Chunks, "Should receive streaming chunks for text responses")
			} else if len(turn1ToolCalls) > 0 && turn1Text == "" {
				// Only tool calls - streaming is optional/provider-dependent
				t.Logf("ℹ️ Turn 1 generated tool calls without text - streaming chunks optional (provider-dependent)")
			} else if len(turn1ToolCalls) > 0 && turn1Text != "" {
				// Both text and tool calls - should have some chunks
				assert.NotEmpty(t, turn1Chunks, "Should receive streaming chunks when generating both text and tool calls")
			}

			// Track conversation history (handle each output separately)
			conversationHistory := []conversation.ConversationInput{
				// User's question
				{
					Role: conversation.RoleUser,
					Parts: []conversation.ContentPart{
						conversation.TextContentPart{Text: "I'm planning a trip to Tokyo. Can you get me the current weather in Tokyo, the current time there, and the USD to JPY exchange rate?"},
					},
				},
			}

			// Add each output separately to conversation history
			for i, output := range turn1Resp.Outputs {
				t.Logf("🔄 Adding assistant output %d with %d parts to conversation history", i+1, len(output.Parts))
				conversationHistory = append(conversationHistory, conversation.ConversationInput{
					Role:  conversation.RoleAssistant,
					Parts: output.Parts, // Each output separately
				})
			}

			// If tools were called, process them with streaming
			if len(turn1ToolCalls) > 0 {
				t.Logf("✅ Provider called %d tool(s) in Turn 1 (Streaming)!", len(turn1ToolCalls))

				// Add tool results for each tool call
				for _, toolCall := range turn1ToolCalls {
					var toolResult string
					switch toolCall.Function.Name {
					case "get_weather":
						toolResult = `{"temperature": 22, "condition": "sunny", "humidity": 60, "location": "Tokyo, Japan"}`
					case "get_time":
						toolResult = `{"time": "14:30", "timezone": "JST", "date": "2025-01-13", "location": "Tokyo, Japan"}`
					case "get_exchange_rate":
						toolResult = `{"rate": 157.25, "from": "USD", "to": "JPY", "timestamp": "2025-01-13T14:30:00Z"}`
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

				// ========== TURN 2: Process tool results (STREAMING) ==========
				t.Log("🔄 TURN 2: Process tool results (Streaming)")
				turn2Req := &conversation.ConversationRequest{
					Tools:  []conversation.Tool{weatherTool, timeTool, exchangeRateTool},
					Inputs: conversationHistory,
				}

				// Collect Turn 2 streaming chunks
				var turn2Chunks [][]byte
				turn2StreamFunc := func(ctx context.Context, chunk []byte) error {
					turn2Chunks = append(turn2Chunks, chunk)
					t.Logf("📦 Turn 2 Chunk: %s", string(chunk))
					return nil
				}

				turn2Resp, err2 := streamingConv.ConverseStream(ctx, turn2Req, turn2StreamFunc)
				require.NoError(t, err2)

				t.Logf("📤 Turn 2 Streaming Response: %d outputs, %d chunks received", len(turn2Resp.Outputs), len(turn2Chunks))

				// Verify streaming chunks were received
				assert.NotEmpty(t, turn2Chunks, "Should receive streaming chunks in Turn 2")

				// Process Turn 2 outputs
				var allTurn2Text strings.Builder
				for i, output := range turn2Resp.Outputs {
					outputText := conversation.ExtractTextFromParts(output.Parts)
					if outputText != "" {
						allTurn2Text.WriteString(outputText)
						if i < len(turn2Resp.Outputs)-1 {
							allTurn2Text.WriteString(" ")
						}
					}
					t.Logf("📝 Turn 2 Output %d Text: %q", i+1, outputText)
				}

				turn2Text := allTurn2Text.String()
				t.Logf("📤 Combined Turn 2 Response: %q", turn2Text)

				// Validate tool result processing in streaming mode
				if len(turn1ToolCalls) >= 2 {
					// Look for evidence that multiple tool results were used
					hasWeatherData := strings.Contains(turn2Text, "22") || strings.Contains(turn2Text, "sunny") || strings.Contains(turn2Text, "temperature")
					hasTimeData := strings.Contains(turn2Text, "14:30") || strings.Contains(turn2Text, "JST") || strings.Contains(turn2Text, "time")
					hasExchangeData := strings.Contains(turn2Text, "157") || strings.Contains(turn2Text, "JPY") || strings.Contains(turn2Text, "rate")

					toolsUsedCount := 0
					if hasWeatherData {
						toolsUsedCount++
					}
					if hasTimeData {
						toolsUsedCount++
					}
					if hasExchangeData {
						toolsUsedCount++
					}

					t.Logf("🎯 Provider integrated %d/%d tool results in streaming response", toolsUsedCount, len(turn1ToolCalls))

					if toolsUsedCount >= 2 {
						t.Log("🎯 SUCCESS: Provider streaming response incorporates data from multiple tool calls!")
					}
				}

				assert.NotEmpty(t, turn2Text, "Should provide response using tool results in streaming mode")
				t.Logf("✅ Provider streaming multi-tool test completed with %d tool calls", len(turn1ToolCalls))

				return
			}

			// If provider didn't call tools in Turn 1, test follow-up approach with streaming
			t.Log("⚠️ Provider didn't call tools in Turn 1 (Streaming) - testing follow-up approach")

			// Add follow-up to encourage tool usage
			conversationHistory = append(conversationHistory, conversation.ConversationInput{
				Role: conversation.RoleUser,
				Parts: []conversation.ContentPart{
					conversation.TextContentPart{Text: "Please get that information for me using the available tools."},
				},
			})

			turn2Req := &conversation.ConversationRequest{Inputs: conversationHistory}

			// Collect Turn 2 streaming chunks
			var turn2Chunks [][]byte
			turn2StreamFunc := func(ctx context.Context, chunk []byte) error {
				turn2Chunks = append(turn2Chunks, chunk)
				return nil
			}

			turn2Resp, err := streamingConv.ConverseStream(ctx, turn2Req, turn2StreamFunc)
			require.NoError(t, err)

			turn2ToolCalls := conversation.ExtractToolCallsFromParts(turn2Resp.Outputs[0].Parts)
			turn2Text := conversation.ExtractTextFromParts(turn2Resp.Outputs[0].Parts)

			t.Logf("📤 Turn 2 Response (Streaming): %q", turn2Text)
			t.Logf("🔧 Turn 2 Tool Calls (Streaming): %d", len(turn2ToolCalls))
			t.Logf("📦 Turn 2 Chunks received: %d", len(turn2Chunks))

			// Verify streaming chunks were received
			assert.NotEmpty(t, turn2Chunks, "Should receive streaming chunks in Turn 2")

			if len(turn2ToolCalls) > 0 {
				t.Logf("🎯 Provider called %d tool(s) in Turn 2 after follow-up (Streaming)!", len(turn2ToolCalls))
				t.Log("✅ Provider responsive streaming tool calling validated!")
			} else {
				t.Log("⚠️ Provider still not calling tools in streaming mode - conservative behavior")
				// Validate conversation memory at least
				assert.Contains(t, turn2Text, "Tokyo", "Should remember the location from previous turn")
			}

			t.Log("✅ Sophisticated streaming multi-tool calling test completed")
		})
	})

	t.Run("streaming", func(t *testing.T) {
		// Check if component supports streaming
		streamingConv, supportsStreaming := conv.(conversation.StreamingConversation)
		if !supportsStreaming {
			t.Skip("Component does not support streaming")
			return
		}

		t.Run("basic streaming functionality", func(t *testing.T) {
			ensureComponentInitialized()
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
			defer cancel()

			req := &conversation.ConversationRequest{
				Inputs: []conversation.ConversationInput{
					{
						Message: "Tell me a short story",
					},
				},
			}

			var chunks [][]byte
			streamFunc := func(ctx context.Context, chunk []byte) error {
				chunks = append(chunks, chunk)
				return nil
			}

			resp, err := streamingConv.ConverseStream(ctx, req, streamFunc)

			// Check if streaming is actually supported (some components implement the interface but disable streaming)
			if err != nil && strings.Contains(err.Error(), "streaming is not supported") {
				t.Skipf("Skipping streaming test: %v", err)
				return
			}

			require.NoError(t, err)
			assert.Len(t, resp.Outputs, 1)
			assert.NotEmpty(t, resp.Outputs[0].Result) //nolint:staticcheck

			// Should have received streaming chunks
			assert.NotEmpty(t, chunks, "Should have received streaming chunks")
		})

		t.Run("streaming with content parts", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
			defer cancel()

			req := &conversation.ConversationRequest{
				Inputs: []conversation.ConversationInput{
					{
						Role: conversation.RoleUser,
						Parts: []conversation.ContentPart{
							conversation.TextContentPart{Text: "Count to five"},
						},
					},
				},
			}

			var chunks [][]byte
			streamFunc := func(ctx context.Context, chunk []byte) error {
				chunks = append(chunks, chunk)
				return nil
			}

			resp, err := streamingConv.ConverseStream(ctx, req, streamFunc)

			// Check if streaming is actually supported (some components implement the interface but disable streaming)
			if err != nil && strings.Contains(err.Error(), "streaming is not supported") {
				t.Skipf("Skipping streaming test: %v", err)
				return
			}

			require.NoError(t, err)
			assert.Len(t, resp.Outputs, 1)
			assert.NotEmpty(t, resp.Outputs[0].Parts, "Should have content parts in response")
			assert.NotEmpty(t, resp.Outputs[0].Result, "Should have legacy result field") //nolint:staticcheck

			// Should have received streaming chunks
			assert.NotEmpty(t, chunks, "Should have received streaming chunks")
		})
	})
}
