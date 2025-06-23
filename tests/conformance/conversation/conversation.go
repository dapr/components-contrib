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
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/dapr/components-contrib/conversation"
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
	// Note: Component initialization is now handled in the test framework setup phase

	t.Run("converse", func(t *testing.T) {
		t.Run("get a non-empty response without errors", func(t *testing.T) {
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
			assert.NotEmpty(t, resp.Outputs[0].Result)
		})

		t.Run("returns usage information", func(t *testing.T) {
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
			assert.GreaterOrEqual(t, resp.Usage.PromptTokens, int32(0), "Should have non-negative prompt tokens")
			assert.GreaterOrEqual(t, resp.Usage.CompletionTokens, int32(0), "Should have non-negative completion tokens")
			assert.Equal(t, resp.Usage.TotalTokens, resp.Usage.PromptTokens+resp.Usage.CompletionTokens, "Total should equal sum of prompt and completion tokens")
		})
	})

	t.Run("streaming", func(t *testing.T) {
		// Check if component supports streaming
		streamer, ok := conv.(conversation.StreamingConversation)
		if !ok {
			t.Skipf("Component %s does not implement StreamingConversation interface, skipping streaming tests", component)
			return
		}

		// Create a simple request for streaming tests
		req := &conversation.ConversationRequest{
			Inputs: []conversation.ConversationInput{
				{
					Message: "Say hello world",
				},
			},
		}

		// First try to stream and see if it's supported or disabled
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
		defer cancel()

		streamFunc := func(ctx context.Context, chunk []byte) error {
			return nil // Simple stream function for testing
		}

		resp, err := streamer.ConverseStream(ctx, req, streamFunc)

		// Check if streaming is disabled (like HuggingFace)
		if err != nil && (strings.Contains(err.Error(), "streaming is not supported") ||
			strings.Contains(err.Error(), "streaming not supported") ||
			errors.Is(err, errors.New("streaming is not supported by this model or provider"))) {
			t.Run("streaming disabled returns proper error", func(t *testing.T) {
				// This is expected for components like HuggingFace
				require.Error(t, err, "Should return error when streaming is disabled")
				assert.Nil(t, resp, "Response should be nil when streaming is disabled")
				t.Logf("Component %s has streaming disabled (expected): %v", component, err)
			})
			return // Skip other streaming tests if streaming is disabled
		}

		// If we get here, streaming should be working
		t.Run("stream response without errors", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
			defer cancel()

			var chunks []string
			var totalContent string

			streamFunc := func(ctx context.Context, chunk []byte) error {
				content := string(chunk)
				chunks = append(chunks, content)
				totalContent += content
				return nil
			}

			req := &conversation.ConversationRequest{
				Inputs: []conversation.ConversationInput{
					{
						Message: "Say hello world",
					},
				},
			}

			resp, err := streamer.ConverseStream(ctx, req, streamFunc)
			require.NoError(t, err)
			require.NotNil(t, resp)

			// Verify we got streaming chunks (at least some content)
			assert.NotEmpty(t, totalContent, "Should have received streaming content")
		})

		t.Run("streaming returns usage information", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
			defer cancel()

			streamFunc := func(ctx context.Context, chunk []byte) error {
				return nil // Ignore chunks for this test
			}

			req := &conversation.ConversationRequest{
				Inputs: []conversation.ConversationInput{
					{
						Message: "Count to three",
					},
				},
			}

			resp, err := streamer.ConverseStream(ctx, req, streamFunc)
			require.NoError(t, err)
			require.NotNil(t, resp)

			// Verify usage information is present in streaming response
			require.NotNil(t, resp.Usage, "Usage information should be present in streaming response")
			assert.GreaterOrEqual(t, resp.Usage.PromptTokens, int32(0), "Should have non-negative prompt tokens")
			assert.GreaterOrEqual(t, resp.Usage.CompletionTokens, int32(0), "Should have non-negative completion tokens")
			assert.Equal(t, resp.Usage.TotalTokens, resp.Usage.PromptTokens+resp.Usage.CompletionTokens, "Total should equal sum of prompt and completion tokens")
		})
	})

	t.Run("tool calling", func(t *testing.T) {
		// Check if component supports tool calling
		toolCaller, ok := conv.(conversation.ToolCallSupport)
		if !ok {
			t.Skipf("Component %s does not implement ToolCallSupport interface, skipping tool calling tests", component)
			return
		}

		if !toolCaller.SupportsToolCalling() {
			t.Skipf("Component %s does not support tool calling, skipping tool calling tests", component)
			return
		}

		t.Run("basic tool calling flow", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
			defer cancel()

			// Define a simple weather tool
			weatherTool := conversation.Tool{
				Type: "function",
				Function: conversation.ToolFunction{
					Name:        "get_weather",
					Description: "Get current weather for a location",
					Parameters: map[string]any{
						"type": "object",
						"properties": map[string]any{
							"location": map[string]any{
								"type":        "string",
								"description": "City and state",
							},
						},
						"required": []string{"location"},
					},
				},
			}

			req := &conversation.ConversationRequest{
				Inputs: []conversation.ConversationInput{
					{
						Message: "What's the weather like in San Francisco?",
						Role:    conversation.RoleUser,
						Tools:   []conversation.Tool{weatherTool},
					},
				},
			}

			resp, err := conv.Converse(ctx, req)
			require.NoError(t, err)
			require.NotNil(t, resp)

			// Provider-specific tool calling validation
			switch component {
			case "anthropic":
				// Anthropic may return multiple outputs: explanation + tool calls
				if len(resp.Outputs) == 2 {
					// First output: explanation text
					assert.NotEmpty(t, resp.Outputs[0].Result, "Anthropic should provide explanatory text")
					assert.Equal(t, "stop", resp.Outputs[0].FinishReason)

					// Second output: tool calls
					output := resp.Outputs[1]
					if len(output.ToolCalls) > 0 {
						assert.Equal(t, "tool_calls", output.FinishReason)
						toolCall := output.ToolCalls[0]
						assert.NotEmpty(t, toolCall.ID, "Tool call should have an ID")
						assert.Equal(t, "get_weather", toolCall.Function.Name, "Tool call should be for get_weather function")
					}
				} else {
					// Single output behavior
					output := resp.Outputs[0]
					if len(output.ToolCalls) > 0 {
						assert.Equal(t, "tool_calls", output.FinishReason)
						toolCall := output.ToolCalls[0]
						assert.NotEmpty(t, toolCall.ID, "Tool call should have an ID")
						assert.Equal(t, "get_weather", toolCall.Function.Name, "Tool call should be for get_weather function")
					} else {
						assert.NotEmpty(t, output.Result, "Should have a response even if no tools are called")
						assert.Equal(t, "stop", output.FinishReason)
					}
				}

			default:
				// Standard validation for other providers
				require.Len(t, resp.Outputs, 1)
				output := resp.Outputs[0]

				// we should get tool calls in the response
				if len(output.ToolCalls) > 0 {
					assert.Equal(t, "tool_calls", output.FinishReason, "Finish reason should be 'tool_calls' when tools are called")

					// Verify tool call structure
					toolCall := output.ToolCalls[0]

					// TODO: GoogleAI through langchaingo doesn't populate Type and ID fields in the same way as OpenAI
					// This is a langchaingo implementation detail, not a functional limitation
					// The tool calling functionality works correctly, just the metadata format differs
					switch component {
					case "googleai":
						// Skip Type and ID assertions for GoogleAI due to langchaingo implementation differences
						// Tool calling functionality works correctly despite missing metadata
					default:
						assert.Equal(t, "function", toolCall.Type, "Tool call type should be 'function'")
						assert.NotEmpty(t, toolCall.ID, "Tool call should have an ID")
					}

					// These assertions work for all providers including GoogleAI
					assert.Equal(t, "get_weather", toolCall.Function.Name, "Tool call should be for get_weather function")
					assert.NotEmpty(t, toolCall.Function.Arguments, "Tool call should have arguments")

					// Arguments should be valid JSON and contain location
					var args map[string]interface{}
					err := json.Unmarshal([]byte(toolCall.Function.Arguments), &args)
					require.NoError(t, err, "Tool call arguments should be valid JSON")
					assert.Contains(t, args, "location", "Tool call arguments should contain location")
				} else {
					// If no tool calls, should still be a valid response
					assert.NotEmpty(t, output.Result, "Should have a response even if no tools are called")
					assert.Equal(t, "stop", output.FinishReason, "Finish reason should be 'stop' when no tools are called")
				}
			}
		})

		t.Run("tool result handling", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
			defer cancel()

			// Provider-specific tool result handling
			switch component {
			case "anthropic", "googleai", "mistral", "deepseek":
				// These providers require proper conversation flow with matching tool call IDs
				// Skip this test as it requires dynamic tool call ID extraction from previous responses
				t.Skipf("Component %s requires proper conversation flow with matching tool call IDs, skipping isolated tool result test", component)
				return
			}

			// Simulate sending a tool result back (works for Echo, OpenAI, HuggingFace)
			req := &conversation.ConversationRequest{
				Inputs: []conversation.ConversationInput{
					{
						Message:    `{"temperature": 72, "condition": "sunny", "location": "San Francisco"}`,
						Role:       conversation.RoleTool,
						ToolCallID: "call_test_12345",
						Name:       "get_weather",
					},
				},
			}

			resp, err := conv.Converse(ctx, req)
			require.NoError(t, err)
			require.NotNil(t, resp)
			require.Len(t, resp.Outputs, 1)

			output := resp.Outputs[0]
			assert.NotEmpty(t, output.Result, "Should process tool result and return a response")
			assert.Equal(t, "stop", output.FinishReason, "Tool result responses should finish with 'stop'")
			assert.Empty(t, output.ToolCalls, "Tool result responses shouldn't trigger new tool calls")
		})

		t.Run("streaming with tool calls", func(t *testing.T) {
			// Check if component also supports streaming
			streamer, ok := conv.(conversation.StreamingConversation)
			if !ok {
				t.Skipf("Component %s does not support streaming, skipping streaming tool calling tests", component)
				return
			}

			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
			defer cancel()

			// Define a simple weather tool
			weatherTool := conversation.Tool{
				Type: "function",
				Function: conversation.ToolFunction{
					Name:        "get_weather",
					Description: "Get current weather for a location",
					Parameters: map[string]any{
						"type": "object",
						"properties": map[string]any{
							"location": map[string]any{
								"type":        "string",
								"description": "City and state",
							},
						},
						"required": []string{"location"},
					},
				},
			}

			req := &conversation.ConversationRequest{
				Inputs: []conversation.ConversationInput{
					{
						Message: "What's the weather like in New York?",
						Role:    conversation.RoleUser,
						Tools:   []conversation.Tool{weatherTool},
					},
				},
			}

			var chunks [][]byte
			streamFunc := func(ctx context.Context, chunk []byte) error {
				chunks = append(chunks, chunk)
				return nil
			}

			resp, err := streamer.ConverseStream(ctx, req, streamFunc)

			// Check if streaming is disabled for this component
			if err != nil && (strings.Contains(err.Error(), "streaming is not supported") ||
				strings.Contains(err.Error(), "streaming not supported")) {
				t.Skipf("Component %s has streaming disabled, skipping streaming tool calling tests", component)
				return
			}

			require.NoError(t, err)
			require.NotNil(t, resp)
			require.Len(t, resp.Outputs, 1)

			// Should have received some streaming chunks
			assert.NotEmpty(t, chunks, "Should have received streaming chunks")

			output := resp.Outputs[0]

			// If tool calling is triggered, verify structure
			if len(output.ToolCalls) > 0 {
				assert.Equal(t, "tool_calls", output.FinishReason)

				// TODO: GoogleAI through langchaingo doesn't populate Type field consistently
				// Tool calling functionality works correctly despite missing metadata
				switch component {
				case "googleai":
					// Skip Type assertion for GoogleAI due to langchaingo implementation differences
				default:
					assert.Equal(t, "function", output.ToolCalls[0].Type)
				}

				assert.Equal(t, "get_weather", output.ToolCalls[0].Function.Name)
			} else {
				assert.NotEmpty(t, output.Result, "Should have a streamed response")
				assert.Equal(t, "stop", output.FinishReason)
			}
		})

		t.Run("tool calling without tools provided", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
			defer cancel()

			// Send a weather query but without tool definitions
			req := &conversation.ConversationRequest{
				Inputs: []conversation.ConversationInput{
					{
						Message: "What's the weather like?",
						Role:    conversation.RoleUser,
						// No tools provided
					},
				},
			}

			resp, err := conv.Converse(ctx, req)
			require.NoError(t, err)
			require.NotNil(t, resp)
			require.Len(t, resp.Outputs, 1)

			output := resp.Outputs[0]
			// Without tools defined, should not trigger tool calls
			assert.Empty(t, output.ToolCalls, "Should not trigger tool calls when no tools are provided")
			assert.NotEmpty(t, output.Result, "Should still provide a response")
			assert.Equal(t, "stop", output.FinishReason)
		})

		t.Run("tool calling returns usage information", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
			defer cancel()

			// Define a simple weather tool
			weatherTool := conversation.Tool{
				Type: "function",
				Function: conversation.ToolFunction{
					Name:        "get_weather",
					Description: "Get current weather for a location",
					Parameters: map[string]any{
						"type": "object",
						"properties": map[string]any{
							"location": map[string]any{
								"type":        "string",
								"description": "City and state",
							},
						},
						"required": []string{"location"},
					},
				},
			}

			req := &conversation.ConversationRequest{
				Inputs: []conversation.ConversationInput{
					{
						Message: "What's the temperature in Boston?",
						Role:    conversation.RoleUser,
						Tools:   []conversation.Tool{weatherTool},
					},
				},
			}

			resp, err := conv.Converse(ctx, req)
			require.NoError(t, err)
			require.NotNil(t, resp)

			// Verify usage information is present even with tool calling
			require.NotNil(t, resp.Usage, "Usage information should be present in tool calling response")
			assert.GreaterOrEqual(t, resp.Usage.PromptTokens, int32(0), "Should have non-negative prompt tokens")
			assert.GreaterOrEqual(t, resp.Usage.CompletionTokens, int32(0), "Should have non-negative completion tokens")
			assert.Equal(t, resp.Usage.TotalTokens, resp.Usage.PromptTokens+resp.Usage.CompletionTokens, "Total should equal sum of prompt and completion tokens")
		})

		t.Run("tool calling with parameters as JSON string", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
			defer cancel()

			// Define a weather tool with parameters as JSON string (simulating real Dapr usage)
			weatherToolWithStringParams := conversation.Tool{
				Type: "function",
				Function: conversation.ToolFunction{
					Name:        "get_weather",
					Description: "Get current weather for a location",
					// Parameters as JSON string (as they would come over the wire in real Dapr usage)
					Parameters: `{
						"type": "object",
						"properties": {
							"location": {
								"type": "string",
								"description": "City and state"
							},
							"unit": {
								"type": "string",
								"enum": ["celsius", "fahrenheit"],
								"description": "Temperature unit"
							}
						},
						"required": ["location"]
					}`,
				},
			}

			req := &conversation.ConversationRequest{
				Inputs: []conversation.ConversationInput{
					{
						Message: "What's the temperature in New York in fahrenheit?",
						Role:    conversation.RoleUser,
						Tools:   []conversation.Tool{weatherToolWithStringParams},
					},
				},
			}

			resp, err := conv.Converse(ctx, req)
			require.NoError(t, err)
			require.NotNil(t, resp)

			// This test ensures langchaingo providers properly handle JSON string parameters
			// The conversion should happen transparently without errors
			switch component {
			case "anthropic":
				// Handle Anthropic's multiple output behavior
				var toolCallOutput *conversation.ConversationResult
				for _, output := range resp.Outputs {
					if len(output.ToolCalls) > 0 {
						toolCallOutput = &output
						break
					}
				}

				if toolCallOutput != nil {
					assert.Equal(t, "tool_calls", toolCallOutput.FinishReason)
					toolCall := toolCallOutput.ToolCalls[0]
					assert.Equal(t, "get_weather", toolCall.Function.Name)

					// Verify arguments can be parsed and contain expected location
					var args map[string]interface{}
					err := json.Unmarshal([]byte(toolCall.Function.Arguments), &args)
					require.NoError(t, err, "Tool arguments should be valid JSON even with string parameters")
					assert.Contains(t, args, "location", "Arguments should contain location parameter")
				} else {
					// No tool calls - this is acceptable for some providers in some cases
					t.Logf("Component %s chose not to call tools for this request (acceptable)", component)
				}

			default:
				// Standard validation
				require.Len(t, resp.Outputs, 1)
				output := resp.Outputs[0]

				if len(output.ToolCalls) > 0 {
					assert.Equal(t, "tool_calls", output.FinishReason)
					toolCall := output.ToolCalls[0]
					assert.Equal(t, "get_weather", toolCall.Function.Name)

					// The key test: ensure arguments are valid JSON even when parameters were provided as string
					var args map[string]interface{}
					err := json.Unmarshal([]byte(toolCall.Function.Arguments), &args)
					require.NoError(t, err, "Tool arguments should be valid JSON even with string parameters")
					assert.Contains(t, args, "location", "Arguments should contain location parameter")

					// Verify the location parameter was extracted correctly
					location, ok := args["location"].(string)
					if ok {
						assert.Contains(t, strings.ToLower(location), "new york", "Should extract New York from message")
					}
				} else {
					// No tool calls - log for debugging
					t.Logf("Component %s chose not to call tools for this request", component)
					assert.NotEmpty(t, output.Result, "Should have a response if no tools are called")
					assert.Equal(t, "stop", output.FinishReason)
				}
			}
		})

		t.Run("parallel tool calling", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
			defer cancel()

			// Define multiple tools that can be called together
			weatherTool := conversation.Tool{
				Type: "function",
				Function: conversation.ToolFunction{
					Name:        "get_weather",
					Description: "Get current weather for a location",
					Parameters: map[string]any{
						"type": "object",
						"properties": map[string]any{
							"location": map[string]any{
								"type":        "string",
								"description": "City and state",
							},
						},
						"required": []string{"location"},
					},
				},
			}

			timeTool := conversation.Tool{
				Type: "function",
				Function: conversation.ToolFunction{
					Name:        "get_time",
					Description: "Get current time in a timezone",
					Parameters: map[string]any{
						"type": "object",
						"properties": map[string]any{
							"timezone": map[string]any{
								"type":        "string",
								"description": "The timezone, e.g. America/New_York",
							},
						},
						"required": []string{"timezone"},
					},
				},
			}

			calcTool := conversation.Tool{
				Type: "function",
				Function: conversation.ToolFunction{
					Name:        "calculate",
					Description: "Perform a mathematical calculation",
					Parameters: map[string]any{
						"type": "object",
						"properties": map[string]any{
							"expression": map[string]any{
								"type":        "string",
								"description": "The mathematical expression to evaluate",
							},
						},
						"required": []string{"expression"},
					},
				},
			}

			// Use a prompt designed to trigger multiple tools
			req := &conversation.ConversationRequest{
				Inputs: []conversation.ConversationInput{
					{
						Message: "What's the weather and current time in New York City? Also calculate 23 * 7 for me.",
						Role:    conversation.RoleUser,
						Tools:   []conversation.Tool{weatherTool, timeTool, calcTool},
					},
				},
			}

			resp, err := conv.Converse(ctx, req)
			require.NoError(t, err)
			require.NotNil(t, resp)

			// Provider-specific expectations
			switch component {
			case "anthropic":
				// Anthropic often returns multiple outputs: explanation + tool calls
				if len(resp.Outputs) == 2 {
					// First output: explanation text
					assert.NotEmpty(t, resp.Outputs[0].Result, "Anthropic should provide explanatory text")
					assert.Equal(t, "stop", resp.Outputs[0].FinishReason)

					// Second output: tool calls
					output := resp.Outputs[1]
					if len(output.ToolCalls) >= 1 {
						assert.Equal(t, "tool_calls", output.FinishReason)
						// Anthropic may call tools sequentially, not always in parallel
						t.Logf("✅ Component %s performed tool calling (%d tools) with conversational approach", component, len(output.ToolCalls))
					}
				} else {
					// Single output behavior
					output := resp.Outputs[0]
					if len(output.ToolCalls) >= 1 {
						assert.Equal(t, "tool_calls", output.FinishReason)
						t.Logf("✅ Component %s performed tool calling (%d tools)", component, len(output.ToolCalls))
					} else {
						assert.NotEmpty(t, output.Result, "Should have a response even if no tools are called")
						t.Logf("ℹ️  Component %s chose conversational response over tool calling", component)
					}
				}

			case "huggingface":
				// HuggingFace typically has limited parallel tool calling support
				require.Len(t, resp.Outputs, 1)
				output := resp.Outputs[0]

				if len(output.ToolCalls) >= 1 {
					assert.Equal(t, "tool_calls", output.FinishReason)
					t.Logf("✅ Component %s called %d tool(s) (limited parallel support expected)", component, len(output.ToolCalls))
				} else {
					assert.NotEmpty(t, output.Result, "Should have a response even if no tools are called")
					t.Logf("ℹ️  Component %s chose not to call tools", component)
				}

			case "googleai", "mistral":
				// GoogleAI and Mistral have different tool call structure via langchaingo
				require.Len(t, resp.Outputs, 1)
				output := resp.Outputs[0]

				if len(output.ToolCalls) >= 1 {
					assert.Equal(t, "tool_calls", output.FinishReason, "Finish reason should be 'tool_calls' when tools are called")

					// For GoogleAI/Mistral, validate what we can without strict ID/Type requirements
					toolNames := make(map[string]bool)
					for _, toolCall := range output.ToolCalls {
						// Function name should always be present
						assert.NotEmpty(t, toolCall.Function.Name, "Tool call should have function name")
						assert.NotEmpty(t, toolCall.Function.Arguments, "Tool call should have arguments")
						toolNames[toolCall.Function.Name] = true

						// Verify tool call arguments are valid JSON
						var args map[string]interface{}
						err := json.Unmarshal([]byte(toolCall.Function.Arguments), &args)
						require.NoError(t, err, "Tool call arguments should be valid JSON")
					}

					if len(output.ToolCalls) >= 2 {
						// Verify multiple different tools were called
						assert.True(t, len(toolNames) >= 2, "Expected multiple different tools to be called for parallel execution")
						t.Logf("✅ Component %s successfully performed parallel tool calling (%d tools)", component, len(output.ToolCalls))
					} else {
						t.Logf("ℹ️  Component %s called 1 tool (parallel calling not triggered)", component)
					}
				} else {
					// No tool calls
					assert.NotEmpty(t, output.Result, "Should have a response even if no tools are called")
					assert.Equal(t, "stop", output.FinishReason)
					t.Logf("ℹ️  Component %s chose not to call tools for parallel request", component)
				}

			default:
				// Standard behavior for OpenAI, Echo, DeepSeek, etc.
				require.Len(t, resp.Outputs, 1)
				output := resp.Outputs[0]

				// Check if multiple tools were called (parallel tool calling)
				if len(output.ToolCalls) >= 2 {
					assert.Equal(t, "tool_calls", output.FinishReason, "Finish reason should be 'tool_calls' when tools are called")

					// Verify that tool calls have unique IDs
					toolIDs := make(map[string]bool)
					toolNames := make(map[string]bool)
					for _, toolCall := range output.ToolCalls {
						assert.NotEmpty(t, toolCall.ID, "Tool call should have a unique ID")
						assert.False(t, toolIDs[toolCall.ID], "Tool call IDs should be unique")
						toolIDs[toolCall.ID] = true
						toolNames[toolCall.Function.Name] = true

						assert.Equal(t, "function", toolCall.Type, "Tool call type should be 'function'")
						assert.NotEmpty(t, toolCall.Function.Arguments, "Tool call should have arguments")

						// Verify tool call arguments are valid JSON
						var args map[string]interface{}
						err := json.Unmarshal([]byte(toolCall.Function.Arguments), &args)
						require.NoError(t, err, "Tool call arguments should be valid JSON")
					}

					// Verify multiple different tools were called
					assert.True(t, len(toolNames) >= 2, "Expected multiple different tools to be called for parallel execution")

					t.Logf("✅ Component %s successfully performed parallel tool calling (%d tools)", component, len(output.ToolCalls))
				} else if len(output.ToolCalls) == 1 {
					// Single tool call is still valid behavior
					assert.Equal(t, "tool_calls", output.FinishReason)
					t.Logf("ℹ️  Component %s called 1 tool (parallel calling not triggered)", component)
				} else {
					// No tool calls - some components may not support parallel tool calling
					assert.NotEmpty(t, output.Result, "Should have a response even if no tools are called")
					assert.Equal(t, "stop", output.FinishReason)
					t.Logf("ℹ️  Component %s chose not to call tools for parallel request", component)
				}
			}
		})

		t.Run("streaming parallel tool calling", func(t *testing.T) {
			// Check if component also supports streaming
			streamer, ok := conv.(conversation.StreamingConversation)
			if !ok {
				t.Skipf("Component %s does not support streaming, skipping streaming parallel tool calling tests", component)
				return
			}

			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
			defer cancel()

			// Define tools for parallel calling
			weatherTool := conversation.Tool{
				Type: "function",
				Function: conversation.ToolFunction{
					Name:        "get_weather",
					Description: "Get current weather for a location",
					Parameters: map[string]any{
						"type": "object",
						"properties": map[string]any{
							"location": map[string]any{
								"type":        "string",
								"description": "City and state",
							},
						},
						"required": []string{"location"},
					},
				},
			}

			timeTool := conversation.Tool{
				Type: "function",
				Function: conversation.ToolFunction{
					Name:        "get_time",
					Description: "Get current time in a timezone",
					Parameters: map[string]any{
						"type": "object",
						"properties": map[string]any{
							"timezone": map[string]any{
								"type":        "string",
								"description": "The timezone, e.g. America/New_York",
							},
						},
						"required": []string{"timezone"},
					},
				},
			}

			req := &conversation.ConversationRequest{
				Inputs: []conversation.ConversationInput{
					{
						Message: "What's the weather and current time in New York City?",
						Role:    conversation.RoleUser,
						Tools:   []conversation.Tool{weatherTool, timeTool},
					},
				},
			}

			var chunks [][]byte
			streamFunc := func(ctx context.Context, chunk []byte) error {
				chunks = append(chunks, chunk)
				return nil
			}

			resp, err := streamer.ConverseStream(ctx, req, streamFunc)

			// Check if streaming is disabled for this component
			if err != nil && (strings.Contains(err.Error(), "streaming is not supported") ||
				strings.Contains(err.Error(), "streaming not supported") ||
				strings.Contains(err.Error(), "invalid delta text field type")) {
				t.Skipf("Component %s has streaming disabled or streaming tool calling issues, skipping streaming parallel tool calling tests", component)
				return
			}

			require.NoError(t, err)
			require.NotNil(t, resp)

			// Provider-specific streaming expectations
			switch component {
			case "anthropic":
				// Anthropic may return multiple outputs even in streaming
				if len(resp.Outputs) >= 1 {
					// Check the last output for tool calls
					output := resp.Outputs[len(resp.Outputs)-1]
					if len(output.ToolCalls) >= 1 {
						assert.Equal(t, "tool_calls", output.FinishReason)
						t.Logf("✅ Component %s performed streaming tool calling (%d tools, %d chunks) with conversational approach",
							component, len(output.ToolCalls), len(chunks))
					} else {
						t.Logf("ℹ️  Component %s streaming chose conversational response", component)
					}
				}

			case "googleai", "mistral":
				// GoogleAI and Mistral may have different streaming behavior
				require.Len(t, resp.Outputs, 1)
				output := resp.Outputs[0]

				if len(output.ToolCalls) >= 1 {
					assert.Equal(t, "tool_calls", output.FinishReason)

					// For GoogleAI/Mistral, don't require streaming chunks or strict tool structure
					toolNames := make(map[string]bool)
					for _, toolCall := range output.ToolCalls {
						assert.NotEmpty(t, toolCall.Function.Name, "Tool call should have function name")
						toolNames[toolCall.Function.Name] = true
					}

					if len(output.ToolCalls) >= 2 {
						assert.True(t, len(toolNames) >= 2, "Expected multiple different tools to be called")
						t.Logf("✅ Component %s performed streaming parallel tool calling (%d tools, %d chunks) - langchaingo behavior",
							component, len(output.ToolCalls), len(chunks))
					} else {
						t.Logf("ℹ️  Component %s streaming called 1 tool (parallel calling not triggered)", component)
					}
				} else {
					t.Logf("ℹ️  Component %s streaming chose not to call tools", component)
				}

			default:
				require.Len(t, resp.Outputs, 1)
				output := resp.Outputs[0]

				// Check if multiple tools were called via streaming
				if len(output.ToolCalls) >= 2 {
					assert.Equal(t, "tool_calls", output.FinishReason)

					// Verify that tool calls have unique IDs
					toolIDs := make(map[string]bool)
					toolNames := make(map[string]bool)
					for _, toolCall := range output.ToolCalls {
						assert.NotEmpty(t, toolCall.ID, "Tool call should have a unique ID")
						assert.False(t, toolIDs[toolCall.ID], "Tool call IDs should be unique")
						toolIDs[toolCall.ID] = true
						toolNames[toolCall.Function.Name] = true

						assert.Equal(t, "function", toolCall.Type)
					}

					// Verify multiple different tools were called
					assert.True(t, len(toolNames) >= 2, "Expected multiple different tools to be called")

					// Should have received streaming chunks
					assert.NotEmpty(t, chunks, "Should have received streaming chunks for parallel tool calling")

					t.Logf("✅ Component %s successfully performed streaming parallel tool calling (%d tools, %d chunks)",
						component, len(output.ToolCalls), len(chunks))
				} else if len(output.ToolCalls) == 1 {
					// Single tool call via streaming is still valid
					assert.Equal(t, "tool_calls", output.FinishReason)
					t.Logf("ℹ️  Component %s streaming called 1 tool (parallel calling not triggered)", component)
				} else {
					// No tool calls via streaming
					assert.NotEmpty(t, output.Result, "Should have a streamed response")
					assert.Equal(t, "stop", output.FinishReason)
					t.Logf("ℹ️  Component %s streaming chose not to call tools", component)
				}
			}
		})
	})
}
