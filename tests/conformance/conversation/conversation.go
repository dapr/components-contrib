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
			assert.NotEmpty(t, resp.Outputs[0].Result)
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
			assert.GreaterOrEqual(t, resp.Usage.PromptTokens, int32(0), "Should have non-negative prompt tokens")
			assert.GreaterOrEqual(t, resp.Usage.CompletionTokens, int32(0), "Should have non-negative completion tokens")
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
			assert.NotEmpty(t, output.Result, "Legacy result field should be populated")
		})

		t.Run("tool definitions in content parts", func(t *testing.T) {
			ensureComponentInitialized()
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
			defer cancel()

			req := &conversation.ConversationRequest{
				Inputs: []conversation.ConversationInput{
					{
						Role: conversation.RoleUser,
						Parts: []conversation.ContentPart{
							conversation.TextContentPart{Text: "What can you do?"},
							conversation.ToolDefinitionsContentPart{
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
							},
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

		t.Run("backward compatibility with legacy fields", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
			defer cancel()

			// Test using legacy Message field
			req := &conversation.ConversationRequest{
				Inputs: []conversation.ConversationInput{
					{
						Role:    conversation.RoleUser,
						Message: "Legacy message test",
					},
				},
			}
			resp, err := conv.Converse(ctx, req)

			require.NoError(t, err)
			assert.Len(t, resp.Outputs, 1)

			output := resp.Outputs[0]
			// Should work with legacy Message field
			assert.NotEmpty(t, output.Result, "Should respond to legacy message field")
		})

		t.Run("content parts utility functions", func(t *testing.T) {
			// Test the utility functions work correctly
			parts := []conversation.ContentPart{
				conversation.TextContentPart{Text: "Hello"},
				conversation.TextContentPart{Text: "World"},
				conversation.ToolDefinitionsContentPart{
					Tools: []conversation.Tool{
						{
							ToolType: "function",
							Function: conversation.ToolFunction{
								Name:        "test_tool",
								Description: "A test tool",
							},
						},
					},
				},
			}

			// Test text extraction
			text := conversation.ExtractTextFromParts(parts)
			assert.Equal(t, "Hello World", text)

			// Test tool definitions extraction
			tools := conversation.ExtractToolDefinitionsFromParts(parts)
			assert.Len(t, tools, 1)
			assert.Equal(t, "test_tool", tools[0].Function.Name)
		})

		t.Run("tool calling with content parts", func(t *testing.T) {
			// Tool calling support is component-dependent
			// This test will run for all components but expectations vary
			ensureComponentInitialized()
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
			defer cancel()

			req := &conversation.ConversationRequest{
				Inputs: []conversation.ConversationInput{
					{
						Role: conversation.RoleUser,
						Parts: []conversation.ContentPart{
							conversation.TextContentPart{Text: "What's the weather like?"},
							conversation.ToolDefinitionsContentPart{
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
							},
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
				Inputs: []conversation.ConversationInput{
					{
						Role: conversation.RoleUser,
						Parts: []conversation.ContentPart{
							conversation.TextContentPart{Text: "I need the weather in New York and the time in London"},
							conversation.ToolDefinitionsContentPart{
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
							},
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
			assert.NotEmpty(t, output.Result, "Should have legacy result field")
		})

		t.Run("tool result processing with content parts", func(t *testing.T) {
			// Test processing tool results in content parts
			// This test is component-dependent - some providers have strict conversation flow requirements
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
			defer cancel()

			// Skip this test for providers that require strict tool call flows
			if component == "openai" || component == "anthropic" || component == "googleai" {
				t.Skip("Skipping tool result processing test for provider with strict conversation flow requirements")
				return
			}

			req := &conversation.ConversationRequest{
				Inputs: []conversation.ConversationInput{
					// Initial user message with tool definitions
					{
						Role: conversation.RoleUser,
						Parts: []conversation.ContentPart{
							conversation.TextContentPart{Text: "What's the weather like?"},
							conversation.ToolDefinitionsContentPart{
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
							},
						},
					},
					// Simulated assistant response with tool call
					{
						Role: conversation.RoleAssistant,
						Parts: []conversation.ContentPart{
							conversation.TextContentPart{Text: "I'll check the weather for you."},
							conversation.ToolCallContentPart{
								ID:       "call_weather_123",
								CallType: "function",
								Function: conversation.ToolCallFunction{
									Name:      "get_weather",
									Arguments: `{"location":"New York"}`,
								},
							},
						},
					},
					// Tool result
					{
						Role: conversation.RoleTool,
						Parts: []conversation.ContentPart{
							conversation.ToolResultContentPart{
								ToolCallID: "call_weather_123",
								Name:       "get_weather",
								Content:    "Sunny, 75°F in New York",
								IsError:    false,
							},
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

			// Should have some response to the tool result
			textContent := conversation.ExtractTextFromParts(allParts)
			assert.NotEmpty(t, textContent, "Should have text response to tool result")
			t.Logf("Component response to tool result: %s", textContent)
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

			require.NoError(t, err)
			assert.Len(t, resp.Outputs, 1)
			assert.NotEmpty(t, resp.Outputs[0].Result)

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

			require.NoError(t, err)
			assert.Len(t, resp.Outputs, 1)
			assert.NotEmpty(t, resp.Outputs[0].Parts, "Should have content parts in response")
			assert.NotEmpty(t, resp.Outputs[0].Result, "Should have legacy result field")

			// Should have received streaming chunks
			assert.NotEmpty(t, chunks, "Should have received streaming chunks")
		})
	})
}
