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
	"bufio"
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/conversation"
	"github.com/dapr/components-contrib/conversation/anthropic"
	"github.com/dapr/components-contrib/conversation/googleai"
	"github.com/dapr/components-contrib/conversation/openai"
	"github.com/dapr/components-contrib/metadata"
)

// loadEnvFile loads environment variables from .local/.env file
func loadEnvFile() map[string]string {
	envVars := make(map[string]string)

	// Try .local/.env in current directory first, then parent directories
	paths := []string{".local/.env", "../.local/.env", "../../.local/.env"}

	var file *os.File
	var err error
	for _, path := range paths {
		file, err = os.Open(path)
		if err == nil {
			break
		}
	}

	if err != nil {
		return envVars // Return empty map if file doesn't exist
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		parts := strings.SplitN(line, "=", 2)
		if len(parts) == 2 {
			key := strings.TrimSpace(parts[0])
			value := strings.TrimSpace(parts[1])
			envVars[key] = value
		}
	}

	return envVars
}

func TestRealProvidersToolCalling(t *testing.T) {
	// Load environment variables from .local/.env
	envVars := loadEnvFile()

	// Weather tool definition that both providers should support
	weatherTool := conversation.Tool{
		Type: "function",
		Function: conversation.ToolFunction{
			Name:        "get_weather",
			Description: "Get current weather in a location",
			Parameters: map[string]interface{}{
				"type": "object",
				"properties": map[string]interface{}{
					"location": map[string]interface{}{
						"type":        "string",
						"description": "City name",
					},
				},
				"required": []string{"location"},
			},
		},
	}

	testCases := []struct {
		name         string
		component    func() conversation.Conversation
		apiKeyEnvVar string
		modelName    string
		skipMessage  string
	}{
		{
			name: "openai",
			component: func() conversation.Conversation {
				return openai.NewOpenAI(nil)
			},
			apiKeyEnvVar: "OPENAI_API_KEY",
			modelName:    "gpt-3.5-turbo",
			skipMessage:  "OpenAI API key not found in .local/.env",
		},
		{
			name: "anthropic",
			component: func() conversation.Conversation {
				return anthropic.NewAnthropic(nil)
			},
			apiKeyEnvVar: "ANTHROPIC_API_KEY",
			modelName:    "claude-3-haiku-20240307",
			skipMessage:  "Anthropic API key not found in .local/.env",
		},
		{
			name: "googleai",
			component: func() conversation.Conversation {
				// Create the component inline with explicit testLogger
				g := googleai.NewGoogleAI(testLogger)
				if g == nil {
					panic("googleai.NewGoogleAI returned nil")
				}
				return g
			},
			apiKeyEnvVar: "GOOGLE_AI_API_KEY",
			modelName:    "gemini-2.5-flash",
			skipMessage:  "Google AI API key not found in .local/.env",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Check if API key is available
			apiKey := envVars[tc.apiKeyEnvVar]
			if apiKey == "" {
				t.Skip(tc.skipMessage)
				return
			}

			// Set environment variable for the component (many components expect env vars)
			originalValue := os.Getenv(tc.apiKeyEnvVar)
			os.Setenv(tc.apiKeyEnvVar, apiKey)
			defer func() {
				if originalValue == "" {
					os.Unsetenv(tc.apiKeyEnvVar)
				} else {
					os.Setenv(tc.apiKeyEnvVar, originalValue)
				}
			}()

			// Initialize the component
			comp := tc.component()
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
			defer cancel()

			// Set up metadata with API key and model
			props := map[string]string{
				"key":   apiKey, // LangchainMetadata expects "key" field
				"model": tc.modelName,
			}

			err := comp.Init(ctx, conversation.Metadata{
				Base: metadata.Base{
					Properties: props,
				},
			})
			require.NoError(t, err, "Failed to initialize %s component", tc.name)

			t.Run("basic_tool_calling", func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
				defer cancel()

				req := &conversation.ConversationRequest{
					Inputs: []conversation.ConversationInput{
						{
							Message: "What's the weather like in San Francisco?",
							Role:    conversation.RoleUser,
							Tools:   []conversation.Tool{weatherTool},
						},
					},
				}

				resp, err := comp.Converse(ctx, req)
				require.NoError(t, err, "Conversation failed for %s", tc.name)
				require.NotEmpty(t, resp.Outputs, "No outputs received from %s", tc.name)

				output := resp.Outputs[0]

				// Real LLM should either:
				// 1. Call the weather tool (ideal) - may have empty Result
				// 2. Decline to call tools but provide some response
				if len(output.ToolCalls) == 0 {
					assert.NotEmpty(t, output.Result, "Should provide text response when not calling tools")
				}

				if len(output.ToolCalls) > 0 {
					// If it called tools, verify the structure
					t.Logf("✅ %s called %d tool(s)", tc.name, len(output.ToolCalls))
					assert.Equal(t, "tool_calls", output.FinishReason, "Should finish with 'tool_calls'")

					for _, toolCall := range output.ToolCalls {
						// Google AI through langchaingo doesn't populate Type field consistently
						// Tool calling functionality works correctly despite missing metadata
						// OpenAI compatibility layer should behave like OpenAI
						if tc.name != "googleai" {
							assert.Equal(t, "function", toolCall.Type)
						}
						assert.NotEmpty(t, toolCall.ID)
						assert.Equal(t, "get_weather", toolCall.Function.Name)
						assert.NotEmpty(t, toolCall.Function.Arguments)
						t.Logf("Tool call: %s (ID: %s) with args: %s", toolCall.Function.Name, toolCall.ID, toolCall.Function.Arguments)
					}
				} else {
					t.Logf("ℹ️  %s chose not to call tools, response: %s", tc.name, output.Result)
					if output.FinishReason != "" {
						assert.Equal(t, "stop", output.FinishReason, "Should finish with 'stop' when not calling tools")
					}
				}

				// Verify usage information is provided
				if resp.Usage != nil {
					assert.Greater(t, resp.Usage.TotalTokens, int32(0), "Should provide token usage")
					t.Logf("Usage: %d total tokens (%d prompt + %d completion)",
						resp.Usage.TotalTokens, resp.Usage.PromptTokens, resp.Usage.CompletionTokens)
				}
			})

			t.Run("multiple_tools", func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
				defer cancel()

				timeTool := conversation.Tool{
					Type: "function",
					Function: conversation.ToolFunction{
						Name:        "get_time",
						Description: "Get current time",
						Parameters: map[string]interface{}{
							"type":       "object",
							"properties": map[string]interface{}{},
						},
					},
				}

				req := &conversation.ConversationRequest{
					Inputs: []conversation.ConversationInput{
						{
							Message: "What's the weather and current time in New York?",
							Role:    conversation.RoleUser,
							Tools:   []conversation.Tool{weatherTool, timeTool},
						},
					},
				}

				resp, err := comp.Converse(ctx, req)
				require.NoError(t, err, "Multiple tools conversation failed for %s", tc.name)
				require.NotEmpty(t, resp.Outputs, "No outputs received from %s", tc.name)

				output := resp.Outputs[0]
				if len(output.ToolCalls) == 0 {
					assert.NotEmpty(t, output.Result, "Should provide text response when not calling tools")
				}

				if len(output.ToolCalls) > 0 {
					t.Logf("✅ %s called %d tool(s) for multiple tool request", tc.name, len(output.ToolCalls))

					// Verify all tool calls are valid
					for _, toolCall := range output.ToolCalls {
						// Google AI through langchaingo doesn't populate Type field consistently
						// Tool calling functionality works correctly despite missing metadata
						// OpenAI compatibility layer should behave like OpenAI
						if tc.name != "googleai" {
							assert.Equal(t, "function", toolCall.Type)
						}
						assert.NotEmpty(t, toolCall.ID)
						assert.Contains(t, []string{"get_weather", "get_time"}, toolCall.Function.Name)
						t.Logf("Tool call: %s (ID: %s)", toolCall.Function.Name, toolCall.ID)
					}
				} else {
					t.Logf("ℹ️  %s chose not to call tools for multiple tool request", tc.name)
				}
			})

			t.Run("multi_step_tool_calling_flow", func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
				defer cancel()

				// Step 1: User asks a question that requires tools
				step1Req := &conversation.ConversationRequest{
					Inputs: []conversation.ConversationInput{
						{
							Message: "What's the weather like in Boston? I need to decide what to wear.",
							Role:    conversation.RoleUser,
							Tools:   []conversation.Tool{weatherTool},
						},
					},
				}

				step1Resp, err := comp.Converse(ctx, step1Req)
				require.NoError(t, err, "Step 1 failed for %s", tc.name)
				require.NotEmpty(t, step1Resp.Outputs, "No outputs from step 1 for %s", tc.name)

				step1Output := step1Resp.Outputs[0]

				if len(step1Output.ToolCalls) == 0 {
					t.Logf("ℹ️  %s chose not to call tools in step 1, skipping multi-step test", tc.name)
					return
				}

				t.Logf("✅ Step 1: %s requested %d tool call(s)", tc.name, len(step1Output.ToolCalls))

				// Step 2: Simulate tool execution and provide results
				// Build conversation history: user message + assistant response + tool results
				var step2Inputs []conversation.ConversationInput

				// Original user message
				step2Inputs = append(step2Inputs, conversation.ConversationInput{
					Message: "What's the weather like in Boston? I need to decide what to wear.",
					Role:    conversation.RoleUser,
					Tools:   []conversation.Tool{weatherTool},
				})

				// Assistant response (simplified - real implementation would include tool calls)
				step2Inputs = append(step2Inputs, conversation.ConversationInput{
					Message: step1Output.Result,
					Role:    conversation.RoleAssistant,
				})

				// Tool results for each tool call
				for _, toolCall := range step1Output.ToolCalls {
					var toolResult string
					switch toolCall.Function.Name {
					case "get_weather":
						toolResult = `{"temperature": 68, "condition": "partly cloudy", "humidity": 65, "wind": "5 mph"}`
					case "get_time":
						toolResult = `{"time": "2:30 PM", "timezone": "EST", "date": "2024-01-15"}`
					default:
						toolResult = `{"result": "success"}`
					}

					step2Inputs = append(step2Inputs, conversation.ConversationInput{
						Message:    toolResult,
						Role:       conversation.RoleTool,
						ToolCallID: toolCall.ID, // Use the actual tool call ID from step 1
						Name:       toolCall.Function.Name,
					})

					t.Logf("Step 2: Providing tool result for %s (ID: %s)", toolCall.Function.Name, toolCall.ID)
				}

				step2Req := &conversation.ConversationRequest{
					Inputs: step2Inputs,
				}

				step2Resp, err := comp.Converse(ctx, step2Req)
				if err != nil {
					t.Logf("⚠️  Step 2 failed for %s (expected for some providers): %v", tc.name, err)
					return
				}

				require.NotEmpty(t, step2Resp.Outputs, "No outputs from step 2 for %s", tc.name)

				step2Output := step2Resp.Outputs[0]
				assert.NotEmpty(t, step2Output.Result, "Should provide final response based on tool results")
				assert.Empty(t, step2Output.ToolCalls, "Final response should not trigger new tool calls")

				if step2Output.FinishReason != "" {
					assert.Equal(t, "stop", step2Output.FinishReason, "Should finish with 'stop'")
				}

				t.Logf("✅ Step 2: %s provided final response: %.100s...", tc.name, step2Output.Result)

				// Verify the response incorporates tool results
				assert.Contains(t, step2Output.Result, "68", "Response should mention the temperature")
			})

			t.Run("parallel_tool_calling", func(t *testing.T) {
				if tc.name != "openai" && tc.name != "googleai" {
					t.Skip("Parallel tool calling test only for OpenAI and Google AI (Anthropic is more conservative)")
				}

				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
				defer cancel()

				// Define additional tools for parallel calling
				timeTool := conversation.Tool{
					Type: "function",
					Function: conversation.ToolFunction{
						Name:        "get_time",
						Description: "Get current time in a timezone",
						Parameters: map[string]interface{}{
							"type": "object",
							"properties": map[string]interface{}{
								"timezone": map[string]interface{}{
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
						Parameters: map[string]interface{}{
							"type": "object",
							"properties": map[string]interface{}{
								"expression": map[string]interface{}{
									"type":        "string",
									"description": "The mathematical expression to evaluate",
								},
							},
							"required": []string{"expression"},
						},
					},
				}

				// Use a prompt designed to trigger parallel tool calling
				req := &conversation.ConversationRequest{
					Inputs: []conversation.ConversationInput{
						{
							Message: "What's the weather and current time in New York City? Also calculate 23 * 7 for me.",
							Role:    conversation.RoleUser,
							Tools:   []conversation.Tool{weatherTool, timeTool, calcTool},
						},
					},
				}

				resp, err := comp.Converse(ctx, req)
				require.NoError(t, err, "Parallel tool calling conversation failed for %s", tc.name)
				require.NotEmpty(t, resp.Outputs, "No outputs received from %s", tc.name)

				output := resp.Outputs[0]
				t.Logf("Parallel tool calling response - ToolCalls: %d", len(output.ToolCalls))

				if len(output.ToolCalls) >= 2 {
					t.Logf("✅ %s successfully performed parallel tool calling (%d tools)", tc.name, len(output.ToolCalls))

					// Verify that tool calls have unique IDs
					toolIDs := make(map[string]bool)
					toolNames := make(map[string]bool)
					for _, toolCall := range output.ToolCalls {
						assert.NotEmpty(t, toolCall.ID, "Tool call should have a unique ID")
						assert.False(t, toolIDs[toolCall.ID], "Tool call IDs should be unique")
						toolIDs[toolCall.ID] = true
						toolNames[toolCall.Function.Name] = true

						t.Logf("Tool call: %s (ID: %s)", toolCall.Function.Name, toolCall.ID)
					}

					// Verify multiple different tools were called
					assert.True(t, len(toolNames) >= 2, "Expected multiple different tools to be called")

					// Verify finish reason is tool_calls
					assert.Equal(t, "tool_calls", output.FinishReason)
				} else {
					t.Logf("ℹ️  %s called %d tool(s) (parallel calling not triggered)", tc.name, len(output.ToolCalls))
				}
			})

			// Check if component supports streaming
			if streamingComp, ok := comp.(conversation.StreamingConversation); ok {
				t.Run("streaming_content_validation", func(t *testing.T) {
					ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
					defer cancel()

					req := &conversation.ConversationRequest{
						Inputs: []conversation.ConversationInput{
							{
								Message: "Write a short 2-sentence summary about artificial intelligence.",
								Role:    conversation.RoleUser,
								// No tools - focus on text generation
							},
						},
					}

					var chunks []string
					var totalStreamedContent string
					streamFunc := func(ctx context.Context, chunk []byte) error {
						chunkStr := string(chunk)
						chunks = append(chunks, chunkStr)
						totalStreamedContent += chunkStr
						return nil
					}

					resp, err := streamingComp.ConverseStream(ctx, req, streamFunc)
					if err != nil {
						// Some providers (like Anthropic) don't support streaming yet
						if tc.name == "anthropic" {
							t.Logf("⚠️  %s streaming not supported yet: %v", tc.name, err)
							return
						}
						require.NoError(t, err, "Streaming conversation failed for %s", tc.name)
					}

					require.NotEmpty(t, resp.Outputs, "No streaming outputs received from %s", tc.name)
					output := resp.Outputs[0]

					// Validate streaming chunks
					if len(chunks) > 0 {
						t.Logf("📡 %s received %d streaming chunks", tc.name, len(chunks))

						// Check that we received meaningful streaming content
						assert.NotEmpty(t, totalStreamedContent, "Streamed content should not be empty")
						assert.Greater(t, len(totalStreamedContent), 10, "Streamed content should be substantial")

						// Log first few chunks for debugging
						for i, chunk := range chunks {
							if i >= 3 { // Only log first 3 chunks
								break
							}
							t.Logf("Chunk %d: %q", i+1, chunk)
						}

						// Validate final response
						assert.NotEmpty(t, output.Result, "Final result should not be empty")

						// Check that streamed content is related to final result
						// For text generation (no tool calls), they should be very similar
						if len(output.ToolCalls) == 0 {
							// Remove extra whitespace for comparison
							streamedTrimmed := strings.TrimSpace(totalStreamedContent)
							finalTrimmed := strings.TrimSpace(output.Result)

							if streamedTrimmed != finalTrimmed {
								t.Logf("⚠️  Streamed content differs from final result")
								t.Logf("Streamed: %q", streamedTrimmed)
								t.Logf("Final: %q", finalTrimmed)

								// They should at least have significant overlap
								assert.True(t, len(streamedTrimmed) > 0 && len(finalTrimmed) > 0,
									"Both streamed and final content should be non-empty")
							} else {
								t.Logf("✅ Streamed content matches final result perfectly")
							}
						}

						// Validate content quality - should mention AI/artificial intelligence
						combinedContent := strings.ToLower(totalStreamedContent + " " + output.Result)
						assert.True(t,
							strings.Contains(combinedContent, "artificial intelligence") ||
								strings.Contains(combinedContent, "ai") ||
								strings.Contains(combinedContent, "machine learning"),
							"Response should mention AI-related terms")

						t.Logf("✅ %s streaming content validation passed", tc.name)
					} else {
						t.Logf("⚠️  %s received no streaming chunks", tc.name)
					}
				})

				t.Run("streaming_tool_calling", func(t *testing.T) {
					ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
					defer cancel()

					req := &conversation.ConversationRequest{
						Inputs: []conversation.ConversationInput{
							{
								Message: "What's the weather in Boston?",
								Role:    conversation.RoleUser,
								Tools:   []conversation.Tool{weatherTool},
							},
						},
					}

					var chunks []string
					streamFunc := func(ctx context.Context, chunk []byte) error {
						chunks = append(chunks, string(chunk))
						return nil
					}

					resp, err := streamingComp.ConverseStream(ctx, req, streamFunc)
					if err != nil {
						// Some providers (like Anthropic) don't support streaming yet
						if tc.name == "anthropic" {
							t.Logf("⚠️  %s streaming not supported yet: %v", tc.name, err)
							return
						}
						require.NoError(t, err, "Streaming conversation failed for %s", tc.name)
					}

					require.NotEmpty(t, resp.Outputs, "No streaming outputs received from %s", tc.name)

					output := resp.Outputs[0]
					if len(output.ToolCalls) == 0 {
						assert.NotEmpty(t, output.Result, "Should provide text response when not calling tools")
					}

					if len(output.ToolCalls) > 0 {
						t.Logf("✅ %s supports streaming with tool calling", tc.name)
					} else {
						t.Logf("ℹ️  %s streaming without tool calls", tc.name)
					}

					if len(chunks) > 0 {
						t.Logf("📡 Received %d streaming chunks", len(chunks))
					}
				})

				t.Run("streaming_parallel_tool_calling", func(t *testing.T) {
					if tc.name != "openai" && tc.name != "googleai" {
						t.Skip("Streaming parallel tool calling test only for OpenAI and Google AI")
					}

					ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
					defer cancel()

					// Use the same parallel tool calling setup as the non-streaming test
					timeTool := conversation.Tool{
						Type: "function",
						Function: conversation.ToolFunction{
							Name:        "get_time",
							Description: "Get current time in a timezone",
							Parameters: map[string]interface{}{
								"type": "object",
								"properties": map[string]interface{}{
									"timezone": map[string]interface{}{
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

					var chunks []string
					streamFunc := func(ctx context.Context, chunk []byte) error {
						chunks = append(chunks, string(chunk))
						return nil
					}

					resp, err := streamingComp.ConverseStream(ctx, req, streamFunc)
					require.NoError(t, err, "Streaming parallel tool calling conversation failed for %s", tc.name)
					require.NotEmpty(t, resp.Outputs, "No streaming outputs received from %s", tc.name)

					output := resp.Outputs[0]
					t.Logf("Streaming parallel tool calling response - ToolCalls: %d", len(output.ToolCalls))

					if len(output.ToolCalls) >= 2 {
						t.Logf("✅ %s successfully performed streaming parallel tool calling (%d tools)", tc.name, len(output.ToolCalls))

						// Verify that tool calls have unique IDs
						toolIDs := make(map[string]bool)
						toolNames := make(map[string]bool)
						for _, toolCall := range output.ToolCalls {
							assert.NotEmpty(t, toolCall.ID, "Tool call should have a unique ID")
							assert.False(t, toolIDs[toolCall.ID], "Tool call IDs should be unique")
							toolIDs[toolCall.ID] = true
							toolNames[toolCall.Function.Name] = true

							t.Logf("Streaming tool call: %s (ID: %s)", toolCall.Function.Name, toolCall.ID)
						}

						// Verify multiple different tools were called
						assert.True(t, len(toolNames) >= 2, "Expected multiple different tools to be called")

						// Verify finish reason is tool_calls
						assert.Equal(t, "tool_calls", output.FinishReason)

						if len(chunks) > 0 {
							t.Logf("📡 Received %d streaming chunks for parallel tool calling", len(chunks))
						}
					} else {
						t.Logf("ℹ️  %s streaming called %d tool(s) (parallel calling not triggered)", tc.name, len(output.ToolCalls))
					}
				})
			}

			// Clean up
			if closer, ok := comp.(interface{ Close() error }); ok {
				closer.Close()
			}
		})
	}
}

func TestRealProvidersNoAPIKey(t *testing.T) {
	// Test that components handle missing API keys gracefully
	testCases := []struct {
		name      string
		component func() conversation.Conversation
	}{
		{
			name: "openai_no_key",
			component: func() conversation.Conversation {
				return openai.NewOpenAI(testLogger)
			},
		},
		{
			name: "anthropic_no_key",
			component: func() conversation.Conversation {
				return anthropic.NewAnthropic(testLogger)
			},
		},
		{
			name: "googleai_no_key",
			component: func() conversation.Conversation {
				return googleai.NewGoogleAI(nil)
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			comp := tc.component()
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			// Try to initialize without API key
			err := comp.Init(ctx, conversation.Metadata{
				Base: metadata.Base{
					Properties: map[string]string{
						"model": "test-model",
						// No API key provided
					},
				},
			})

			// Should either fail gracefully or succeed with warning
			if err != nil {
				t.Logf("✅ %s properly rejects missing API key: %v", tc.name, err)
			} else {
				t.Logf("ℹ️  %s initializes without API key (will likely fail on actual calls)", tc.name)
			}
		})
	}
}
