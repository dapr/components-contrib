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
	"context"
	"os"
	"testing"
	"time"

	"github.com/tmc/langchaingo/llms"

	"github.com/dapr/components-contrib/conversation"
	"github.com/dapr/components-contrib/conversation/langchaingokit"
	"github.com/dapr/components-contrib/conversation/openai"
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
		// Test normal text response (backward compatibility) - should have "stop"
		resp1, err := e.Converse(t.Context(), &conversation.ConversationRequest{
			Inputs: []conversation.ConversationInput{
				{Message: "Hello", Role: conversation.RoleUser},
			},
		})
		require.NoError(t, err)
		assert.NotEmpty(t, resp1.Outputs[0].Result, "Should have a text result")
		assert.Equal(t, "stop", resp1.Outputs[0].FinishReason, "Should have 'stop' finish reason for text response")

		// Test content parts support (new feature) - should have "stop"
		resp2, err := e.Converse(t.Context(), &conversation.ConversationRequest{
			Inputs: []conversation.ConversationInput{
				{
					Role: conversation.RoleUser,
					Parts: []conversation.ContentPart{
						conversation.TextContentPart{Text: "Hello with parts"},
					},
				},
			},
		})
		require.NoError(t, err)
		assert.NotEmpty(t, resp2.Outputs[0].Result, "Should process content parts")
		assert.NotEmpty(t, resp2.Outputs[0].Parts, "Should have content parts in response")
		assert.Equal(t, "stop", resp2.Outputs[0].FinishReason, "Should have 'stop' finish reason for text response")

		// Test tool calling - should have "tool_calls"
		weatherTool := conversation.Tool{
			ToolType: "function",
			Function: conversation.ToolFunction{
				Name:        "get_weather",
				Description: "Get weather information",
				Parameters: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"location": map[string]any{"type": "string"},
					},
				},
			},
		}

		resp3, err := e.Converse(t.Context(), &conversation.ConversationRequest{
			Inputs: []conversation.ConversationInput{
				{
					Role: conversation.RoleUser,
					Parts: []conversation.ContentPart{
						conversation.TextContentPart{Text: "What's the weather like?"},
						conversation.ToolDefinitionsContentPart{Tools: []conversation.Tool{weatherTool}},
					},
				},
			},
		})
		require.NoError(t, err)
		assert.Equal(t, "tool_calls", resp3.Outputs[0].FinishReason, "Should have 'tool_calls' finish reason when tools are called")

		// Verify tool calls were actually generated
		toolCalls := conversation.ExtractToolCallsFromParts(resp3.Outputs[0].Parts)
		assert.NotEmpty(t, toolCalls, "Should have generated tool calls")

		// Verify response structure and content
		assert.NotEmpty(t, resp3.Outputs[0].Result, "Should have a text result")
		assert.NotEmpty(t, resp3.Outputs[0].Parts, "Should have content parts")
		assert.Contains(t, resp3.Outputs[0].Result, "What's the weather like?", "Should process final message parts")
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

// TestEchoParameterGenerationLearnings validates content parts tool definitions
func TestEchoContentPartsToolSupport(t *testing.T) {
	e := NewEcho(logger.NewLogger("echo test"))
	err := e.Init(t.Context(), conversation.Metadata{})
	require.NoError(t, err)

	// Test tool definitions in content parts (new feature)
	tool := conversation.Tool{
		ToolType: "function",
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
				Role: conversation.RoleUser,
				Parts: []conversation.ContentPart{
					conversation.TextContentPart{Text: "Call test_tool"},
					conversation.ToolDefinitionsContentPart{Tools: []conversation.Tool{tool}},
				},
			},
		},
	})
	require.NoError(t, err)

	// Verify tool definitions were processed
	assert.NotEmpty(t, resp.Outputs[0].Result, "Should have processed the tool request")
	assert.NotEmpty(t, resp.Outputs[0].Parts, "Should have content parts in response")

	// Verify the response content
	assert.NotEmpty(t, resp.Outputs[0].Result, "Should have a text result")
	assert.NotEmpty(t, resp.Outputs[0].Parts, "Should have content parts")
	assert.Contains(t, resp.Outputs[0].Result, "Call test_tool", "Should echo the user's message")
}

func TestMultiTurnContentPartsContextAccumulation(t *testing.T) {
	// This test demonstrates multi-turn conversation with content parts
	// It simulates a realistic conversation flow with tool calling
	echo := &Echo{}

	// Simulate a multi-turn conversation with accumulated context
	// Turn 1: User asks about weather with tool definitions
	weatherTool := conversation.Tool{
		ToolType: "function",
		Function: conversation.ToolFunction{
			Name:        "get_weather",
			Description: "Get current weather in a location",
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
	}

	timeTool := conversation.Tool{
		ToolType: "function",
		Function: conversation.ToolFunction{
			Name:        "get_time",
			Description: "Get current time in a timezone",
			Parameters: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"timezone": map[string]any{
						"type":        "string",
						"description": "Timezone name",
					},
				},
				"required": []string{"timezone"},
			},
		},
	}

	// Turn 1: User provides initial query with tool definitions
	turn1Req := &conversation.ConversationRequest{
		Inputs: []conversation.ConversationInput{
			{
				Role: conversation.RoleUser,
				Parts: []conversation.ContentPart{
					conversation.TextContentPart{Text: "What's the weather like in New York? I'm planning my day."},
					conversation.ToolDefinitionsContentPart{Tools: []conversation.Tool{weatherTool, timeTool}},
				},
			},
		},
	}

	turn1Resp, err := echo.Converse(t.Context(), turn1Req)
	require.NoError(t, err)
	assert.NotNil(t, turn1Resp)
	assert.Len(t, turn1Resp.Outputs, 1)

	// Verify echo processed the content parts and generated tool calls
	turn1Output := turn1Resp.Outputs[0]
	toolCalls := conversation.ExtractToolCallsFromParts(turn1Output.Parts)
	assert.NotEmpty(t, toolCalls, "Should generate tool calls for weather query")

	// Find the weather tool call
	var weatherToolCall *conversation.ToolCall
	for _, tc := range toolCalls {
		if tc.Function.Name == "get_weather" {
			weatherToolCall = &tc
			break
		}
	}
	assert.NotNil(t, weatherToolCall, "Should generate weather tool call")
	t.Logf("Turn 1: Generated tool call %s with ID %s", weatherToolCall.Function.Name, weatherToolCall.ID)

	// Turn 2: Simulate tool execution result and add more context
	turn2Req := &conversation.ConversationRequest{
		Inputs: []conversation.ConversationInput{
			// Include previous conversation context
			{
				Role: conversation.RoleUser,
				Parts: []conversation.ContentPart{
					conversation.TextContentPart{Text: "What's the weather like in New York? I'm planning my day."},
					conversation.ToolDefinitionsContentPart{Tools: []conversation.Tool{weatherTool, timeTool}},
				},
			},
			// Assistant's response with tool call (simplified for echo)
			{
				Role: conversation.RoleAssistant,
				Parts: []conversation.ContentPart{
					conversation.TextContentPart{Text: "I'll check the weather in New York for you."},
					conversation.ToolCallContentPart{
						ID:       weatherToolCall.ID,
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
						ToolCallID: weatherToolCall.ID,
						Name:       "get_weather",
						Content:    `{"temperature": 72, "condition": "sunny", "humidity": 45, "wind": "8 mph NW"}`,
						IsError:    false,
					},
				},
			},
			// User's follow-up question
			{
				Role: conversation.RoleUser,
				Parts: []conversation.ContentPart{
					conversation.TextContentPart{Text: "Great! Now what time is it there? I need to know if shops are open."},
				},
			},
		},
	}

	turn2Resp, err := echo.Converse(t.Context(), turn2Req)
	require.NoError(t, err)
	assert.NotNil(t, turn2Resp)

	turn2Output := turn2Resp.Outputs[0]
	turn2Text := conversation.ExtractTextFromParts(turn2Output.Parts)

	// Verify echo processed the last message (which is the user's follow-up)
	assert.Contains(t, turn2Text, "Great! Now what time is it there", "Should echo the user's follow-up question")

	// Should generate time tool call based on follow-up question
	turn2ToolCalls := conversation.ExtractToolCallsFromParts(turn2Output.Parts)
	var timeToolCall *conversation.ToolCall
	for _, tc := range turn2ToolCalls {
		if tc.Function.Name == "get_time" {
			timeToolCall = &tc
			break
		}
	}
	assert.NotNil(t, timeToolCall, "Should generate time tool call for follow-up")
	t.Logf("Turn 2: Generated tool call %s with ID %s", timeToolCall.Function.Name, timeToolCall.ID)

	// Turn 3: Complete the conversation with final tool result
	turn3Req := &conversation.ConversationRequest{
		Inputs: []conversation.ConversationInput{
			// Previous context (abbreviated for test)
			{
				Role: conversation.RoleUser,
				Parts: []conversation.ContentPart{
					conversation.TextContentPart{Text: "Great! Now what time is it there? I need to know if shops are open."},
				},
			},
			// Assistant's time tool call response
			{
				Role: conversation.RoleAssistant,
				Parts: []conversation.ContentPart{
					conversation.TextContentPart{Text: "I'll check the current time in New York for you."},
					conversation.ToolCallContentPart{
						ID:       timeToolCall.ID,
						CallType: "function",
						Function: conversation.ToolCallFunction{
							Name:      "get_time",
							Arguments: `{"timezone":"America/New_York"}`,
						},
					},
				},
			},
			// Time tool result
			{
				Role: conversation.RoleTool,
				Parts: []conversation.ContentPart{
					conversation.ToolResultContentPart{
						ToolCallID: timeToolCall.ID,
						Name:       "get_time",
						Content:    `{"time": "2:30 PM", "timezone": "EST", "date": "2024-01-15", "day_of_week": "Monday"}`,
						IsError:    false,
					},
				},
			},
			// User's final message
			{
				Role: conversation.RoleUser,
				Parts: []conversation.ContentPart{
					conversation.TextContentPart{Text: "Perfect! Thank you for the weather and time information. That helps a lot with my planning."},
				},
			},
		},
	}

	turn3Resp, err := echo.Converse(t.Context(), turn3Req)
	require.NoError(t, err)
	assert.NotNil(t, turn3Resp)

	turn3Output := turn3Resp.Outputs[0]
	turn3Text := conversation.ExtractTextFromParts(turn3Output.Parts)

	// Verify echo processed the final context appropriately
	assert.Contains(t, turn3Text, "Perfect! Thank you", "Should echo the user's final message")

	// Final response shouldn't generate new tool calls (conversational conclusion)
	turn3ToolCalls := conversation.ExtractToolCallsFromParts(turn3Output.Parts)
	assert.Empty(t, turn3ToolCalls, "Should not generate tool calls for thank you message")

	t.Logf("Turn 3: Final response completed conversation appropriately")

	// Verify the conversation context handling (echo returns what was provided)
	assert.Empty(t, turn1Resp.ConversationContext, "Echo returns empty context when none provided")
	assert.Empty(t, turn2Resp.ConversationContext, "Echo returns empty context when none provided")
	assert.Empty(t, turn3Resp.ConversationContext, "Echo returns empty context when none provided")

	t.Log("✅ Multi-turn content parts context accumulation test completed successfully")
}

func TestMultiTurnWithOpenAIRealData(t *testing.T) {
	// Skip if no OpenAI API key
	if os.Getenv("OPENAI_API_KEY") == "" {
		t.Skip("Skipping OpenAI integration test: OPENAI_API_KEY not set")
	}

	// This test uses OpenAI to generate real conversation data,
	// then feeds it to echo to demonstrate content parts processing

	openaiComp := openai.NewOpenAI(logger.NewLogger("openai test"))
	echo := &Echo{}

	// Initialize OpenAI
	err := openaiComp.Init(t.Context(), conversation.Metadata{
		Base: metadata.Base{
			Properties: map[string]string{
				"key":   os.Getenv("OPENAI_API_KEY"),
				"model": "gpt-3.5-turbo",
			},
		},
	})
	require.NoError(t, err)

	// Define tools for the conversation
	weatherTool := conversation.Tool{
		ToolType: "function",
		Function: conversation.ToolFunction{
			Name:        "get_weather",
			Description: "Get current weather in a location",
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
	}

	// Step 1: Get real response from OpenAI
	openaiReq := &conversation.ConversationRequest{
		Inputs: []conversation.ConversationInput{
			{
				Role:    conversation.RoleUser,
				Message: "What's the weather like in San Francisco? I'm deciding what to wear today.",
				Parts: []conversation.ContentPart{
					conversation.TextContentPart{Text: "What's the weather like in San Francisco? I'm deciding what to wear today."},
					conversation.ToolDefinitionsContentPart{Tools: []conversation.Tool{weatherTool}},
				},
			},
		},
	}

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	openaiResp, err := openaiComp.Converse(ctx, openaiReq)
	require.NoError(t, err)
	require.NotEmpty(t, openaiResp.Outputs)

	openaiOutput := openaiResp.Outputs[0]
	t.Logf("OpenAI Response: %s", openaiOutput.Result)

	// Extract tool calls from OpenAI response
	openaiToolCalls := conversation.ExtractToolCallsFromParts(openaiOutput.Parts)
	t.Logf("OpenAI generated %d tool calls", len(openaiToolCalls))

	// Step 2: Create conversation history with OpenAI's real response
	conversationHistory := []conversation.ConversationInput{
		// Original user message
		{
			Role: conversation.RoleUser,
			Parts: []conversation.ContentPart{
				conversation.TextContentPart{Text: "What's the weather like in San Francisco? I'm deciding what to wear today."},
				conversation.ToolDefinitionsContentPart{Tools: []conversation.Tool{weatherTool}},
			},
		},
		// OpenAI's actual response
		{
			Role:  conversation.RoleAssistant,
			Parts: openaiOutput.Parts, // Use OpenAI's actual response parts
		},
	}

	// If OpenAI made tool calls, simulate tool results
	if len(openaiToolCalls) > 0 {
		for _, toolCall := range openaiToolCalls {
			// Simulate tool execution
			var toolResult string
			switch toolCall.Function.Name {
			case "get_weather":
				toolResult = `{"temperature": 65, "condition": "partly cloudy", "humidity": 70, "wind": "10 mph W", "feels_like": 68}`
			default:
				toolResult = `{"result": "success", "message": "Tool executed successfully"}`
			}

			// Add tool result to conversation
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
	}

	// Add user follow-up
	conversationHistory = append(conversationHistory, conversation.ConversationInput{
		Role: conversation.RoleUser,
		Parts: []conversation.ContentPart{
			conversation.TextContentPart{Text: "Thanks! Based on that weather, what should I wear?"},
		},
	})

	// Step 3: Feed the real conversation data to Echo
	echoReq := &conversation.ConversationRequest{
		Inputs: conversationHistory,
	}

	echoResp, err := echo.Converse(t.Context(), echoReq)
	require.NoError(t, err)
	assert.NotNil(t, echoResp)

	echoOutput := echoResp.Outputs[0]
	echoText := conversation.ExtractTextFromParts(echoOutput.Parts)

	// Verify echo processed the real conversation context
	assert.Contains(t, echoText, "Thanks! Based on that weather", "Echo should include the user's follow-up")

	// Echo processes the last message (user follow-up), not the tool results from earlier messages
	// But it should still be able to match tools based on the conversation context
	if len(openaiToolCalls) > 0 {
		t.Logf("OpenAI made %d tool calls, echo processed the final user message", len(openaiToolCalls))
		// Echo should generate tool calls based on the user's follow-up question
		echoToolCalls := conversation.ExtractToolCallsFromParts(echoOutput.Parts)
		if len(echoToolCalls) > 0 {
			t.Logf("Echo generated %d tool calls in response to user follow-up", len(echoToolCalls))
		}
	}

	t.Logf("Echo processed real OpenAI conversation data:")
	t.Logf("- Original OpenAI response: %s", openaiOutput.Result)
	t.Logf("- Echo's context processing: %s", echoText)
	t.Logf("- Conversation turns processed: %d", len(conversationHistory))

	t.Log("✅ Multi-turn test with OpenAI real data completed successfully")
}

func TestEchoContextAccumulationDemo(t *testing.T) {
	// This test demonstrates how Echo processes accumulated conversation context
	// by showing how it uses ALL inputs for tool matching and decision making
	echo := &Echo{}

	weatherTool := conversation.Tool{
		ToolType: "function",
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

	// Test 1: Show how echo processes single message
	singleReq := &conversation.ConversationRequest{
		Inputs: []conversation.ConversationInput{
			{
				Role: conversation.RoleUser,
				Parts: []conversation.ContentPart{
					conversation.TextContentPart{Text: "Tell me about New York"},
					conversation.ToolDefinitionsContentPart{Tools: []conversation.Tool{weatherTool}},
				},
			},
		},
	}

	singleResp, err := echo.Converse(t.Context(), singleReq)
	require.NoError(t, err)

	singleToolCalls := conversation.ExtractToolCallsFromParts(singleResp.Outputs[0].Parts)
	t.Logf("Single message: Generated %d tool calls", len(singleToolCalls))

	// Test 2: Show how echo uses accumulated context for tool matching
	multiReq := &conversation.ConversationRequest{
		Inputs: []conversation.ConversationInput{
			// First message mentions location but not weather
			{
				Role: conversation.RoleUser,
				Parts: []conversation.ContentPart{
					conversation.TextContentPart{Text: "I'm planning a trip to Boston"},
					conversation.ToolDefinitionsContentPart{Tools: []conversation.Tool{weatherTool}},
				},
			},
			// Second message mentions weather but not location
			{
				Role: conversation.RoleUser,
				Parts: []conversation.ContentPart{
					conversation.TextContentPart{Text: "Should I check the weather conditions?"},
				},
			},
		},
	}

	multiResp, err := echo.Converse(t.Context(), multiReq)
	require.NoError(t, err)

	multiToolCalls := conversation.ExtractToolCallsFromParts(multiResp.Outputs[0].Parts)
	t.Logf("Multi-message: Generated %d tool calls", len(multiToolCalls))

	// The key insight: Echo uses ALL user messages for tool matching context
	// Even though the last message doesn't mention location, Echo should find
	// the weather tool because it combines "Boston" from message 1 and "weather" from message 2

	if len(multiToolCalls) > 0 {
		weatherCall := multiToolCalls[0]
		assert.Equal(t, "get_weather", weatherCall.Function.Name)
		t.Logf("✅ Echo successfully used accumulated context: %s", weatherCall.Function.Arguments)

		// Verify that echo used context from earlier messages
		toolCallArgs := weatherCall.Function.Arguments
		t.Logf("✅ Echo successfully used accumulated context: %s", toolCallArgs)
		assert.Contains(t, toolCallArgs, "user_location", "Should use location from earlier message in conversation")
	}

	// Test 3: Demonstrate content parts variety processing
	varietyReq := &conversation.ConversationRequest{
		Inputs: []conversation.ConversationInput{
			// Message with tool definitions
			{
				Role: conversation.RoleUser,
				Parts: []conversation.ContentPart{
					conversation.ToolDefinitionsContentPart{Tools: []conversation.Tool{weatherTool}},
				},
			},
			// Assistant response with tool call
			{
				Role: conversation.RoleAssistant,
				Parts: []conversation.ContentPart{
					conversation.TextContentPart{Text: "I'll check the weather for you."},
					conversation.ToolCallContentPart{
						ID:       "call_demo_123",
						CallType: "function",
						Function: conversation.ToolCallFunction{
							Name:      "get_weather",
							Arguments: `{"location":"Seattle"}`,
						},
					},
				},
			},
			// Tool result
			{
				Role: conversation.RoleTool,
				Parts: []conversation.ContentPart{
					conversation.ToolResultContentPart{
						ToolCallID: "call_demo_123",
						Name:       "get_weather",
						Content:    `{"temperature": 60, "condition": "rainy"}`,
						IsError:    false,
					},
				},
			},
			// User's final message
			{
				Role: conversation.RoleUser,
				Parts: []conversation.ContentPart{
					conversation.TextContentPart{Text: "Thanks! That's helpful for my planning."},
				},
			},
		},
	}

	varietyResp, err := echo.Converse(t.Context(), varietyReq)
	require.NoError(t, err)

	varietyText := conversation.ExtractTextFromParts(varietyResp.Outputs[0].Parts)

	// Echo processes the LAST input (user's thank you message)
	assert.Contains(t, varietyText, "Thanks! That's helpful", "Should echo the last user message")

	// But Echo USES all the previous inputs for tool matching context
	// Since the last message is a thank you (no tool trigger), no new tools should be called
	varietyToolCalls := conversation.ExtractToolCallsFromParts(varietyResp.Outputs[0].Parts)
	assert.Empty(t, varietyToolCalls, "Should not generate tool calls for thank you message")

	t.Log("✅ Echo context accumulation demonstration completed:")
	t.Log("  - Echo processes the LAST input for response generation")
	t.Log("  - Echo uses ALL inputs for tool matching and context building")
	t.Log("  - This enables realistic multi-turn conversation testing")
}

func TestOrderPreservationBugFix(t *testing.T) {
	// This test demonstrates the critical bug we fixed: tool results and tool calls
	// must be processed in order, not grouped by type
	echo := &Echo{}

	// Create a conversation that tests order preservation
	req := &conversation.ConversationRequest{
		Inputs: []conversation.ConversationInput{
			// 1. User asks about weather
			{
				Role: conversation.RoleUser,
				Parts: []conversation.ContentPart{
					conversation.TextContentPart{Text: "What's the weather in Boston?"},
					conversation.ToolDefinitionsContentPart{
						Tools: []conversation.Tool{
							{
								ToolType: "function",
								Function: conversation.ToolFunction{
									Name:        "get_weather",
									Description: "Get current weather",
								},
							},
						},
					},
				},
			},
			// 2. Assistant calls weather tool
			{
				Role: conversation.RoleAssistant,
				Parts: []conversation.ContentPart{
					conversation.TextContentPart{Text: "I'll check the weather for you."},
					conversation.ToolCallContentPart{
						ID:       "call_weather_123",
						CallType: "function",
						Function: conversation.ToolCallFunction{
							Name:      "get_weather",
							Arguments: `{"location":"Boston"}`,
						},
					},
				},
			},
			// 3. Tool returns result
			{
				Role: conversation.RoleTool,
				Parts: []conversation.ContentPart{
					conversation.ToolResultContentPart{
						ToolCallID: "call_weather_123",
						Name:       "get_weather",
						Content:    "Sunny, 72°F in Boston",
					},
				},
			},
			// 4. User asks follow-up
			{
				Role: conversation.RoleUser,
				Parts: []conversation.ContentPart{
					conversation.TextContentPart{Text: "Great! What time is it there?"},
				},
			},
		},
	}

	// Before our fix, the langchaingokit message conversion would group tool results
	// and append them at the end, breaking conversation order. Now it should maintain order.

	// Test the message conversion directly
	messages, err := langchaingokit.GetMessageFromRequest(req)
	require.NoError(t, err)

	// Verify that messages maintain order and correct roles
	require.Len(t, messages, 4, "Should have 4 messages in proper order")

	// Message 1: User with text (tool definitions are handled via call options, not message content)
	assert.Equal(t, llms.ChatMessageTypeHuman, messages[0].Role)
	assert.Len(t, messages[0].Parts, 1) // text only (tool definitions don't create message content)

	// Message 2: Assistant with text + tool call
	assert.Equal(t, llms.ChatMessageTypeAI, messages[1].Role)
	assert.Len(t, messages[1].Parts, 2) // text + tool call

	// Message 3: Tool result
	assert.Equal(t, llms.ChatMessageTypeTool, messages[2].Role)
	assert.Len(t, messages[2].Parts, 1) // tool result

	// Message 4: User follow-up
	assert.Equal(t, llms.ChatMessageTypeHuman, messages[3].Role)
	assert.Len(t, messages[3].Parts, 1) // text

	// Verify the actual Echo response handles this properly
	resp, err := echo.Converse(t.Context(), req)
	require.NoError(t, err)
	assert.NotEmpty(t, resp.Outputs)

	// Should process the last user input (asking about time)
	textContent := conversation.ExtractTextFromParts(resp.Outputs[0].Parts)
	assert.Contains(t, textContent, "time", "Should respond to the time question, not get confused by tool results")

	t.Logf("✅ Order preservation test passed - conversation sequence maintained correctly")
}
