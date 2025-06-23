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
package echo

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/conversation"
	"github.com/dapr/kit/logger"
)

func TestEcho_ToolCalling_SupportsInterface(t *testing.T) {
	e := NewEcho(logger.NewLogger("test"))
	err := e.Init(context.Background(), conversation.Metadata{})
	require.NoError(t, err)

	// Test that echo component implements ToolCallSupport interface
	toolCallSupport, ok := e.(conversation.ToolCallSupport)
	require.True(t, ok, "Echo component should implement ToolCallSupport interface")
	assert.True(t, toolCallSupport.SupportsToolCalling(), "Echo component should support tool calling")
}

func TestEcho_ToolCalling_CustomTools(t *testing.T) {
	e := NewEcho(logger.NewLogger("test"))
	err := e.Init(context.Background(), conversation.Metadata{})
	require.NoError(t, err)

	// Test with custom tools that are NOT hardcoded
	customTools := []conversation.Tool{
		{
			Type: "function",
			Function: conversation.ToolFunction{
				Name:        "send_email",
				Description: "Send an email to someone",
				Parameters: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"to": map[string]any{
							"type":        "string",
							"description": "Email recipient",
						},
						"subject": map[string]any{
							"type":        "string",
							"description": "Email subject",
						},
						"body": map[string]any{
							"type":        "string",
							"description": "Email content",
						},
					},
					"required": []string{"to", "subject", "body"},
				},
			},
		},
		{
			Type: "function",
			Function: conversation.ToolFunction{
				Name:        "create_user",
				Description: "Create a new user account",
				Parameters: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"username": map[string]any{
							"type":        "string",
							"description": "Username for the account",
						},
						"email": map[string]any{
							"type":        "string",
							"description": "Email address",
						},
						"admin": map[string]any{
							"type":        "boolean",
							"description": "Whether user should be admin",
						},
					},
					"required": []string{"username", "email"},
				},
			},
		},
		{
			Type: "function",
			Function: conversation.ToolFunction{
				Name:        "search_database",
				Description: "Search the database for records",
				Parameters: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"query": map[string]any{
							"type":        "string",
							"description": "Search query",
						},
						"limit": map[string]any{
							"type":        "integer",
							"description": "Maximum number of results",
						},
					},
					"required": []string{"query"},
				},
			},
		},
	}

	testCases := []struct {
		name            string
		userMessage     string
		expectedTools   []string
		shouldCallTools bool
	}{
		{
			name:            "Direct tool name mention",
			userMessage:     "Please send_email to my colleague",
			expectedTools:   []string{"send_email"},
			shouldCallTools: true,
		},
		{
			name:            "Keyword matching for email",
			userMessage:     "I need to send an email to john@example.com",
			expectedTools:   []string{"send_email"},
			shouldCallTools: true,
		},
		{
			name:            "Create user request",
			userMessage:     "Create a new user account for Jane",
			expectedTools:   []string{"create_user"},
			shouldCallTools: true,
		},
		{
			name:            "Search database request",
			userMessage:     "Search for all customers in the database",
			expectedTools:   []string{"search_database"},
			shouldCallTools: true,
		},
		{
			name:            "Multiple tools triggered",
			userMessage:     "Create a user and then search the database",
			expectedTools:   []string{"create_user", "search_database"},
			shouldCallTools: true,
		},
		{
			name:            "Explicit tool execution",
			userMessage:     "Please call create_user function",
			expectedTools:   []string{"create_user"},
			shouldCallTools: true,
		},
		{
			name:            "No matching tools",
			userMessage:     "Just having a normal conversation",
			expectedTools:   []string{},
			shouldCallTools: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			req := &conversation.ConversationRequest{
				Inputs: []conversation.ConversationInput{
					{
						Message: tc.userMessage,
						Role:    conversation.RoleUser,
						Tools:   customTools,
					},
				},
			}

			resp, err := e.Converse(context.Background(), req)
			require.NoError(t, err)
			require.NotNil(t, resp)
			require.Len(t, resp.Outputs, 1)

			output := resp.Outputs[0]

			if tc.shouldCallTools {
				assert.Equal(t, "tool_calls", output.FinishReason, "Should trigger tool calls")
				assert.Len(t, output.ToolCalls, len(tc.expectedTools), "Should call expected number of tools")

				// Verify correct tools were called
				calledToolNames := make([]string, len(output.ToolCalls))
				for i, toolCall := range output.ToolCalls {
					calledToolNames[i] = toolCall.Function.Name
					assert.Equal(t, "function", toolCall.Type, "Tool call type should be 'function'")
					assert.NotEmpty(t, toolCall.ID, "Tool call should have an ID")
					assert.NotEmpty(t, toolCall.Function.Arguments, "Tool call should have arguments")

					// Verify arguments are valid JSON
					var args map[string]any
					err := json.Unmarshal([]byte(toolCall.Function.Arguments), &args)
					assert.NoError(t, err, "Tool arguments should be valid JSON")
				}

				for _, expectedTool := range tc.expectedTools {
					assert.Contains(t, calledToolNames, expectedTool, "Should call expected tool: %s", expectedTool)
				}
			} else {
				assert.Equal(t, "stop", output.FinishReason, "Should not trigger tool calls")
				assert.Empty(t, output.ToolCalls, "Should not call any tools")
				assert.Equal(t, tc.userMessage, output.Result, "Should echo the user message")
			}
		})
	}
}

func TestEcho_ToolCalling_ParameterGeneration(t *testing.T) {
	e := NewEcho(logger.NewLogger("test"))
	err := e.Init(context.Background(), conversation.Metadata{})
	require.NoError(t, err)

	// Test parameter generation with different parameter types
	tool := conversation.Tool{
		Type: "function",
		Function: conversation.ToolFunction{
			Name:        "complex_tool",
			Description: "A tool with various parameter types",
			Parameters: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"location": map[string]any{
						"type":        "string",
						"description": "A location",
					},
					"count": map[string]any{
						"type":        "integer",
						"description": "A number",
					},
					"enabled": map[string]any{
						"type":        "boolean",
						"description": "A boolean flag",
					},
					"query": map[string]any{
						"type":        "string",
						"description": "Search query",
					},
				},
				"required": []string{"location", "query"},
			},
		},
	}

	req := &conversation.ConversationRequest{
		Inputs: []conversation.ConversationInput{
			{
				Message: "Use complex_tool to search for data in New York",
				Role:    conversation.RoleUser,
				Tools:   []conversation.Tool{tool},
			},
		},
	}

	resp, err := e.Converse(context.Background(), req)
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Len(t, resp.Outputs, 1)

	output := resp.Outputs[0]
	assert.Equal(t, "tool_calls", output.FinishReason)
	require.Len(t, output.ToolCalls, 1)

	toolCall := output.ToolCalls[0]
	assert.Equal(t, "complex_tool", toolCall.Function.Name)

	// Parse and verify arguments
	var args map[string]any
	err = json.Unmarshal([]byte(toolCall.Function.Arguments), &args)
	require.NoError(t, err)

	// Should have extracted location
	assert.Contains(t, args, "location")
	assert.Equal(t, "New York", args["location"])

	// Should have the query
	assert.Contains(t, args, "query")
	assert.Equal(t, "Use complex_tool to search for data in New York", args["query"])

	// Should have default values for other types
	assert.Contains(t, args, "count")
	assert.Equal(t, float64(42), args["count"]) // JSON unmarshals numbers as float64

	assert.Contains(t, args, "enabled")
	assert.Equal(t, true, args["enabled"])
}

func TestEcho_ToolCalling_JSONStringParameters(t *testing.T) {
	e := NewEcho(logger.NewLogger("test"))
	err := e.Init(context.Background(), conversation.Metadata{})
	require.NoError(t, err)

	// Test with parameters as JSON string (our parameter conversion fix)
	tool := conversation.Tool{
		Type: "function",
		Function: conversation.ToolFunction{
			Name:        "json_param_tool",
			Description: "Tool with JSON string parameters",
			Parameters: `{
				"type": "object",
				"properties": {
					"message": {
						"type": "string",
						"description": "The message to process"
					}
				},
				"required": ["message"]
			}`,
		},
	}

	req := &conversation.ConversationRequest{
		Inputs: []conversation.ConversationInput{
			{
				Message: "Call json_param_tool with this message",
				Role:    conversation.RoleUser,
				Tools:   []conversation.Tool{tool},
			},
		},
	}

	resp, err := e.Converse(context.Background(), req)
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Len(t, resp.Outputs, 1)

	output := resp.Outputs[0]
	assert.Equal(t, "tool_calls", output.FinishReason)
	require.Len(t, output.ToolCalls, 1)

	toolCall := output.ToolCalls[0]
	assert.Equal(t, "json_param_tool", toolCall.Function.Name)

	// Parse and verify arguments
	var args map[string]any
	err = json.Unmarshal([]byte(toolCall.Function.Arguments), &args)
	require.NoError(t, err)

	assert.Contains(t, args, "message")
	assert.Equal(t, "Call json_param_tool with this message", args["message"])
}

func TestEcho_ToolCalling_BasicFlow(t *testing.T) {
	e := NewEcho(logger.NewLogger("test")).(*Echo)
	err := e.Init(context.Background(), conversation.Metadata{})
	require.NoError(t, err)

	// Test basic tool calling flow
	req := &conversation.ConversationRequest{
		Inputs: []conversation.ConversationInput{
			{
				Message: "What's the weather like in San Francisco?",
				Role:    conversation.RoleUser,
				Tools: []conversation.Tool{
					{
						Type: "function",
						Function: conversation.ToolFunction{
							Name:        "get_weather",
							Description: "Get current weather for a location",
							Parameters: map[string]interface{}{
								"type": "object",
								"properties": map[string]interface{}{
									"location": map[string]interface{}{
										"type":        "string",
										"description": "City and state",
									},
								},
								"required": []string{"location"},
							},
						},
					},
				},
			},
		},
	}

	resp, err := e.Converse(context.Background(), req)

	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Len(t, resp.Outputs, 1)

	output := resp.Outputs[0]
	assert.NotEmpty(t, output.ToolCalls, "Expected tool calls in response")
	assert.Equal(t, "tool_calls", output.FinishReason)

	// Verify tool call structure
	toolCall := output.ToolCalls[0]
	assert.Equal(t, "function", toolCall.Type)
	assert.Equal(t, "get_weather", toolCall.Function.Name)
	assert.NotEmpty(t, toolCall.ID)

	// Verify arguments contain location
	var args map[string]interface{}
	err = json.Unmarshal([]byte(toolCall.Function.Arguments), &args)
	require.NoError(t, err)
	assert.Contains(t, args, "location")
	location := args["location"].(string)
	assert.Contains(t, strings.ToLower(location), "san francisco") // Should extract some form of San Francisco
}

func TestEcho_ToolCalling_ToolResultHandling(t *testing.T) {
	e := NewEcho(logger.NewLogger("test")).(*Echo)
	err := e.Init(context.Background(), conversation.Metadata{})
	require.NoError(t, err)

	// Test tool result handling
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

	resp, err := e.Converse(context.Background(), req)

	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Len(t, resp.Outputs, 1)

	output := resp.Outputs[0]
	assert.Contains(t, output.Result, "72")
	assert.Contains(t, output.Result, "sunny")
	assert.Equal(t, "stop", output.FinishReason)
	assert.Empty(t, output.ToolCalls, "Tool result responses shouldn't trigger new tool calls")
}

func TestEcho_ToolCalling_StreamingWithTools(t *testing.T) {
	e := NewEcho(logger.NewLogger("test")).(*Echo)
	err := e.Init(context.Background(), conversation.Metadata{})
	require.NoError(t, err)

	// Test streaming with tool calls
	req := &conversation.ConversationRequest{
		Inputs: []conversation.ConversationInput{
			{
				Message: "What's the weather like in New York?",
				Role:    conversation.RoleUser,
				Tools: []conversation.Tool{
					{
						Type: "function",
						Function: conversation.ToolFunction{
							Name:        "get_weather",
							Description: "Get current weather for a location",
							Parameters:  map[string]interface{}{"type": "object"},
						},
					},
				},
			},
		},
	}

	var chunks [][]byte
	streamFunc := func(ctx context.Context, chunk []byte) error {
		chunks = append(chunks, chunk)
		return nil
	}

	resp, err := e.ConverseStream(context.Background(), req, streamFunc)

	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.NotEmpty(t, chunks, "Expected streaming chunks")

	output := resp.Outputs[0]
	assert.NotEmpty(t, output.ToolCalls, "Expected tool calls in streaming response")
	assert.Equal(t, "tool_calls", output.FinishReason)
}

func TestEcho_ToolCalling_StreamingToolResults(t *testing.T) {
	e := NewEcho(logger.NewLogger("test")).(*Echo)
	err := e.Init(context.Background(), conversation.Metadata{})
	require.NoError(t, err)

	// Test streaming with tool result
	req := &conversation.ConversationRequest{
		Inputs: []conversation.ConversationInput{
			{
				Message:    `{"temperature": 68, "condition": "cloudy", "location": "Seattle"}`,
				Role:       conversation.RoleTool,
				ToolCallID: "call_test_67890",
				Name:       "get_weather",
			},
		},
	}

	var chunks [][]byte
	streamFunc := func(ctx context.Context, chunk []byte) error {
		chunks = append(chunks, chunk)
		return nil
	}

	resp, err := e.ConverseStream(context.Background(), req, streamFunc)

	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.NotEmpty(t, chunks, "Expected streaming chunks for tool result")

	output := resp.Outputs[0]
	assert.Contains(t, output.Result, "68")
	assert.Contains(t, output.Result, "cloudy")
	assert.Equal(t, "stop", output.FinishReason)
	assert.Empty(t, output.ToolCalls, "Tool result responses shouldn't trigger new tool calls")
}

func TestEcho_ToolCalling_NoToolsProvided(t *testing.T) {
	e := NewEcho(logger.NewLogger("test")).(*Echo)
	err := e.Init(context.Background(), conversation.Metadata{})
	require.NoError(t, err)

	// Test weather query without tools
	req := &conversation.ConversationRequest{
		Inputs: []conversation.ConversationInput{
			{
				Message: "What's the weather like in Miami?",
				Role:    conversation.RoleUser,
				// No tools provided
			},
		},
	}

	resp, err := e.Converse(context.Background(), req)

	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Len(t, resp.Outputs, 1)

	output := resp.Outputs[0]
	assert.Empty(t, output.ToolCalls, "Should not trigger tool calls when no tools are provided")
	assert.Equal(t, "stop", output.FinishReason)
	assert.Equal(t, "What's the weather like in Miami?", output.Result) // Should echo the message
}

func TestEcho_ToolCalling_NonWeatherQuery(t *testing.T) {
	e := NewEcho(logger.NewLogger("test")).(*Echo)
	err := e.Init(context.Background(), conversation.Metadata{})
	require.NoError(t, err)

	// Test non-weather query with tools provided
	req := &conversation.ConversationRequest{
		Inputs: []conversation.ConversationInput{
			{
				Message: "Hello, how are you?",
				Role:    conversation.RoleUser,
				Tools: []conversation.Tool{
					{
						Type: "function",
						Function: conversation.ToolFunction{
							Name:        "get_weather",
							Description: "Get current weather for a location",
							Parameters:  map[string]interface{}{"type": "object"},
						},
					},
				},
			},
		},
	}

	resp, err := e.Converse(context.Background(), req)

	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Len(t, resp.Outputs, 1)

	output := resp.Outputs[0]
	assert.Empty(t, output.ToolCalls, "Should not trigger tool calls for non-weather queries")
	assert.Equal(t, "stop", output.FinishReason)
	assert.Equal(t, "Hello, how are you?", output.Result) // Should echo the message
}

func TestEcho_ToolCalling_UsageInformation(t *testing.T) {
	e := NewEcho(logger.NewLogger("test")).(*Echo)
	err := e.Init(context.Background(), conversation.Metadata{})
	require.NoError(t, err)

	// Test that usage information is provided with tool calling
	req := &conversation.ConversationRequest{
		Inputs: []conversation.ConversationInput{
			{
				Message: "What's the temperature in Boston?",
				Role:    conversation.RoleUser,
				Tools: []conversation.Tool{
					{
						Type: "function",
						Function: conversation.ToolFunction{
							Name:        "get_weather",
							Description: "Get current weather for a location",
							Parameters:  map[string]interface{}{"type": "object"},
						},
					},
				},
			},
		},
	}

	resp, err := e.Converse(context.Background(), req)

	require.NoError(t, err)
	require.NotNil(t, resp)

	// Verify usage information is present
	require.NotNil(t, resp.Usage, "Usage information should be present")
	assert.GreaterOrEqual(t, resp.Usage.PromptTokens, int32(0))
	assert.GreaterOrEqual(t, resp.Usage.CompletionTokens, int32(0))
	assert.Equal(t, resp.Usage.TotalTokens, resp.Usage.PromptTokens+resp.Usage.CompletionTokens)
}

func TestEcho_ToolCalling_LocationExtraction(t *testing.T) {
	e := &Echo{}

	testCases := []struct {
		message  string
		expected string
	}{
		{"What's the weather like in San Francisco?", "San Francisco"},
		{"How's the temperature in New York City?", "New York"},
		{"Weather forecast for Miami FL", "Miami"},
		{"Tell me about the weather at Boston", "Boston"},
		{"Temperature for Chicago today", "Chicago"},
		{"What's the weather like?", "Default Location"},
	}

	for _, tc := range testCases {
		t.Run(tc.message, func(t *testing.T) {
			result := e.extractLocation(tc.message)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestEcho_ToolCalling_ToolResultParsing(t *testing.T) {
	e := NewEcho(logger.NewLogger("test")).(*Echo)

	tests := []struct {
		name     string
		input    conversation.ConversationInput
		contains []string
	}{
		{
			name: "valid weather JSON",
			input: conversation.ConversationInput{
				Message: `{"temperature": 75, "condition": "partly cloudy", "location": "Austin"}`,
				Role:    conversation.RoleTool,
			},
			contains: []string{"75", "partly cloudy"},
		},
		{
			name: "invalid JSON",
			input: conversation.ConversationInput{
				Message: "invalid json",
				Role:    conversation.RoleTool,
			},
			contains: []string{"invalid json", "Thanks for the information"},
		},
		{
			name: "incomplete weather data",
			input: conversation.ConversationInput{
				Message: `{"location": "Denver"}`,
				Role:    conversation.RoleTool,
			},
			contains: []string{"Denver", "Thanks for the information"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := e.generateToolResultResponse(tt.input)
			for _, expected := range tt.contains {
				assert.Contains(t, result, expected)
			}
		})
	}
}

func TestEcho_ToolCalling_ParallelWeatherTimeAndCalc(t *testing.T) {
	testLogger := logger.NewLogger("echo-test")
	e := NewEcho(testLogger)

	ctx := context.Background()
	err := e.Init(ctx, conversation.Metadata{})
	require.NoError(t, err)

	// Define the same tools that trigger parallel calling in OpenAI
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

	// Use the exact same prompt that triggered parallel tool calling in OpenAI
	req := &conversation.ConversationRequest{
		Inputs: []conversation.ConversationInput{
			{
				Message: "What's the weather and current time in New York City? Also calculate 23 * 7 for me.",
				Role:    conversation.RoleUser,
				Tools:   []conversation.Tool{weatherTool, timeTool, calcTool},
			},
		},
	}

	resp, err := e.Converse(ctx, req)
	assert.NoError(t, err)
	assert.NotNil(t, resp)
	assert.Len(t, resp.Outputs, 1)

	output := resp.Outputs[0]
	t.Logf("Echo parallel tool calling response - FinishReason: %s", output.FinishReason)
	t.Logf("Echo parallel tool calling response - ToolCalls: %d", len(output.ToolCalls))

	// Echo should also perform parallel tool calling like OpenAI
	assert.True(t, len(output.ToolCalls) >= 2, "Expected multiple tool calls for parallel execution")
	assert.Equal(t, "tool_calls", output.FinishReason)

	// Verify that tool calls have unique IDs
	toolIDs := make(map[string]bool)
	toolNames := make(map[string]bool)
	for _, toolCall := range output.ToolCalls {
		assert.NotEmpty(t, toolCall.ID, "Tool call should have a unique ID")
		assert.False(t, toolIDs[toolCall.ID], "Tool call IDs should be unique")
		toolIDs[toolCall.ID] = true
		toolNames[toolCall.Function.Name] = true

		t.Logf("Tool call: %s (ID: %s) with args: %s", toolCall.Function.Name, toolCall.ID, toolCall.Function.Arguments)
	}

	// Echo should call all relevant tools like OpenAI does
	assert.True(t, len(toolNames) >= 2, "Expected multiple different tools to be called")

	// Verify specific tools were called based on the query
	expectedTools := map[string]bool{
		"get_weather": false,
		"get_time":    false,
		"calculate":   false,
	}

	for toolName := range toolNames {
		if _, exists := expectedTools[toolName]; exists {
			expectedTools[toolName] = true
		}
	}

	// Should call at least 2 of the 3 expected tools
	calledCount := 0
	for _, called := range expectedTools {
		if called {
			calledCount++
		}
	}
	assert.True(t, calledCount >= 2, "Expected at least 2 of the relevant tools to be called")

	t.Logf("✅ Echo successfully performed parallel tool calling (%d tools) like OpenAI", len(output.ToolCalls))
}

func TestEcho_ToolCalling_CaseAgnosticToolNames(t *testing.T) {
	e := NewEcho(logger.NewLogger("test"))
	err := e.Init(context.Background(), conversation.Metadata{})
	require.NoError(t, err)

	// Test tools with different naming conventions
	tools := []conversation.Tool{
		{
			Type: "function",
			Function: conversation.ToolFunction{
				Name:        "sendEmail", // camelCase
				Description: "Send an email",
				Parameters:  map[string]any{"type": "object"},
			},
		},
		{
			Type: "function",
			Function: conversation.ToolFunction{
				Name:        "CreateUser", // PascalCase
				Description: "Create a user",
				Parameters:  map[string]any{"type": "object"},
			},
		},
		{
			Type: "function",
			Function: conversation.ToolFunction{
				Name:        "delete_file", // snake_case
				Description: "Delete a file",
				Parameters:  map[string]any{"type": "object"},
			},
		},
		{
			Type: "function",
			Function: conversation.ToolFunction{
				Name:        "search-database", // kebab-case
				Description: "Search database",
				Parameters:  map[string]any{"type": "object"},
			},
		},
	}

	testCases := []struct {
		name           string
		userMessage    string
		expectedTool   string
		shouldCallTool bool
		description    string
	}{
		// camelCase tool: sendEmail
		{
			name:           "camelCase_exact_match",
			userMessage:    "Please sendEmail to the team",
			expectedTool:   "sendEmail",
			shouldCallTool: true,
			description:    "Exact camelCase match",
		},
		{
			name:           "camelCase_snake_case_user_input",
			userMessage:    "Please send_email to the team",
			expectedTool:   "sendEmail",
			shouldCallTool: true,
			description:    "User uses snake_case for camelCase tool",
		},
		{
			name:           "camelCase_kebab_case_user_input",
			userMessage:    "Please send-email to the team",
			expectedTool:   "sendEmail",
			shouldCallTool: true,
			description:    "User uses kebab-case for camelCase tool",
		},
		{
			name:           "camelCase_space_separated_user_input",
			userMessage:    "Please send email to the team",
			expectedTool:   "sendEmail",
			shouldCallTool: true,
			description:    "User uses space-separated for camelCase tool",
		},

		// PascalCase tool: CreateUser
		{
			name:           "PascalCase_exact_match",
			userMessage:    "Please CreateUser for the system",
			expectedTool:   "CreateUser",
			shouldCallTool: true,
			description:    "Exact PascalCase match",
		},
		{
			name:           "PascalCase_lowercase_user_input",
			userMessage:    "Please createuser for the system",
			expectedTool:   "CreateUser",
			shouldCallTool: true,
			description:    "User uses lowercase for PascalCase tool",
		},
		{
			name:           "PascalCase_snake_case_user_input",
			userMessage:    "Please create_user for the system",
			expectedTool:   "CreateUser",
			shouldCallTool: true,
			description:    "User uses snake_case for PascalCase tool",
		},

		// snake_case tool: delete_file
		{
			name:           "snake_case_exact_match",
			userMessage:    "Please delete_file from storage",
			expectedTool:   "delete_file",
			shouldCallTool: true,
			description:    "Exact snake_case match",
		},
		{
			name:           "snake_case_camelCase_user_input",
			userMessage:    "Please deleteFile from storage",
			expectedTool:   "delete_file",
			shouldCallTool: true,
			description:    "User uses camelCase for snake_case tool",
		},
		{
			name:           "snake_case_space_separated_user_input",
			userMessage:    "Please delete file from storage",
			expectedTool:   "delete_file",
			shouldCallTool: true,
			description:    "User uses space-separated for snake_case tool",
		},

		// kebab-case tool: search-database
		{
			name:           "kebab_case_exact_match",
			userMessage:    "Please search-database for records",
			expectedTool:   "search-database",
			shouldCallTool: true,
			description:    "Exact kebab-case match",
		},
		{
			name:           "kebab_case_snake_case_user_input",
			userMessage:    "Please search_database for records",
			expectedTool:   "search-database",
			shouldCallTool: true,
			description:    "User uses snake_case for kebab-case tool",
		},
		{
			name:           "kebab_case_space_separated_user_input",
			userMessage:    "Please search database for records",
			expectedTool:   "search-database",
			shouldCallTool: true,
			description:    "User uses space-separated for kebab-case tool",
		},

		// Explicit tool execution with different cases
		{
			name:           "explicit_call_mixed_case",
			userMessage:    "Call CreateUser function",
			expectedTool:   "CreateUser",
			shouldCallTool: true,
			description:    "Explicit call with mixed case",
		},
		{
			name:           "explicit_use_snake_case",
			userMessage:    "Use delete_file tool",
			expectedTool:   "delete_file",
			shouldCallTool: true,
			description:    "Explicit use with snake_case",
		},
		{
			name:           "explicit_execute_kebab_case",
			userMessage:    "Execute search-database",
			expectedTool:   "search-database",
			shouldCallTool: true,
			description:    "Explicit execute with kebab-case",
		},

		// No match cases
		{
			name:           "no_tool_match",
			userMessage:    "Just a regular conversation",
			expectedTool:   "",
			shouldCallTool: false,
			description:    "No tool should be called",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			req := &conversation.ConversationRequest{
				Inputs: []conversation.ConversationInput{
					{
						Message: tc.userMessage,
						Role:    conversation.RoleUser,
						Tools:   tools,
					},
				},
			}

			resp, err := e.Converse(context.Background(), req)
			require.NoError(t, err)
			require.NotNil(t, resp)
			require.Len(t, resp.Outputs, 1)

			output := resp.Outputs[0]

			if tc.shouldCallTool {
				assert.Equal(t, "tool_calls", output.FinishReason, "Should trigger tool calls for: %s", tc.description)
				require.Len(t, output.ToolCalls, 1, "Should call exactly one tool for: %s", tc.description)

				toolCall := output.ToolCalls[0]
				assert.Equal(t, tc.expectedTool, toolCall.Function.Name, "Should call correct tool for: %s", tc.description)
				assert.Equal(t, "function", toolCall.Type, "Tool call type should be 'function'")
				assert.NotEmpty(t, toolCall.ID, "Tool call should have an ID")
				assert.NotEmpty(t, toolCall.Function.Arguments, "Tool call should have arguments")

				// Verify arguments are valid JSON
				var args map[string]any
				err := json.Unmarshal([]byte(toolCall.Function.Arguments), &args)
				assert.NoError(t, err, "Tool arguments should be valid JSON")
			} else {
				assert.Equal(t, "stop", output.FinishReason, "Should not trigger tool calls for: %s", tc.description)
				assert.Empty(t, output.ToolCalls, "Should not call any tools for: %s", tc.description)
				assert.Equal(t, tc.userMessage, output.Result, "Should echo the user message for: %s", tc.description)
			}
		})
	}
}

func TestEcho_ToolCalling_ToolNameVariations(t *testing.T) {
	e := &Echo{}

	testCases := []struct {
		toolName           string
		expectedVariations []string
		description        string
	}{
		{
			toolName: "sendEmail",
			expectedVariations: []string{
				"sendemail",  // lowercase original
				"send_email", // snake_case
				"send-email", // kebab-case
				"send email", // space separated
			},
			description: "camelCase tool name",
		},
		{
			toolName: "CreateUser",
			expectedVariations: []string{
				"createuser",  // lowercase original
				"create_user", // snake_case
				"create-user", // kebab-case
				"create user", // space separated
			},
			description: "PascalCase tool name",
		},
		{
			toolName: "delete_file",
			expectedVariations: []string{
				"delete_file", // original (already snake_case)
				"delete-file", // kebab-case
				"delete file", // space separated
			},
			description: "snake_case tool name",
		},
		{
			toolName: "search-database",
			expectedVariations: []string{
				"search-database", // original (already kebab-case)
				"search_database", // snake_case
				"search database", // space separated
			},
			description: "kebab-case tool name",
		},
		{
			toolName: "simpleword",
			expectedVariations: []string{
				"simpleword", // original (no changes needed)
			},
			description: "single word tool name",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			variations := e.generateToolNameVariations(tc.toolName)

			// Check that all expected variations are present
			for _, expected := range tc.expectedVariations {
				assert.Contains(t, variations, expected, "Should contain variation '%s' for tool '%s'", expected, tc.toolName)
			}

			// Check that there are no duplicates
			seen := make(map[string]bool)
			for _, variation := range variations {
				assert.False(t, seen[variation], "Should not have duplicate variation '%s'", variation)
				seen[variation] = true
			}
		})
	}
}
