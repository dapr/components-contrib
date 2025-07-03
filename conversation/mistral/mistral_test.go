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

package mistral

import (
	"testing"

	mistral2 "github.com/gage-technologies/mistral-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tmc/langchaingo/llms"

	"github.com/dapr/components-contrib/conversation"
	"github.com/dapr/kit/logger"
)

func TestMistralToolResultConversion(t *testing.T) {
	mistral := &Mistral{
		logger: logger.NewLogger("mistral-test"),
	}

	t.Run("converts tool results to text", func(t *testing.T) {
		// Create a request with tool results
		req := &conversation.ConversationRequest{
			Inputs: []conversation.ConversationInput{
				{
					Role: conversation.RoleUser,
					Parts: []conversation.ContentPart{
						conversation.TextContentPart{Text: "What's the weather like?"},
					},
				},
				{
					Role: conversation.RoleAssistant,
					Parts: []conversation.ContentPart{
						conversation.TextContentPart{Text: "I'll check the weather for you."},
						conversation.ToolCallContentPart{
							ID:       "call_123",
							CallType: "function",
							Function: conversation.ToolCallFunction{
								Name:      "get_weather",
								Arguments: `{"location":"San Francisco"}`,
							},
						},
					},
				},
				{
					Role: conversation.RoleTool,
					Parts: []conversation.ContentPart{
						conversation.ToolResultContentPart{
							ToolCallID: "call_123",
							Name:       "get_weather",
							Content:    "Sunny, 72°F",
							IsError:    false,
						},
					},
				},
			},
			Temperature: 0.7,
		}

		// Convert the request
		converted := mistral.convertToolResultsToText(req)

		// Verify the conversion
		require.NotNil(t, converted)
		assert.Equal(t, req.Temperature, converted.Temperature)

		// With tool call history present, Mistral creates a fresh context with a single input
		assert.Len(t, converted.Inputs, 1) // Should have 1 input (fresh context)

		// The single input should be a user message with the fresh context
		assert.Equal(t, "user", string(converted.Inputs[0].Role))
		assert.Len(t, converted.Inputs[0].Parts, 1)
		textPart, ok := converted.Inputs[0].Parts[0].(conversation.TextContentPart)
		require.True(t, ok)

		// Should contain both the tool result and the original user request
		assert.Contains(t, textPart.Text, "get_weather result: Sunny, 72°F")
		assert.Contains(t, textPart.Text, "User's original request was: What's the weather like?")
	})

	t.Run("handles error tool results", func(t *testing.T) {
		req := &conversation.ConversationRequest{
			Inputs: []conversation.ConversationInput{
				{
					Role: conversation.RoleTool,
					Parts: []conversation.ContentPart{
						conversation.ToolResultContentPart{
							ToolCallID: "call_456",
							Name:       "get_weather",
							Content:    "Location not found",
							IsError:    true,
						},
					},
				},
			},
		}

		converted := mistral.convertToolResultsToText(req)

		require.NotNil(t, converted)
		assert.Len(t, converted.Inputs, 1)
		assert.Equal(t, conversation.Role("user"), converted.Inputs[0].Role) // Should be converted to user role

		textPart, ok := converted.Inputs[0].Parts[0].(conversation.TextContentPart)
		require.True(t, ok)
		assert.Contains(t, textPart.Text, "get_weather function returned an error: Location not found")
	})

	t.Run("preserves inputs without tool results", func(t *testing.T) {
		req := &conversation.ConversationRequest{
			Inputs: []conversation.ConversationInput{
				{
					Role: conversation.RoleUser,
					Parts: []conversation.ContentPart{
						conversation.TextContentPart{Text: "Hello"},
					},
				},
				{
					Role: conversation.RoleAssistant,
					Parts: []conversation.ContentPart{
						conversation.TextContentPart{Text: "Hi there!"},
					},
				},
			},
		}

		converted := mistral.convertToolResultsToText(req)

		require.NotNil(t, converted)
		assert.Len(t, converted.Inputs, 2)

		// Inputs should be unchanged
		assert.Equal(t, req.Inputs[0].Role, converted.Inputs[0].Role)
		assert.Equal(t, req.Inputs[0].Parts, converted.Inputs[0].Parts)
		assert.Equal(t, req.Inputs[1].Role, converted.Inputs[1].Role)
		assert.Equal(t, req.Inputs[1].Parts, converted.Inputs[1].Parts)
	})

	t.Run("handles multiple tool results in one input", func(t *testing.T) {
		req := &conversation.ConversationRequest{
			Inputs: []conversation.ConversationInput{
				{
					Role: conversation.RoleTool,
					Parts: []conversation.ContentPart{
						conversation.ToolResultContentPart{
							ToolCallID: "call_1",
							Name:       "get_weather",
							Content:    "Sunny, 72°F",
							IsError:    false,
						},
						conversation.ToolResultContentPart{
							ToolCallID: "call_2",
							Name:       "get_time",
							Content:    "3:30 PM",
							IsError:    false,
						},
					},
				},
			},
		}

		converted := mistral.convertToolResultsToText(req)

		require.NotNil(t, converted)
		assert.Len(t, converted.Inputs, 1)
		assert.Equal(t, conversation.Role("user"), converted.Inputs[0].Role) // Should be converted to user role

		textPart, ok := converted.Inputs[0].Parts[0].(conversation.TextContentPart)
		require.True(t, ok)
		assert.Contains(t, textPart.Text, "get_weather function returned: Sunny, 72°F")
		assert.Contains(t, textPart.Text, "get_time function returned: 3:30 PM")
		assert.Contains(t, textPart.Text, "Please use this information to respond")
	})
}

func TestMistralParameterConversion(t *testing.T) {
	mistral := &Mistral{
		logger: logger.NewLogger("mistral-test"),
	}

	t.Run("converts string parameters to map", func(t *testing.T) {
		jsonParams := `{"type":"object","properties":{"location":{"type":"string"}}}`
		result := mistral.convertParametersToMap(jsonParams)

		paramMap, ok := result.(map[string]any)
		require.True(t, ok)
		assert.Equal(t, "object", paramMap["type"])

		properties, ok := paramMap["properties"].(map[string]any)
		require.True(t, ok)
		location, ok := properties["location"].(map[string]any)
		require.True(t, ok)
		assert.Equal(t, "string", location["type"])
	})

	t.Run("preserves map parameters", func(t *testing.T) {
		mapParams := map[string]any{
			"type": "object",
			"properties": map[string]any{
				"location": map[string]any{
					"type": "string",
				},
			},
		}

		result := mistral.convertParametersToMap(mapParams)
		assert.Equal(t, mapParams, result)
	})

	t.Run("handles invalid JSON gracefully", func(t *testing.T) {
		invalidJSON := `{"invalid": json`
		result := mistral.convertParametersToMap(invalidJSON)
		// Should return original string when JSON is invalid
		resultStr, ok := result.(string)
		require.True(t, ok, "result should be a string when JSON is invalid")
		assert.Equal(t, invalidJSON, resultStr) //nolint:testifylint
	})
}

func TestUsageGetter(t *testing.T) {
	t.Run("extracts usage from valid response", func(t *testing.T) {
		// Create proper mistral2.UsageInfo struct
		usage := mistral2.UsageInfo{
			PromptTokens:     10,
			CompletionTokens: 20,
			TotalTokens:      30,
		}

		resp := &llms.ContentResponse{
			Choices: []*llms.ContentChoice{
				{
					GenerationInfo: map[string]any{
						"usage": usage,
					},
				},
			},
		}

		result := usageGetter(resp)
		require.NotNil(t, result)
		assert.Equal(t, uint64(10), result.PromptTokens)
		assert.Equal(t, uint64(20), result.CompletionTokens)
		assert.Equal(t, uint64(30), result.TotalTokens)
	})

	t.Run("returns nil for nil response", func(t *testing.T) {
		result := usageGetter(nil)
		assert.Nil(t, result)
	})

	t.Run("returns nil for response with no choices", func(t *testing.T) {
		resp := &llms.ContentResponse{
			Choices: []*llms.ContentChoice{},
		}

		result := usageGetter(resp)
		assert.Nil(t, result)
	})

	t.Run("returns nil when usage is not found", func(t *testing.T) {
		resp := &llms.ContentResponse{
			Choices: []*llms.ContentChoice{
				{
					GenerationInfo: map[string]any{
						"other_field": "value",
					},
				},
			},
		}

		result := usageGetter(resp)
		assert.Nil(t, result)
	})

	t.Run("returns nil when usage is wrong type", func(t *testing.T) {
		resp := &llms.ContentResponse{
			Choices: []*llms.ContentChoice{
				{
					GenerationInfo: map[string]any{
						"usage": "invalid_type",
					},
				},
			},
		}

		result := usageGetter(resp)
		assert.Nil(t, result)
	})
}

func TestConvertDaprToolsToLangchainTools(t *testing.T) {
	mistral := &Mistral{
		logger: logger.NewLogger("mistral-test"),
	}

	t.Run("converts tools with all fields", func(t *testing.T) {
		tools := []conversation.Tool{
			{
				ToolType: "function",
				Function: conversation.ToolFunction{
					Name:        "get_weather",
					Description: "Get weather information",
					Parameters: map[string]any{
						"type": "object",
						"properties": map[string]any{
							"location": map[string]any{
								"type": "string",
							},
						},
					},
				},
			},
		}

		result := mistral.convertDaprToolsToLangchainTools(tools)
		require.Len(t, result, 1)

		tool := result[0]
		assert.Equal(t, "function", tool.Type)
		assert.Equal(t, "get_weather", tool.Function.Name)
		assert.Equal(t, "Get weather information", tool.Function.Description)

		params, ok := tool.Function.Parameters.(map[string]any)
		require.True(t, ok)
		assert.Equal(t, "object", params["type"])
	})

	t.Run("defaults tool type to function when empty", func(t *testing.T) {
		tools := []conversation.Tool{
			{
				Function: conversation.ToolFunction{
					Name:        "test_tool",
					Description: "Test tool",
					Parameters:  map[string]any{},
				},
			},
		}

		result := mistral.convertDaprToolsToLangchainTools(tools)
		require.Len(t, result, 1)
		assert.Equal(t, "function", result[0].Type)
	})

	t.Run("handles string parameters", func(t *testing.T) {
		tools := []conversation.Tool{
			{
				Function: conversation.ToolFunction{
					Name:       "test_tool",
					Parameters: `{"type":"object","properties":{"param":{"type":"string"}}}`,
				},
			},
		}

		result := mistral.convertDaprToolsToLangchainTools(tools)
		require.Len(t, result, 1)

		params, ok := result[0].Function.Parameters.(map[string]any)
		require.True(t, ok)
		assert.Equal(t, "object", params["type"])
	})

	t.Run("returns nil for empty tools", func(t *testing.T) {
		result := mistral.convertDaprToolsToLangchainTools([]conversation.Tool{})
		assert.Nil(t, result)
	})

	t.Run("returns nil for nil tools", func(t *testing.T) {
		result := mistral.convertDaprToolsToLangchainTools(nil)
		assert.Nil(t, result)
	})
}

func TestGetMistralCompatibleMessages(t *testing.T) {
	mistral := &Mistral{
		logger: logger.NewLogger("mistral-test"),
	}

	t.Run("converts simple text messages", func(t *testing.T) {
		req := &conversation.ConversationRequest{
			Inputs: []conversation.ConversationInput{
				{
					Role: conversation.RoleUser,
					Parts: []conversation.ContentPart{
						conversation.TextContentPart{Text: "Hello"},
					},
				},
				{
					Role: conversation.RoleAssistant,
					Parts: []conversation.ContentPart{
						conversation.TextContentPart{Text: "Hi there!"},
					},
				},
			},
		}

		messages := mistral.getMistralCompatibleMessages(req)
		require.Len(t, messages, 2)

		assert.Equal(t, llms.ChatMessageTypeHuman, messages[0].Role)
		assert.Len(t, messages[0].Parts, 1)
		textPart, ok := messages[0].Parts[0].(llms.TextContent)
		require.True(t, ok)
		assert.Equal(t, "Hello", textPart.Text)

		assert.Equal(t, llms.ChatMessageTypeAI, messages[1].Role)
		assert.Len(t, messages[1].Parts, 1)
		textPart, ok = messages[1].Parts[0].(llms.TextContent)
		require.True(t, ok)
		assert.Equal(t, "Hi there!", textPart.Text)
	})

	t.Run("converts multiple text parts in one message", func(t *testing.T) {
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

		messages := mistral.getMistralCompatibleMessages(req)
		require.Len(t, messages, 1)

		assert.Equal(t, llms.ChatMessageTypeHuman, messages[0].Role)
		assert.Len(t, messages[0].Parts, 1)
		textPart, ok := messages[0].Parts[0].(llms.TextContent)
		require.True(t, ok)
		assert.Equal(t, "Hello World", textPart.Text)
	})

	t.Run("converts assistant message with tool calls", func(t *testing.T) {
		req := &conversation.ConversationRequest{
			Inputs: []conversation.ConversationInput{
				{
					Role: conversation.RoleAssistant,
					Parts: []conversation.ContentPart{
						conversation.TextContentPart{Text: "I'll check the weather."},
						conversation.ToolCallContentPart{
							ID:       "call_123",
							CallType: "function",
							Function: conversation.ToolCallFunction{
								Name:      "get_weather",
								Arguments: `{"location":"SF"}`,
							},
						},
					},
				},
			},
		}

		messages := mistral.getMistralCompatibleMessages(req)
		require.Len(t, messages, 1)

		assert.Equal(t, llms.ChatMessageTypeAI, messages[0].Role)
		assert.Len(t, messages[0].Parts, 2)

		// First part should be text
		textPart, ok := messages[0].Parts[0].(llms.TextContent)
		require.True(t, ok)
		assert.Equal(t, "I'll check the weather.", textPart.Text)

		// Second part should be tool call
		toolCall, ok := messages[0].Parts[1].(llms.ToolCall)
		require.True(t, ok)
		assert.Equal(t, "call_123", toolCall.ID)
		assert.Equal(t, "function", toolCall.Type)
		assert.Equal(t, "get_weather", toolCall.FunctionCall.Name)
		assert.Equal(t, `{"location":"SF"}`, toolCall.FunctionCall.Arguments) //nolint:testifylint
	})

	t.Run("converts tool result messages", func(t *testing.T) {
		req := &conversation.ConversationRequest{
			Inputs: []conversation.ConversationInput{
				{
					Role: conversation.RoleTool,
					Parts: []conversation.ContentPart{
						conversation.ToolResultContentPart{
							ToolCallID: "call_123",
							Name:       "get_weather",
							Content:    "Sunny, 72°F",
							IsError:    false,
						},
					},
				},
			},
		}

		messages := mistral.getMistralCompatibleMessages(req)
		require.Len(t, messages, 1)

		assert.Equal(t, llms.ChatMessageTypeTool, messages[0].Role)
		assert.Len(t, messages[0].Parts, 1)

		toolResponse, ok := messages[0].Parts[0].(llms.ToolCallResponse)
		require.True(t, ok)
		assert.Equal(t, "call_123", toolResponse.ToolCallID)
		assert.Equal(t, "get_weather", toolResponse.Name)
		assert.Equal(t, "Sunny, 72°F", toolResponse.Content)
	})

	t.Run("groups consecutive tool result inputs", func(t *testing.T) {
		req := &conversation.ConversationRequest{
			Inputs: []conversation.ConversationInput{
				{
					Role: conversation.RoleTool,
					Parts: []conversation.ContentPart{
						conversation.ToolResultContentPart{
							ToolCallID: "call_1",
							Name:       "get_weather",
							Content:    "Sunny",
							IsError:    false,
						},
					},
				},
				{
					Role: conversation.RoleTool,
					Parts: []conversation.ContentPart{
						conversation.ToolResultContentPart{
							ToolCallID: "call_2",
							Name:       "get_time",
							Content:    "3:30 PM",
							IsError:    false,
						},
					},
				},
			},
		}

		messages := mistral.getMistralCompatibleMessages(req)
		require.Len(t, messages, 1)

		assert.Equal(t, llms.ChatMessageTypeTool, messages[0].Role)
		assert.Len(t, messages[0].Parts, 2)

		// Should have both tool responses in one message
		toolResponse1, ok := messages[0].Parts[0].(llms.ToolCallResponse)
		require.True(t, ok)
		assert.Equal(t, "call_1", toolResponse1.ToolCallID)

		toolResponse2, ok := messages[0].Parts[1].(llms.ToolCallResponse)
		require.True(t, ok)
		assert.Equal(t, "call_2", toolResponse2.ToolCallID)
	})

	t.Run("skips empty messages", func(t *testing.T) {
		req := &conversation.ConversationRequest{
			Inputs: []conversation.ConversationInput{
				{
					Role:  conversation.RoleUser,
					Parts: []conversation.ContentPart{},
				},
				{
					Role: conversation.RoleUser,
					Parts: []conversation.ContentPart{
						conversation.TextContentPart{Text: "Hello"},
					},
				},
			},
		}

		messages := mistral.getMistralCompatibleMessages(req)
		require.Len(t, messages, 1)

		assert.Equal(t, llms.ChatMessageTypeHuman, messages[0].Role)
		textPart, ok := messages[0].Parts[0].(llms.TextContent)
		require.True(t, ok)
		assert.Equal(t, "Hello", textPart.Text)
	})

	t.Run("handles legacy message field", func(t *testing.T) {
		req := &conversation.ConversationRequest{
			Inputs: []conversation.ConversationInput{
				{
					Role:    conversation.RoleUser,
					Message: "Legacy message",
				},
			},
		}

		messages := mistral.getMistralCompatibleMessages(req)
		require.Len(t, messages, 1)

		assert.Equal(t, llms.ChatMessageTypeHuman, messages[0].Role)
		textPart, ok := messages[0].Parts[0].(llms.TextContent)
		require.True(t, ok)
		assert.Equal(t, "Legacy message", textPart.Text)
	})
}
