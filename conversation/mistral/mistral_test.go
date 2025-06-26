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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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
		assert.Len(t, converted.Inputs, 3) // Should still have 3 inputs

		// First input should be unchanged (user message)
		assert.Equal(t, req.Inputs[0].Role, converted.Inputs[0].Role)
		assert.Len(t, converted.Inputs[0].Parts, 1)
		textPart, ok := converted.Inputs[0].Parts[0].(conversation.TextContentPart)
		require.True(t, ok)
		assert.Equal(t, "What's the weather like?", textPart.Text)

		// Second input should be unchanged (assistant with tool call)
		assert.Equal(t, req.Inputs[1].Role, converted.Inputs[1].Role)
		assert.Len(t, converted.Inputs[1].Parts, 2)

		// Third input should be converted from tool result to user text message
		assert.Equal(t, req.Inputs[0].Role, converted.Inputs[2].Role) // Should be user role like first input
		assert.Len(t, converted.Inputs[2].Parts, 1)
		convertedTextPart, ok := converted.Inputs[2].Parts[0].(conversation.TextContentPart)
		require.True(t, ok)
		assert.Contains(t, convertedTextPart.Text, "get_weather function returned: Sunny, 72°F")
		assert.Contains(t, convertedTextPart.Text, "Please use this information to respond")
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
		assert.Equal(t, invalidJSON, result) // Should return original string
	})
}
