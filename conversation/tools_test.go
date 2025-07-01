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

package conversation

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestToolCallContentPart_Validate(t *testing.T) {
	t.Run("valid tool call", func(t *testing.T) {
		p := ToolCallContentPart{
			ID: "call_123",
			Function: ToolCallFunction{
				Name:      "get_weather",
				Arguments: `{"location": "Seattle"}`,
			},
		}
		require.NoError(t, p.Validate())
	})

	t.Run("missing id", func(t *testing.T) {
		p := ToolCallContentPart{
			Function: ToolCallFunction{
				Name: "get_weather",
			},
		}
		err := p.Validate()
		require.Error(t, err)
		assert.Equal(t, "tool call ID cannot be empty", err.Error())
	})

	t.Run("missing function name", func(t *testing.T) {
		p := ToolCallContentPart{
			ID: "call_123",
			Function: ToolCallFunction{
				Arguments: `{"location": "Seattle"}`,
			},
		}
		err := p.Validate()
		require.Error(t, err)
		assert.Equal(t, "tool call function name cannot be empty", err.Error())
	})
}

func TestToolResultContentPart_Validate(t *testing.T) {
	t.Run("valid tool result", func(t *testing.T) {
		p := ToolResultContentPart{
			ToolCallID: "call_123",
			Name:       "get_weather",
			Content:    "Sunny",
		}
		require.NoError(t, p.Validate())
	})

	t.Run("missing tool call id", func(t *testing.T) {
		p := ToolResultContentPart{
			Name:    "get_weather",
			Content: "Sunny",
		}
		err := p.Validate()
		require.Error(t, err)
		assert.Equal(t, "tool result call ID cannot be empty", err.Error())
	})

	t.Run("missing name", func(t *testing.T) {
		p := ToolResultContentPart{
			ToolCallID: "call_123",
			Content:    "Sunny",
		}
		err := p.Validate()
		require.Error(t, err)
		assert.Equal(t, "tool result name cannot be empty", err.Error())
	})
}

func TestToolDefinitionsContentPart_Validate(t *testing.T) {
	t.Run("valid tool definitions", func(t *testing.T) {
		p := ToolDefinitionsContentPart{
			Tools: []Tool{
				{Function: ToolFunction{Name: "get_weather"}},
			},
		}
		require.NoError(t, p.Validate())
	})

	t.Run("empty tool definitions", func(t *testing.T) {
		p := ToolDefinitionsContentPart{
			Tools: []Tool{},
		}
		err := p.Validate()
		require.Error(t, err)
		assert.Equal(t, "tool definitions cannot be empty", err.Error())
	})
}

func TestToolMessageContentPart_Validate(t *testing.T) {
	t.Run("valid tool message", func(t *testing.T) {
		p := ToolMessageContentPart{
			ToolCallID: "call_123",
			Content:    "some content",
		}
		require.NoError(t, p.Validate())
	})

	t.Run("missing tool call id", func(t *testing.T) {
		p := ToolMessageContentPart{
			Content: "some content",
		}
		err := p.Validate()
		require.Error(t, err)
		assert.Equal(t, "tool message call ID cannot be empty", err.Error())
	})
}

func TestGenerateProviderCompatibleToolCallID(t *testing.T) {
	t.Run("generates correct length id", func(t *testing.T) {
		id := GenerateProviderCompatibleToolCallID()
		assert.Len(t, id, 9, "Generated ID should be 9 characters long")
	})

	t.Run("generates unique ids", func(t *testing.T) {
		id1 := GenerateProviderCompatibleToolCallID()
		id2 := GenerateProviderCompatibleToolCallID()
		assert.NotEqual(t, id1, id2, "Generated IDs should be unique")
	})

	t.Run("generates alphanumeric ids", func(t *testing.T) {
		id := GenerateProviderCompatibleToolCallID()
		assert.Regexp(t, `^[a-f0-9]{9}$`, id, "Generated ID should be alphanumeric")
	})
}
