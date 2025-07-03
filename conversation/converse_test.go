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
)

// mockContentPart is a simple implementation for testing purposes
type mockContentPart struct {
	partType ContentPartType
	content  string
}

func (m mockContentPart) Type() ContentPartType { return m.partType }
func (m mockContentPart) String() string        { return m.content }
func (m mockContentPart) Validate() error       { return nil }

func TestExtractTextFromParts(t *testing.T) {
	t.Run("extracts single text part", func(t *testing.T) {
		parts := []ContentPart{TextContentPart{Text: "Hello"}}
		text := ExtractTextFromParts(parts)
		assert.Equal(t, "Hello", text)
	})

	t.Run("extracts multiple text parts", func(t *testing.T) {
		parts := []ContentPart{
			TextContentPart{Text: "Hello"},
			TextContentPart{Text: "world"},
		}
		text := ExtractTextFromParts(parts)
		assert.Equal(t, "Hello world", text)
	})

	t.Run("mixed content parts", func(t *testing.T) {
		parts := []ContentPart{
			TextContentPart{Text: "Hello"},
			mockContentPart{partType: "other", content: "ignore"},
			TextContentPart{Text: "world"},
		}
		text := ExtractTextFromParts(parts)
		assert.Equal(t, "Hello world", text)
	})

	t.Run("no text parts", func(t *testing.T) {
		parts := []ContentPart{
			mockContentPart{partType: "other", content: "ignore"},
		}
		text := ExtractTextFromParts(parts)
		assert.Equal(t, "", text)
	})

	t.Run("empty parts slice", func(t *testing.T) {
		parts := []ContentPart{}
		text := ExtractTextFromParts(parts)
		assert.Equal(t, "", text)
	})
}

func TestExtractToolDefinitionsFromParts(t *testing.T) {
	t.Run("extracts tool definitions", func(t *testing.T) {
		tools := []Tool{{Function: ToolFunction{Name: "get_weather"}}}
		parts := []ContentPart{
			ToolDefinitionsContentPart{Tools: tools},
		}
		extracted := ExtractToolDefinitionsFromParts(parts)
		assert.Equal(t, tools, extracted)
	})

	t.Run("no tool definitions", func(t *testing.T) {
		parts := []ContentPart{
			TextContentPart{Text: "Hello"},
		}
		extracted := ExtractToolDefinitionsFromParts(parts)
		assert.Nil(t, extracted)
	})

	t.Run("multiple tool definition parts (returns first)", func(t *testing.T) {
		tools1 := []Tool{{Function: ToolFunction{Name: "get_weather"}}}
		tools2 := []Tool{{Function: ToolFunction{Name: "get_time"}}}
		parts := []ContentPart{
			ToolDefinitionsContentPart{Tools: tools1},
			ToolDefinitionsContentPart{Tools: tools2},
		}
		extracted := ExtractToolDefinitionsFromParts(parts)
		assert.Equal(t, tools1, extracted)
	})
}

func TestExtractToolCallsFromParts(t *testing.T) {
	t.Run("extracts single tool call", func(t *testing.T) {
		parts := []ContentPart{
			ToolCallContentPart{ID: "1", Function: ToolCallFunction{Name: "get_weather"}},
		}
		calls := ExtractToolCallsFromParts(parts)
		assert.Len(t, calls, 1)
		assert.Equal(t, "get_weather", calls[0].Function.Name)
	})

	t.Run("extracts multiple tool calls", func(t *testing.T) {
		parts := []ContentPart{
			ToolCallContentPart{ID: "1", Function: ToolCallFunction{Name: "get_weather"}},
			ToolCallContentPart{ID: "2", Function: ToolCallFunction{Name: "get_time"}},
		}
		calls := ExtractToolCallsFromParts(parts)
		assert.Len(t, calls, 2)
		assert.Equal(t, "get_weather", calls[0].Function.Name)
		assert.Equal(t, "get_time", calls[1].Function.Name)
	})

	t.Run("no tool calls", func(t *testing.T) {
		parts := []ContentPart{
			TextContentPart{Text: "Hello"},
		}
		calls := ExtractToolCallsFromParts(parts)
		assert.Empty(t, calls)
	})
}

func TestExtractToolResultsFromParts(t *testing.T) {
	t.Run("extracts single tool result", func(t *testing.T) {
		parts := []ContentPart{
			ToolResultContentPart{ToolCallID: "1", Name: "get_weather", Content: "Sunny"},
		}
		results := ExtractToolResultsFromParts(parts)
		assert.Len(t, results, 1)
		assert.Equal(t, "get_weather", results[0].Name)
	})

	t.Run("extracts multiple tool results", func(t *testing.T) {
		parts := []ContentPart{
			ToolResultContentPart{ToolCallID: "1", Name: "get_weather", Content: "Sunny"},
			ToolResultContentPart{ToolCallID: "2", Name: "get_time", Content: "10:00 AM"},
		}
		results := ExtractToolResultsFromParts(parts)
		assert.Len(t, results, 2)
		assert.Equal(t, "get_weather", results[0].Name)
		assert.Equal(t, "get_time", results[1].Name)
	})

	t.Run("no tool results", func(t *testing.T) {
		parts := []ContentPart{
			TextContentPart{Text: "Hello"},
		}
		results := ExtractToolResultsFromParts(parts)
		assert.Empty(t, results)
	})
}

func TestDefaultFinishReason(t *testing.T) {
	t.Run("returns tool_calls when tool calls are present", func(t *testing.T) {
		parts := []ContentPart{
			ToolCallContentPart{ID: "1", Function: ToolCallFunction{Name: "get_weather"}},
		}
		reason := DefaultFinishReason(parts)
		assert.Equal(t, "tool_calls", reason)
	})

	t.Run("returns stop when no tool calls are present", func(t *testing.T) {
		parts := []ContentPart{
			TextContentPart{Text: "Hello"},
		}
		reason := DefaultFinishReason(parts)
		assert.Equal(t, "stop", reason)
	})

	t.Run("returns stop for empty parts", func(t *testing.T) {
		parts := []ContentPart{}
		reason := DefaultFinishReason(parts)
		assert.Equal(t, "stop", reason)
	})
}
