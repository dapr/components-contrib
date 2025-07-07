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

import "strings"

// ContentPart interface for type-safe content handling
type ContentPart interface {
	Type() ContentPartType
	String() string
	Validate() error
}

type ContentPartType string

const (
	ContentPartText            ContentPartType = "text"
	ContentPartToolCall        ContentPartType = "tool_call"
	ContentPartToolResult      ContentPartType = "tool_result"
	ContentPartToolDefinitions ContentPartType = "tool_definitions"
	ContentPartToolMessage     ContentPartType = "tool_message"
	// Future: ContentPartImage, ContentPartDocument, etc.
)

// TextContentPart text content part implementation
type TextContentPart struct {
	Text string `json:"text"`
}

func (t TextContentPart) Type() ContentPartType { return ContentPartText }
func (t TextContentPart) String() string        { return t.Text }
func (t TextContentPart) Validate() error {
	if t.Text == "" {
		return nil // Allow empty text parts
	}
	return nil
}

// ExtractTextFromParts Extract all text content from parts
func ExtractTextFromParts(parts []ContentPart) string {
	var textParts []string
	for _, part := range parts {
		if textPart, ok := part.(TextContentPart); ok {
			textParts = append(textParts, textPart.Text)
		}
	}
	return strings.Join(textParts, " ")
}

// ExtractToolDefinitionsFromParts Extract tool definitions from parts
func ExtractToolDefinitionsFromParts(parts []ContentPart) []Tool {
	for _, part := range parts {
		if toolDefPart, ok := part.(ToolDefinitionsContentPart); ok {
			return toolDefPart.Tools
		}
	}
	return nil
}

// ExtractToolCallsFromParts Extract tool calls from parts
func ExtractToolCallsFromParts(parts []ContentPart) []ToolCall {
	var toolCalls []ToolCall
	for _, part := range parts {
		if toolCallPart, ok := part.(ToolCallContentPart); ok {
			toolCalls = append(toolCalls, ToolCall{
				ID:       toolCallPart.ID,
				CallType: toolCallPart.CallType,
				Function: toolCallPart.Function,
			})
		}
	}
	return toolCalls
}

// ExtractToolResultsFromParts Extract tool results from parts
func ExtractToolResultsFromParts(parts []ContentPart) []ToolResultContentPart {
	var results []ToolResultContentPart
	for _, part := range parts {
		if resultPart, ok := part.(ToolResultContentPart); ok {
			results = append(results, resultPart)
		}
	}
	return results
}
