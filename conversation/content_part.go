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

// ConversationContent interface for type-safe content handling
type ConversationContent interface {
	Type() ContentType
	String() string
	Validate() error
}

type ContentType string

const (
	ContentPartText            ContentType = "text"
	ContentPartToolCall        ContentType = "tool_call"
	ContentPartToolResult      ContentType = "tool_result"
	ContentPartToolDefinitions ContentType = "tool_definitions"
	ContentPartToolMessage     ContentType = "tool_message"
	// Future: ContentPartImage, ContentPartDocument, etc.
)

// TextContentPart text content part implementation
type TextContentPart struct {
	Text string `json:"text"`
}

func (t TextContentPart) Type() ContentType { return ContentPartText }
func (t TextContentPart) String() string    { return t.Text }
func (t TextContentPart) Validate() error {
	if t.Text == "" {
		return nil // Allow empty text parts
	}
	return nil
}

// ExtractTextFromParts Extract all text content from parts
func ExtractTextFromParts(parts []ConversationContent) string {
	var textParts []string
	for _, part := range parts {
		if textPart, ok := part.(TextContentPart); ok {
			textParts = append(textParts, textPart.Text)
		}
	}
	return strings.Join(textParts, " ")
}

// ExtractToolDefinitionsFromParts Extract tool definitions from parts
func ExtractToolDefinitionsFromParts(parts []ConversationContent) []Tool {
	for _, part := range parts {
		if toolDefPart, ok := part.(ToolDefinitionsContentPart); ok {
			return toolDefPart.Tools
		}
	}
	return nil
}

// ExtractToolCallsFromParts Extract tool calls from parts
func ExtractToolCallsFromParts(parts []ConversationContent) []ToolCall {
	var toolCalls []ToolCall
	for _, part := range parts {
		if toolCallPart, ok := part.(ToolCallRequest); ok {
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
func ExtractToolResultsFromParts(parts []ConversationContent) []ToolCallResponse {
	var results []ToolCallResponse
	for _, part := range parts {
		if resultPart, ok := part.(ToolCallResponse); ok {
			results = append(results, resultPart)
		}
	}
	return results
}
