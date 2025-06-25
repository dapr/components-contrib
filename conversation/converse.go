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
	"errors"
	"fmt"
	"io"
	"strings"

	"google.golang.org/protobuf/types/known/anypb"

	"github.com/dapr/components-contrib/metadata"
)

type Conversation interface {
	metadata.ComponentWithMetadata

	Init(ctx context.Context, meta Metadata) error

	Converse(ctx context.Context, req *ConversationRequest) (*ConversationResponse, error)

	io.Closer
}

// StreamingConversation is an optional interface that conversation components
// can implement to support real-time streaming responses.
type StreamingConversation interface {
	// ConverseStream enables streaming conversation using LangChain Go's WithStreamingFunc.
	// The streamFunc will be called for each chunk of content as it's generated.
	ConverseStream(ctx context.Context, req *ConversationRequest, streamFunc func(ctx context.Context, chunk []byte) error) (*ConversationResponse, error)
}

// ToolCallSupport is an optional interface that conversation components
// can implement to support tool calling functionality.
type ToolCallSupport interface {
	// SupportsToolCalling returns true if the component supports tool calling
	SupportsToolCalling() bool
}

type ConversationInput struct {
	// Deprecated: Use Parts instead for new implementations (text backward compatibility only)
	Message string `json:"string"`
	Role    Role   `json:"role"`

	// NEW: Content parts for rich content within each actor's input
	Parts []ContentPart `json:"parts,omitempty"`
}

type ConversationRequest struct {
	Inputs              []ConversationInput   `json:"inputs"`
	Parameters          map[string]*anypb.Any `json:"parameters"`
	ConversationContext string                `json:"conversationContext"`
	Temperature         float64               `json:"temperature"`

	// from metadata
	Key       string   `json:"key"`
	Model     string   `json:"model"`
	Endpoints []string `json:"endpoints"`
	Policy    string   `json:"loadBalancingPolicy"`
}

type ConversationResult struct {
	// Deprecated: Use Parts instead for new implementations (text backward compatibility only)
	Result     string                `json:"result"`
	Parameters map[string]*anypb.Any `json:"parameters"`

	// NEW: Content parts in response
	Parts        []ContentPart `json:"parts,omitempty"`
	FinishReason string        `json:"finish_reason,omitempty"`
}

type ConversationResponse struct {
	ConversationContext string               `json:"conversationContext"`
	Outputs             []ConversationResult `json:"outputs"`
	Usage               *UsageInfo           `json:"usage,omitempty"`
}

// ContentPart interface for type-safe content handling
type ContentPart interface {
	Type() ContentPartType
	String() string  // For debugging/logging
	Validate() error // For content validation
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

// ToolCallContentPart Tool call content part (assistant generates these)
type ToolCallContentPart struct {
	ID       string           `json:"id"`
	CallType string           `json:"type"` // "function" - renamed to avoid conflict
	Function ToolCallFunction `json:"function"`
}

func (tc ToolCallContentPart) Type() ContentPartType { return ContentPartToolCall }
func (tc ToolCallContentPart) String() string {
	return "ToolCall: " + tc.Function.Name + "(" + tc.Function.Arguments + ")"
}

func (tc ToolCallContentPart) Validate() error {
	if tc.ID == "" {
		return errors.New("tool call ID cannot be empty")
	}
	if tc.Function.Name == "" {
		return errors.New("tool call function name cannot be empty")
	}
	return nil
}

// ToolResultContentPart Tool result content part (tool execution results)
type ToolResultContentPart struct {
	ToolCallID string `json:"tool_call_id"`
	Name       string `json:"name"`
	Content    string `json:"content"`
	IsError    bool   `json:"is_error,omitempty"`
}

func (tr ToolResultContentPart) Type() ContentPartType { return ContentPartToolResult }
func (tr ToolResultContentPart) String() string {
	status := "success"
	if tr.IsError {
		status = "error"
	}
	return "ToolResult[" + tr.ToolCallID + "]: " + tr.Name + " (" + status + ")"
}

func (tr ToolResultContentPart) Validate() error {
	if tr.ToolCallID == "" {
		return errors.New("tool result call ID cannot be empty")
	}
	if tr.Name == "" {
		return errors.New("tool result name cannot be empty")
	}
	return nil
}

// ToolDefinitionsContentPart Tool definitions content part
type ToolDefinitionsContentPart struct {
	Tools []Tool `json:"tools"`
}

func (td ToolDefinitionsContentPart) Type() ContentPartType { return ContentPartToolDefinitions }
func (td ToolDefinitionsContentPart) String() string {
	return "ToolDefinitions: " + fmt.Sprintf("%d tools available", len(td.Tools))
}

func (td ToolDefinitionsContentPart) Validate() error {
	if len(td.Tools) == 0 {
		return errors.New("tool definitions cannot be empty")
	}
	return nil
}

// ToolMessageContentPart Tool message content part (for langchaingo ToolChatMessage compatibility)
type ToolMessageContentPart struct {
	ToolCallID string `json:"tool_call_id"`
	Content    string `json:"content"`
}

func (tm ToolMessageContentPart) Type() ContentPartType { return ContentPartToolMessage }
func (tm ToolMessageContentPart) String() string {
	return "ToolMessage[" + tm.ToolCallID + "]: " + tm.Content
}

func (tm ToolMessageContentPart) Validate() error {
	if tm.ToolCallID == "" {
		return errors.New("tool message call ID cannot be empty")
	}
	return nil
}

// Tool calling types (matching Dapr protobuf structure)
type Tool struct {
	ToolType string       `json:"type"` // Always "function" - renamed to avoid conflict
	Function ToolFunction `json:"function"`
}

type ToolFunction struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	Parameters  any    `json:"parameters"` // JSON schema as map or string
}

type ToolCall struct {
	ID       string           `json:"id"`
	CallType string           `json:"type"` // Always "function" - renamed to avoid conflict
	Function ToolCallFunction `json:"function"`
}

type ToolCallFunction struct {
	Name      string `json:"name"`
	Arguments string `json:"arguments"` // JSON string
}

type Role string

const (
	RoleSystem    = "system"
	RoleUser      = "user"
	RoleAssistant = "assistant"
	RoleFunction  = "function"
	RoleTool      = "tool"
)

// Utility functions for content parts processing

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

// ExtractToolCallsFromParts Extract tool calls from parts (new feature, no legacy compatibility needed)
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

// ExtractToolMessagesFromParts Extract tool messages from parts (langchaingo compatibility)
func ExtractToolMessagesFromParts(parts []ContentPart) []ToolMessageContentPart {
	var messages []ToolMessageContentPart
	for _, part := range parts {
		if messagePart, ok := part.(ToolMessageContentPart); ok {
			messages = append(messages, messagePart)
		}
	}
	return messages
}

// DefaultFinishReason determines the appropriate finish reason based on content parts if no other reason is provided
func DefaultFinishReason(parts []ContentPart) string {
	toolCalls := ExtractToolCallsFromParts(parts)
	if len(toolCalls) > 0 {
		return "tool_calls"
	}
	return "stop"
}
