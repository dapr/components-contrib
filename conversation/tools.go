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
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
)

// ToolCallContentPart Tool call content part (assistant generates these)
type ToolCallContentPart struct {
	ID       string           `json:"id"`
	CallType string           `json:"type"`
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

// ToolResultContentPart Tool result content part (tool execution results for an LLM tool call)
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
	ToolType string       `json:"type"` // Always "function" for now, but in the future it can be other
	Function ToolFunction `json:"function"`
}

type ToolFunction struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	Parameters  any    `json:"parameters"` // JSON schema as map or string
}

type ToolCall struct {
	ID       string           `json:"id"`
	CallType string           `json:"type"`
	Function ToolCallFunction `json:"function"`
}

type ToolCallFunction struct {
	Name      string `json:"name"`
	Arguments string `json:"arguments"` // JSON string
}

// GenerateProviderCompatibleToolCallID generates a tool call ID that is compatible with strict providers
// like Mistral which requires exactly 9 alphanumeric characters
func GenerateProviderCompatibleToolCallID() string {
	// Generate 5 random bytes (10 hex chars), then take first 9
	bytes := make([]byte, 5)
	if _, err := rand.Read(bytes); err != nil {
		// Fallback to a deterministic ID if crypto/rand fails
		// Use a simple counter-based approach for reproducibility in tests
		return "test00001"
	}
	hexStr := hex.EncodeToString(bytes)
	if len(hexStr) >= 9 {
		return hexStr[:9]
	}
	// Pad with zeros if needed (shouldn't happen with 5 bytes)
	return fmt.Sprintf("%09s", hexStr)
}
