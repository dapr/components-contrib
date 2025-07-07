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

// Package conversation provides interfaces and types for conversation components.
//
// This package includes content parts support for rich conversation content including
// tool calling, tool results, and tool definitions. Some implementations include
// temporary workarounds for langchaingo compatibility - see langchaingokit/LANGCHAINGO_WORKAROUNDS.md
// for details.
package conversation

import (
	"context"
	"io"

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

	// Content parts for rich content within each LLM request/response
	Parts []ContentPart `json:"parts,omitempty"`
}

type ConversationRequest struct {
	Inputs              []ConversationInput   `json:"inputs"`
	Parameters          map[string]*anypb.Any `json:"parameters"`
	ConversationContext string                `json:"conversationContext"`
	Temperature         float64               `json:"temperature"`
	MaxTokens           int                   `json:"maxTokens"`
	Tools               []Tool                `json:"tools,omitempty"`

	// from metadata
	Key       string   `json:"key"`
	Model     string   `json:"model"`
	Endpoints []string `json:"endpoints"`
	Policy    string   `json:"loadBalancingPolicy"`
}

type ConversationOutput struct {
	// Deprecated: Use Parts instead for new implementations (text backward compatibility only)
	Result     string                `json:"result"`
	Parameters map[string]*anypb.Any `json:"parameters"`

	// Content parts in response
	Parts        []ContentPart `json:"parts,omitempty"`
	FinishReason string        `json:"finish_reason,omitempty"`
}

type ConversationResponse struct {
	ConversationContext string `json:"conversationContext"`
	// Outputs is the list of outputs from the LLM. Usually there is only one output. This is more like candidates in Google AI.
	// each output can have multiple parts (for example, a list of tool calls, text, etc.)
	Outputs []ConversationOutput `json:"outputs"`
	Usage   *UsageInfo           `json:"usage,omitempty"`
}

// Role represents a conversation role
type Role string

const (
	RoleSystem    = "system"
	RoleUser      = "user"
	RoleAssistant = "assistant"
	RoleFunction  = "function"
	RoleTool      = "tool"
)

// DefaultFinishReason determines the appropriate finish reason based on content parts if no other reason is provided
func DefaultFinishReason(parts []ContentPart) string {
	toolCalls := ExtractToolCallsFromParts(parts)
	if len(toolCalls) > 0 {
		return "tool_calls"
	}
	return "stop"
}
