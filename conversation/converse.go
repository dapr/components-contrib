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
	"io"
	"time"

	"github.com/tmc/langchaingo/llms"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/dapr/components-contrib/metadata"
)

type Conversation interface {
	metadata.ComponentWithMetadata

	Init(ctx context.Context, meta Metadata) error

	Converse(ctx context.Context, req *Request) (*Response, error)

	io.Closer
}

type Request struct {
	// Message can be user input prompt/instructions and/or tool call responses.
	Message     *[]llms.MessageContent
	Tools       *[]llms.Tool
	ToolChoice  *string
	Temperature float64 `json:"temperature"`

	// Metadata fields that are separate from the actual component metadata fields
	// that get passed to the LLM through the conversation.
	// https://github.com/openai/openai-go/blob/main/chatcompletion.go#L3010
	Metadata                   map[string]string `json:"metadata"`
	ResponseFormatAsJSONSchema map[string]any    `json:"responseFormatAsJsonSchema"`
	PromptCacheRetention       *time.Duration    `json:"promptCacheRetention,omitempty"`

	// TODO: rm these in future PR as they are not used
	Parameters          map[string]*anypb.Any `json:"parameters"`
	ConversationContext string                `json:"conversationContext"`
}

type Response struct {
	Outputs             []Result `json:"outputs"`
	Model               string   `json:"model"`
	ConversationContext string   `json:"conversationContext,omitempty"`
	Usage               *Usage   `json:"usage,omitempty"`
}

type Result struct {
	StopReason string   `json:"stopReason"`
	Choices    []Choice `json:"choices,omitempty"`
}

type Choice struct {
	FinishReason string  `json:"finishReason"`
	Index        int64   `json:"index"`
	Message      Message `json:"message"`
}

// Message represents the content of a choice
// where it can be a text message or a tool call.
type Message struct {
	Content         string           `json:"content,omitempty"`
	ToolCallRequest *[]llms.ToolCall `json:"toolCallRequest,omitempty"`

	// Note: we do not have refusal passed back bc langchain does not support it.
}
