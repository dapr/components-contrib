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
	"github.com/tmc/langchaingo/llms"
	"google.golang.org/protobuf/types/known/anypb"
)

type ConversationInputV1Alpha2 struct {
	// Message can be user input prompt/instructions and/or tool call responses.
	Message string `json:"message"`

	// TODO(@Sicoyle): go back and update runtime logic to rm role logic and make these typed and pass the diff types
	// of msg content and just do role stuff here. That does not need to be in runtime, just here for llm provider.
	// Then, this role can just go away i think maybe.
	Role Role `json:"role"`

	ToolCalls []*ConversationInputToolCalls `json:"toolCalls"`
	Refusal   *string                       `json:"refusal,omitempty"`
	ToolId    *string                       `json:"toolId,omitempty"`
}

type ConversationInputToolCalls struct {
	Id string `json:"id"`
	ToolCallFunction
}

type ToolCallFunction struct {
	Name      string  `json:"name"`
	Arguments *string `json:"arguments"` // can be empty
}

// TODO(@Sicoyle): update these fields with new api
type ConversationRequestV1Alpha2 struct {
	Inputs              []ConversationInputV1Alpha2 `json:"inputs"`
	Parameters          map[string]*anypb.Any       `json:"parameters"`
	ConversationContext string                      `json:"conversationContext"`
	Temperature         float64                     `json:"temperature"`

	// from metadata
	ConversationMetadata
}

// TODO(@Sicoyle): update these fields with new api
type ConversationResponseV1Alpha2 struct {
	ConversationContext string               `json:"conversationContext"`
	Outputs             []ConversationResult `json:"outputs"`
}

type ConversationResultV1Alpha2 struct {
	Result     string                `json:"result"`
	Parameters map[string]*anypb.Any `json:"parameters"`

	// ToolCallName is an optional field, that when set,
	// indicates that the parameters contains a tool call request to send back to the client to execute.
	// ToolCallName string `json:"toolCallName"`
	ToolCallRequest []llms.ToolCall
	StopReason      string `json:"stopReason"`
}
