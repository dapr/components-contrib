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

type ConversationInputToolCalls struct {
	Id string `json:"id"`
	ToolCallFunction
}

type ToolCallFunction struct {
	Name      string  `json:"name"`
	Arguments *string `json:"arguments"` // can be empty
}

type ConversationRequestV1Alpha2 struct {
	// Message can be user input prompt/instructions and/or tool call responses.
	Message             *[]llms.MessageContent
	Tools               *[]llms.Tool
	Parameters          map[string]*anypb.Any `json:"parameters"`
	ConversationContext string                `json:"conversationContext"`
	Temperature         float64               `json:"temperature"`

	// from metadata
	ConversationMetadata
}

// TODO: Double check if i need these fields given the api updates i made
type ConversationResponseV1Alpha2 struct {
	ConversationContext string                       `json:"conversationContext"`
	Outputs             []ConversationResultV1Alpha2 `json:"outputs"`
}

type ConversationResultV1Alpha2 struct {
	Result          string                `json:"result"`
	Parameters      map[string]*anypb.Any `json:"parameters"`
	ToolCallRequest []llms.ToolCall
	StopReason      string `json:"stopReason"`
}
