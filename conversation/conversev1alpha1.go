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
	"google.golang.org/protobuf/types/known/anypb"
)

type ConversationInput struct {
	// Message can be user input prompt/instructions and/or tool call responses.
	Message string `json:"message"`
	Role    Role   `json:"role"`
}

type ConversationRequest struct {
	Inputs              []ConversationInput   `json:"inputs"`
	Parameters          map[string]*anypb.Any `json:"parameters"`
	ConversationContext string                `json:"conversationContext"`
	Temperature         float64               `json:"temperature"`

	// from metadata
	ConversationMetadata
}

type ConversationResult struct {
	Result     string                `json:"result"`
	Parameters map[string]*anypb.Any `json:"parameters"`
}

type ConversationResponse struct {
	ConversationContext string               `json:"conversationContext"`
	Outputs             []ConversationResult `json:"outputs"`
}
