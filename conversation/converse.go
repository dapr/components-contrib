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

	"google.golang.org/protobuf/types/known/anypb"

	"github.com/dapr/components-contrib/metadata"
)

type Conversation interface {
	metadata.ComponentWithMetadata

	Init(ctx context.Context, meta Metadata) error

	Converse(ctx context.Context, req *ConversationRequest) (*ConversationResponse, error)

	io.Closer
}

type ConversationInput struct {
	Message string `json:"string"`
	Role    Role   `json:"role"`
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
	Result     string                `json:"result"`
	Parameters map[string]*anypb.Any `json:"parameters"`
}

type ConversationResponse struct {
	ConversationContext string               `json:"conversationContext"`
	Outputs             []ConversationResult `json:"outputs"`
}

type Role string

const (
	RoleSystem    = "system"
	RoleUser      = "user"
	RoleAssistant = "assistant"
	RoleFunction  = "function"
	RoleTool      = "tool"
)
