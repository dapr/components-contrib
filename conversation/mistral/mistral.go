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
package mistral

import (
	"context"
	"reflect"
	"strings"

	"github.com/dapr/components-contrib/conversation"
	"github.com/dapr/components-contrib/conversation/langchaingokit"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
	kmeta "github.com/dapr/kit/metadata"

	"github.com/tmc/langchaingo/llms"
	"github.com/tmc/langchaingo/llms/mistral"
)

type Mistral struct {
	langchaingokit.LLM

	logger logger.Logger
}

func NewMistral(logger logger.Logger) conversation.Conversation {
	m := &Mistral{
		logger: logger,
	}

	return m
}

func (m *Mistral) Init(ctx context.Context, meta conversation.Metadata) error {
	md := conversation.LangchainMetadata{}
	err := kmeta.DecodeMetadata(meta.Properties, &md)
	if err != nil {
		return err
	}

	// Resolve model via central helper (uses metadata, then env var, then default)
	model := conversation.GetMistralModel(md.Model)

	llm, err := mistral.New(
		mistral.WithModel(model),
		mistral.WithAPIKey(md.Key),
	)
	if err != nil {
		return err
	}

	m.LLM.Model = llm

	if md.CacheTTL != "" {
		cachedModel, cacheErr := conversation.CacheModel(ctx, md.CacheTTL, m.LLM.Model)
		if cacheErr != nil {
			return cacheErr
		}

		m.LLM.Model = cachedModel
	}
	return nil
}

func (m *Mistral) GetComponentMetadata() (metadataInfo metadata.MetadataMap) {
	metadataStruct := conversation.LangchainMetadata{}
	metadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, metadata.ConversationType)
	return
}

func (m *Mistral) Close() error {
	return nil
}

// CreateToolCallPart creates mistral api compatible tool call messages.
// Most LLM providers can handle tool calls using the tool call object;
// however, mistral requires it as text in conversation history.
func CreateToolCallPart(toolCall *llms.ToolCall) llms.ContentPart {
	if toolCall == nil {
		return nil
	}

	if toolCall.FunctionCall == nil {
		return llms.TextContent{
			Text: "Tool call [ID: " + toolCall.ID + "]: <no function call>",
		}
	}

	return llms.TextContent{
		Text: "Tool call [ID: " + toolCall.ID + "]: " + toolCall.FunctionCall.Name + "(" + toolCall.FunctionCall.Arguments + ")",
	}
}

// CreateToolResponseMessage creates mistral api compatible tool response message
// using the human role specifically otherwise mistral will reject the tool response message.
// Most LLM providers can handle tool call responses using the tool call response object;
// however, mistral requires it as text in conversation history.
func CreateToolResponseMessage(responses ...llms.ContentPart) llms.MessageContent {
	msg := llms.MessageContent{
		Role: llms.ChatMessageTypeHuman,
	}
	if len(responses) == 0 {
		return msg
	}
	var toolID, name string

	mistralContentParts := make([]string, 0, len(responses))
	for _, response := range responses {
		if resp, ok := response.(llms.ToolCallResponse); ok {
			if toolID == "" {
				toolID = resp.ToolCallID
			}
			if name == "" {
				name = resp.Name
			}
			mistralContentParts = append(mistralContentParts, resp.Content)
		}
	}
	if len(mistralContentParts) > 0 {
		msg.Parts = []llms.ContentPart{
			llms.TextContent{
				Text: "Tool response [ID: " + toolID + ", Name: " + name + "]: " + strings.Join(mistralContentParts, "\n"),
			},
		}
	}
	return msg
}
