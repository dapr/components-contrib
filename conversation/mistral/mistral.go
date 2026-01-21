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
	"errors"
	"reflect"

	"github.com/tmc/langchaingo/llms"
	"github.com/tmc/langchaingo/llms/mistral"

	"github.com/dapr/components-contrib/conversation"
	"github.com/dapr/components-contrib/conversation/langchaingokit"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
	kmeta "github.com/dapr/kit/metadata"
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

	if md.Key == "" {
		return errors.New("mistral api key is required")
	}

	// Resolve model via central helper (uses metadata, then env var, then default)
	model := conversation.GetMistralModel(md.Model)
	options := []mistral.Option{
		mistral.WithModel(model),
		mistral.WithAPIKey(md.Key),
	}

	// NOTE: Mistral has a WithTimeout option; however, it is not used or added to the mistral client so we do not add it,
	// and this is another case of Mistral being an outlier for the conversation components.

	llm, err := mistral.New(options...)
	if err != nil {
		return err
	}

	m.LLM.Model = llm

	if md.ResponseCacheTTL != nil {
		cachedModel, cacheErr := conversation.CacheResponses(ctx, md.ResponseCacheTTL, m.LLM.Model)
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

// CreateToolCallPart creates mistral and ollama api compatible tool call messages.
// This is a wrapper around langchaingokit.CreateToolCallPart for runtime compatibility.
// TODO: rm this in future PR to use the langchaingokit.CreateToolCallPart directly.
func CreateToolCallPart(toolCall *llms.ToolCall) llms.ContentPart {
	return langchaingokit.CreateToolCallPart(toolCall)
}

// CreateToolResponseMessage creates mistral and ollama api compatible tool response message.
// This is a wrapper around langchaingokit.CreateToolResponseMessage for runtime compatibility.
// TODO: rm this in future PR to use the langchaingokit.CreateToolResponseMessage directly.
func CreateToolResponseMessage(responses ...llms.ContentPart) llms.MessageContent {
	return langchaingokit.CreateToolResponseMessage(responses...)
}
