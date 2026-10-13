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
package anthropic

import (
	"context"
	"errors"
	"reflect"
	"strings"

	"github.com/dapr/components-contrib/conversation"
	"github.com/dapr/components-contrib/conversation/langchaingokit"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
	kmeta "github.com/dapr/kit/metadata"

	"github.com/tmc/langchaingo/llms"
	"github.com/tmc/langchaingo/llms/anthropic"
)

const apiTypeFoundry = "foundry"

type Anthropic struct {
	langchaingokit.LLM

	logger logger.Logger
}

func NewAnthropic(logger logger.Logger) conversation.Conversation {
	a := &Anthropic{
		logger: logger,
		LLM:    langchaingokit.New(logger),
	}
	a.SetProvider(langchaingokit.ProviderAnthropic)

	return a
}

func (a *Anthropic) buildClientOptions(md AnthropicLangchainMetadata) (string, []anthropic.Option, error) {
	model := conversation.GetAnthropicModel(md.Model)

	options := []anthropic.Option{
		anthropic.WithModel(model),
		anthropic.WithToken(md.Key),
	}

	if strings.EqualFold(md.APIType, apiTypeFoundry) {
		if md.Endpoint == "" {
			return "", nil, errors.New("endpoint must be provided when apiType is set to 'foundry'")
		}
		options = append(options, anthropic.WithBaseURL(strings.TrimSuffix(md.Endpoint, "/")))
	} else if md.Endpoint != "" {
		options = append(options, anthropic.WithBaseURL(strings.TrimSuffix(md.Endpoint, "/")))
	}

	if httpClient := conversation.BuildHTTPClient(); httpClient != nil {
		options = append(options, anthropic.WithHTTPClient(httpClient))
	}

	return model, options, nil
}

func (a *Anthropic) Init(ctx context.Context, meta conversation.Metadata) error {
	md := AnthropicLangchainMetadata{}
	err := kmeta.DecodeMetadata(meta.Properties, &md)
	if err != nil {
		return err
	}

	model, options, err := a.buildClientOptions(md)
	if err != nil {
		return err
	}

	llm, err := anthropic.New(options...)
	if err != nil {
		return err
	}

	a.Model = splitParts{llm}
	a.SetModel(model)
	a.SetDefaultMaxTokens(md.MaxTokens)

	if md.ResponseCacheTTL != nil {
		cachedModel, cacheErr := conversation.CacheResponses(ctx, md.ResponseCacheTTL, a.Model)
		if cacheErr != nil {
			return cacheErr
		}

		a.Model = cachedModel
	}

	return nil
}

// splitParts works around the pinned langchaingo Anthropic client, which
// converts only the first part of an AI message. daprd sends an assistant turn
// as its text, then one ToolCall part per call, so every tool_use after the
// first part was dropped and Anthropic rejected the tool_result blocks that
// followed. Sending each part as its own AI message keeps them all, and
// Anthropic combines consecutive assistant turns into one.
// TODO: remove once a langchaingo release has both structured output (which
// the pin is on a branch for) and upstream commit f294eeec6.
type splitParts struct {
	*anthropic.LLM
}

func (s splitParts) GenerateContent(ctx context.Context, messages []llms.MessageContent, options ...llms.CallOption) (*llms.ContentResponse, error) {
	split := make([]llms.MessageContent, 0, len(messages))
	for _, msg := range messages {
		if msg.Role != llms.ChatMessageTypeAI || len(msg.Parts) < 2 {
			split = append(split, msg)
			continue
		}
		for _, part := range msg.Parts {
			split = append(split, llms.MessageContent{Role: msg.Role, Parts: []llms.ContentPart{part}})
		}
	}
	return s.LLM.GenerateContent(ctx, split, options...)
}

func (a *Anthropic) GetComponentMetadata() (metadataInfo metadata.MetadataMap) {
	metadataStruct := AnthropicLangchainMetadata{}
	_ = metadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, metadata.ConversationType)
	return
}

func (a *Anthropic) Close() error {
	return nil
}
