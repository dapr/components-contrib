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

	"github.com/tmc/langchaingo/llms"

	"github.com/dapr/components-contrib/conversation"
	"github.com/dapr/components-contrib/conversation/langchaingokit"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
	kmeta "github.com/dapr/kit/metadata"

	mistral2 "github.com/gage-technologies/mistral-go"
	"github.com/tmc/langchaingo/llms/mistral"
)

type Mistral struct {
	langchaingokit.LLM

	logger logger.Logger
}

func usageGetter(resp *llms.ContentResponse) *conversation.UsageInfo {
	if resp == nil || len(resp.Choices) == 0 {
		return nil
	}

	choice := resp.Choices[0]
	usage, ok := (choice.GenerationInfo["usage"]).(mistral2.UsageInfo)
	if !ok {
		return nil
	}

	return &conversation.UsageInfo{
		PromptTokens:     int32(usage.PromptTokens),
		CompletionTokens: int32(usage.CompletionTokens),
		TotalTokens:      int32(usage.TotalTokens),
	}
}

func NewMistral(logger logger.Logger) conversation.Conversation {
	m := &Mistral{
		logger: logger,
	}

	return m
}

const defaultModel = "open-mistral-7b"

func (m *Mistral) Init(ctx context.Context, meta conversation.Metadata) error {
	md := conversation.LangchainMetadata{}
	err := kmeta.DecodeMetadata(meta.Properties, &md)
	if err != nil {
		return err
	}

	model := defaultModel
	if md.Model != "" {
		model = md.Model
	}

	llm, err := mistral.New(
		mistral.WithModel(model),
		mistral.WithAPIKey(md.Key),
	)
	if err != nil {
		return err
	}

	m.LLM.Model = llm
	m.LLM.UsageGetterFunc = usageGetter

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
