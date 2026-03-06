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
	"reflect"

	"github.com/dapr/components-contrib/conversation"
	"github.com/dapr/components-contrib/conversation/langchaingokit"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
	kmeta "github.com/dapr/kit/metadata"

	"github.com/tmc/langchaingo/llms/anthropic"
)

type Anthropic struct {
	langchaingokit.LLM

	logger logger.Logger
}

func NewAnthropic(logger logger.Logger) conversation.Conversation {
	a := &Anthropic{
		logger: logger,
	}

	return a
}

func (a *Anthropic) Init(ctx context.Context, meta conversation.Metadata) error {
	m := conversation.LangchainMetadata{}
	err := kmeta.DecodeMetadata(meta.Properties, &m)
	if err != nil {
		return err
	}

	// Resolve model via central helper (uses metadata, then env var, then default)
	model := conversation.GetAnthropicModel(m.Model)

	options := []anthropic.Option{
		anthropic.WithModel(model),
		anthropic.WithToken(m.Key),
	}

	if httpClient := conversation.BuildHTTPClient(); httpClient != nil {
		options = append(options, anthropic.WithHTTPClient(httpClient))
	}

	llm, err := anthropic.New(options...)
	if err != nil {
		return err
	}

	a.LLM.Model = llm

	if m.ResponseCacheTTL != nil {
		cachedModel, cacheErr := conversation.CacheResponses(ctx, m.ResponseCacheTTL, a.LLM.Model)
		if cacheErr != nil {
			return cacheErr
		}

		a.LLM.Model = cachedModel
	}

	return nil
}

func (a *Anthropic) GetComponentMetadata() (metadataInfo metadata.MetadataMap) {
	metadataStruct := conversation.LangchainMetadata{}
	metadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, metadata.ConversationType)
	return
}

func (a *Anthropic) Close() error {
	return nil
}
