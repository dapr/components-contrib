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
package googleai

import (
	"context"
	"reflect"

	"github.com/dapr/components-contrib/conversation"
	"github.com/dapr/components-contrib/conversation/langchaingokit"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
	kmeta "github.com/dapr/kit/metadata"

	"github.com/tmc/langchaingo/llms/googleai"
)

type GoogleAI struct {
	langchaingokit.LLM

	logger logger.Logger
}

func NewGoogleAI(logger logger.Logger) conversation.Conversation {
	g := &GoogleAI{
		logger: logger,
	}

	return g
}

const defaultModel = "gemini-1.5-flash"

func (g *GoogleAI) Init(ctx context.Context, meta conversation.Metadata) error {
	md := conversation.LangchainMetadata{}
	err := kmeta.DecodeMetadata(meta.Properties, &md)
	if err != nil {
		return err
	}

	model := defaultModel
	if md.Model != "" {
		model = md.Model
	}

	opts := []googleai.Option{
		googleai.WithAPIKey(md.Key),
		googleai.WithDefaultModel(model),
	}
	llm, err := googleai.New(
		ctx,
		opts...,
	)
	if err != nil {
		return err
	}

	g.LLM.Model = llm

	if md.CacheTTL != "" {
		cachedModel, cacheErr := conversation.CacheModel(ctx, md.CacheTTL, g.LLM.Model)
		if cacheErr != nil {
			return cacheErr
		}

		g.LLM.Model = cachedModel
	}
	return nil
}

func (g *GoogleAI) GetComponentMetadata() (metadataInfo metadata.MetadataMap) {
	metadataStruct := conversation.LangchainMetadata{}
	metadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, metadata.ConversationType)
	return
}

func (g *GoogleAI) Close() error {
	return nil
}
