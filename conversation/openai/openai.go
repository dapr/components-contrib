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
package openai

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

	"github.com/tmc/langchaingo/llms/openai"
)

type OpenAI struct {
	langchaingokit.LLM

	logger logger.Logger
	md     OpenAILangchainMetadata
}

func NewOpenAI(logger logger.Logger) conversation.Conversation {
	o := &OpenAI{
		logger: logger,
	}

	return o
}

func (o *OpenAI) buildClientOptions(md OpenAILangchainMetadata) ([]openai.Option, error) {
	// Resolve model via central helper (uses metadata, then env var, then default)
	var model string
	// we support lowercase and uppercase here
	if strings.EqualFold(md.APIType, "azure") {
		model = conversation.GetAzureOpenAIModel(md.Model)
	} else {
		model = conversation.GetOpenAIModel(md.Model)
	}
	options := conversation.BuildOpenAIClientOptions(model, md.Key, md.Endpoint)

	// apply options specifically for azure openai
	// TODO: in future, there is also an openai.APITypeAzureAD that we can add.
	if strings.EqualFold(md.APIType, "azure") {
		if md.Endpoint == "" || md.APIVersion == "" {
			return nil, errors.New("endpoint and apiVersion must be provided when apiType is set to 'azure'")
		}
		options = append(options,
			openai.WithAPIType(openai.APITypeAzure),
			openai.WithAPIVersion(md.APIVersion),

			// apparently this is required for azure openai (but not for openai)
			// https://github.com/tmc/langchaingo/blob/509308ff01c13e662d5613d3aea793fabe18edd2/llms/openai/openaillm_option.go#L78
			openai.WithEmbeddingModel(md.Model),
		)

		// NOTE: This is also an option here.
		// https://github.com/tmc/langchaingo/blob/509308ff01c13e662d5613d3aea793fabe18edd2/llms/openai/openaillm_option.go#L89
		// openai.WithEmbeddingDimentions(),
	}

	return options, nil
}

func (o *OpenAI) Init(ctx context.Context, meta conversation.Metadata) error {
	md := OpenAILangchainMetadata{}
	err := kmeta.DecodeMetadata(meta.Properties, &md)
	if err != nil {
		return err
	}
	o.md = md

	options, err := o.buildClientOptions(md)
	if err != nil {
		return err
	}

	llm, err := openai.New(options...)
	if err != nil {
		return err
	}

	o.LLM.Model = llm

	if md.ResponseCacheTTL != nil {
		cachedModel, cacheErr := conversation.CacheResponses(ctx, md.ResponseCacheTTL, o.LLM.Model)
		if cacheErr != nil {
			return cacheErr
		}

		o.LLM.Model = cachedModel
	}

	return nil
}

func (o *OpenAI) GetComponentMetadata() (metadataInfo metadata.MetadataMap) {
	metadataStruct := OpenAILangchainMetadata{}
	metadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, metadata.ConversationType)
	return
}

func (o *OpenAI) Close() error {
	return nil
}
