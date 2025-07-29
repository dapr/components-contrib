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
}

func NewOpenAI(logger logger.Logger) conversation.Conversation {
	o := &OpenAI{
		logger: logger,
	}

	return o
}

const defaultModel = "gpt-4o"
const defaultApiType = openai.APITypeOpenAI

func (o *OpenAI) Init(ctx context.Context, meta conversation.Metadata) error {
	md := OpenAILangchainMetadata{}
	err := kmeta.DecodeMetadata(meta.Properties, &md)
	if err != nil {
		return err
	}

	model := defaultModel
	if md.Model != "" {
		model = md.Model
	}
	// Create options for OpenAI client
	options := []openai.Option{
		openai.WithModel(model),
		openai.WithToken(md.Key),
	}

	// Add custom endpoint if provided
	if md.Endpoint != "" {
		options = append(options, openai.WithBaseURL(md.Endpoint))
	}

	// Identify correct Api Type
	switch md.ApiType {
	case "", "openai":
		options = append(options, openai.WithAPIType(defaultApiType))
	case "azure":
		options = append(options, openai.WithAPIType(openai.APITypeAzure))
	default:
		return errors.New("apiType must be 'openai' or 'azure'")
	}

	// Return error when apiType is azure but apiVersion isn't provided
	if md.ApiType == "azure" && md.ApiVersion == "" {
		return errors.New("apiVersion must be provided when apiType is set to 'azure'")
	}

	// Set api version if provided
	if md.ApiVersion != "" {
		options = append(options, openai.WithAPIVersion(md.ApiVersion))
	}

	llm, err := openai.New(options...)
	if err != nil {
		return err
	}

	o.LLM.Model = llm

	if md.CacheTTL != "" {
		cachedModel, cacheErr := conversation.CacheModel(ctx, md.CacheTTL, o.LLM.Model)
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
