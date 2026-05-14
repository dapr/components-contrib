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

	a.LLM.Model = llm
	a.LLM.SetModel(model)

	if md.ResponseCacheTTL != nil {
		cachedModel, cacheErr := conversation.CacheResponses(ctx, md.ResponseCacheTTL, a.LLM.Model)
		if cacheErr != nil {
			return cacheErr
		}

		a.LLM.Model = cachedModel
	}

	return nil
}

func (a *Anthropic) GetComponentMetadata() (metadataInfo metadata.MetadataMap) {
	metadataStruct := AnthropicLangchainMetadata{}
	metadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, metadata.ConversationType)
	return
}

func (a *Anthropic) Close() error {
	return nil
}
