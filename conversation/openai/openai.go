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
	"reflect"

	"github.com/dapr/components-contrib/conversation"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
	kmeta "github.com/dapr/kit/metadata"

	"github.com/tmc/langchaingo/llms"
	"github.com/tmc/langchaingo/llms/openai"
)

type OpenAI struct {
	llm llms.Model

	logger logger.Logger
}

func NewOpenAI(logger logger.Logger) conversation.Conversation {
	o := &OpenAI{
		logger: logger,
	}

	return o
}

const defaultModel = "gpt-4o"

func (o *OpenAI) Init(ctx context.Context, meta conversation.Metadata) error {
	md := conversation.LangchainMetadata{}
	err := kmeta.DecodeMetadata(meta.Properties, &md)
	if err != nil {
		return err
	}

	model := defaultModel
	if md.Model != "" {
		model = md.Model
	}

	llm, err := openai.New(
		openai.WithModel(model),
		openai.WithToken(md.Key),
	)
	if err != nil {
		return err
	}

	o.llm = llm

	if md.CacheTTL != "" {
		cachedModel, cacheErr := conversation.CacheModel(ctx, md.CacheTTL, o.llm)
		if cacheErr != nil {
			return cacheErr
		}

		o.llm = cachedModel
	}
	return nil
}

func (o *OpenAI) GetComponentMetadata() (metadataInfo metadata.MetadataMap) {
	metadataStruct := conversation.LangchainMetadata{}
	metadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, metadata.ConversationType)
	return
}

func (o *OpenAI) Converse(ctx context.Context, r *conversation.ConversationRequest) (res *conversation.ConversationResponse, err error) {
	messages := make([]llms.MessageContent, 0, len(r.Inputs))

	for _, input := range r.Inputs {
		role := conversation.ConvertLangchainRole(input.Role)

		messages = append(messages, llms.MessageContent{
			Role: role,
			Parts: []llms.ContentPart{
				llms.TextPart(input.Message),
			},
		})
	}

	opts := []llms.CallOption{}

	if r.Temperature > 0 {
		opts = append(opts, conversation.LangchainTemperature(r.Temperature))
	}

	resp, err := o.llm.GenerateContent(ctx, messages, opts...)
	if err != nil {
		return nil, err
	}

	outputs := make([]conversation.ConversationResult, 0, len(resp.Choices))

	for i := range resp.Choices {
		outputs = append(outputs, conversation.ConversationResult{
			Result:     resp.Choices[i].Content,
			Parameters: r.Parameters,
		})
	}

	res = &conversation.ConversationResponse{
		Outputs: outputs,
	}

	return res, nil
}

func (o *OpenAI) Close() error {
	return nil
}
