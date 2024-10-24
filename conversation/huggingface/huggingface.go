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
package huggingface

import (
	"context"
	"reflect"

	"github.com/dapr/components-contrib/conversation"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
	kmeta "github.com/dapr/kit/metadata"

	"github.com/tmc/langchaingo/llms"
	"github.com/tmc/langchaingo/llms/huggingface"
)

type Huggingface struct {
	llm llms.Model

	logger logger.Logger
}

func NewHuggingface(logger logger.Logger) conversation.Conversation {
	h := &Huggingface{
		logger: logger,
	}

	return h
}

const defaultModel = "meta-llama/Meta-Llama-3-8B"

func (h *Huggingface) Init(ctx context.Context, meta conversation.Metadata) error {
	m := conversation.LangchainMetadata{}
	err := kmeta.DecodeMetadata(meta.Properties, &m)
	if err != nil {
		return err
	}

	model := defaultModel
	if m.Model != "" {
		model = m.Model
	}

	llm, err := huggingface.New(
		huggingface.WithModel(model),
		huggingface.WithToken(m.Key),
	)
	if err != nil {
		return err
	}

	h.llm = llm

	if m.CacheTTL != "" {
		cachedModel, cacheErr := conversation.CacheModel(ctx, m.CacheTTL, h.llm)
		if cacheErr != nil {
			return cacheErr
		}

		h.llm = cachedModel
	}

	return nil
}

func (h *Huggingface) GetComponentMetadata() (metadataInfo metadata.MetadataMap) {
	metadataStruct := conversation.LangchainMetadata{}
	metadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, metadata.ConversationType)
	return
}

func (h *Huggingface) Converse(ctx context.Context, r *conversation.ConversationRequest) (res *conversation.ConversationResponse, err error) {
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

	resp, err := h.llm.GenerateContent(ctx, messages, opts...)
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

func (h *Huggingface) Close() error {
	return nil
}
