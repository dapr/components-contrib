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
	"strings"

	"github.com/dapr/components-contrib/conversation"
	"github.com/dapr/components-contrib/conversation/langchaingokit"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
	kmeta "github.com/dapr/kit/metadata"

	"github.com/tmc/langchaingo/llms/openai"
)

type Huggingface struct {
	langchaingokit.LLM

	logger logger.Logger
}

func NewHuggingface(logger logger.Logger) conversation.Conversation {
	h := &Huggingface{
		logger: logger,
	}

	return h
}

// Default model - using a popular and reliable model
const defaultModel = "deepseek-ai/DeepSeek-R1-Distill-Qwen-32B"

// Default HuggingFace OpenAI-compatible endpoint
const defaultEndpoint = "https://router.huggingface.co/hf-inference/models/{{model}}/v1"

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

	endpoint := strings.Replace(defaultEndpoint, "{{model}}", model, 1)
	if m.Endpoint != "" {
		endpoint = m.Endpoint
	}

	// Create options for OpenAI client using HuggingFace's OpenAI-compatible API
	// This is a workaround for issues with the native HuggingFace langchaingo implementation
	options := []openai.Option{
		openai.WithModel(model),
		openai.WithToken(m.Key),
		openai.WithBaseURL(endpoint),
	}

	llm, err := openai.New(options...)
	if err != nil {
		return err
	}

	h.LLM.Model = llm

	if m.CacheTTL != "" {
		cachedModel, cacheErr := conversation.CacheModel(ctx, m.CacheTTL, h.LLM.Model)
		if cacheErr != nil {
			return cacheErr
		}

		h.LLM.Model = cachedModel
	}

	return nil
}

func (h *Huggingface) GetComponentMetadata() (metadataInfo metadata.MetadataMap) {
	metadataStruct := conversation.LangchainMetadata{}
	metadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, metadata.ConversationType)
	return
}

func (h *Huggingface) Close() error {
	return nil
}
