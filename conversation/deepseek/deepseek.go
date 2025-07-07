/*
Copyright 2025 The Dapr Authors
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

package deepseek

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

type Deepseek struct {
	langchaingokit.LLM

	logger logger.Logger
}

func NewDeepseek(logger logger.Logger) conversation.Conversation {
	return &Deepseek{
		logger: logger,
	}
}

const (
	defaultModel     = "deepseek-chat"
	deepseekProvider = "deepseek"
)

// Default DeepSeek API endpoint
const defaultEndpoint = "https://api.deepseek.com"

func (d *Deepseek) Init(ctx context.Context, meta conversation.Metadata) error {
	m := conversation.LangchainMetadata{}
	err := kmeta.DecodeMetadata(meta.Properties, &m)
	if err != nil {
		return err
	}

	model := defaultModel
	if m.Model != "" {
		model = m.Model
	}

	endpoint := defaultEndpoint
	if m.Endpoint != "" {
		endpoint = m.Endpoint
	}

	key := m.Key
	if key == "" {
		key = conversation.GetEnvKey("DEEPSEEK_API_KEY")
		if key == "" {
			return errors.New("deepseek key is required")
		}
	}

	// Create options for OpenAI client using DeepSeek's OpenAI-compatible API
	options := []openai.Option{
		openai.WithModel(model),
		openai.WithToken(key),
		openai.WithBaseURL(endpoint),
	}

	llm, err := openai.New(options...)
	if err != nil {
		return err
	}

	d.LLM.Model = llm
	d.LLM.SetProviderModelName(deepseekProvider, model)

	if m.CacheTTL != "" {
		cachedModel, cacheErr := conversation.CacheModel(ctx, m.CacheTTL, d.LLM.Model)
		if cacheErr != nil {
			return cacheErr
		}

		d.LLM.Model = cachedModel
	}

	return nil
}

func (d *Deepseek) GetComponentMetadata() (metadataInfo metadata.MetadataMap) {
	metadataStruct := conversation.LangchainMetadata{}
	metadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, metadata.ConversationType)
	return
}

func (d *Deepseek) Close() error {
	return nil
}
