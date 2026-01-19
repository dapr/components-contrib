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
package ollama

import (
	"context"
	"reflect"

	"github.com/dapr/components-contrib/conversation"
	"github.com/dapr/components-contrib/conversation/langchaingokit"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
	kmeta "github.com/dapr/kit/metadata"

	"github.com/tmc/langchaingo/llms/ollama"
)

type Ollama struct {
	langchaingokit.LLM

	logger logger.Logger
}

func NewOllama(logger logger.Logger) conversation.Conversation {
	o := &Ollama{
		logger: logger,
	}

	return o
}

func (o *Ollama) Init(ctx context.Context, meta conversation.Metadata) error {
	md := conversation.LangchainMetadata{}
	err := kmeta.DecodeMetadata(meta.Properties, &md)
	if err != nil {
		return err
	}

	// Resolve model via central helper (uses metadata, then env var, then default)
	model := conversation.GetOllamaModel(md.Model)

	llm, err := ollama.New(
		ollama.WithModel(model),
	)
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

func (o *Ollama) GetComponentMetadata() (metadataInfo metadata.MetadataMap) {
	metadataStruct := conversation.LangchainMetadata{}
	metadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, metadata.ConversationType)
	return
}

func (o *Ollama) Close() error {
	return nil
}
