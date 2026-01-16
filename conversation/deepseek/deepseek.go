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
	md DeepseekMetadata

	logger logger.Logger
}

const (
	defaultModel    = "deepseek-chat"
	defaultEndpoint = "https://api.deepseek.com"
)

func NewDeepseek(logger logger.Logger) conversation.Conversation {
	o := &Deepseek{
		logger: logger,
	}

	return o
}

func (d *Deepseek) Init(ctx context.Context, meta conversation.Metadata) error {
	md := DeepseekMetadata{}
	err := kmeta.DecodeMetadata(meta.Properties, &md)
	if err != nil {
		return err
	}
	model := defaultModel
	if md.Model != "" {
		model = md.Model
	}

	if md.Endpoint == "" {
		md.Endpoint = defaultEndpoint
	}

	options := conversation.BuildOpenAIClientOptions(model, md.Key, md.Endpoint)
	llm, err := openai.New(options...)
	if err != nil {
		return err
	}

	d.LLM.Model = llm
	d.md = md
	return nil
}

func (d *Deepseek) GetComponentMetadata() (metadataInfo metadata.MetadataMap) {
	metadataStruct := DeepseekMetadata{}
	metadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, metadata.ConversationType)
	return
}

func (d *Deepseek) Close() error {
	return nil
}
