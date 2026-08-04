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

package spark

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

type Spark struct {
	langchaingokit.LLM
	md SparkMetadata

	logger logger.Logger
}

const (
	defaultModel    = "generalv3.5"
	defaultEndpoint = "https://spark-api-open.xf-yun.com/v1"
)

func NewSpark(logger logger.Logger) conversation.Conversation {
	o := &Spark{
		logger: logger,
		LLM:    langchaingokit.New(logger),
	}

	return o
}

func (s *Spark) Init(ctx context.Context, meta conversation.Metadata) error {
	md := SparkMetadata{}
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

	s.Model = llm
	s.SetModel(model)
	s.md = md
	return nil
}

func (s *Spark) GetComponentMetadata() (metadataInfo metadata.MetadataMap) {
	metadataStruct := SparkMetadata{}
	_ = metadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, metadata.ConversationType)
	return
}

func (s *Spark) Close() error {
	return nil
}
