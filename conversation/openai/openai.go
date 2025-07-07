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
	"encoding/json"
	"errors"
	"reflect"
	"strings"

	"github.com/tmc/langchaingo/llms/openai"

	"github.com/dapr/components-contrib/conversation"
	"github.com/dapr/components-contrib/conversation/langchaingokit"
	langchaingointernalchat "github.com/dapr/components-contrib/conversation/openai/lanchaingointernalchat"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
	kmeta "github.com/dapr/kit/metadata"
)

type OpenAI struct {
	langchaingokit.LLM

	logger logger.Logger
}

func NewOpenAI(logger logger.Logger) conversation.Conversation {
	return &OpenAI{
		logger: logger,
	}
}

const (
	defaultModel   = "gpt-4.1-2025-04-14"
	openaiProvider = "openai"
)

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

	key := md.Key
	if key == "" {
		key = conversation.GetEnvKey("OPENAI_API_KEY")
		if key == "" {
			return errors.New("openai key is required")
		}
	}
	// Create options for OpenAI client
	options := []openai.Option{
		openai.WithModel(model),
		openai.WithToken(key),
	}

	// Add custom endpoint if provided
	if md.Endpoint != "" {
		options = append(options, openai.WithBaseURL(md.Endpoint))
	}

	llm, err := openai.New(options...)
	if err != nil {
		return err
	}

	o.LLM.Model = llm
	o.LLM.SetProviderModelName(openaiProvider, model)

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
	metadataStruct := conversation.LangchainMetadata{}
	metadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, metadata.ConversationType)
	return
}

func (o *OpenAI) Close() error {
	return nil
}

func (o *OpenAI) fixUnsupportedOptions(r *conversation.ConversationRequest) {
	if strings.HasPrefix(o.LLM.ProviderModelName, "openai/o3-mini") || strings.HasPrefix(o.LLM.ProviderModelName, "openai/o4-mini") {
		o.logger.Debug("fixing unsupported temperature for o3-mini or o4-mini")
		r.Temperature = 1
	}
}

// Converse executes a non-streaming conversation with the LangChain Go model.
func (o *OpenAI) Converse(ctx context.Context, r *conversation.ConversationRequest) (res *conversation.ConversationResponse, err error) {
	o.fixUnsupportedOptions(r)
	return o.LLM.Converse(ctx, r)
}

// ConverseStream executes a streaming conversation with the LangChain Go model.
func (o *OpenAI) ConverseStream(ctx context.Context, r *conversation.ConversationRequest, streamFunc func(ctx context.Context, chunk []byte) error) (*conversation.ConversationResponse, error) {
	o.fixUnsupportedOptions(r)
	return o.LLM.ConverseStream(ctx, r, streamFuncWithoutToolCalls(streamFunc))
}

// streamFuncWithoutToolCalls is a wrapper for the streamFunc that removes tool calls from the chunk to reduce duplication
func streamFuncWithoutToolCalls(streamFunc func(ctx context.Context, chunk []byte) error) func(ctx context.Context, chunk []byte) error {
	return func(ctx context.Context, chunk []byte) error {
		if len(chunk) == 0 {
			return nil
		}
		// check if chunk are tool calls, openai langchaingo would create a json object with tool_calls []ToolCall
		tc := []langchaingointernalchat.ToolCall{}
		err := json.Unmarshal(chunk, &tc)
		if err == nil {
			return nil
		}
		return streamFunc(ctx, chunk)
	}
}
