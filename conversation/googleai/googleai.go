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
package googleai

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

type GoogleAI struct {
	langchaingokit.LLM

	logger logger.Logger
}

func NewGoogleAI(logger logger.Logger) conversation.Conversation {
	return &GoogleAI{
		logger: logger,
	}
}

const (
	defaultModel                 = "gemini-2.5-pro"
	googleAIOpenAICompatEndpoint = "https://generativelanguage.googleapis.com/v1beta/openai/"
	googleaiProvider             = "googleai"
)

func (g *GoogleAI) Init(ctx context.Context, meta conversation.Metadata) error {
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
		key = conversation.GetEnvKey("GOOGLE_API_KEY", "GEMINI_API_KEY", "GOOGLE_AI_API_KEY")
		if key == "" {
			return errors.New("google key is required")
		}
	}

	g.logger.Infof("GoogleAI Init (OpenAI Compatibility): API Key length=%d, Model=%s", len(md.Key), model)

	// Use OpenAI client with Google AI's OpenAI compatibility endpoint
	// This provides much better tool calling support than the native langchaingo Google AI implementation
	// TODO: This is a temporary workaround until langchaingo provides better native tool calling support
	options := []openai.Option{
		openai.WithModel(model),
		openai.WithToken(key),
		openai.WithBaseURL(googleAIOpenAICompatEndpoint),
	}

	// Allow custom endpoint override if provided
	if md.Endpoint != "" {
		options = options[:len(options)-1] // Remove the default base URL
		options = append(options, openai.WithBaseURL(md.Endpoint))
		g.logger.Infof("GoogleAI Init: Using custom endpoint: %s", md.Endpoint)
	}

	llm, err := openai.New(options...)
	if err != nil {
		g.logger.Errorf("GoogleAI Init: openai.New() failed: %v", err)
		return err
	}

	g.LLM.Model = llm
	g.LLM.SetProviderModelName(googleaiProvider, model)

	g.logger.Info("GoogleAI Init: Successfully initialized model using OpenAI compatibility layer")

	if md.CacheTTL != "" {
		cachedModel, cacheErr := conversation.CacheModel(ctx, md.CacheTTL, g.LLM.Model)
		if cacheErr != nil {
			return cacheErr
		}

		g.LLM.Model = cachedModel
	}
	return nil
}

func (g *GoogleAI) GetComponentMetadata() (metadataInfo metadata.MetadataMap) {
	metadataStruct := conversation.LangchainMetadata{}
	metadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, metadata.ConversationType)
	return
}

func (g *GoogleAI) Close() error {
	return nil
}
