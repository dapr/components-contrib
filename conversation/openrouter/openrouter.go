/*
Copyright 2026 The Dapr Authors
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

package openrouter

import (
	"context"
	"net/http"
	"reflect"

	"github.com/tmc/langchaingo/llms/openai"

	"github.com/dapr/components-contrib/conversation"
	"github.com/dapr/components-contrib/conversation/langchaingokit"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
	kmeta "github.com/dapr/kit/metadata"
)

const (
	// defaultEndpoint is the OpenRouter OpenAI-compatible API base URL.
	defaultEndpoint = "https://openrouter.ai/api/v1"

	// headerReferer and headerTitle are the attribution headers OpenRouter recommends.
	headerReferer = "HTTP-Referer"
	headerTitle   = "X-Title"
)

// OpenRouter is a Dapr conversation component that routes requests through
// OpenRouter (https://openrouter.ai), providing access to 200+ LLM models
// from all major providers via a single OpenAI-compatible API.
type OpenRouter struct {
	langchaingokit.LLM

	logger logger.Logger
}

// NewOpenRouter returns a new OpenRouter conversation component.
func NewOpenRouter(logger logger.Logger) conversation.Conversation {
	return &OpenRouter{
		LLM:    langchaingokit.New(logger),
		logger: logger,
	}
}

// Init initialises the OpenRouter client from the supplied metadata.
func (o *OpenRouter) Init(ctx context.Context, meta conversation.Metadata) error {
	m := OpenRouterMetadata{}
	if err := kmeta.DecodeMetadata(meta.Properties, &m); err != nil {
		return err
	}

	model := conversation.GetOpenRouterModel(m.Model)

	endpoint := m.Endpoint
	if endpoint == "" {
		endpoint = defaultEndpoint
	}

	// Build HTTP client — inject optional attribution headers when configured,
	// otherwise fall back to the standard client with no extra transport wrapping.
	var httpClient *http.Client
	if m.SiteURL != "" || m.SiteTitle != "" {
		extraHeaders := map[string]string{}
		if m.SiteURL != "" {
			extraHeaders[headerReferer] = m.SiteURL
		}
		if m.SiteTitle != "" {
			extraHeaders[headerTitle] = m.SiteTitle
		}
		httpClient = conversation.BuildHTTPClientWithHeaders(extraHeaders)
	} else {
		httpClient = conversation.BuildHTTPClient()
	}

	options := []openai.Option{
		openai.WithModel(model),
		openai.WithToken(m.Key),
		openai.WithBaseURL(endpoint),
		openai.WithHTTPClient(httpClient),
	}

	llm, err := openai.New(options...)
	if err != nil {
		return err
	}

	o.LLM.Model = llm
	o.LLM.SetModel(model)

	if m.ResponseCacheTTL != nil {
		cached, cacheErr := conversation.CacheResponses(ctx, m.ResponseCacheTTL, o.LLM.Model)
		if cacheErr != nil {
			return cacheErr
		}
		o.LLM.Model = cached
	}

	return nil
}

// GetComponentMetadata returns metadata info for this component derived from the metadata struct.
func (o *OpenRouter) GetComponentMetadata() (metadataInfo metadata.MetadataMap) {
	metadataStruct := OpenRouterMetadata{}
	metadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, metadata.ConversationType)
	return
}

// Close is a no-op; the underlying HTTP client does not hold open connections.
func (o *OpenRouter) Close() error { return nil }
