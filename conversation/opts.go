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
package conversation

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/tmc/langchaingo/httputil"
	"github.com/tmc/langchaingo/llms"
	"github.com/tmc/langchaingo/llms/cache"
	"github.com/tmc/langchaingo/llms/cache/inmemory"
	"github.com/tmc/langchaingo/llms/openai"
)

// BuildOpenAIClientOptions is a helper function that is used by conversation components that use the OpenAI client under the hood.
// HTTP client timeout is set from resiliency policy configuration.
func BuildOpenAIClientOptions(model, key, endpoint string) []openai.Option {
	options := []openai.Option{
		openai.WithModel(model),
		openai.WithToken(key),
	}

	if endpoint != "" {
		options = append(options, openai.WithBaseURL(endpoint))
	}

	if httpClient := BuildHTTPClient(); httpClient != nil {
		options = append(options, openai.WithHTTPClient(httpClient))
	}

	return options
}

// CacheResponses creates a response cache with a configured TTL.
// This caches the final LLM responses (outputs) based on the input messages and call options.
// When the same prompt with the same options is requested, the cached response is returned
// without making an API call to the LLM provider, reducing latency and cost.
func CacheResponses(ctx context.Context, ttl *time.Duration, model llms.Model) (llms.Model, error) {
	mem, err := inmemory.New(ctx, inmemory.WithExpiration(*ttl))
	if err != nil {
		return model, fmt.Errorf("failed to create llm cache: %s", err)
	}

	return cache.New(model, mem), nil
}

// BuildHTTPClient creates an HTTP client with timeout set to 0 to rely on context deadlines.
// The context deadline will be respected via http.NewRequestWithContext within Langchain.
// This allows resiliency policy timeouts from runtime to propagate through to the HTTP client for the LLM provider.
func BuildHTTPClient() *http.Client {
	httpClient := &http.Client{
		// wrap with httputil.Transport to preserve user-agent
		Transport: &httputil.Transport{
			Transport: http.DefaultTransport,
		},
		// Timeout is set to 0 to rely on context deadlines set by any configured resiliency policies
		// The context deadline will be respected via http.NewRequestWithContext in Langchain.
		Timeout: 0,
	}

	return httpClient
}
