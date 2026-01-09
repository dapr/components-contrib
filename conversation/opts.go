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
// It optionally configures HTTP client timeouts if provided.
func BuildOpenAIClientOptions(model, key, endpoint string, httpClientTimeout, idleConnectionTimeout *time.Duration) []openai.Option {
	options := []openai.Option{
		openai.WithModel(model),
		openai.WithToken(key),
	}

	if endpoint != "" {
		options = append(options, openai.WithBaseURL(endpoint))
	}

	if httpClient := BuildHTTPClient(httpClientTimeout, idleConnectionTimeout); httpClient != nil {
		options = append(options, openai.WithHTTPClient(httpClient))
	}

	return options
}

// CacheModel creates a prompt query cache with a configured TTL
func CacheModel(ctx context.Context, ttl string, model llms.Model) (llms.Model, error) {
	d, err := time.ParseDuration(ttl)
	if err != nil {
		return model, fmt.Errorf("failed to parse cacheTTL duration: %s", err)
	}

	mem, err := inmemory.New(ctx, inmemory.WithExpiration(d))
	if err != nil {
		return model, fmt.Errorf("failed to create llm cache: %s", err)
	}

	return cache.New(model, mem), nil
}

// BuildHTTPClient creates an HTTP client with custom timeouts if specified in the metadata.
func BuildHTTPClient(httpClientTimeout, idleConnectionTimeout *time.Duration) *http.Client {
	if httpClientTimeout == nil && idleConnectionTimeout == nil {
		return nil
	}

	transport := &http.Transport{}
	if idleConnectionTimeout != nil {
		transport.IdleConnTimeout = *idleConnectionTimeout
	}

	httpClient := &http.Client{
		// wrap with httputil.Transport to preserve user-agent
		Transport: &httputil.Transport{
			Transport: transport,
		},
	}

	if httpClientTimeout != nil {
		httpClient.Timeout = *httpClientTimeout
	}

	return httpClient
}
