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
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/tmc/langchaingo/httputil"
	"github.com/tmc/langchaingo/llms"
	"github.com/tmc/langchaingo/llms/cache"
	"github.com/tmc/langchaingo/llms/cache/inmemory"
	"github.com/tmc/langchaingo/llms/openai"
	"golang.org/x/oauth2"
)

// PlaceholderToken satisfies langchaingo clients that require a token when the
// credential is set by BuildBearerHTTPClient instead. It is never sent.
const PlaceholderToken = "dapr-workload-identity" //nolint:gosec

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

// BuildBearerHTTPClient is BuildHTTPClient with an Authorization: Bearer header
// taken from ts on every request. The x-api-key and api-key headers langchaingo
// clients set from their placeholder token are removed.
func BuildBearerHTTPClient(ts oauth2.TokenSource) *http.Client {
	client := BuildHTTPClient()
	client.Transport = &bearerTransport{base: client.Transport, ts: ts}
	client.CheckRedirect = sameOriginRedirect
	return client
}

// sameOriginRedirect stops redirects to another scheme, host, or port. The
// transport sets the bearer token on every request, including redirects, so
// following them elsewhere would send the token to that destination.
func sameOriginRedirect(req *http.Request, via []*http.Request) error {
	if len(via) >= 10 {
		return errors.New("stopped after 10 redirects")
	}
	if req.URL.Scheme != via[0].URL.Scheme || req.URL.Host != via[0].URL.Host {
		return fmt.Errorf("refusing to follow redirect from %s to another origin %s://%s", via[0].URL.Host, req.URL.Scheme, req.URL.Host)
	}
	return nil
}

type bearerTransport struct {
	base http.RoundTripper
	ts   oauth2.TokenSource
}

func (t *bearerTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	tok, err := t.ts.Token()
	if err != nil {
		if req.Body != nil {
			_ = req.Body.Close()
		}
		return nil, err
	}

	req = req.Clone(req.Context())
	req.Header.Del("x-api-key")
	req.Header.Del("api-key")
	tok.SetAuthHeader(req)

	return t.base.RoundTrip(req)
}
