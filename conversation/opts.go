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
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"time"

	"github.com/tmc/langchaingo/httputil"
	"github.com/tmc/langchaingo/llms"
	"github.com/tmc/langchaingo/llms/cache"
	"github.com/tmc/langchaingo/llms/cache/inmemory"
	"github.com/tmc/langchaingo/llms/openai"
	"golang.org/x/net/http/httpguts"
	"golang.org/x/oauth2"
)

// PlaceholderToken satisfies langchaingo clients that require a token when the
// credential is set by BuildBearerHTTPClient instead. It is never sent.
const PlaceholderToken = "dapr-workload-identity" //nolint:gosec

// BuildOpenAIClientOptions is a helper function that is used by conversation components that use the OpenAI client under the hood.
// HTTP client timeout is set from resiliency policy configuration.
// headers are set on every request, see WithHeaders.
func BuildOpenAIClientOptions(model, key, endpoint string, headers http.Header) []openai.Option {
	options := []openai.Option{
		openai.WithModel(model),
		openai.WithToken(key),
	}

	if endpoint != "" {
		options = append(options, openai.WithBaseURL(endpoint))
	}

	if httpClient := BuildHTTPClient(); httpClient != nil {
		options = append(options, openai.WithHTTPClient(WithHeaders(httpClient, headers)))
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

// clientManagedHeaders are set by the HTTP client from the request itself, or
// are hop-by-hop headers, so they cannot be set with the headers metadata.
var clientManagedHeaders = map[string]struct{}{
	"Host":              {},
	"Content-Length":    {},
	"Transfer-Encoding": {},
	"Trailer":           {},
	"Connection":        {},
	"Keep-Alive":        {},
	"Proxy-Connection":  {},
	"Te":                {},
	"Upgrade":           {},
}

// ParseHeaders parses the headers metadata field, a JSON object of HTTP header
// names to values. An empty field returns no headers. Errors do not contain
// header values, because they can be credentials.
func ParseHeaders(raw string) (http.Header, error) {
	if raw == "" {
		return nil, nil
	}

	var values map[string]string
	if err := json.Unmarshal([]byte(raw), &values); err != nil {
		return nil, fmt.Errorf("headers must be a JSON object of header names to string values: %w", err)
	}

	headers := make(http.Header, len(values))
	for name, value := range values {
		if !httpguts.ValidHeaderFieldName(name) {
			return nil, fmt.Errorf("headers: invalid header name %q", name)
		}
		if !httpguts.ValidHeaderFieldValue(value) {
			return nil, fmt.Errorf("headers: invalid value for header %q", name)
		}
		key := http.CanonicalHeaderKey(name)
		if _, ok := clientManagedHeaders[key]; ok {
			return nil, fmt.Errorf("headers: header %q is set by the HTTP client and cannot be set", name)
		}
		if _, ok := headers[key]; ok {
			return nil, fmt.Errorf("headers: header %q is set more than once", name)
		}
		headers[key] = []string{value}
	}

	return headers, nil
}

// WithHeaders returns a copy of client that sets headers on every request,
// replacing the values of headers with the same name. The transport of client
// runs after the headers are set, so a transport that sets headers itself, such
// as the bearer transport of BuildBearerHTTPClient, replaces them. Redirects to
// another origin are refused, so the headers are not sent there. With no
// headers, client is returned unchanged.
func WithHeaders(client *http.Client, headers http.Header) *http.Client {
	if len(headers) == 0 {
		return client
	}

	base := client.Transport
	if base == nil {
		base = http.DefaultTransport
	}

	c := *client
	c.Transport = &headerTransport{base: base, headers: headers}
	c.CheckRedirect = sameOriginRedirect
	return &c
}

type headerTransport struct {
	base    http.RoundTripper
	headers http.Header
}

func (t *headerTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	req = req.Clone(req.Context())
	for name, values := range t.headers {
		req.Header[name] = slices.Clone(values)
	}

	return t.base.RoundTrip(req)
}
