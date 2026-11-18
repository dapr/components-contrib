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
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strings"
)

var (
	providerSorts   = []string{"price", "throughput", "latency"}
	dataCollections = []string{"allow", "deny"}
	quantizations   = []string{"int4", "int8", "fp4", "mxfp4", "nvfp4", "fp6", "fp8", "mxfp8", "fp16", "bf16", "fp32", "unknown"}
)

// providerPreferences is the provider object of an OpenRouter request.
type providerPreferences struct {
	Order             []string  `json:"order,omitempty"`
	Only              []string  `json:"only,omitempty"`
	Ignore            []string  `json:"ignore,omitempty"`
	Sort              string    `json:"sort,omitempty"`
	AllowFallbacks    *bool     `json:"allow_fallbacks,omitempty"`
	RequireParameters *bool     `json:"require_parameters,omitempty"`
	DataCollection    string    `json:"data_collection,omitempty"`
	ZDR               *bool     `json:"zdr,omitempty"`
	Quantizations     []string  `json:"quantizations,omitempty"`
	MaxPrice          *maxPrice `json:"max_price,omitempty"`
}

type maxPrice struct {
	Prompt     *float64 `json:"prompt,omitempty"`
	Completion *float64 `json:"completion,omitempty"`
}

// buildRoutingFields returns the top-level request fields for the model
// fallbacks and provider routing configured in the metadata, keyed by field
// name. It returns an empty map when none are configured.
func buildRoutingFields(md OpenrouterMetadata) (map[string]json.RawMessage, error) {
	fields := map[string]json.RawMessage{}

	// The model is tried first and the models list after it.
	if models := cleanList(md.FallbackModels); len(models) > 0 {
		raw, err := json.Marshal(models)
		if err != nil {
			return nil, err
		}
		fields["models"] = raw
	}

	providerSort, err := oneOf("providerSort", md.ProviderSort, providerSorts)
	if err != nil {
		return nil, err
	}
	dataCollection, err := oneOf("dataCollection", md.DataCollection, dataCollections)
	if err != nil {
		return nil, err
	}
	quants := cleanList(md.Quantizations)
	for i, q := range quants {
		if quants[i], err = oneOf("quantizations", q, quantizations); err != nil {
			return nil, err
		}
	}
	if md.MaxPromptPrice != nil && *md.MaxPromptPrice < 0 {
		return nil, errors.New("maxPromptPrice must not be negative")
	}
	if md.MaxCompletionPrice != nil && *md.MaxCompletionPrice < 0 {
		return nil, errors.New("maxCompletionPrice must not be negative")
	}

	provider := providerPreferences{
		Order:             cleanList(md.ProviderOrder),
		Only:              cleanList(md.ProviderOnly),
		Ignore:            cleanList(md.ProviderIgnore),
		Sort:              providerSort,
		AllowFallbacks:    md.AllowProviderFallbacks,
		RequireParameters: md.RequireParameters,
		DataCollection:    dataCollection,
		ZDR:               md.ZDR,
		Quantizations:     quants,
	}
	if md.MaxPromptPrice != nil || md.MaxCompletionPrice != nil {
		provider.MaxPrice = &maxPrice{Prompt: md.MaxPromptPrice, Completion: md.MaxCompletionPrice}
	}

	raw, err := json.Marshal(provider)
	if err != nil {
		return nil, err
	}
	if string(raw) != "{}" {
		fields["provider"] = raw
	}

	return fields, nil
}

// cleanList trims the items of a comma-separated metadata list and drops the
// empty ones.
func cleanList(items []string) []string {
	var out []string
	for _, item := range items {
		if item = strings.TrimSpace(item); item != "" {
			out = append(out, item)
		}
	}
	return out
}

// oneOf returns the lowercased value if it is empty or one of allowed.
func oneOf(field, value string, allowed []string) (string, error) {
	value = strings.ToLower(strings.TrimSpace(value))
	if value != "" && !slices.Contains(allowed, value) {
		return "", fmt.Errorf("invalid value %q for %s: must be one of %s", value, field, strings.Join(allowed, ", "))
	}
	return value, nil
}

// bodyFieldsTransport adds fields to the JSON body of chat completion
// requests. The OpenAI client cannot send OpenRouter's own request fields,
// such as provider and models. A field already in the body is left as is.
type bodyFieldsTransport struct {
	base   http.RoundTripper
	fields map[string]json.RawMessage
}

func (t *bodyFieldsTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.Method != http.MethodPost || req.Body == nil || !strings.HasSuffix(req.URL.Path, "/chat/completions") {
		return t.base.RoundTrip(req)
	}

	raw, err := io.ReadAll(req.Body)
	_ = req.Body.Close()
	if err != nil {
		return nil, fmt.Errorf("failed to read the request body: %w", err)
	}

	var body map[string]json.RawMessage
	if err = json.Unmarshal(raw, &body); err != nil {
		return nil, fmt.Errorf("failed to add OpenRouter fields to the request body: %w", err)
	}
	for name, value := range t.fields {
		if _, ok := body[name]; !ok {
			body[name] = value
		}
	}
	if raw, err = json.Marshal(body); err != nil {
		return nil, err
	}

	req = req.Clone(req.Context())
	req.Body = io.NopCloser(bytes.NewReader(raw))
	req.ContentLength = int64(len(raw))
	req.GetBody = func() (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(raw)), nil
	}

	return t.base.RoundTrip(req)
}
