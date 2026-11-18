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
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"strings"

	"github.com/tmc/langchaingo/llms/openai"
	"golang.org/x/net/http/httpguts"

	"github.com/dapr/components-contrib/conversation"
	"github.com/dapr/components-contrib/conversation/langchaingokit"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
	kmeta "github.com/dapr/kit/metadata"
)

const defaultEndpoint = "https://openrouter.ai/api/v1"

// App attribution headers.
// ref: https://openrouter.ai/docs/app-attribution
const (
	headerAppURL        = "HTTP-Referer"
	headerAppTitle      = "X-OpenRouter-Title"
	headerAppCategories = "X-OpenRouter-Categories"
)

type Openrouter struct {
	langchaingokit.LLM

	logger logger.Logger
}

type OpenrouterMetadata struct {
	conversation.LangchainMetadata `json:",inline" mapstructure:",squash"`

	// AppURL is sent as the HTTP-Referer header, which OpenRouter uses to
	// attribute usage to an app.
	AppURL string `json:"appURL,omitempty" mapstructure:"appURL"`
	// AppTitle is sent as the X-OpenRouter-Title header, the app's display name.
	AppTitle string `json:"appTitle,omitempty" mapstructure:"appTitle"`
	// AppCategories is sent as the X-OpenRouter-Categories header, a
	// comma-separated list of marketplace categories.
	AppCategories string `json:"appCategories,omitempty" mapstructure:"appCategories"`

	// FallbackModels are tried in order when the model returns an error.
	FallbackModels []string `json:"fallbackModels,omitempty" mapstructure:"fallbackModels"`

	// Provider routing preferences, sent in the request's provider object.
	// ref: https://openrouter.ai/docs/guides/routing/provider-selection
	ProviderOrder          []string `json:"providerOrder,omitempty" mapstructure:"providerOrder"`
	ProviderOnly           []string `json:"providerOnly,omitempty" mapstructure:"providerOnly"`
	ProviderIgnore         []string `json:"providerIgnore,omitempty" mapstructure:"providerIgnore"`
	ProviderSort           string   `json:"providerSort,omitempty" mapstructure:"providerSort"`
	AllowProviderFallbacks *bool    `json:"allowProviderFallbacks,omitempty" mapstructure:"allowProviderFallbacks"`
	RequireParameters      *bool    `json:"requireParameters,omitempty" mapstructure:"requireParameters"`
	DataCollection         string   `json:"dataCollection,omitempty" mapstructure:"dataCollection"`
	ZDR                    *bool    `json:"zdr,omitempty" mapstructure:"zdr"`
	Quantizations          []string `json:"quantizations,omitempty" mapstructure:"quantizations"`
	// MaxPromptPrice and MaxCompletionPrice are in USD per million tokens.
	MaxPromptPrice     *float64 `json:"maxPromptPrice,omitempty" mapstructure:"maxPromptPrice"`
	MaxCompletionPrice *float64 `json:"maxCompletionPrice,omitempty" mapstructure:"maxCompletionPrice"`
}

func NewOpenrouter(logger logger.Logger) conversation.Conversation {
	o := &Openrouter{
		logger: logger,
		LLM:    langchaingokit.New(logger),
	}

	return o
}

func (o *Openrouter) Init(ctx context.Context, meta conversation.Metadata) error {
	md := OpenrouterMetadata{}
	err := kmeta.DecodeMetadata(meta.Properties, &md)
	if err != nil {
		return err
	}

	if md.Key == "" {
		return errors.New("openrouter api key is required")
	}

	if md.Endpoint == "" {
		md.Endpoint = defaultEndpoint
	}

	// Resolve model via central helper (uses metadata, then env var, then default)
	model := conversation.GetOpenrouterModel(md.Model)

	headers, err := o.buildHeaders(md)
	if err != nil {
		return err
	}

	routingFields, err := buildRoutingFields(md)
	if err != nil {
		return err
	}

	httpClient := conversation.BuildHTTPClient()
	if len(routingFields) > 0 {
		httpClient.Transport = &bodyFieldsTransport{base: httpClient.Transport, fields: routingFields}
	}

	// OpenRouter accepts max_completion_tokens with the same meaning as
	// max_tokens, so the legacy field is not forced.
	options := conversation.BuildOpenAIClientOptions(model, md.Key, md.Endpoint, nil)
	options = append(options, openai.WithHTTPClient(conversation.WithHeaders(httpClient, headers)))
	llm, err := openai.New(options...)
	if err != nil {
		return err
	}

	o.Model = llm
	o.SetModel(model)
	o.SetDefaultMaxTokens(md.MaxTokens)

	if md.ResponseCacheTTL != nil {
		cachedModel, cacheErr := conversation.CacheResponses(ctx, md.ResponseCacheTTL, o.Model)
		if cacheErr != nil {
			return cacheErr
		}

		o.Model = cachedModel
	}
	return nil
}

// buildHeaders returns the app attribution headers, overlaid with the headers
// metadata field so that a header set there replaces the one built here.
func (o *Openrouter) buildHeaders(md OpenrouterMetadata) (http.Header, error) {
	userHeaders, err := conversation.ParseHeaders(md.Headers)
	if err != nil {
		return nil, err
	}

	headers := http.Header{}
	for _, h := range []struct {
		field string
		name  string
		value string
	}{
		{"appURL", headerAppURL, md.AppURL},
		{"appTitle", headerAppTitle, md.AppTitle},
		{"appCategories", headerAppCategories, md.AppCategories},
	} {
		value := strings.TrimSpace(h.value)
		if value == "" {
			continue
		}
		if !httpguts.ValidHeaderFieldValue(value) {
			return nil, fmt.Errorf("invalid value for %s", h.field)
		}
		headers.Set(h.name, value)
	}

	for name, values := range userHeaders {
		headers[name] = values
	}

	if headers.Get(headerAppURL) == "" && (headers.Get(headerAppTitle) != "" || headers.Get(headerAppCategories) != "") {
		o.logger.Warn("appTitle and appCategories are only used for OpenRouter app attribution when appURL is also set")
	}

	if len(headers) == 0 {
		return nil, nil
	}
	return headers, nil
}

func (o *Openrouter) GetComponentMetadata() (metadataInfo metadata.MetadataMap) {
	metadataStruct := OpenrouterMetadata{}
	_ = metadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, metadata.ConversationType)
	return
}

func (o *Openrouter) Close() error {
	return nil
}
