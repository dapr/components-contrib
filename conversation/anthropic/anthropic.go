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
package anthropic

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"time"

	anthropicsdk "github.com/anthropics/anthropic-sdk-go"
	"github.com/anthropics/anthropic-sdk-go/option"

	"github.com/dapr/components-contrib/conversation"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
	kmeta "github.com/dapr/kit/metadata"

	"github.com/tmc/langchaingo/llms"
)

const (
	apiTypeFoundry = "foundry"

	// defaultMaxTokens matches the fallback the pinned langchaingo client used
	// when no max_tokens was provided, keeping behavior identical for callers
	// that never set one.
	defaultMaxTokens = 2048
)

type Anthropic struct {
	client    anthropicsdk.Client
	model     string
	maxTokens int64

	cache *responseCache

	logger logger.Logger
}

func NewAnthropic(logger logger.Logger) conversation.Conversation {
	return &Anthropic{
		logger: logger,
	}
}

func (a *Anthropic) buildClientOptions(md AnthropicLangchainMetadata) (string, []option.RequestOption, error) {
	model := conversation.GetAnthropicModel(md.Model)

	options := []option.RequestOption{
		option.WithAPIKey(md.Key),
	}

	if strings.EqualFold(md.APIType, apiTypeFoundry) {
		if md.Endpoint == "" {
			return "", nil, errors.New("endpoint must be provided when apiType is set to 'foundry'")
		}
		options = append(options, option.WithBaseURL(strings.TrimSuffix(md.Endpoint, "/")))
	} else if md.Endpoint != "" {
		options = append(options, option.WithBaseURL(strings.TrimSuffix(md.Endpoint, "/")))
	}

	if httpClient := conversation.BuildHTTPClient(); httpClient != nil {
		options = append(options, option.WithHTTPClient(httpClient))
	}

	return model, options, nil
}

func (a *Anthropic) Init(ctx context.Context, meta conversation.Metadata) error {
	md := AnthropicLangchainMetadata{}
	err := kmeta.DecodeMetadata(meta.Properties, &md)
	if err != nil {
		return err
	}

	model, options, err := a.buildClientOptions(md)
	if err != nil {
		return err
	}

	a.client = anthropicsdk.NewClient(options...)
	a.model = model

	a.maxTokens = defaultMaxTokens
	if md.MaxTokens > 0 {
		a.maxTokens = md.MaxTokens
	}

	if md.ResponseCacheTTL != nil {
		a.cache = newResponseCache(*md.ResponseCacheTTL)
	}

	return nil
}

func (a *Anthropic) Converse(ctx context.Context, r *conversation.Request) (*conversation.Response, error) {
	var messages []llms.MessageContent
	if r.Message != nil {
		messages = *r.Message
	}

	var tools []llms.Tool
	if r.Tools != nil {
		tools = *r.Tools
	}

	params, err := a.buildParams(messages, tools, r)
	if err != nil {
		return nil, err
	}

	if a.cache != nil {
		if cached, ok := a.cache.get(params); ok {
			return cached, nil
		}
	}

	msg, err := a.client.Messages.New(ctx, params)
	if err != nil {
		return nil, fmt.Errorf("anthropic: failed to create message: %w", err)
	}

	resp := buildResponse(a.model, msg)

	// If tool_choice was "required" but the model returned neither content nor a
	// tool call, surface it as a retriable error rather than a silent success.
	// This mirrors the shared langchaingokit behavior.
	if r.ToolChoice != nil && strings.EqualFold(*r.ToolChoice, "required") && len(tools) > 0 {
		if !hasUsefulResponse(resp) {
			return nil, fmt.Errorf("LLM returned empty response with no tool calls despite %d tools being available", len(tools))
		}
	}

	if a.cache != nil {
		a.cache.set(params, resp)
	}

	return resp, nil
}

func (a *Anthropic) buildParams(messages []llms.MessageContent, tools []llms.Tool, r *conversation.Request) (anthropicsdk.MessageNewParams, error) {
	msgs, system, err := buildMessages(messages)
	if err != nil {
		return anthropicsdk.MessageNewParams{}, fmt.Errorf("anthropic: failed to process messages: %w", err)
	}

	params := anthropicsdk.MessageNewParams{
		Model:     a.model,
		MaxTokens: a.maxTokens,
		Messages:  msgs,
	}

	if system != "" {
		params.System = []anthropicsdk.TextBlockParam{{Text: system}}
	}

	// The pinned langchaingo serialized temperature unconditionally, which breaks
	// newer Claude generations that reject the parameter. Set it only when the
	// caller actually provided one (dapr uses 0 to mean unset).
	if r.Temperature > 0 {
		params.Temperature = anthropicsdk.Float(r.Temperature)
	}

	toolParams, err := buildTools(tools)
	if err != nil {
		return anthropicsdk.MessageNewParams{}, err
	}
	if len(toolParams) > 0 {
		params.Tools = toolParams
		params.ToolChoice = buildToolChoice(r.ToolChoice)
	}

	return params, nil
}

func hasUsefulResponse(resp *conversation.Response) bool {
	for _, output := range resp.Outputs {
		for _, choice := range output.Choices {
			if choice.Message.Content != "" ||
				(choice.Message.ToolCallRequest != nil && len(*choice.Message.ToolCallRequest) > 0) {
				return true
			}
		}
	}
	return false
}

// GetModel returns the resolved model name used for this component.
func (a *Anthropic) GetModel() string {
	return a.model
}

func (a *Anthropic) GetComponentMetadata() (metadataInfo metadata.MetadataMap) {
	metadataStruct := AnthropicLangchainMetadata{}
	_ = metadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, metadata.ConversationType)
	return
}

func (a *Anthropic) Close() error {
	return nil
}

// responseCache is a small TTL cache over Converse responses, keyed by the
// request parameters. It preserves the responseCacheTTL/cacheTTL behavior the
// langchaingo-backed component provided.
type responseCache struct {
	ttl     time.Duration
	mu      sync.Mutex
	entries map[string]cacheEntry
}

type cacheEntry struct {
	response *conversation.Response
	expiry   time.Time
}

func newResponseCache(ttl time.Duration) *responseCache {
	return &responseCache{
		ttl:     ttl,
		entries: make(map[string]cacheEntry),
	}
}

func cacheKey(params anthropicsdk.MessageNewParams) string {
	b, err := json.Marshal(params)
	if err != nil {
		return ""
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func (c *responseCache) get(params anthropicsdk.MessageNewParams) (*conversation.Response, bool) {
	key := cacheKey(params)
	if key == "" {
		return nil, false
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	entry, ok := c.entries[key]
	if !ok {
		return nil, false
	}
	if time.Now().After(entry.expiry) {
		delete(c.entries, key)
		return nil, false
	}
	return entry.response, true
}

func (c *responseCache) set(params anthropicsdk.MessageNewParams, resp *conversation.Response) {
	key := cacheKey(params)
	if key == "" {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	c.entries[key] = cacheEntry{
		response: resp,
		expiry:   time.Now().Add(c.ttl),
	}
}
