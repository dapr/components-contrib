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
package openai

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"

	"github.com/tmc/langchaingo/llms"

	"github.com/dapr/components-contrib/conversation"
)

const (
	defaultOpenAIBaseURL = "https://api.openai.com/v1"

	// promptCacheRetentionKey is the request metadata key langchaingokit uses to
	// carry prompt_cache_retention. The Responses API takes it as a top-level field.
	promptCacheRetentionKey = "prompt_cache_retention"
)

// responsesModel is an llms.Model that calls OpenAI's Responses API
// (/v1/responses) instead of Chat Completions.
//
// Reasoning models such as gpt-6.1-sol reject function tools on Chat Completions
// unless reasoning is turned off, and some do not allow turning it off at all.
// The Responses API accepts tools together with reasoning. langchaingo has no
// Responses API client, so this one is written against the HTTP API directly.
//
// Requests are sent with store set to false, so OpenAI keeps no conversation
// state and every call carries its full history, as with Chat Completions.
type responsesModel struct {
	model   string
	key     string
	baseURL string
	client  *http.Client
}

func newResponsesModel(model, key, endpoint string) (*responsesModel, error) {
	if key == "" {
		key = os.Getenv("OPENAI_API_KEY")
	}
	if key == "" {
		return nil, errors.New("missing the OpenAI API key, set it in the key metadata field or the OPENAI_API_KEY environment variable")
	}
	if endpoint == "" {
		endpoint = os.Getenv("OPENAI_BASE_URL")
	}
	if endpoint == "" {
		endpoint = defaultOpenAIBaseURL
	}
	return &responsesModel{
		model:   model,
		key:     key,
		baseURL: strings.TrimSuffix(endpoint, "/"),
		client:  conversation.BuildHTTPClient(),
	}, nil
}

type responsesRequest struct {
	Model                string          `json:"model"`
	Input                []any           `json:"input"`
	Tools                []responsesTool `json:"tools,omitempty"`
	ToolChoice           any             `json:"tool_choice,omitempty"`
	MaxOutputTokens      int             `json:"max_output_tokens,omitempty"`
	Temperature          float64         `json:"temperature,omitempty"`
	Text                 *responsesText  `json:"text,omitempty"`
	Metadata             map[string]any  `json:"metadata,omitempty"`
	PromptCacheRetention any             `json:"prompt_cache_retention,omitempty"`
	Store                bool            `json:"store"`
}

type responsesTool struct {
	Type        string `json:"type"`
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
	Parameters  any    `json:"parameters,omitempty"`
	Strict      bool   `json:"strict,omitempty"`
}

type responsesText struct {
	Format map[string]any `json:"format"`
}

type responsesResponse struct {
	Status            string `json:"status"`
	IncompleteDetails *struct {
		Reason string `json:"reason"`
	} `json:"incomplete_details"`
	Output []struct {
		Type    string `json:"type"`
		Content []struct {
			Type    string `json:"type"`
			Text    string `json:"text"`
			Refusal string `json:"refusal"`
		} `json:"content"`
		CallID    string `json:"call_id"`
		Name      string `json:"name"`
		Arguments string `json:"arguments"`
	} `json:"output"`
	Usage *struct {
		InputTokens        int64 `json:"input_tokens"`
		OutputTokens       int64 `json:"output_tokens"`
		TotalTokens        int64 `json:"total_tokens"`
		InputTokensDetails struct {
			CachedTokens int64 `json:"cached_tokens"`
		} `json:"input_tokens_details"`
		OutputTokensDetails struct {
			ReasoningTokens int64 `json:"reasoning_tokens"`
		} `json:"output_tokens_details"`
	} `json:"usage"`
}

func (m *responsesModel) Call(ctx context.Context, prompt string, options ...llms.CallOption) (string, error) {
	return llms.GenerateFromSinglePrompt(ctx, m, prompt, options...)
}

func (m *responsesModel) GenerateContent(ctx context.Context, messages []llms.MessageContent, options ...llms.CallOption) (*llms.ContentResponse, error) {
	opts := llms.CallOptions{}
	for _, opt := range options {
		opt(&opts)
	}

	req, err := m.buildRequest(messages, opts)
	if err != nil {
		return nil, err
	}
	body, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("failed to encode responses request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, m.baseURL+"/responses", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Authorization", "Bearer "+m.key)

	httpRes, err := m.client.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("failed to send responses request: %w", err)
	}
	defer httpRes.Body.Close()

	resBody, err := io.ReadAll(httpRes.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read responses response: %w", err)
	}
	if httpRes.StatusCode < 200 || httpRes.StatusCode >= 300 {
		return nil, fmt.Errorf("API returned unexpected status code: %d: %s", httpRes.StatusCode, apiErrorMessage(resBody))
	}

	var res responsesResponse
	if err = json.Unmarshal(resBody, &res); err != nil {
		return nil, fmt.Errorf("failed to decode responses response: %w", err)
	}
	return toContentResponse(&res), nil
}

func (m *responsesModel) buildRequest(messages []llms.MessageContent, opts llms.CallOptions) (*responsesRequest, error) {
	req := &responsesRequest{
		Model:           m.model,
		MaxOutputTokens: opts.MaxTokens,
		Temperature:     opts.Temperature,
		ToolChoice:      toResponsesToolChoice(opts.ToolChoice),
	}
	if opts.Model != "" {
		req.Model = opts.Model
	}

	for _, msg := range messages {
		items, err := toResponsesInput(msg)
		if err != nil {
			return nil, err
		}
		req.Input = append(req.Input, items...)
	}

	for _, tool := range opts.Tools {
		if tool.Type != "function" || tool.Function == nil {
			return nil, fmt.Errorf("unsupported tool type %q", tool.Type)
		}
		req.Tools = append(req.Tools, responsesTool{
			Type:        "function",
			Name:        tool.Function.Name,
			Description: tool.Function.Description,
			Parameters:  tool.Function.Parameters,
			Strict:      tool.Function.Strict,
		})
	}

	switch {
	case opts.StructuredOutput != nil:
		if opts.StructuredOutput.Schema == nil {
			return nil, errors.New("structured output schema cannot be nil")
		}
		format := map[string]any{
			"type":   "json_schema",
			"name":   opts.StructuredOutput.Name,
			"schema": toJSONSchema(opts.StructuredOutput.Schema),
			"strict": opts.StructuredOutput.Strict,
		}
		if opts.StructuredOutput.Description != "" {
			format["description"] = opts.StructuredOutput.Description
		}
		req.Text = &responsesText{Format: format}
	case opts.JSONMode:
		req.Text = &responsesText{Format: map[string]any{"type": "json_object"}}
	}

	for k, v := range opts.Metadata {
		switch {
		case k == promptCacheRetentionKey:
			req.PromptCacheRetention = v
		case k == "thinking_config" || strings.HasPrefix(k, "openai:"):
			// Internal options for langchaingo's own client; never sent to the API.
		default:
			if req.Metadata == nil {
				req.Metadata = map[string]any{}
			}
			req.Metadata[k] = v
		}
	}
	return req, nil
}

// toResponsesInput converts one message into Responses API input items. An
// assistant message becomes a message item plus one function_call item per tool
// call, and a tool message becomes a function_call_output item.
func toResponsesInput(msg llms.MessageContent) ([]any, error) {
	var role string
	switch msg.Role {
	case llms.ChatMessageTypeSystem:
		role = "system"
	case llms.ChatMessageTypeHuman, llms.ChatMessageTypeGeneric:
		role = "user"
	case llms.ChatMessageTypeAI:
		role = "assistant"
	case llms.ChatMessageTypeTool, llms.ChatMessageTypeFunction:
		items := make([]any, 0, len(msg.Parts))
		for _, part := range msg.Parts {
			resp, ok := part.(llms.ToolCallResponse)
			if !ok {
				return nil, fmt.Errorf("expected part of type ToolCallResponse for role %v, got %T", msg.Role, part)
			}
			items = append(items, map[string]any{
				"type":    "function_call_output",
				"call_id": resp.ToolCallID,
				"output":  resp.Content,
			})
		}
		return items, nil
	default:
		return nil, fmt.Errorf("role %v not supported", msg.Role)
	}

	// Assistant text is output_text; everything else the model reads is input_text.
	textType := "input_text"
	if role == "assistant" {
		textType = "output_text"
	}

	var content []map[string]any
	var calls []any
	for _, part := range msg.Parts {
		switch p := part.(type) {
		case llms.TextContent:
			content = append(content, map[string]any{"type": textType, "text": p.Text})
		case llms.ImageURLContent:
			image := map[string]any{"type": "input_image", "image_url": p.URL}
			if p.Detail != "" {
				image["detail"] = p.Detail
			}
			content = append(content, image)
		case llms.BinaryContent:
			if !strings.HasPrefix(p.MIMEType, "image/") {
				return nil, fmt.Errorf("unsupported binary content type %q", p.MIMEType)
			}
			content = append(content, map[string]any{
				"type":      "input_image",
				"image_url": "data:" + p.MIMEType + ";base64," + base64.StdEncoding.EncodeToString(p.Data),
			})
		case llms.ToolCall:
			if p.FunctionCall == nil {
				return nil, fmt.Errorf("tool call %q has no function call", p.ID)
			}
			calls = append(calls, map[string]any{
				"type":      "function_call",
				"call_id":   p.ID,
				"name":      p.FunctionCall.Name,
				"arguments": p.FunctionCall.Arguments,
			})
		default:
			return nil, fmt.Errorf("unsupported content part %T for role %v", part, msg.Role)
		}
	}

	items := make([]any, 0, 1+len(calls))
	if len(content) > 0 {
		items = append(items, map[string]any{"type": "message", "role": role, "content": content})
	}
	return append(items, calls...), nil
}

// toResponsesToolChoice converts the tool choice langchaingokit produces for
// OpenAI, a bare string, into the Responses API form: the three modes pass
// through and anything else names a function.
func toResponsesToolChoice(choice any) any {
	name, ok := choice.(string)
	if !ok || name == "" {
		return choice
	}
	switch name {
	case "auto", "none", "required":
		return name
	default:
		return map[string]any{"type": "function", "name": name}
	}
}

func toJSONSchema(s *llms.StructuredOutputSchema) map[string]any {
	out := map[string]any{"type": string(s.Type)}
	if s.Description != "" {
		out["description"] = s.Description
	}
	if len(s.Enum) > 0 {
		out["enum"] = s.Enum
	}
	if s.Type == llms.SchemaTypeObject {
		props := make(map[string]any, len(s.Properties))
		for name, sub := range s.Properties {
			props[name] = toJSONSchema(sub)
		}
		out["properties"] = props
		if len(s.Required) > 0 {
			out["required"] = s.Required
		}
		out["additionalProperties"] = s.AdditionalProperties
	}
	if s.Type == llms.SchemaTypeArray && s.Items != nil {
		out["items"] = toJSONSchema(s.Items)
	}
	return out
}

// toContentResponse folds the response's output items into a single choice, in
// the shape langchaingo's Chat Completions client returns, so the rest of the
// component handles both the same way.
func toContentResponse(res *responsesResponse) *llms.ContentResponse {
	choice := &llms.ContentChoice{}
	var text strings.Builder
	for _, item := range res.Output {
		switch item.Type {
		case "message":
			for _, c := range item.Content {
				switch c.Type {
				case "output_text":
					text.WriteString(c.Text)
				case "refusal":
					text.WriteString(c.Refusal)
				}
			}
		case "function_call":
			choice.ToolCalls = append(choice.ToolCalls, llms.ToolCall{
				ID:   item.CallID,
				Type: "function",
				FunctionCall: &llms.FunctionCall{
					Name:      item.Name,
					Arguments: item.Arguments,
				},
			})
		}
	}
	choice.Content = text.String()
	if len(choice.ToolCalls) > 0 {
		choice.FuncCall = choice.ToolCalls[0].FunctionCall
	}

	switch {
	case len(choice.ToolCalls) > 0:
		choice.StopReason = "tool_calls"
	case res.Status == "incomplete" && res.IncompleteDetails != nil && res.IncompleteDetails.Reason == "max_output_tokens":
		choice.StopReason = "length"
	case res.Status == "incomplete" && res.IncompleteDetails != nil && res.IncompleteDetails.Reason == "content_filter":
		choice.StopReason = "content_filter"
	default:
		choice.StopReason = "stop"
	}

	if res.Usage != nil {
		// The keys langchaingokit reads usage from, as langchaingo's own client sets them.
		choice.GenerationInfo = map[string]any{
			"PromptTokens":              res.Usage.InputTokens,
			"CompletionTokens":          res.Usage.OutputTokens,
			"TotalTokens":               res.Usage.TotalTokens,
			"PromptCachedTokens":        res.Usage.InputTokensDetails.CachedTokens,
			"CompletionReasoningTokens": res.Usage.OutputTokensDetails.ReasoningTokens,
		}
	}
	return &llms.ContentResponse{Choices: []*llms.ContentChoice{choice}}
}

// apiErrorMessage returns the message of an OpenAI error body, or the raw body
// when it is not one.
func apiErrorMessage(body []byte) string {
	var e struct {
		Error struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	if json.Unmarshal(body, &e) == nil && e.Error.Message != "" {
		return e.Error.Message
	}
	return string(body)
}
