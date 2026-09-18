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
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"time"

	"github.com/anthropics/anthropic-sdk-go"
	"github.com/anthropics/anthropic-sdk-go/option"
	"github.com/tmc/langchaingo/llms"

	"github.com/dapr/components-contrib/conversation"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
	kmeta "github.com/dapr/kit/metadata"
)

const apiTypeFoundry = "foundry"

type Anthropic struct {
	client *anthropic.Client
	model  string
	logger logger.Logger
}

func NewAnthropic(logger logger.Logger) conversation.Conversation {
	return &Anthropic{
		logger: logger,
	}
}

func (a *Anthropic) GetModel() string {
	return a.model
}

func (a *Anthropic) buildClientOptions(md AnthropicMetadata) (string, []option.RequestOption, error) {
	model := conversation.GetAnthropicModel(md.Model)

	opts := []option.RequestOption{
		option.WithAPIKey(md.Key),
	}

	if strings.EqualFold(md.APIType, apiTypeFoundry) {
		if md.Endpoint == "" {
			return "", nil, errors.New("endpoint must be provided when apiType is set to 'foundry'")
		}
		opts = append(opts, option.WithBaseURL(strings.TrimSuffix(md.Endpoint, "/")))
	} else if md.Endpoint != "" {
		opts = append(opts, option.WithBaseURL(strings.TrimSuffix(md.Endpoint, "/")))
	}

	if httpClient := conversation.BuildHTTPClient(); httpClient != nil {
		opts = append(opts, option.WithHTTPClient(httpClient))
	}

	return model, opts, nil
}

func (a *Anthropic) Init(ctx context.Context, meta conversation.Metadata) error {
	md := AnthropicMetadata{}
	err := kmeta.DecodeMetadata(meta.Properties, &md)
	if err != nil {
		return err
	}

	model, opts, err := a.buildClientOptions(md)
	if err != nil {
		return err
	}

	client := anthropic.NewClient(opts...)
	a.client = &client
	a.model = model

	return nil
}

func (a *Anthropic) Converse(ctx context.Context, req *conversation.Request) (*conversation.Response, error) {
	params, err := a.convertRequest(req)
	if err != nil {
		return nil, err
	}

	msg, err := a.client.Messages.New(ctx, params)
	if err != nil {
		return nil, err
	}

	resp, err := a.convertResponse(msg, req)
	if err != nil {
		return nil, err
	}

	return resp, nil
}

func (a *Anthropic) convertRequest(req *conversation.Request) (anthropic.MessageNewParams, error) {
	params := anthropic.MessageNewParams{
		Model:     a.model,
		MaxTokens: 4096,
	}

	if req.Temperature > 0 {
		params.Temperature = anthropic.Float(req.Temperature)
	}

	var systemBlocks []anthropic.TextBlockParam
	var messageParams []anthropic.MessageParam

	if req.Message != nil {
		for _, msg := range *req.Message {
			switch msg.Role {
			case llms.ChatMessageTypeSystem:
				for _, part := range msg.Parts {
					if tc, ok := part.(llms.TextContent); ok {
						systemBlocks = append(systemBlocks, anthropic.TextBlockParam{
							Text: tc.Text,
						})
					}
				}

			case llms.ChatMessageTypeHuman:
				blocks := make([]anthropic.ContentBlockParamUnion, 0, len(msg.Parts))
				for _, part := range msg.Parts {
					switch p := part.(type) {
					case llms.TextContent:
						blocks = append(blocks, anthropic.NewTextBlock(p.Text))
					case llms.ImageURLContent:
						// Handle image if present
						blocks = append(blocks, anthropic.NewTextBlock(p.URL))
					}
				}
				if len(blocks) > 0 {
					messageParams = append(messageParams, anthropic.NewUserMessage(blocks...))
				}

			case llms.ChatMessageTypeAI:
				blocks := make([]anthropic.ContentBlockParamUnion, 0, len(msg.Parts))
				for _, part := range msg.Parts {
					switch p := part.(type) {
					case llms.TextContent:
						blocks = append(blocks, anthropic.NewTextBlock(p.Text))
					case llms.ToolCall:
						var inputMap map[string]any
						if p.FunctionCall != nil && p.FunctionCall.Arguments != "" {
							_ = json.Unmarshal([]byte(p.FunctionCall.Arguments), &inputMap)
						}
						if inputMap == nil {
							inputMap = make(map[string]any)
						}
						toolName := ""
						if p.FunctionCall != nil {
							toolName = p.FunctionCall.Name
						}
						blocks = append(blocks, anthropic.NewToolUseBlock(p.ID, inputMap, toolName))
					}
				}
				if len(blocks) > 0 {
					messageParams = append(messageParams, anthropic.NewAssistantMessage(blocks...))
				}

			case llms.ChatMessageTypeTool:
				blocks := make([]anthropic.ContentBlockParamUnion, 0, len(msg.Parts))
				for _, part := range msg.Parts {
					switch p := part.(type) {
					case llms.ToolCallResponse:
						blocks = append(blocks, anthropic.NewToolResultBlock(p.ToolCallID, p.Content, false))
					case llms.TextContent:
						blocks = append(blocks, anthropic.NewTextBlock(p.Text))
					}
				}
				if len(blocks) > 0 {
					messageParams = append(messageParams, anthropic.NewUserMessage(blocks...))
				}
			}
		}
	}

	if len(systemBlocks) > 0 {
		params.System = systemBlocks
	}

	if len(messageParams) > 0 {
		params.Messages = messageParams
	}

	if req.Tools != nil && len(*req.Tools) > 0 {
		tools := make([]anthropic.ToolUnionParam, 0, len(*req.Tools))
		for _, t := range *req.Tools {
			if t.Type == "function" && t.Function != nil {
				var props any
				var reqFields []string
				if t.Function.Parameters != nil {
					var paramsMap map[string]any
					if m, ok := t.Function.Parameters.(map[string]any); ok {
						paramsMap = m
					} else {
						if b, err := json.Marshal(t.Function.Parameters); err == nil {
							_ = json.Unmarshal(b, &paramsMap)
						}
					}
					if paramsMap != nil {
						if p, ok := paramsMap["properties"]; ok {
							props = p
						}
						if r, ok := paramsMap["required"]; ok {
							if rSlice, ok := r.([]string); ok {
								reqFields = rSlice
							} else if rSliceAny, ok := r.([]any); ok {
								for _, item := range rSliceAny {
									if s, ok := item.(string); ok {
										reqFields = append(reqFields, s)
									}
								}
							}
						}
					}
				}

				tp := anthropic.ToolParam{
					Name: t.Function.Name,
					InputSchema: anthropic.ToolInputSchemaParam{
						Properties: props,
						Required:   reqFields,
					},
				}
				if t.Function.Description != "" {
					tp.Description = anthropic.String(t.Function.Description)
				}
				tools = append(tools, anthropic.ToolUnionParam{OfTool: &tp})
			}
		}
		params.Tools = tools
	}

	if req.ToolChoice != nil && req.Tools != nil && len(*req.Tools) > 0 {
		if len(params.Tools) > 0 {
			tc := *req.ToolChoice
			switch tc {
			case "auto":
				params.ToolChoice = anthropic.ToolChoiceUnionParam{
					OfAuto: &anthropic.ToolChoiceAutoParam{},
				}
			case "required", "any":
				params.ToolChoice = anthropic.ToolChoiceUnionParam{
					OfAny: &anthropic.ToolChoiceAnyParam{},
				}
			case "none":
				params.ToolChoice = anthropic.ToolChoiceUnionParam{
					OfNone: &anthropic.ToolChoiceNoneParam{},
				}
			default:
				params.ToolChoice = anthropic.ToolChoiceParamOfTool(tc)
			}
		}
	}

	if req.PromptCacheRetention != nil {
		threshold := 32*time.Minute + 30*time.Second
		ttl := anthropic.CacheControlEphemeralTTLTTL5m
		if *req.PromptCacheRetention > threshold {
			ttl = anthropic.CacheControlEphemeralTTLTTL1h
		}
		params.CacheControl = anthropic.CacheControlEphemeralParam{TTL: ttl}
	}

	return params, nil
}

func (a *Anthropic) convertResponse(msg *anthropic.Message, req *conversation.Request) (*conversation.Response, error) {
	if msg == nil {
		return nil, errors.New("received nil response from Anthropic API")
	}

	finishReason := normalizeFinishReason(string(msg.StopReason))

	var contentBuilder strings.Builder
	var toolCalls []llms.ToolCall

	for _, block := range msg.Content {
		switch block.Type {
		case "text":
			textBlock := block.AsText()
			contentBuilder.WriteString(textBlock.Text)
		case "tool_use":
			toolUseBlock := block.AsToolUse()
			toolCalls = append(toolCalls, llms.ToolCall{
				ID:   toolUseBlock.ID,
				Type: "function",
				FunctionCall: &llms.FunctionCall{
					Name:      toolUseBlock.Name,
					Arguments: string(toolUseBlock.Input),
				},
			})
		}
	}

	if req.ToolChoice != nil && (*req.ToolChoice == "required" || *req.ToolChoice == "any") && req.Tools != nil && len(*req.Tools) > 0 {
		if contentBuilder.Len() == 0 && len(toolCalls) == 0 {
			return nil, errors.New("LLM returned empty response with no tool calls despite tools being required")
		}
	}

	choice := conversation.Choice{
		FinishReason: finishReason,
		Index:        0,
		Message: conversation.Message{
			Content: contentBuilder.String(),
		},
	}
	if len(toolCalls) > 0 {
		choice.Message.ToolCallRequest = &toolCalls
	}

	output := conversation.Result{
		StopReason: finishReason,
		Choices:    []conversation.Choice{choice},
	}

	var usage *conversation.Usage
	//nolint:gosec // token counts from the API are always non-negative
	inputTokens := uint64(msg.Usage.InputTokens)
	//nolint:gosec // token counts from the API are always non-negative
	outputTokens := uint64(msg.Usage.OutputTokens)

	if inputTokens > 0 || outputTokens > 0 || msg.Usage.CacheReadInputTokens > 0 || msg.Usage.CacheCreationInputTokens > 0 {
		usage = &conversation.Usage{
			PromptTokens:     inputTokens,
			CompletionTokens: outputTokens,
			TotalTokens:      inputTokens + outputTokens,
		}
		if msg.Usage.CacheReadInputTokens > 0 || msg.Usage.CacheCreationInputTokens > 0 {
			usage.PromptTokensDetails = &conversation.PromptTokensDetails{
				//nolint:gosec // token counts from the API are always non-negative
				CachedTokens: uint64(msg.Usage.CacheReadInputTokens),
			}
		}
	}

	return &conversation.Response{
		Model:   msg.Model,
		Outputs: []conversation.Result{output},
		Usage:   usage,
	}, nil
}

func normalizeFinishReason(stopReason string) string {
	if stopReason == "" {
		return "unknown"
	}
	return stopReason
}

func (a *Anthropic) GetComponentMetadata() (metadataInfo metadata.MetadataMap) {
	metadataStruct := AnthropicMetadata{}
	_ = metadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, metadata.ConversationType)
	return
}

func (a *Anthropic) Close() error {
	return nil
}
